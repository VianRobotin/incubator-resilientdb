#!/usr/bin/env python3
# DAS5 Benchmark Orchestrator — throughput vs latency sweep (L-curve).
#
# Runs a rate-sweep experiment: for each (N, mode), inject at increasing
# rates from well below saturation to above saturation.  Each (rate, N, mode)
# point gives one (throughput, latency) pair; together they trace the L-curve.
#
# Usage (from the scripts/deploy/ directory):
#   python3 performance/das.py               # run full sweep
#   python3 performance/das.py --dry-run     # print plan without reserving
#   python3 performance/das.py --nodes 10    # only sweep N=10
#   python3 performance/das.py --skip-build  # skip bazel build

import argparse
import datetime
import errno
import gzip
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

from preserve import PreserveManager

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

PROJ_ROOT = Path(__file__).resolve().parents[3]
USERNAME  = os.environ.get("USER", "vrobotin")

SERVER_BIN = PROJ_ROOT / "bazel-bin/benchmark/protocols/autobahn/kv_server_performance"
CLIENT_BIN = PROJ_ROOT / "bazel-bin/benchmark/protocols/autobahn/kv_service_tools"
KEY_GEN    = PROJ_ROOT / "bazel-bin/tools/key_generator_tools"
CERT_TOOL  = PROJ_ROOT / "bazel-bin/tools/certificate_tools"
# Replicas may run a different build of the server (e.g. a sanitizer build);
# clients always use SERVER_BIN.
REPLICA_BIN = Path(os.environ.get("PEARL_REPLICA_BIN", SERVER_BIN))

BASE_PORT      = 10001
READY_TIMEOUT  = 120   # seconds to wait for all replicas to connect
RUN_ATTEMPTS   = 3     # tries per point when a replica that should be live dies

RESULTS_DIR  = PROJ_ROOT / "das_results" / "pearl" / "2026-10-26" / "rate"  # current Pearl run
RUNTIME_BASE = PROJ_ROOT / "das_runtime"
# Replicas and clients log to node-local disk; the logs are copied into the
# run directory once the nodes are killed.  /var/scratch is a hard-mounted
# NFS share on the compute nodes: a stalled NFS write blocks every thread
# that logs, which stalled replicas for seconds and broke the Delta bound.
LOCAL_LOG_BASE = f"/local/{USERNAME}/pearl"
# A run is not started with less than this much room left under the soft
# disk quota of RUNTIME_BASE.  Past the hard limit, run directories and log
# copies fail; every remaining point of the 2026-10-26 sweep failed so.
MIN_QUOTA_HEADROOM_GB = 5

TEE_ENCLAVE_PATH = PROJ_ROOT / "platform/consensus/ordering/autobahn/tee/tee_enclave.signed.so"
LD_LIBRARY_PATH  = "/var/scratch/vrobotin/sgxsdk/lib64"

# ---------------------------------------------------------------------------
# Experiment parameters
# ---------------------------------------------------------------------------

BLOCK_SIZE  = 100   # transactions per block
RUNTIME     = 60    # seconds per run (matches Giulio)
WARMUP      = 15    # seconds excluded from metrics
REPETITIONS = 5     # runs per (N, mode, rate) point

# For each N: injection rates (total tx/s across all clients) to sweep.
# Must span from well below saturation to well above it so the L-curve knee
# is visible.  Adjust after seeing where saturation falls.
#
# N values chosen for the 2f+1 committee claim:
#   N=7  → f=3  (compare against Giulio's 3f+1 = N=10 at f=3)
#   N=11 → f=5  (compare against Giulio's 3f+1 = N=16 at f=5)
#   N=15 → f=7  (compare against Giulio's 3f+1 = N=22 at f=7)
# Thesis claim: same fault tolerance f, but 28-32% fewer nodes.
RATE_SWEEP: dict = {
     7: [500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000, 20000, 25000, 30000, 40000, 50000],
    11: [500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000, 20000, 25000, 30000, 40000, 50000],
    15: [500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000, 20000, 25000, 30000, 40000, 50000],
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

_BAZEL_SEARCH_PATHS = [
    "/usr/local/bin/bazel", "/usr/bin/bazel",
    str(Path.home() / "bin" / "bazel"),
    str(Path.home() / ".local" / "bin" / "bazel"),
]

def _find_bazel() -> str:
    found = shutil.which("bazel")
    if found:
        return found
    for candidate in _BAZEL_SEARCH_PATHS:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError(
        "bazel not found. Install it or use --skip-build if binaries exist."
    )


def build_binaries():
    print("\n=== Building binaries ===")
    bazel = _find_bazel()
    subprocess.check_call([
        bazel, "build",
        "//benchmark/protocols/autobahn:kv_server_performance",
        "//benchmark/protocols/autobahn:kv_service_tools",
        "//tools:key_generator_tools",
        "//tools:certificate_tools",
    ], cwd=str(PROJ_ROOT))
    print("Build done.")


_REQUIRED_BINARIES = {
    "kv_server_performance": SERVER_BIN,
    "kv_service_tools":      CLIENT_BIN,
    "key_generator_tools":   KEY_GEN,
    "certificate_tools":     CERT_TOOL,
}

def check_binaries():
    missing = [name for name, path in _REQUIRED_BINARIES.items() if not path.exists()]
    if missing:
        print("ERROR: missing binaries:", file=sys.stderr)
        for name in missing:
            print(f"  {_REQUIRED_BINARIES[name]}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Certificate generation
# ---------------------------------------------------------------------------

def generate_certs(cert_dir: Path, replica_ips: List[str], client_ips: List[str]):
    cert_dir.mkdir(parents=True, exist_ok=True)
    num_replicas = len(replica_ips)
    num_clients  = len(client_ips)
    total_nodes  = num_replicas + num_clients

    subprocess.check_call([str(KEY_GEN), str(cert_dir / "admin")])
    for i in range(1, total_nodes + 1):
        subprocess.check_call([str(KEY_GEN), str(cert_dir / f"node_{i}")])

    for i, ip in enumerate(replica_ips, 1):
        subprocess.check_call([
            str(CERT_TOOL), str(cert_dir),
            str(cert_dir / "admin.key.pri"), str(cert_dir / "admin.key.pub"),
            str(cert_dir / f"node_{i}.key.pub"),
            str(i), ip, str(BASE_PORT + i - 1), "replica",
        ])
    for j, ip in enumerate(client_ips, 1):
        node_id = num_replicas + j
        subprocess.check_call([
            str(CERT_TOOL), str(cert_dir),
            str(cert_dir / "admin.key.pri"), str(cert_dir / "admin.key.pub"),
            str(cert_dir / f"node_{node_id}.key.pub"),
            str(node_id), ip, str(BASE_PORT + node_id - 1), "client",
        ])


# ---------------------------------------------------------------------------
# Config generation
# ---------------------------------------------------------------------------

def write_server_config(config_path: Path, node_ips: List[str], mode: str,
                        block_size: int, target_input_tps_per_client: int,
                        runtime: int, fault_num: int = 0):
    bof_flag = (mode == "bof")
    replica_info = [
        {"id": i, "ip": ip, "port": BASE_PORT + i - 1}
        for i, ip in enumerate(node_ips, 1)
    ]
    client_batch_num = 1 if target_input_tps_per_client > 0 else 400
    config = {
        "region": [{"replicaInfo": replica_info}],
        "clientBatchNum": client_batch_num,
        "enable_viewchange": False,
        "recovery_enabled": False,
        "max_client_complaint_num": 10,
        # The client stops sending once max_process_txn requests await a
        # reply.  Pearl replicas do not reply (latency is measured at the
        # replicas), so with a target rate the client must run open-loop:
        # with 4096 every run sent only 4096 transactions in total.
        "max_process_txn": (1 << 30) if target_input_tps_per_client > 0 else 4096,
        # Network workers verify each incoming message's Ed25519 signature
        # (about 130 us on DAS5), i.e. several cores at 50k client tx/s.
        "worker_num": 20,
        # Client requests beyond this backlog are dropped, so that client
        # overload never delays messages between replicas (synchrony).
        "max_client_queue": 5000,
        # Outgoing batches wait at most 1 ms to fill (ResilientDB defaults:
        # 10 ms for broadcasts, 50 ms = Delta for single targets).
        "tcp_batch_timeout_us": 1000,
        # Threads reading incoming connections (about 100 MB/s at 50k tx/s).
        "input_worker_num": 4,
        "output_worker_num": 5,
        "block_size": block_size,
        "batch_order_fairness": bof_flag,
    }
    if target_input_tps_per_client > 0:
        config["target_input_tps"] = target_input_tps_per_client
    # Pin committee fault tolerance f independently of n (e.g. a 3f+1 = n
    # deployment at the baselines' f). Unset/0 => protocol uses f=(n-1)/2.
    if fault_num > 0:
        config["fault_num"] = fault_num
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)


def write_client_trigger_config(path: Path, node_id: int, ip: str):
    port = BASE_PORT + node_id - 1
    path.write_text(f"{node_id} {ip} {port}\n")


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------

def ssh_cmd(hostname: str, remote_cmd: str) -> Optional[subprocess.Popen]:
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
           hostname, remote_cmd]
    return subprocess.Popen(cmd)


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

def trimmed_mean(values: list, drop: int = 1) -> float:
    v = sorted(v for v in values if v > 0)
    if len(v) > 2 * drop:
        v = v[drop:-drop]
    return sum(v) / len(v) if v else 0.0


# slot_committed: every replica emits one line per committed slot.
#   txns = transactions newly committed by Sync HotStuff in this slot.
#   consensus_latency_us = commit_time − avg(create_time) over own-originated
#   txns (clock-consistent: create_time and commit_time share one local clock).
#   TPS per replica ≈ system-wide (every replica commits every slot); take
#   the median across replicas.
_COMMIT_RE = re.compile(
    r'^[EW](\d{8}) (\d{2}:\d{2}:\d{2}\.\d+).*?'
    r'slot_committed slot:\d+ txns:(\d+) consensus_latency_us:(\d+)',
    re.MULTILINE,
)
# executed_batch: every replica emits one per slot, counting txns for which the
# execution threshold τ passed the ordering indicator in this slot (either
# newly committed or drained from the pending buffer). avg_latency_us =
# execute_time − avg(create_time) over own-originated executed txns.  Every
# replica executes every eligible tx, so per-replica ≈ system-wide — take the
# median across replicas (same treatment as consensus).
_EXEC_RE = re.compile(
    r'^[EW](\d{8}) (\d{2}:\d{2}:\d{2}\.\d+).*?'
    r'executed_batch slot:\d+ count:(\d+) avg_latency_us:(\d+) max_latency_us:\d+ pending:\d+',
    re.MULTILINE,
)


def _extract_entries(text: str, pattern) -> list:
    entries = []
    for m in pattern.finditer(text):
        try:
            dt = datetime.datetime.strptime(
                f"{m.group(1)} {m.group(2)}", "%Y%m%d %H:%M:%S.%f"
            )
        except ValueError:
            continue
        entries.append((dt, int(m.group(3)), int(m.group(4))))
    return entries


def _apply_warmup(entries: list, experiment_start: datetime.datetime,
                  warmup_secs: int, runtime: int) -> Tuple[list, bool]:
    """
    Return (selected_entries, fallback_used).

    Primary filter: entries in [experiment_start+warmup, experiment_start+runtime].
    Fallback: if the primary window is empty but there are entries inside
    [experiment_start, experiment_start+runtime], return those instead.
    This salvages bursty runs where the entire commit activity fits inside
    the warmup window — reporting a burst rate is strictly more informative
    than reporting zero.
    """
    if not entries:
        return [], False
    warmup_cutoff = experiment_start + datetime.timedelta(seconds=warmup_secs)
    end_cutoff    = experiment_start + datetime.timedelta(seconds=runtime)
    strict = [e for e in entries if warmup_cutoff <= e[0] <= end_cutoff]
    if strict:
        return strict, False
    fallback = [e for e in entries if experiment_start <= e[0] <= end_cutoff]
    return fallback, bool(fallback)


def read_log(log_file: Path) -> Optional[str]:
    """Text of a run log, whether still plain or already gzipped
    (<name>.gz, see compress_logs); None if neither exists."""
    if log_file.exists():
        return log_file.read_text(errors="replace")
    gz = Path(str(log_file) + ".gz")
    if gz.exists():
        with gzip.open(str(gz), "rt", errors="replace") as f:
            return f.read()
    return None


def parse_log_file(log_file: Path, experiment_start: datetime.datetime,
                   warmup_secs: int, runtime: int):
    """
    Returns (commit_meas, exec_meas, commit_total, exec_total,
             commit_fallback, exec_fallback).

    commit_meas entries: (dt, txns_in_slot, consensus_latency_us)
    exec_meas   entries: (dt, executed_count, avg_latency_us)
    """
    text = read_log(log_file)
    if text is None:
        return [], [], 0, 0, False, False
    commit_all = _extract_entries(text, _COMMIT_RE)
    exec_all   = _extract_entries(text, _EXEC_RE)
    commit_meas, commit_fb = _apply_warmup(commit_all, experiment_start, warmup_secs, runtime)
    exec_meas,   exec_fb   = _apply_warmup(exec_all,   experiment_start, warmup_secs, runtime)
    return commit_meas, exec_meas, len(commit_all), len(exec_all), commit_fb, exec_fb


def _compute_tps_latency(entries: list, fallback_duration: float):
    if not entries:
        return 0.0, 0.0
    if len(entries) >= 2:
        span_s = (entries[-1][0] - entries[0][0]).total_seconds()
        avg_slot_s = span_s / (len(entries) - 1)
        duration = span_s + avg_slot_s
    else:
        duration = max(1.0, fallback_duration)
    tps = sum(e[1] for e in entries) / max(0.001, duration)
    lat_us_vals = [e[2] for e in entries]
    latency_s = trimmed_mean(lat_us_vals) / 1_000_000
    return round(tps, 1), round(latency_s, 4)


# ---------------------------------------------------------------------------
# Core experiment runner
# ---------------------------------------------------------------------------

class ReplicaDied(RuntimeError):
    """A replica that should have been live for the whole run was not."""


class DiskFull(BaseException):
    """RUNTIME_BASE is (nearly) out of disk quota.  A BaseException so that
    the sweeps' per-point `except Exception` stops instead of failing every
    remaining point."""


_DISK_FULL_ERRNOS = (errno.EDQUOT, errno.ENOSPC)


def quota_headroom_gb() -> Optional[float]:
    """GB left under the soft (else hard) disk quota of the filesystem
    holding RUNTIME_BASE; None if no quota applies or it can't be read."""
    try:
        path = RUNTIME_BASE if RUNTIME_BASE.exists() else PROJ_ROOT
        dev = subprocess.run(["df", "--output=source", str(path)],
                             stdout=subprocess.PIPE, universal_newlines=True).stdout.split()[-1]
        # quota exits non-zero when a quota is exceeded: don't check it.
        out = subprocess.run(["quota", "-w"], stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, universal_newlines=True).stdout
    except (OSError, IndexError):
        return None
    for line in out.splitlines():
        fields = line.split()
        if len(fields) >= 4 and fields[0] == dev:
            used, soft, hard = int(fields[1].rstrip("*")), int(fields[2]), int(fields[3])
            limit = soft or hard
            return (limit - used) / 1024 ** 2 if limit else None  # 1 KB blocks
    return None


def check_disk_headroom():
    headroom = quota_headroom_gb()
    if headroom is not None and headroom < MIN_QUOTA_HEADROOM_GB:
        raise DiskFull(f"only {headroom:.1f} GB left under the disk quota of {RUNTIME_BASE} "
                       f"(need {MIN_QUOTA_HEADROOM_GB} GB)")


# Health scan of a finished run, over its live replicas' logs.
_SCAN_HEADER = ("run_id,n,silent_faults,mode,input_rate,recorded,ok,missing_logs,"
                "fatal_lines,crash_lines,bad_exits,view_entries,blames,rejects,"
                "abstains,max_stall_ms,digest_disagree_slots")
_FATAL_RE  = re.compile(r"^F\d{8} ", re.MULTILINE)
_CRASH_RE  = re.compile(r"Segmentation|Aborted|double free|corrupt|core dumped|"
                        r"terminate called|AddressSanitizer|ThreadSanitizer")
_STATUS_RE = re.compile(r"das: replica exited with status (\d+)")
_DIGEST_RE = re.compile(r"executed_batch slot:(\d+) .*order_digest:([0-9a-f]+)")
_STALL_RE  = re.compile(r"protocol thread: .* took (\d+)us")
_VIEW_EVENTS = (("view_entries", "entering view"), ("blames", "blaming leader"),
                ("rejects", "rejecting block"), ("abstains", "abstaining on block"))


def scan_run(log_dir: Path, live_replicas: int) -> dict:
    """Count, over the live replicas' logs: missing logs, glog F lines,
    crash messages, exits other than our SIGKILL (137, or no exit line),
    view-change events, the worst protocol-thread stall, and the slots at
    which replicas' order digests disagree.  ok iff nothing but the stall
    is nonzero."""
    s = {k: 0 for k in ("missing_logs", "fatal_lines", "crash_lines", "bad_exits",
                        "max_stall_ms", "digest_disagree_slots")}
    s.update({k: 0 for k, _ in _VIEW_EVENTS})
    digests: dict = {}
    for i in range(1, live_replicas + 1):
        text = read_log(log_dir / f"server_{i}.log")
        if text is None:
            s["missing_logs"] += 1
            continue
        s["fatal_lines"] += len(_FATAL_RE.findall(text))
        s["crash_lines"] += len(_CRASH_RE.findall(text))
        statuses = _STATUS_RE.findall(text)
        s["bad_exits"] += sum(1 for st in statuses if st != "137") + (0 if statuses else 1)
        for key, needle in _VIEW_EVENTS:
            s[key] += text.count(needle)
        for us in _STALL_RE.findall(text):
            s["max_stall_ms"] = max(s["max_stall_ms"], int(us) // 1000)
        for slot, digest in _DIGEST_RE.findall(text):
            digests.setdefault(slot, set()).add(digest)
    s["digest_disagree_slots"] = sum(1 for d in digests.values() if len(d) > 1)
    s["ok"] = int(all(v == 0 for k, v in s.items() if k != "max_stall_ms"))
    return s


def compress_logs(log_dir: Path):
    """gzip every log of a run once it has been parsed and scanned (about
    6x smaller); read_log reads them back transparently."""
    for p in sorted(log_dir.glob("*.log")):
        with open(str(p), "rb") as src, gzip.open(str(p) + ".gz", "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        p.unlink()


def kill_nodes(hosts: List[str], local_dir: Optional[str] = None,
               log_dir: Optional[Path] = None):
    """Kill the Pearl processes on hosts; with local_dir, then move each
    host's node-local logs into log_dir."""
    procs = []
    for host in dict.fromkeys(hosts):
        p = ssh_cmd(host, "killall -9 kv_server_performance 2>/dev/null || true")
        if p:
            procs.append(p)
    for p in procs:
        p.wait()
    time.sleep(2)
    if local_dir is None:
        return
    procs = []
    for host in dict.fromkeys(hosts):
        p = ssh_cmd(host, f"cp {local_dir}/* {log_dir}/ 2>/dev/null; rm -rf {local_dir}")
        if p:
            procs.append(p)
    for p in procs:
        p.wait()


def remote_check(checks: List[Tuple[int, str, str]]) -> List[int]:
    """Run each (key, host, command) over ssh in parallel; keys of the
    commands that succeeded."""
    procs = [(k, subprocess.Popen(
                ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
                 host, cmd],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
             for k, host, cmd in checks]
    return [k for k, p in procs if p.wait() == 0]


def dead_replicas(replicas: List[Tuple[int, str]], local_dir: str) -> List[int]:
    """Ids of the (id, host) replicas whose process is no longer running."""
    alive = set(remote_check([(i, host, f"kill -0 $(cat {local_dir}/server_{i}.pid)")
                              for i, host in replicas]))
    dead = [i for i, _ in replicas if i not in alive]
    hosts = dict(replicas)
    for i in dead:
        tail = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
             hosts[i], f"tail -c 300 {local_dir}/server_{i}.log"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.decode(
                errors="replace")
        print(f"  Replica {i} is not running; log ends with:\n{tail}")
    return dead


def run_experiment(*args, **kwargs) -> Tuple[float, float, float, float, int]:
    """
    Run one experiment (see _run_once), repeating it when a replica that
    should have been live died or executed nothing: such a run measures a
    crash, not the configured setting.  After RUN_ATTEMPTS failures it
    returns zeros, which the sweeps treat as not done and rerun on resume.
    """
    for attempt in range(1, RUN_ATTEMPTS + 1):
        try:
            return _run_once(*args, **kwargs)
        except ReplicaDied as e:
            print(f"INVALID RUN (attempt {attempt}/{RUN_ATTEMPTS}): {e}")
            time.sleep(10)
    print(f"ERROR: giving up after {RUN_ATTEMPTS} attempts; recording zeros")
    return 0.0, 0.0, 0.0, 0.0, 0


def _run_once(
    num_replicas: int,
    mode: str,
    block_size: int,
    runtime: int,
    warmup: int,
    dry_run: bool,
    num_clients: int = 1,
    target_tps: int = 0,
    silent_faults: int = 0,
    fault_num: int = 0,
) -> Tuple[float, float, float, float, int]:
    """
    Reserve machines, run one experiment, return:
      (consensus_tps, consensus_latency_s, execution_tps, execution_latency_s, slots)

    silent_faults: number of replicas crashed silently (silent-crash model).
    All N replicas start; the last silent_faults are killed once the cluster
    has formed, before the clients are triggered.  The honest replicas
    must still form quorum (f+1 of 2f+1) — caller is responsible for choosing
    a tolerable silent_faults value.
    """
    if silent_faults < 0 or silent_faults >= num_replicas:
        raise ValueError(
            f"silent_faults={silent_faults} must be in [0, {num_replicas})")
    live_replicas = num_replicas - silent_faults
    fault_tag = f"_f{silent_faults}" if silent_faults else ""
    run_id = (f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
              f"_n{num_replicas}{fault_tag}_{mode}_r{target_tps}")
    print(f"\n{'='*60}")
    print(f"  {run_id}")
    print(f"{'='*60}")

    total_secs = runtime + 120
    duration_str = str(datetime.timedelta(seconds=total_secs))
    pm = PreserveManager(USERNAME)
    # Each node hosts one replica and up to one co-located client (paper
    # Sec. 8.1).
    total_machines = max(num_replicas, num_clients)
    per_client_tps = (target_tps // num_clients) if target_tps > 0 else 0

    if dry_run:
        print(f"[DRY-RUN] Reserve {total_machines} machines for {duration_str}")
        print(f"[DRY-RUN] target_tps={target_tps}  per_client_tps={per_client_tps}")
        print(f"[DRY-RUN] disk quota headroom: {quota_headroom_gb()} GB")
        return 0.0, 0.0, 0.0, 0.0, 0

    check_disk_headroom()
    reservation_id = pm.create_reservation(total_machines, duration_str)
    print(f"Reservation: {reservation_id}")
    time.sleep(5)

    local_dir = None
    log_dir = None
    recorded = False
    try:
        # Wait for machines
        hostnames = []
        deadline = time.time() + 300
        while time.time() < deadline:
            reservations = pm.get_own_reservations()
            if reservation_id in reservations:
                v = reservations[reservation_id]
                if len(v.assigned_machines) >= total_machines:
                    hostnames = v.assigned_machines[:total_machines]
                    break
            time.sleep(5)
        if not hostnames:
            raise RuntimeError(f"Timed out waiting for {total_machines} machines")

        replica_hosts = hostnames[:num_replicas]
        client_hosts  = [hostnames[j % total_machines] for j in range(num_clients)]
        replica_ips   = [socket.gethostbyname(h) for h in replica_hosts]
        client_ips    = [socket.gethostbyname(h) for h in client_hosts]
        print(f"Replicas: {replica_hosts}")
        print(f"Clients:  {client_hosts}")

        runtime_dir = RUNTIME_BASE / run_id
        cert_dir    = runtime_dir / "cert"
        log_dir     = runtime_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        local_dir   = f"{LOCAL_LOG_BASE}/{run_id}"

        print("Generating certificates...")
        generate_certs(cert_dir, replica_ips, client_ips)

        config_path = runtime_dir / "server.config"
        write_server_config(config_path, replica_ips, mode, block_size,
                            per_client_tps, runtime, fault_num=fault_num)
        print(f"Config: {config_path}  (per_client_tps={per_client_tps})")

        for j, ip in enumerate(client_ips, 1):
            node_id = num_replicas + j
            write_client_trigger_config(
                runtime_dir / f"client_{node_id}.config", node_id, ip)

        env_prefix = (
            f"export TEE_ENCLAVE_PATH={TEE_ENCLAVE_PATH}; "
            f"export LD_LIBRARY_PATH={LD_LIBRARY_PATH}:$LD_LIBRARY_PATH; "
        )
        for var in ("ASAN_OPTIONS", "TSAN_OPTIONS"):
            if var in os.environ:
                env_prefix += f"export {var}='{os.environ[var]}'; "

        # Start ALL N replicas — silent faults are induced AFTER the cluster
        # forms, by killing the last `silent_faults` replicas just before
        # client trigger.  Don't-start was broken for Pearl: live replicas
        # and client nodes block on connect-refused to silent peers and
        # never reach `is ready`.  Killing after handshake completes
        # avoids that and matches the standard BFT silent-crash model.
        print(f"Starting {num_replicas} replicas "
              f"(silent_faults={silent_faults} will be killed post-ready)...")
        procs = []
        for i, host in enumerate(replica_hosts, 1):
            log_file  = f"{local_dir}/server_{i}.log"
            pid_file  = f"{local_dir}/server_{i}.pid"
            priv_key  = cert_dir / f"node_{i}.key.pri"
            cert_file = cert_dir / f"cert_{i}.cert"
            # The wrapper shell records how the replica ended (137 = SIGKILL),
            # so that a replica dying mid-run can be told apart from a crash.
            p = ssh_cmd(host,
                f"mkdir -p {local_dir}; {env_prefix}nohup sh -c '{REPLICA_BIN} {config_path} "
                f"{priv_key} {cert_file} & echo $! > {pid_file}; wait $!; "
                f"echo \"das: replica exited with status $? at $(date +%T.%N)\"' "
                f"> {log_file} 2>&1 &")
            if p:
                procs.append(p)
        for p in procs:
            p.wait()

        # Wait for all N replicas to connect.
        print(f"Waiting for {num_replicas} replicas (timeout={READY_TIMEOUT}s)...")
        ready_pattern = f"receive public size:{num_replicas}"
        deadline = time.time() + READY_TIMEOUT
        ready = [False] * num_replicas
        while not all(ready) and time.time() < deadline:
            for i in remote_check([
                    (i, replica_hosts[i],
                     f"grep -qF '{ready_pattern}' {local_dir}/server_{i+1}.log")
                    for i in range(num_replicas) if not ready[i]]):
                ready[i] = True
                print(f"  Replica {i+1} ready")
            if not all(ready):
                time.sleep(2)
        if not all(ready):
            not_ready = [i+1 for i, r in enumerate(ready) if not r]
            print(f"WARNING: replicas {not_ready} not ready — continuing anyway")

        # Start client nodes
        print("Starting client nodes...")
        client_procs = []
        for j, host in enumerate(client_hosts, 1):
            node_id   = num_replicas + j
            log_file  = f"{local_dir}/client_{node_id}.log"
            priv_key  = cert_dir / f"node_{node_id}.key.pri"
            cert_file = cert_dir / f"cert_{node_id}.cert"
            p = ssh_cmd(host,
                f"mkdir -p {local_dir}; {env_prefix}nohup {SERVER_BIN} {config_path} {priv_key} "
                f"{cert_file} > {log_file} 2>&1 &")
            if p:
                client_procs.append(p)
        for p in client_procs:
            p.wait()

        # Wait for client ports to open
        print("Waiting for client nodes to be ready...")
        for j, (host, ip) in enumerate(zip(client_hosts, client_ips), 1):
            node_id = num_replicas + j
            port    = BASE_PORT + node_id - 1
            deadline_c = time.time() + 60
            ok = False
            while time.time() < deadline_c:
                try:
                    s = socket.create_connection((ip, port), timeout=1)
                    s.close()
                    ok = True
                    print(f"  Client {node_id} ({ip}:{port}) ready")
                    break
                except OSError:
                    time.sleep(1)
            if not ok:
                print(f"  WARNING: client {node_id} not listening — triggering anyway")

        # Induce silent faults AFTER all TCP handshakes complete: kill the
        # last `silent_faults` replicas before triggering txn injection so
        # the run actually models "f peers crashed silently" rather than
        # "f peers were never reachable" (the latter blocks Pearl).
        if silent_faults:
            silent_hosts = replica_hosts[live_replicas:]
            print(f"Inducing silent faults: killing replicas "
                  f"{list(range(live_replicas+1, num_replicas+1))} "
                  f"on {silent_hosts}")
            kill_silent = []
            # Kill the replica process only: the node's co-located client
            # keeps running.
            for i, host in enumerate(silent_hosts, live_replicas + 1):
                pid_file = f"{local_dir}/server_{i}.pid"
                p = ssh_cmd(host, f"kill -9 $(cat {pid_file}) 2>/dev/null || true")
                if p:
                    kill_silent.append(p)
            for p in kill_silent:
                p.wait()
            time.sleep(1)

        live = list(enumerate(replica_hosts[:live_replicas], 1))
        dead = dead_replicas(live, local_dir)
        if dead:
            kill_nodes(replica_hosts + client_hosts, local_dir, log_dir)
            raise ReplicaDied(f"replicas {dead} died before the clients started")

        print("Triggering clients...")
        experiment_start = datetime.datetime.now()
        trigger_procs = []
        for j in range(1, num_clients + 1):
            node_id    = num_replicas + j
            client_cfg = runtime_dir / f"client_{node_id}.config"
            trigger_procs.append(subprocess.Popen([str(CLIENT_BIN), str(client_cfg)]))
        for p in trigger_procs:
            p.wait()

        print(f"Running for {runtime}s...")
        time.sleep(runtime)

        dead = dead_replicas(live, local_dir)
        if dead:
            kill_nodes(replica_hosts + client_hosts, local_dir, log_dir)
            raise ReplicaDied(f"replicas {dead} died during the run")

        print("Killing nodes...")
        kill_nodes(replica_hosts + client_hosts, local_dir, log_dir)

        print("Parsing logs...")
        fallback_dur = max(1.0, runtime - warmup)
        per_replica_con_tps: list = []
        per_replica_con_lat: list = []
        per_replica_exec_tps: list = []
        per_replica_exec_lat: list = []
        slots_committed = 0
        any_commit_fallback = False
        any_exec_fallback = False
        stalled: list = []

        for i in range(1, live_replicas + 1):
            lf = log_dir / f"server_{i}.log"
            commit_meas, exec_meas, commit_total, exec_total, commit_fb, exec_fb = parse_log_file(
                lf, experiment_start, warmup, runtime)
            slots_committed = max(slots_committed, commit_total)
            any_commit_fallback = any_commit_fallback or commit_fb
            any_exec_fallback   = any_exec_fallback   or exec_fb
            con_tps_i, con_lat_i = _compute_tps_latency(commit_meas, fallback_dur)
            exec_tps_i, exec_lat_i = _compute_tps_latency(exec_meas, fallback_dur)
            flag = ""
            if commit_fb or exec_fb:
                flag = f"  [fallback:{('c' if commit_fb else '')}{('x' if exec_fb else '')}]"
            print(f"  Replica {i}: con={con_tps_i:.0f}tps/{con_lat_i*1000:.0f}ms"
                  f"  exec={exec_tps_i:.0f}tps/{exec_lat_i*1000:.0f}ms"
                  f"  slots={commit_total}  exec_slots={exec_total}{flag}")
            if exec_total == 0:
                stalled.append(i)
            if con_tps_i > 0:
                per_replica_con_tps.append(con_tps_i)
                per_replica_con_lat.append(con_lat_i)
            if exec_tps_i > 0:
                per_replica_exec_tps.append(exec_tps_i)
                per_replica_exec_lat.append(exec_lat_i)

        if stalled:
            raise ReplicaDied(f"replicas {stalled} executed nothing")

        if any_commit_fallback or any_exec_fallback:
            which = []
            if any_commit_fallback: which.append("commit")
            if any_exec_fallback:   which.append("exec")
            print(f"  NOTE: bursty run — {'/'.join(which)} activity fell inside "
                  f"the {warmup}s warmup window; using full [start, start+runtime] "
                  f"window as fallback.")

        def _median(xs):
            s = sorted(xs)
            n = len(s)
            return (s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2) if s else 0.0

        consensus_tps   = round(_median(per_replica_con_tps), 1)
        consensus_lat_s = _median(per_replica_con_lat)
        execution_tps   = round(_median(per_replica_exec_tps), 1)
        execution_lat_s = _median(per_replica_exec_lat)

        print(f"=> con_TPS={consensus_tps}  con_lat={consensus_lat_s*1000:.0f}ms"
              f"  exec_TPS={execution_tps}  exec_lat={execution_lat_s*1000:.0f}ms"
              f"  slots={slots_committed}")
        recorded = True
        return consensus_tps, consensus_lat_s, execution_tps, execution_lat_s, slots_committed

    except BaseException as e:
        # Don't leave replicas running into the next point.
        if hostnames:
            if local_dir is not None:
                kill_nodes(hostnames, local_dir, log_dir)
            else:
                kill_nodes(hostnames)
        if isinstance(e, OSError) and e.errno in _DISK_FULL_ERRNOS:
            raise DiskFull(str(e)) from e
        raise

    finally:
        try:
            pm.kill_reservation(reservation_id)
            print(f"Reservation {reservation_id} released.")
        except Exception as e:
            print(f"Warning: could not release reservation: {e}")
        # Scan every attempt, including the rejected ones (retries otherwise
        # hide crashes), then compress its logs.
        if log_dir is not None and log_dir.is_dir():
            try:
                s = scan_run(log_dir, live_replicas)
                scan_csv = RESULTS_DIR / "scan.csv"
                init_csv(scan_csv, _SCAN_HEADER)
                append_csv(scan_csv, ",".join(
                    [run_id, str(num_replicas), str(silent_faults), mode,
                     str(target_tps), str(int(recorded))] +
                    [str(s[k]) for k in _SCAN_HEADER.split(",")[6:]]))
                issues = [f"{k}={v}" for k, v in s.items()
                          if v and k not in ("ok", "max_stall_ms")]
                print(f"Scan: {'OK' if s['ok'] else 'ISSUES ' + ' '.join(issues)}"
                      f"  (max stall {s['max_stall_ms']} ms)")
                compress_logs(log_dir)
            except Exception as e:
                print(f"Warning: could not scan/compress {log_dir}: {e}")


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def init_csv(path: Path, header: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(header + "\n")
        return
    content = path.read_text()
    if not content.startswith(header):
        path.write_text(header + "\n" + content)


def append_csv(path: Path, row: str):
    with open(path, "a") as f:
        f.write(row + "\n")


def _load_completed_runs(csv_path: Path) -> set:
    """
    Return set of (n, mode, rate, rep) tuples that already have a successful
    result (consensus_tps > 0) in the CSV.  Zero-result rows are NOT counted
    as completed so they get re-run.
    """
    completed: set = set()
    if not csv_path.exists():
        return completed
    with open(csv_path) as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) < 5:
                continue
            try:
                n, mode, rate, rep = int(parts[0]), parts[1], int(parts[2]), int(parts[3])
                if float(parts[4]) > 0:
                    completed.add((n, mode, rate, rep))
            except (ValueError, IndexError):
                continue
    return completed


# ---------------------------------------------------------------------------
# Rate-sweep experiment  (produces the L-curve data)
# ---------------------------------------------------------------------------

def run_rate_sweep(dry_run: bool, nodes_filter: Optional[List[int]] = None,
                   modes_filter: Optional[List[str]] = None):
    """
    For each N (committee size) and mode (ol/bof), sweep injection rates
    from below saturation to above.  Each (N, mode, rate) → 5 repetitions.

    Produces tput_latency.csv with columns:
      n, mode, input_rate, rep,
      consensus_tps, consensus_latency_ms,
      execution_tps, execution_latency_ms,
      slots_committed

    Resume behaviour: any (n, mode, rate, rep) already present in the CSV
    with consensus_tps > 0 is skipped.  Zero-result rows are re-run.
    """
    csv_path = RESULTS_DIR / "tput_latency.csv"
    init_csv(csv_path,
             "n,mode,input_rate,rep,"
             "consensus_tps,consensus_latency_ms,"
             "execution_tps,execution_latency_ms,"
             "slots_committed")

    completed = _load_completed_runs(csv_path)
    if completed:
        print(f"Resuming: {len(completed)} run(s) already complete, will skip them.")

    n_values = sorted(RATE_SWEEP.keys())
    if nodes_filter:
        n_values = [n for n in n_values if n in nodes_filter]

    modes = modes_filter if modes_filter else ["ol", "bof"]

    print(f"\n=== Throughput-Latency rate sweep  (N={n_values}, modes={modes}) ===")
    failures = []

    for n in n_values:
        rates = RATE_SWEEP[n]
        for mode in modes:
            for rate in rates:
                for rep in range(1, REPETITIONS + 1):
                    if (n, mode, rate, rep) in completed:
                        print(f"  skip n={n} mode={mode} rate={rate} rep={rep} (done)")
                        continue
                    print(f"\n--- n={n}  mode={mode}  rate={rate}  rep={rep}/{REPETITIONS} ---")
                    # A failed point (e.g. machines not granted in time) is
                    # skipped without a row, so that rerunning the sweep fills it.
                    try:
                        con_tps, con_lat_s, exec_tps, exec_lat_s, slots = run_experiment(
                            num_replicas=n,
                            mode=mode,
                            block_size=BLOCK_SIZE,
                            runtime=RUNTIME,
                            warmup=WARMUP,
                            dry_run=dry_run,
                            num_clients=n,   # one client per replica
                            target_tps=rate,
                        )
                    except DiskFull as e:
                        print(f"\n!! STOPPING the sweep: {e}.\n"
                              f"Free space, then rerun the same command to resume.")
                        return
                    except Exception as e:  # keep the sweep alive
                        failures.append((n, mode, rate, rep))
                        print(f"  !! FAILED n={n} mode={mode} rate={rate} rep={rep}: {e}")
                        time.sleep(30)
                        continue
                    if dry_run:
                        continue  # a dry run measures nothing: record nothing
                    append_csv(csv_path,
                        f"{n},{mode},{rate},{rep},"
                        f"{con_tps},{round(con_lat_s * 1000, 2)},"
                        f"{exec_tps},{round(exec_lat_s * 1000, 2)},"
                        f"{slots}")
                    time.sleep(10)

    if failures:
        print(f"\n{len(failures)} point(s) failed and have no row; "
              f"rerun the same command to fill them: {failures}")


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def print_summary():
    import csv as csv_mod
    from collections import defaultdict

    csv_path = RESULTS_DIR / "tput_latency.csv"
    if not csv_path.exists():
        return

    print(f"\n=== Rate-sweep results ({csv_path}) ===")
    rows = list(csv_mod.DictReader(csv_path.open()))
    if not rows:
        return

    # Aggregate per (n, mode, input_rate)
    agg: dict = defaultdict(lambda: {
        "con_tps": [], "con_lat": [], "exec_tps": [], "exec_lat": []
    })
    for r in rows:
        key = (r["n"], r["mode"], r["input_rate"])
        try:
            agg[key]["con_tps"].append(float(r["consensus_tps"]))
            agg[key]["con_lat"].append(float(r["consensus_latency_ms"]))
            agg[key]["exec_tps"].append(float(r["execution_tps"]))
            agg[key]["exec_lat"].append(float(r["execution_latency_ms"]))
        except (ValueError, KeyError):
            pass

    def tmean(xs):
        xs = sorted(x for x in xs if x > 0)
        if len(xs) > 2:
            xs = xs[1:-1]
        return sum(xs) / len(xs) if xs else 0.0

    print(f"\n{'n':>4}  {'mode':>5}  {'rate':>6}"
          f"  {'exec_tps':>9}  {'exec_lat_ms':>12}"
          f"  {'con_tps':>9}  {'con_lat_ms':>10}")
    for (n, mode, rate), v in sorted(agg.items()):
        print(f"{n:>4}  {mode:>5}  {rate:>6}"
              f"  {tmean(v['exec_tps']):>9.1f}  {tmean(v['exec_lat']):>12.1f}"
              f"  {tmean(v['con_tps']):>9.1f}  {tmean(v['con_lat']):>10.1f}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DAS5 throughput-latency rate sweep (L-curve experiment)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without reserving machines or running SSH")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip bazel build (use pre-built binaries)")
    parser.add_argument("--nodes", type=int, nargs="+",
                        help="Only sweep these N values (e.g. --nodes 10 16)")
    parser.add_argument("--mode", choices=["ol", "bof"], nargs="+",
                        help="Only sweep these modes (e.g. --mode bof)")
    parser.add_argument("--results-dir",
                        help=f"Write tput_latency.csv here instead of {RESULTS_DIR.relative_to(PROJ_ROOT)} "
                             "(relative to the repo root); resuming reads only this file")
    parser.add_argument("--reps", type=int,
                        help=f"Repetitions per (N, mode, rate) point (default {REPETITIONS})")
    args = parser.parse_args()
    if args.results_dir:
        RESULTS_DIR = (PROJ_ROOT / args.results_dir).resolve()
    if args.reps:
        REPETITIONS = args.reps

    if not args.dry_run:
        if not args.skip_build:
            build_binaries()
        check_binaries()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_BASE.mkdir(parents=True, exist_ok=True)

    run_rate_sweep(dry_run=args.dry_run, nodes_filter=args.nodes,
                   modes_filter=args.mode)

    print_summary()
    print("\nDone. Results in:", RESULTS_DIR)
