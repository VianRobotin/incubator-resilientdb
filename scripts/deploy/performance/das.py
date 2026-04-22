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

BASE_PORT      = 10001
READY_TIMEOUT  = 120   # seconds to wait for all replicas to connect

RESULTS_DIR  = PROJ_ROOT / "das_results"
RUNTIME_BASE = PROJ_ROOT / "das_runtime"

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
     7: [500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000],
    11: [500, 1000, 2000, 4000, 6000, 8000, 10000, 12000],
    15: [300,  600, 1200, 2500, 4000, 6000,  8000, 10000],
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
                        runtime: int):
    bof_flag = (mode == "bof")
    replica_info = [
        {"id": i, "ip": ip, "port": BASE_PORT + i - 1}
        for i, ip in enumerate(node_ips, 1)
    ]
    client_batch_num = 1 if target_input_tps_per_client > 0 else 400
    max_txn = max(target_input_tps_per_client * runtime * 3, 2_000_000)
    config = {
        "region": [{"replicaInfo": replica_info}],
        "clientBatchNum": client_batch_num,
        "enable_viewchange": False,
        "recovery_enabled": False,
        "max_client_complaint_num": 10,
        "max_process_txn": max_txn,
        "worker_num": 10,
        "input_worker_num": 1,
        "output_worker_num": 5,
        "block_size": block_size,
        "batch_order_fairness": bof_flag,
    }
    if target_input_tps_per_client > 0:
        config["target_input_tps"] = target_input_tps_per_client
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


_CERT_RE = re.compile(
    r'^[EW](\d{8}) (\d{2}:\d{2}:\d{2}\.\d+).*?'
    r'block_certified block_id:\d+ txns:(\d+) consensus_latency_us:(\d+)',
    re.MULTILINE,
)
_EXEC_RE = re.compile(
    r'^[EW](\d{8}) (\d{2}:\d{2}:\d{2}\.\d+).*?'
    r'execution commit slot:\d+ txns:(\d+) execution_latency_us:(\d+)',
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


def parse_log_file(log_file: Path, experiment_start: datetime.datetime,
                   warmup_secs: int, runtime: int):
    """
    Returns (cert_meas, exec_meas, cert_total, exec_total,
             cert_fallback, exec_fallback).
    """
    if not log_file.exists():
        return [], [], 0, 0, False, False
    text = log_file.read_text(errors="replace")
    cert_all = _extract_entries(text, _CERT_RE)
    exec_all = _extract_entries(text, _EXEC_RE)
    cert_meas, cert_fb = _apply_warmup(cert_all, experiment_start, warmup_secs, runtime)
    exec_meas, exec_fb = _apply_warmup(exec_all, experiment_start, warmup_secs, runtime)
    return cert_meas, exec_meas, len(cert_all), len(exec_all), cert_fb, exec_fb


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

def run_experiment(
    num_replicas: int,
    mode: str,
    block_size: int,
    runtime: int,
    warmup: int,
    dry_run: bool,
    num_clients: int = 1,
    target_tps: int = 0,
) -> Tuple[float, float, float, float, int]:
    """
    Reserve machines, run one experiment, return:
      (consensus_tps, consensus_latency_s, execution_tps, execution_latency_s, slots)
    """
    run_id = (f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
              f"_n{num_replicas}_{mode}_r{target_tps}")
    print(f"\n{'='*60}")
    print(f"  {run_id}")
    print(f"{'='*60}")

    total_secs = runtime + 120
    duration_str = str(datetime.timedelta(seconds=total_secs))
    pm = PreserveManager(USERNAME)
    total_machines = num_replicas + num_clients
    per_client_tps = (target_tps // num_clients) if target_tps > 0 else 0

    if dry_run:
        print(f"[DRY-RUN] Reserve {total_machines} machines for {duration_str}")
        print(f"[DRY-RUN] target_tps={target_tps}  per_client_tps={per_client_tps}")
        return 0.0, 0.0, 0.0, 0.0, 0

    reservation_id = pm.create_reservation(total_machines, duration_str)
    print(f"Reservation: {reservation_id}")
    time.sleep(5)

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
        client_hosts  = hostnames[num_replicas:]
        replica_ips   = [socket.gethostbyname(h) for h in replica_hosts]
        client_ips    = [socket.gethostbyname(h) for h in client_hosts]
        print(f"Replicas: {replica_hosts}")
        print(f"Clients:  {client_hosts}")

        runtime_dir = RUNTIME_BASE / run_id
        cert_dir    = runtime_dir / "cert"
        log_dir     = runtime_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        print("Generating certificates...")
        generate_certs(cert_dir, replica_ips, client_ips)

        config_path = runtime_dir / "server.config"
        write_server_config(config_path, replica_ips, mode, block_size,
                            per_client_tps, runtime)
        print(f"Config: {config_path}  (per_client_tps={per_client_tps})")

        for j, ip in enumerate(client_ips, 1):
            node_id = num_replicas + j
            write_client_trigger_config(
                runtime_dir / f"client_{node_id}.config", node_id, ip)

        env_prefix = (
            f"export TEE_ENCLAVE_PATH={TEE_ENCLAVE_PATH}; "
            f"export LD_LIBRARY_PATH={LD_LIBRARY_PATH}:$LD_LIBRARY_PATH; "
        )

        # Start replicas
        print("Starting replicas...")
        procs = []
        for i, host in enumerate(replica_hosts, 1):
            log_file  = log_dir / f"server_{i}.log"
            priv_key  = cert_dir / f"node_{i}.key.pri"
            cert_file = cert_dir / f"cert_{i}.cert"
            p = ssh_cmd(host,
                f"{env_prefix}nohup {SERVER_BIN} {config_path} {priv_key} "
                f"{cert_file} > {log_file} 2>&1 &")
            if p:
                procs.append(p)
        for p in procs:
            p.wait()

        # Wait for all replicas to connect
        print(f"Waiting for {num_replicas} replicas (timeout={READY_TIMEOUT}s)...")
        ready_pattern = f"receive public size:{num_replicas}"
        deadline = time.time() + READY_TIMEOUT
        ready = [False] * num_replicas
        while not all(ready) and time.time() < deadline:
            for i in range(num_replicas):
                if ready[i]:
                    continue
                lf = log_dir / f"server_{i+1}.log"
                if lf.exists() and ready_pattern in lf.read_text(errors="replace"):
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
            log_file  = log_dir / f"client_{node_id}.log"
            priv_key  = cert_dir / f"node_{node_id}.key.pri"
            cert_file = cert_dir / f"cert_{node_id}.cert"
            p = ssh_cmd(host,
                f"{env_prefix}nohup {SERVER_BIN} {config_path} {priv_key} "
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

        # Trigger clients.
        # experiment_start is captured BEFORE the trigger subprocess is
        # spawned: the trigger is just a kv_client.Set("start", ...) call,
        # but on an overloaded / misconfigured client it can block for
        # seconds waiting for an ACK.  Anchoring to now() before the Set
        # guarantees the measurement window starts at-or-before the first
        # commit.  (It is fine that the window begins very slightly before
        # the trigger actually arrives — no activity can have happened yet,
        # so no extra entries get included.)
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

        # Kill all
        print("Killing nodes...")
        kill_procs = []
        for host in replica_hosts + client_hosts:
            p = ssh_cmd(host, "killall -9 kv_server_performance 2>/dev/null || true")
            if p:
                kill_procs.append(p)
        for p in kill_procs:
            p.wait()
        time.sleep(2)

        # Parse logs
        print("Parsing logs...")
        fallback_dur = max(1.0, runtime - warmup)
        per_replica_con_tps: list = []
        per_replica_con_lat: list = []
        per_replica_exec_tps: list = []
        per_replica_exec_lat: list = []
        slots_committed = 0
        any_cert_fallback = False
        any_exec_fallback = False

        for i in range(1, num_replicas + 1):
            lf = log_dir / f"server_{i}.log"
            cert_meas, exec_meas, cert_total, exec_total, cert_fb, exec_fb = parse_log_file(
                lf, experiment_start, warmup, runtime)
            slots_committed = max(slots_committed, exec_total)
            any_cert_fallback = any_cert_fallback or cert_fb
            any_exec_fallback = any_exec_fallback or exec_fb
            con_tps_i, con_lat_i = _compute_tps_latency(cert_meas, fallback_dur)
            exec_tps_i, exec_lat_i = _compute_tps_latency(exec_meas, fallback_dur)
            flag = ""
            if cert_fb or exec_fb:
                flag = f"  [fallback:{('c' if cert_fb else '')}{('e' if exec_fb else '')}]"
            print(f"  Replica {i}: con={con_tps_i:.0f}tps/{con_lat_i*1000:.0f}ms"
                  f"  exec={exec_tps_i:.0f}tps/{exec_lat_i*1000:.0f}ms"
                  f"  cert_blocks={cert_total}  slots={exec_total}{flag}")
            if con_tps_i > 0:
                per_replica_con_tps.append(con_tps_i)
                per_replica_con_lat.append(con_lat_i)
            if exec_tps_i > 0:
                per_replica_exec_tps.append(exec_tps_i)
                per_replica_exec_lat.append(exec_lat_i)

        if any_cert_fallback or any_exec_fallback:
            which = []
            if any_cert_fallback: which.append("cert")
            if any_exec_fallback: which.append("exec")
            print(f"  NOTE: bursty run — {'/'.join(which)} activity fell inside "
                  f"the {warmup}s warmup window; using full [start, start+runtime] "
                  f"window as fallback.")

        # Both metrics: median across replicas.
        # consensus_tps: each replica logs only its OWN block certifications
        #   (block_certified fires only on the originating replica), so one
        #   replica's cert rate = that replica's share ≈ total/N.
        # execution_tps: every replica commits every slot (Commit() runs on all),
        #   so each replica's exec rate = the full system throughput.  Taking the
        #   median (vs the old "best replica") avoids inflating with outliers and
        #   is more robust to lagging replicas.
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
        return consensus_tps, consensus_lat_s, execution_tps, execution_lat_s, slots_committed

    finally:
        try:
            pm.kill_reservation(reservation_id)
            print(f"Reservation {reservation_id} released.")
        except Exception as e:
            print(f"Warning: could not release reservation: {e}")


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

def run_rate_sweep(dry_run: bool, nodes_filter: Optional[List[int]] = None):
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

    print(f"\n=== Throughput-Latency rate sweep  (N={n_values}) ===")

    for n in n_values:
        rates = RATE_SWEEP[n]
        for mode in ["ol", "bof"]:
            for rate in rates:
                for rep in range(1, REPETITIONS + 1):
                    if (n, mode, rate, rep) in completed:
                        print(f"  skip n={n} mode={mode} rate={rate} rep={rep} (done)")
                        continue
                    print(f"\n--- n={n}  mode={mode}  rate={rate}  rep={rep}/{REPETITIONS} ---")
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
                    append_csv(csv_path,
                        f"{n},{mode},{rate},{rep},"
                        f"{con_tps},{round(con_lat_s * 1000, 2)},"
                        f"{exec_tps},{round(exec_lat_s * 1000, 2)},"
                        f"{slots}")
                    if not dry_run:
                        time.sleep(10)


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
    args = parser.parse_args()

    if not args.dry_run:
        if not args.skip_build:
            build_binaries()
        check_binaries()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_BASE.mkdir(parents=True, exist_ok=True)

    run_rate_sweep(dry_run=args.dry_run, nodes_filter=args.nodes)

    print_summary()
    print("\nDone. Results in:", RESULTS_DIR)
