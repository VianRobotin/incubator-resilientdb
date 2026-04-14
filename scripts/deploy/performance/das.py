#!/usr/bin/env python3
# DAS5 Benchmark Orchestrator for OL vs BOF thesis experiments.
#
# Runs n-scaling experiment: n=3/5/7 replicas, fixed block_size=100, modes=[ol,bof]
#
# Usage (from the scripts/deploy/ directory):
#   python3 performance/das.py               # run experiment
#   python3 performance/das.py --dry-run     # print plan without reserving or SSHing
#
# Assumes:
#   - /var/scratch is NFS-shared across all DAS5 nodes (no SCP needed for binary)
#   - SSH from headnode to compute nodes is passwordless (shared ~/.ssh via NFS home)
#   - preserve command is available on PATH

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

# Resolve project root: this file lives at <root>/scripts/deploy/performance/das.py
PROJ_ROOT = Path(__file__).resolve().parents[3]

USERNAME = os.environ.get("USER", "vrobotin")

SERVER_BIN  = PROJ_ROOT / "bazel-bin/benchmark/protocols/autobahn/kv_server_performance"
CLIENT_BIN  = PROJ_ROOT / "bazel-bin/benchmark/protocols/autobahn/kv_service_tools"
KEY_GEN     = PROJ_ROOT / "bazel-bin/tools/key_generator_tools"
CERT_TOOL   = PROJ_ROOT / "bazel-bin/tools/certificate_tools"

BASE_PORT      = 10001
STATS_INTERVAL = 5   # seconds between stats log lines (matches Stats default)
READY_TIMEOUT  = 120 # seconds to wait for all replicas to connect

RESULTS_DIR  = PROJ_ROOT / "das_results"
RUNTIME_BASE = PROJ_ROOT / "das_runtime"

# SGX paths — replicas fall back to software simulation if enclave is absent
TEE_ENCLAVE_PATH = PROJ_ROOT / "platform/consensus/ordering/autobahn/tee/tee_enclave.signed.so"
LD_LIBRARY_PATH  = "/var/scratch/vrobotin/sgxsdk/lib64"

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

# Search order for the bazel executable when it is not on $PATH.
_BAZEL_SEARCH_PATHS = [
    "/usr/local/bin/bazel",
    "/usr/bin/bazel",
    str(Path.home() / "bin" / "bazel"),
    str(Path.home() / ".local" / "bin" / "bazel"),
]

def _find_bazel() -> str:
    """Return the path to the bazel executable, or raise RuntimeError."""
    found = shutil.which("bazel")
    if found:
        return found
    for candidate in _BAZEL_SEARCH_PATHS:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError(
        "bazel not found on $PATH or in common locations.\n"
        "Install bazel (https://bazel.build/install) or use --skip-build "
        "if the binaries are already built."
    )


def build_binaries():
    print("\n=== Building binaries ===")
    bazel = _find_bazel()
    print(f"Using bazel: {bazel}")
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
    """Abort with a clear message if any required binary is missing."""
    missing = [name for name, path in _REQUIRED_BINARIES.items() if not path.exists()]
    if missing:
        print("\nERROR: the following required binaries are missing:", file=sys.stderr)
        for name in missing:
            print(f"  {_REQUIRED_BINARIES[name]}", file=sys.stderr)
        print(
            "\nBuild them with:\n"
            "  bazel build "
            "//benchmark/protocols/autobahn:kv_server_performance "
            "//benchmark/protocols/autobahn:kv_service_tools "
            "//tools:key_generator_tools "
            "//tools:certificate_tools",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Certificate generation
# ---------------------------------------------------------------------------

def generate_certs(cert_dir: Path, replica_ips: List[str], client_ips: List[str]):
    """Generate admin + per-node keypairs and certificates into cert_dir.

    Replica nodes get IDs 1..n with 'replica' type certs.
    Client nodes get IDs n+1..n+m with 'client' type certs.
    """
    cert_dir.mkdir(parents=True, exist_ok=True)

    num_replicas = len(replica_ips)
    num_clients  = len(client_ips)
    total_nodes  = num_replicas + num_clients

    # Admin keypair
    subprocess.check_call([str(KEY_GEN), str(cert_dir / "admin")])

    # Per-node keypairs
    for i in range(1, total_nodes + 1):
        subprocess.check_call([str(KEY_GEN), str(cert_dir / f"node_{i}")])

    # Replica certificates
    for i, ip in enumerate(replica_ips, 1):
        port = str(BASE_PORT + i - 1)
        subprocess.check_call([
            str(CERT_TOOL),
            str(cert_dir),
            str(cert_dir / "admin.key.pri"),
            str(cert_dir / "admin.key.pub"),
            str(cert_dir / f"node_{i}.key.pub"),
            str(i), ip, port, "replica",
        ])

    # Client certificates
    for j, ip in enumerate(client_ips, 1):
        node_id = num_replicas + j
        port = str(BASE_PORT + node_id - 1)
        subprocess.check_call([
            str(CERT_TOOL),
            str(cert_dir),
            str(cert_dir / "admin.key.pri"),
            str(cert_dir / "admin.key.pub"),
            str(cert_dir / f"node_{node_id}.key.pub"),
            str(node_id), ip, port, "client",
        ])


# ---------------------------------------------------------------------------
# Config generation
# ---------------------------------------------------------------------------

def write_server_config(config_path: Path, node_ips: List[str], mode: str, block_size: int,
                        target_input_tps_per_client: int = 0, runtime: int = 60):
    """Write server.config shared by all nodes (replicas and clients).

    target_input_tps_per_client: per-client TPS limit (0 = unlimited).
    When non-zero, clientBatchNum is set to 1 so each send is exactly one
    transaction, matching gsegalin's rate-control approach.
    """
    bof_flag = mode == "bof"
    replica_info = [
        {"id": i, "ip": ip, "port": BASE_PORT + i - 1}
        for i, ip in enumerate(node_ips, 1)
    ]
    # clientBatchNum=1 + target_input_tps gives precise TPS control.
    # Without rate control, use larger batches for max throughput.
    client_batch_num = 1 if target_input_tps_per_client > 0 else 400
    # max_process_txn must be large enough so the client doesn't exhaust its
    # transaction budget before the experiment ends.  At 15k TPS for 60s we
    # need ~900k; use target * runtime * 3 with a floor of 2 000 000.
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
    """Write a plain-text config for kv_service_tools to trigger one client node.

    Format expected by GenerateResDBConfig(config_file): one 'id ip port' line.
    kv_service_tools connects to this address and sends a TYPE_CLIENT_REQUEST,
    which causes the client node's PerformanceManager to call StartEval and
    begin sending transactions to the replicas.
    """
    port = BASE_PORT + node_id - 1
    path.write_text(f"{node_id} {ip} {port}\n")


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------

def ssh_cmd(hostname: str, remote_cmd: str, dry_run: bool = False) -> Optional[subprocess.Popen]:
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", hostname, remote_cmd]
    if dry_run:
        print(f"  [DRY-RUN] ssh {hostname}: {remote_cmd}")
        return None
    return subprocess.Popen(cmd)


def ssh_output(hostname: str, remote_cmd: str) -> str:
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", hostname, remote_cmd]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout


# ---------------------------------------------------------------------------
# Log parsing (Python translation of parse_log from benchmark_autobahn.sh)
# ---------------------------------------------------------------------------

def trimmed_mean(values: list, drop: int = 1) -> float:
    """Mean after dropping `drop` lowest and highest values. Returns 0.0 if empty."""
    v = sorted(v for v in values if v > 0)
    if len(v) > 2 * drop:
        v = v[drop:-drop]
    return sum(v) / len(v) if v else 0.0


TXN_SIZE_BYTES = 128  # matches gsegalin's fixed 128-byte transaction size


def parse_log(log_file: Path, warmup_secs: int, runtime: int) -> Tuple[float, float, float, float, int]:
    """
    Returns (e2e_tps, e2e_bps, consensus_tps, consensus_latency_s, slots_committed).

    All metrics are derived from structured "consensus commit slot:N txns:M consensus_latency_us:X"
    lines that Autobahn emits once per committed slot.  Each such line carries a glog timestamp
    which we use to apply a time-based warmup filter (avoids the too-aggressive position-based
    skip that left only 1–5 samples for small n).

    e2e_tps          — sum(txns in measurement window) / measurement_duration
    e2e_bps          — e2e_tps * TXN_SIZE_BYTES
    Consensus TPS    — count(slots in measurement window) / measurement_duration
    Consensus latency — trimmed mean of per-slot consensus_latency_us (→ seconds)
    slots_committed  — total number of committed slots (full run)
    """
    if not log_file.exists():
        return 0.0, 0.0, 0.0, 0.0, 0

    text = log_file.read_text(errors="replace")

    # Each committed slot produces a line like:
    #   E20260413 22:51:27.281818 <tid> autobahn.cpp:969] consensus commit slot:1 txns:396 consensus_latency_us:2002282
    commit_re = re.compile(
        r'^[EW](\d{8}) (\d{2}:\d{2}:\d{2}\.\d+).*?'
        r'consensus commit slot:(\d+) txns:(\d+) consensus_latency_us:(\d+)',
        re.MULTILINE,
    )

    commits = []  # list of (datetime, slot_num, txns, latency_us)
    for m in commit_re.finditer(text):
        date_s, time_s = m.group(1), m.group(2)
        slot, txns, lat_us = int(m.group(3)), int(m.group(4)), int(m.group(5))
        try:
            dt = datetime.datetime.strptime(f"{date_s} {time_s}", "%Y%m%d %H:%M:%S.%f")
        except ValueError:
            continue
        commits.append((dt, slot, txns, lat_us))

    if not commits:
        return 0.0, 0.0, 0.0, 0.0, 0

    slots_committed = len(commits)

    # Time-based warmup filter: skip slots committed within the first warmup_secs
    t0 = commits[0][0]
    warmup_td  = datetime.timedelta(seconds=warmup_secs)
    runtime_td = datetime.timedelta(seconds=runtime)

    meas_commits = [c for c in commits
                    if warmup_td <= (c[0] - t0) <= runtime_td]

    if not meas_commits:
        # Nothing after warmup — fall back to all data (edge case for very short runs)
        meas_commits = commits

    # Measurement window duration: span of filtered commits + one slot's worth of time
    # (the last slot's txns are committed at its timestamp but the slot itself took ~2s)
    if len(meas_commits) >= 2:
        span_s = (meas_commits[-1][0] - meas_commits[0][0]).total_seconds()
        avg_slot_s = span_s / (len(meas_commits) - 1)
        meas_duration = span_s + avg_slot_s
    else:
        # Single slot: estimate duration from its latency
        meas_duration = meas_commits[0][3] / 1_000_000 or 2.0

    e2e_tps       = sum(c[2] for c in meas_commits) / meas_duration
    e2e_bps       = e2e_tps * TXN_SIZE_BYTES
    consensus_tps = len(meas_commits) / meas_duration

    lat_us_values    = [c[3] for c in meas_commits]
    consensus_latency_s = trimmed_mean(lat_us_values) / 1_000_000 if lat_us_values else 0.0

    return round(e2e_tps, 1), round(e2e_bps, 1), round(consensus_tps, 3), round(consensus_latency_s, 4), slots_committed



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
    Reserve machines, run one experiment, return (e2e_tps, e2e_bps, con_tps, con_lat, slots).
    target_tps: total target TPS across all clients (0 = unlimited).
    Reservation is released after the run completes (or on error).
    """
    run_id = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_n{num_replicas}_{mode}_bs{block_size}"
    print(f"\n{'='*60}")
    print(f"  Experiment: {run_id}")
    print(f"{'='*60}")

    # Extra time: startup + warmup + runtime + teardown buffer
    total_secs = runtime + 120
    duration_str = str(datetime.timedelta(seconds=total_secs))

    pm = PreserveManager(USERNAME)

    total_machines = num_replicas + num_clients

    per_client_tps = (target_tps // num_clients) if target_tps > 0 else 0

    if dry_run:
        print(f"[DRY-RUN] Would reserve {total_machines} machines ({num_replicas} replicas + {num_clients} clients) for {duration_str}")
        print(f"[DRY-RUN] target_tps={target_tps}  per_client_tps={per_client_tps}")
        print(f"[DRY-RUN] Would generate certs, write server.config, start replicas and clients")
        print(f"[DRY-RUN] Would wait {runtime}s, kill all nodes, parse logs")
        return 0.0, 0.0, 0.0, 0.0, 0

    # ---- Reserve machines ----
    reservation_id = pm.create_reservation(total_machines, duration_str)
    print(f"Reservation ID: {reservation_id}")
    time.sleep(5)

    try:
        # Wait for machines to be assigned
        hostnames = []
        print("Waiting for machines to be assigned...")
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
        print(f"Replica hosts: {replica_hosts}  IPs: {replica_ips}")
        print(f"Client hosts:  {client_hosts}  IPs: {client_ips}")

        # ---- Prepare runtime directory (on shared NFS path) ----
        runtime_dir = RUNTIME_BASE / run_id
        cert_dir = runtime_dir / "cert"
        log_dir = runtime_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # ---- Generate certs with real IPs ----
        print("Generating certificates...")
        generate_certs(cert_dir, replica_ips, client_ips)

        # ---- Write server.config (replica topology only) ----
        config_path = runtime_dir / "server.config"
        write_server_config(config_path, replica_ips, mode, block_size, per_client_tps, runtime)
        print(f"Config written: {config_path}  (per_client_tps={per_client_tps})")

        # ---- Write per-client trigger configs for kv_service_tools ----
        for j, ip in enumerate(client_ips, 1):
            node_id = num_replicas + j
            write_client_trigger_config(runtime_dir / f"client_{node_id}.config", node_id, ip)

        env_prefix = (
            f"export TEE_ENCLAVE_PATH={TEE_ENCLAVE_PATH}; "
            f"export LD_LIBRARY_PATH={LD_LIBRARY_PATH}:$LD_LIBRARY_PATH; "
        )

        # ---- Start replicas ----
        print("Starting replicas...")
        procs = []
        for i, host in enumerate(replica_hosts, 1):
            log_file = log_dir / f"server_{i}.log"
            priv_key = cert_dir / f"node_{i}.key.pri"
            cert_file = cert_dir / f"cert_{i}.cert"
            remote_cmd = (
                f"{env_prefix}"
                f"nohup {SERVER_BIN} {config_path} {priv_key} {cert_file} "
                f"> {log_file} 2>&1 &"
            )
            p = ssh_cmd(host, remote_cmd, dry_run=False)
            if p:
                procs.append(p)
        for p in procs:
            p.wait()

        # ---- Wait for all replicas to connect ----
        print(f"Waiting for {num_replicas} replicas to connect (timeout={READY_TIMEOUT}s)...")
        ready_pattern = f"receive public size:{num_replicas}"
        deadline = time.time() + READY_TIMEOUT
        ready = [False] * num_replicas
        while not all(ready) and time.time() < deadline:
            for i in range(num_replicas):
                if ready[i]:
                    continue
                log_file = log_dir / f"server_{i+1}.log"
                if log_file.exists() and ready_pattern in log_file.read_text(errors="replace"):
                    ready[i] = True
                    print(f"  Replica {i+1} ready")
            if not all(ready):
                time.sleep(2)

        if not all(ready):
            not_ready = [i+1 for i, r in enumerate(ready) if not r]
            print(f"WARNING: replicas {not_ready} did not signal ready — continuing anyway")

        # ---- Start client nodes ----
        print("Starting client nodes...")
        client_procs = []
        for j, host in enumerate(client_hosts, 1):
            node_id = num_replicas + j
            log_file = log_dir / f"client_{node_id}.log"
            priv_key = cert_dir / f"node_{node_id}.key.pri"
            cert_file = cert_dir / f"cert_{node_id}.cert"
            remote_cmd = (
                f"{env_prefix}"
                f"nohup {SERVER_BIN} {config_path} {priv_key} {cert_file} "
                f"> {log_file} 2>&1 &"
            )
            p = ssh_cmd(host, remote_cmd, dry_run=False)
            if p:
                client_procs.append(p)
        for p in client_procs:
            p.wait()

        # ---- Wait for each client node to be listening on its port ----
        print("Waiting for client nodes to be ready (port open)...")
        client_ready_timeout = 60  # seconds
        for j, (host, ip) in enumerate(zip(client_hosts, client_ips), 1):
            node_id = num_replicas + j
            port = BASE_PORT + node_id - 1
            deadline_c = time.time() + client_ready_timeout
            ready = False
            while time.time() < deadline_c:
                try:
                    s = socket.create_connection((ip, port), timeout=1)
                    s.close()
                    ready = True
                    print(f"  Client {node_id} ({ip}:{port}) ready")
                    break
                except OSError:
                    time.sleep(1)
            if not ready:
                print(f"  WARNING: client {node_id} ({ip}:{port}) not listening after "
                      f"{client_ready_timeout}s — triggering anyway")

        # ---- Trigger each client node via kv_service_tools (runs on headnode) ----
        # kv_service_tools sends a TYPE_CLIENT_REQUEST to the client node, which
        # calls PerformanceManager::StartEval() and starts BatchProposeMsg.
        print("Triggering client nodes...")
        trigger_procs = []
        for j in range(1, num_clients + 1):
            node_id = num_replicas + j
            client_cfg = runtime_dir / f"client_{node_id}.config"
            trigger_procs.append(subprocess.Popen([str(CLIENT_BIN), str(client_cfg)]))
        for p in trigger_procs:
            p.wait()

        # ---- Run for experiment duration ----
        print(f"Running for {runtime}s...")
        time.sleep(runtime)

        # ---- Kill replicas and clients ----
        print("Killing replicas and clients...")
        kill_procs = []
        for host in replica_hosts + client_hosts:
            p = ssh_cmd(host, "killall -9 kv_server_performance 2>/dev/null || true")
            if p:
                kill_procs.append(p)
        for p in kill_procs:
            p.wait()
        time.sleep(2)

        # ---- Parse replica logs for throughput and consensus metrics ----
        print("Parsing logs...")
        best_e2e_tps = 0.0
        best_e2e_bps = 0.0
        best_con_tps = 0.0
        best_con_lat = 0.0
        best_slots = 0
        for i in range(1, num_replicas + 1):
            log_file = log_dir / f"server_{i}.log"
            e2e_tps, e2e_bps, con_tps, con_lat, slots = parse_log(log_file, warmup, runtime)
            print(f"  Replica {i}: e2e_TPS={e2e_tps}  e2e_BPS={e2e_bps}  con_TPS={con_tps}  con_lat={con_lat}s  slots={slots}")
            if e2e_tps > best_e2e_tps:
                best_e2e_tps, best_e2e_bps, best_con_tps, best_con_lat = e2e_tps, e2e_bps, con_tps, con_lat
                best_slots = slots

        print(f"=> Best: e2e_TPS={best_e2e_tps}  e2e_BPS={best_e2e_bps}  con_TPS={best_con_tps}  con_lat={best_con_lat}s")
        return best_e2e_tps, best_e2e_bps, best_con_tps, best_con_lat, best_slots

    finally:
        try:
            pm.kill_reservation(reservation_id)
            print(f"Reservation {reservation_id} released.")
        except Exception as e:
            print(f"Warning: could not release reservation {reservation_id}: {e}")


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def init_csv(path: Path, header: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(header + "\n")


def append_csv(path: Path, row: str):
    with open(path, "a") as f:
        f.write(row + "\n")


# ---------------------------------------------------------------------------
# Experiment suite — mirrors gsegalin's Tilikum thesis experiments
# ---------------------------------------------------------------------------

BLOCK_SIZE = 100  # fixed block size for all runs

# Per-N input rates from gsegalin's Tilikum n-scaling experiments.
# These are the total tx/s sent by all clients combined.
N_SCALING_INPUT_RATES = {
    10: 15_000,
    13: 13_000,
    16: 10_000,
    19:  8_018,
    22:  7_018,
    25:  5_000,
}

RUNTIME     = 60   # matches gsegalin's ~60s execution time
WARMUP      = 15
REPETITIONS = 5    # matches gsegalin's >=5 runs per configuration
NUM_CLIENTS = 1    # ResilientDB TPS is client-limited; target_input_tps controls rate


def run_n_scaling(dry_run: bool):
    """N-scaling: committee size N=10,13,16,19,22,25, modes=[ol,bof].

    Mirrors gsegalin's experiment: local-0-1-0-1-{N}.txt
      - Faults: 0
      - Workers per node: 1 (collocated)
      - Input rate: varies per N (see N_SCALING_INPUT_RATES)
      - At least 5 runs per (N, mode) for statistical validity
    """
    csv_path = RESULTS_DIR / "n_scaling.csv"
    init_csv(csv_path, "n,mode,rep,e2e_tps,e2e_bps,consensus_tps,consensus_latency_s,slots_committed")
    print("\n\n=== N-scaling experiment (OL vs BOF) ===")

    for n in [10, 13, 16, 19, 22, 25]:
        target_tps = N_SCALING_INPUT_RATES[n]
        for mode in ["ol", "bof"]:
            for rep in range(1, REPETITIONS + 1):
                print(f"\n--- n={n}  mode={mode}  rep={rep}/{REPETITIONS}  target_tps={target_tps} ---")
                e2e_tps, e2e_bps, con_tps, con_lat, slots = run_experiment(
                    num_replicas=n,
                    mode=mode,
                    block_size=BLOCK_SIZE,
                    runtime=RUNTIME,
                    warmup=WARMUP,
                    dry_run=dry_run,
                    num_clients=NUM_CLIENTS,
                    target_tps=target_tps,
                )
                append_csv(csv_path,
                    f"{n},{mode},{rep},{e2e_tps},{e2e_bps},{con_tps},{con_lat},{slots}")
                if not dry_run:
                    time.sleep(10)




# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def _tmean(xs: list) -> float:
    """Trimmed mean: drop lowest and highest, then average."""
    xs = sorted(x for x in xs if x > 0)
    if len(xs) > 2:
        xs = xs[1:-1]
    return sum(xs) / len(xs) if xs else 0.0


def print_summary():
    import csv as csv_mod
    from collections import defaultdict

    # --- N-scaling ---
    csv_path = RESULTS_DIR / "n_scaling.csv"
    if csv_path.exists():
        print(f"\n=== N-scaling results ({csv_path}) ===")
        rows = list(csv_mod.DictReader(csv_path.open()))
        if rows:
            print(f"\n{'n':>4}  {'mode':>6}  {'rep':>4}  {'e2e_tps':>9}  {'e2e_bps':>11}  {'con_tps':>9}  {'con_lat_s':>10}")
            for r in rows:
                print(f"{r['n']:>4}  {r['mode']:>6}  {r['rep']:>4}  {r['e2e_tps']:>9}  {r['e2e_bps']:>11}  {r['consensus_tps']:>9}  {r['consensus_latency_s']:>10}")

            agg: dict = defaultdict(lambda: {"e2e_tps": [], "e2e_bps": [], "con_tps": [], "con_lat": []})
            for r in rows:
                key = (r["n"], r["mode"])
                try:
                    agg[key]["e2e_tps"].append(float(r["e2e_tps"]))
                    agg[key]["e2e_bps"].append(float(r["e2e_bps"]))
                    agg[key]["con_tps"].append(float(r["consensus_tps"]))
                    agg[key]["con_lat"].append(float(r["consensus_latency_s"]))
                except ValueError:
                    pass
            print(f"\n{'n':>4}  {'mode':>6}  {'mean_e2e_tps':>13}  {'mean_e2e_bps':>14}  {'mean_con_tps':>14}  {'mean_con_lat_s':>16}")
            for (n, mode), v in sorted(agg.items()):
                print(f"{n:>4}  {mode:>6}  {_tmean(v['e2e_tps']):>13.1f}  {_tmean(v['e2e_bps']):>14.1f}  {_tmean(v['con_tps']):>14.3f}  {_tmean(v['con_lat']):>16.4f}")



# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DAS5 benchmark orchestrator for OL vs BOF thesis")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be done without reserving machines or running SSH")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip the bazel build step (use pre-built binaries in bazel-bin/)")
    args = parser.parse_args()

    if not args.dry_run:
        if not args.skip_build:
            build_binaries()
        check_binaries()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_BASE.mkdir(parents=True, exist_ok=True)

    run_n_scaling(dry_run=args.dry_run)

    print_summary()
    print("\nDone. Results in:", RESULTS_DIR)
