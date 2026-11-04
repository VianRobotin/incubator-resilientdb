# pyright: basic

import datetime
from re import findall
import socket
import subprocess
from preserve import *
import os
import json
from typing import Optional
import glob
import importlib.util

# Pearl-consistent metrics (scripts/deploy/performance/baseline_metrics.py in
# the Pearl repo), loaded by path: this tree has its own `das` module.
_spec = importlib.util.spec_from_file_location(
    "baseline_metrics",
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 "../../../../../scripts/deploy/performance/baseline_metrics.py"))
baseline_metrics = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(baseline_metrics)

# preserve machines

# generate .conf

# iplist = (1 ip per line)
# client_num=X

RUN_RL = True

username = os.environ.get("USER", "vrobotin")

# Default values used by the historical sweep at the bottom of this file.
# When called from the faulty-node orchestrator, run() picks the script and
# template from its `rl` argument so OL and BOF can be driven by one process.
BENCHMARK_FILE = "./performance/fair_performance.sh" if not RUN_RL else "./performance/fairrl_performance.sh"

CONF_FILENAME = "./config/kv_performance_server_genned.conf"
TEMPLATE_CONFIG_PATH = "./config/fair.config" if not RUN_RL else "./config/fairrl.config"


# Nodes with a broken /local disk (processes cannot write their logs there):
# node067 gave "Input/output error" on 2026-10-31.  Reserve extra, drop these.
BANNED_NODES = ["node067"]

BOF_CLIENT_BATCH = 100  # max transactions per FairDAG-BOF client request


def set_input_rate_config(template_path: str, target_tps: int, client_num: int):
    """Write the client rate into the config template; returns
    (per-client tx/s, transactions per client request)."""
    if target_tps <= 0:
        raise ValueError("target_tps must be > 0")
    per_client_tps = max(1, int(round(target_tps / client_num)))
    batch = 1
    if os.path.basename(template_path) == "fairrl.config":
        # FairDAG-BOF compares every pair of pending client requests each round
        # (AddImplictWeight, O(n * pending^2)), so with one transaction per
        # request it collapses even at 500 tx/s.  Upstream FairDAG batches 100
        # transactions per request; do the same, choosing the batch so the
        # client sends a whole number of batches per second
        # (target_input_tps counts batches and is an integer).
        batches_per_s = -(-per_client_tps // BOF_CLIENT_BATCH)  # ceil
        batch = max(1, round(per_client_tps / batches_per_s))
    with open(template_path, "r") as f:
        config = json.load(f)
    config["clientBatchNum"] = batch
    config["client_batch_wait_time_us"] = 0
    config["target_input_tps"] = max(1, round(per_client_tps / batch))
    with open(template_path, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")
    return per_client_tps, batch


def run(
    amount_replicas: int,
    duration: int,
    client_num: int,
    target_tps: Optional[int] = None,
    faults: int = 0,
    rl: Optional[bool] = None,
    block_size: Optional[int] = None,
    system: str = "fairdag",
):
    if system == "tusk":
        benchmark_file = "./performance/tusk_performance.sh"
        template_config_path = "./config/tusk.config"
        use_rl = False
    elif system == "fairdag":
        # Resolve mode (rl flag) and the bash entry point + config template.
        use_rl = RUN_RL if rl is None else rl
        benchmark_file = (
            "./performance/fairrl_performance.sh"
            if use_rl else "./performance/fair_performance.sh"
        )
        template_config_path = (
            "./config/fairrl.config" if use_rl else "./config/fair.config"
        )
    else:
        raise ValueError(f"unknown system: {system!r} (expected 'fairdag' or 'tusk')")

    subprocess.call(
        ["bazel", "build", "//service/kv:kv_service"],
        env=dict(os.environ, USE_BAZEL_VERSION="5.0.0"),
    )

    fault_tag = f"-f{faults}" if faults else ""
    if system == "tusk":
        results_file = f"tusk-{amount_replicas}{fault_tag}.txt"
    elif use_rl:
        results_file = f"fairdagrl-{amount_replicas}{fault_tag}.txt"
    else:
        results_file = f"fairdag-{amount_replicas}{fault_tag}.txt"

    if client_num > amount_replicas:
        raise ValueError(f"client_num={client_num} > n={amount_replicas}: clients "
                         f"are co-located, one per replica machine")
    preserve_manager = PreserveManager(username)
    # One machine per replica; client j runs on replica j's machine (paper §8.1).
    num_machines = amount_replicas + len(BANNED_NODES)
    time_string = str(
        datetime.timedelta(seconds=duration + 60)
    )  # extra time to set up things
    reservation_id = preserve_manager.create_reservation(num_machines, time_string)
    try:
        return _run_reserved(preserve_manager, reservation_id, amount_replicas,
                             client_num, num_machines, target_tps, faults,
                             block_size, template_config_path, benchmark_file,
                             results_file)
    finally:
        # Release only our own reservation: "last" could be another sweep's.
        try:
            preserve_manager.kill_reservation(str(reservation_id))
        except Exception as e:
            print(f"WARN: could not release reservation {reservation_id}: {e}")


def _run_reserved(preserve_manager, reservation_id, amount_replicas, client_num,
                  num_machines, target_tps, faults, block_size,
                  template_config_path, benchmark_file, results_file):
    time.sleep(5)
    deadline = time.time() + 900
    reservations = preserve_manager.get_own_reservations()
    while reservation_id not in reservations:
        if time.time() > deadline:
            raise RuntimeError(
                f"reservation {reservation_id} never appeared in preserve -llist "
                f"(likely expired before assignment); aborting"
            )
        reservations = preserve_manager.get_own_reservations()
        print(f"Can't find res_id {reservation_id} in reservations")
        time.sleep(1)

    v = reservations[reservation_id]
    hostnames = v.assigned_machines
    while len(hostnames) < num_machines:
        if time.time() > deadline:
            raise RuntimeError(
                f"reservation {reservation_id} only assigned {len(hostnames)}/"
                f"{num_machines} machines before deadline; aborting"
            )
        reservations = preserve_manager.get_own_reservations()
        if reservation_id not in reservations:
            raise RuntimeError(
                f"reservation {reservation_id} disappeared while waiting for hosts"
            )
        v = reservations[reservation_id]
        hostnames = v.assigned_machines
        time.sleep(10)

    usable = [h for h in hostnames if h not in BANNED_NODES]
    if len(usable) < amount_replicas:
        raise RuntimeError(f"only {len(usable)} usable machines after removing "
                           f"banned nodes, need {amount_replicas}")
    replica_hosts = usable[:amount_replicas]
    clients_hosts = replica_hosts[:client_num]  # co-located with replicas 1..client_num

    replica_ips = [socket.gethostbyname(h) for h in replica_hosts]
    clients_ips = [socket.gethostbyname(h) for h in clients_hosts]

    to_write = "iplist=("

    for ip in replica_ips + clients_ips:
        to_write += f"\n  {ip}"

    to_write += "\n)\n"
    to_write += f"client_num={client_num}"
    print(to_write)

    with open(CONF_FILENAME, "w") as f:
        f.write(to_write)

    client_batch = 1
    if target_tps is not None:
        per_client_tps, client_batch = set_input_rate_config(
            template_config_path, target_tps, client_num)
        print(
            f"target_tps={target_tps} client_num={client_num} per_client_tps={per_client_tps}"
            f" client_batch={client_batch}"
        )

    sub_env = dict(os.environ, USE_BAZEL_VERSION="5.0.0")
    if faults > 0:
        sub_env["FAULTS"] = str(faults)
        print(f"FAULTS={faults}: silencing last {faults} replica(s)")
    # Tusk fills a block with FAIRDAG_BLOCK_SIZE client requests and has no
    # timeout, so with batched requests (BOF) a block of 100 requests would
    # take 100x the load to fill.  Keep blocks at ~block_size transactions
    # (default 100, as for every system) by sizing them in requests.
    block_txns = block_size if block_size is not None and block_size > 0 else 100
    if block_size is not None and block_size > 0 or client_batch > 1:
        block_reqs = max(1, round(block_txns / client_batch))
        sub_env["FAIRDAG_BLOCK_SIZE"] = str(block_reqs)
        print(f"FAIRDAG_BLOCK_SIZE={block_reqs} requests (~{block_reqs * client_batch} tx)")

    # Logs of an earlier run must not stand in for a replica that failed.
    for f in glob.glob("result_*_log"):
        os.remove(f)

    # this copies replica/client i's log to result_i_log
    subprocess.call(["sh", benchmark_file, CONF_FILENAME], env=sub_env)

    # Execution throughput and latency computed as for Pearl: from the
    # replicas' executed_batch lines (client arrival at the replica -> commit
    # in the final order), not from calculate_result.py's monitor windows.
    # The consensus columns mirror them.
    m = baseline_metrics.compute(
        [f"result_{i}_log" for i in range(1, amount_replicas + 1)])
    print(f"baseline_metrics: {m}")
    throughput = m["execution_tps"]
    latency = m["execution_latency_ms"] / 1000
    cthroughput, clatency = throughput, latency

    os.makedirs("results", exist_ok=True)
    with open(os.path.join("results", results_file), "a") as f:
        if target_tps:
            f.write(f"Input Rate (tx/s): {target_tps}\n")
        if faults:
            f.write(f"Faults: {faults}\n")
        f.write(f"Throughput: {round(throughput, 2)}\n")
        f.write(f"Latency: {round(latency * 1000, 2)}\n")
        f.write(f"Consensus throughput: {round(cthroughput, 2)}\n")
        f.write(f"Consensus latency: {round(clatency * 1000, 2)}\n")

    return {
        "throughput": throughput,
        "latency_ms": latency * 1000,
        "consensus_throughput": cthroughput,
        "consensus_latency_ms": clatency * 1000,
    }


if __name__ == "__main__":
    for nodes, duration, clients, rs, tps in [(i, 60, i, 5, 350) for i in range(10, 26, 3)]:
        for _ in range(rs):
            run(nodes, duration, clients, tps)
