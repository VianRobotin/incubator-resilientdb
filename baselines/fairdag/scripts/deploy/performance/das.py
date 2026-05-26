# pyright: basic

import datetime
from re import findall
import socket
import subprocess
from preserve import *
import os
import json
from typing import Optional

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


def set_input_rate_config(template_path: str, target_tps: int, client_num: int) -> int:
    if target_tps <= 0:
        raise ValueError("target_tps must be > 0")
    per_client_tps = max(1, int(round(target_tps / client_num)))
    with open(template_path, "r") as f:
        config = json.load(f)
    config["clientBatchNum"] = 1
    config["client_batch_wait_time_us"] = 0
    config["target_input_tps"] = per_client_tps
    with open(template_path, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")
    return per_client_tps


def run(
    amount_replicas: int,
    duration: int,
    client_num: int,
    target_tps: Optional[int] = None,
    faults: int = 0,
    rl: Optional[bool] = None,
    block_size: Optional[int] = None,
):
    # Resolve mode (rl flag) and the bash entry point + config template.
    use_rl = RUN_RL if rl is None else rl
    benchmark_file = (
        "./performance/fairrl_performance.sh"
        if use_rl else "./performance/fair_performance.sh"
    )
    template_config_path = (
        "./config/fairrl.config" if use_rl else "./config/fair.config"
    )

    subprocess.call(
        ["bazel", "build", "//service/kv:kv_service"],
        env=dict(os.environ, USE_BAZEL_VERSION="5.0.0"),
    )

    fault_tag = f"-f{faults}" if faults else ""
    results_file = (
        f"fairdagrl-{amount_replicas}{fault_tag}.txt"
        if use_rl else f"fairdag-{amount_replicas}{fault_tag}.txt"
    )

    preserve_manager = PreserveManager(username)
    try:
        preserve_manager.kill_reservation("last")
    except:
        print("No last reservation")
    num_machines = amount_replicas + client_num  # pompe runs 4 clients per machine
    time_string = str(
        datetime.timedelta(seconds=duration + 60)
    )  # extra time to set up things
    reservation_id = preserve_manager.create_reservation(num_machines, time_string)
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

    replica_hosts = hostnames[
        :amount_replicas
    ]  # generate with preserve, these are replicas
    clients_hosts = hostnames[amount_replicas:]  # get with preserve

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

    if target_tps is not None:
        per_client_tps = set_input_rate_config(template_config_path, target_tps, client_num)
        print(
            f"target_tps={target_tps} client_num={client_num} per_client_tps={per_client_tps}"
        )

    sub_env = dict(os.environ, USE_BAZEL_VERSION="5.0.0")
    if faults > 0:
        sub_env["FAULTS"] = str(faults)
        print(f"FAULTS={faults}: silencing last {faults} replica(s)")
    if block_size is not None and block_size > 0:
        sub_env["FAIRDAG_BLOCK_SIZE"] = str(block_size)
        print(f"FAIRDAG_BLOCK_SIZE={block_size}")

    # this saves to results.log
    subprocess.call(["sh", benchmark_file, CONF_FILENAME], env=sub_env)

    # search for avg_throughput
    with open("results.log", "r") as f:
        text = f.read()
        found_throughput = findall(r"average throughput: (\d+.?\d*)", text)
        throughput = float(found_throughput[0].replace(",", ""))
        found_latency = findall(r"average latency: (\d+.?\d*)", text)
        latency = float(found_latency[0].replace(",", ""))
        found_c_throughput = findall(r"average consensus throughput: (\d+.?\d*)", text)
        cthroughput = float(found_c_throughput[0].replace(",", ""))
        found_c_latency = findall(r"average consensus latency: (\d+.?\d*)", text)
        clatency = float(found_c_latency[0].replace(",", ""))

    with open(os.path.join("results", results_file), "a") as f:
        if target_tps:
            f.write(f"Input Rate (tx/s): {target_tps}\n")
        if faults:
            f.write(f"Faults: {faults}\n")
        f.write(f"Throughput: {round(throughput, 2)}\n")
        f.write(f"Latency: {round(latency * 1000, 2)}\n")
        f.write(f"Consensus throughput: {round(cthroughput, 2)}\n")
        f.write(f"Consensus latency: {round(clatency * 1000, 2)}\n")

    try:
        preserve_manager.kill_reservation("last")
    except Exception:
        pass
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
