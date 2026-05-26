#pyright: basic

import datetime
from re import findall
import socket
import subprocess
from preserve import *
import os

# preserve machines

# generate .conf

# iplist = (1 ip per line)
# client_num=X

username="gsegalin"

BENCHMARK_FILE = "./performance/fairrl_performance.sh"

CONF_FILENAME="./config/kv_performance_server_genned.conf"

def run(amount_replicas:int, duration:int, client_num:int):

    subprocess.call(["bazel", "build", "//service/kv:kv_service"], env=dict(os.environ, USE_BAZEL_VERSION="5.0.0"))

    results_file = f"fairdagrl-{amount_replicas}.txt"

    preserve_manager = PreserveManager(username)
    num_machines = amount_replicas + client_num # pompe runs 4 clients per machine
    time_string = str(datetime.timedelta(seconds=duration + 180)) # extra time to set up things
    reservation_id = preserve_manager.create_reservation(num_machines, time_string)
    time.sleep(5)
    reservations = preserve_manager.get_own_reservations()
    for v in reservations.values():
        # print(v)
        # should be exactly one
        hostnames = v.assigned_machines
        break

    replica_hosts = hostnames[:amount_replicas] # generate with preserve, these are replicas
    clients_hosts = hostnames[amount_replicas:] # get with preserve

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

    # this saves to results.log
    subprocess.call(["sh", BENCHMARK_FILE, CONF_FILENAME], env=dict(os.environ, USE_BAZEL_VERSION="5.0.0"))

    # search for avg_throughput
    with open("results.log", "r") as f:
        text = f.read()
        found_throughput = findall(r"average throughput: (\d+.?\d*)", text)
        throughput = float(found_throughput[0].replace(",", ""))
        found_latency = findall(r"average latency: (\d+.?\d*)", text)
        latency = float(found_latency[0].replace(",", ""))
        found_c_throughput = findall(r"average consensus throughput: (\d+.?\d*)", text)
        cthroughput = float(found_c_throughput[0].replace(",", ""))
        #found_c_latency = findall(r"average consensus latency: (\d+.?\d*)", text)
        clatency = -1.0 #float(found_c_latency[0].replace(",", ""))
    
    with open(os.path.join("results", results_file), "a") as f:
        f.write(f"Throughput: {round(throughput, 2)}\n")
        f.write(f"Latency: {round(latency * 1000, 2)}\n")
        f.write(f"Consensus throughput: {round(cthroughput, 2)}\n")
        f.write(f"Consensus latency: {round(clatency * 1000, 2)}\n")
    
    preserve_manager.kill_reservation("last")


if __name__ == "__main__":
    for nodes, duration, clients, rs in [(i, 60, i, 5) for i in range(5, 26)]:
        for _ in range(rs):
            run(nodes, duration, clients)

