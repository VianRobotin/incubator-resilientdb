# pyright: basic
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from math import isnan
import sys

total = 0


def read_tps(file):
    tps = []
    lat = []
    execute_latencies = []
    commit_tps = []

    with open(file) as f:
        for l in f.readlines():
            s = l.split()
            for r in s:
                try:
                    if r.split(":")[0] == "execute":
                        tps.append(int(r.split(":")[1]))
                    elif r.split(":")[0] == "execute_delay_latency":
                        execute_latencies.append(float(r.split(":")[1]))
                    elif r.split(":")[0] == "commit_txn":
                        commit_tps.append(int(r.split(":")[1]))
                except Exception as e:
                    print(e)
                    print("s:", s)
            if l.find("req_client_latency") > 0:
                #print("get lat:", s)
                lat.append(float(s[-1].split(":")[-1]))
    #print("execs:", tps)
    #print("lat:", lat)
    return tps, lat, execute_latencies, commit_tps


def cal_tps(tps, red, time=5):
    tps_sum = []
    tps_max = 0
    #print(tps)

    for v in tps:
        if v == 0:
            continue

        thr = v / time / red # it was not divided in the stats.cpp output, divide by redundancy too
        tps_max = max(tps_max, thr)
        tps_sum.append(thr)


    #print("tps:", tps_sum)
    # print("max throughput:",tps_max)
    # print("average throughput:",sum(tps_sum)/len(tps_sum))
    return tps_max, sum(tps_sum) / len(tps_sum)


def cal_lat(lat):
    lat_sum = []
    lat_max = 0
    
    for v in lat:
        if v <= 0 or isnan(v):
            continue
        lat_max = max(lat_max, v)
        lat_sum.append(v)

    #print("max latency:", lat_max)
    #print("average latency:", sum(lat_sum) / len(lat_sum))
    return lat_max, sum(lat_sum) / len(lat_sum)


def cal_clat(avg_lat: float, els: list) -> float:
    lat_sum = []
    for v in els:
        if v <= 0 or isnan(v):
            continue
        lat_sum.append(v)
    
    if len(lat_sum) == 0:
        return 0.0

    #print("average execution delay:", sum(lat_sum) / len(lat_sum))
    return max(0.0, avg_lat - sum(lat_sum) / len(lat_sum))


if __name__ == "__main__":
    files = sys.argv[1:]
    #print("calculate results, number of nodes:", len(files))

    tps = []
    lat = []
    els = []
    ctps = []
    total = len(files)
    for f in files:
        t, l, el, ct = read_tps(f)
        tps += t
        lat += l
        els += el
        ctps += ct

    max_tps, avg_tps = cal_tps(tps, 1)
    max_lat, avg_lat = cal_lat(lat)
    # print(els)
    avg_clat = cal_clat(avg_lat, els)
    max_cput, avg_cput = cal_tps(ctps, len(files) / 2) # redundancy is amount of nodes, we have clients == nodes so /2

    print(
        f"max throughput: {max_tps} average throughput: {avg_tps} max latency: {max_lat} average latency: {avg_lat}\n"
        f"average consensus latency: {avg_clat} average consensus throughput: {avg_cput}"
    )
