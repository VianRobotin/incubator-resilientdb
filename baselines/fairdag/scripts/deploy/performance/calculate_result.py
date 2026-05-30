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
    arrivals = []   # per-window request-arrival indicator ("txn:" field); marks
                    # the offered-load window, used as the throughput denominator

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
                    elif r.split(":")[0] == "txn":
                        arrivals.append(int(r.split(":")[1]))
                except Exception as e:
                    print(e)
                    print("s:", s)
            if l.find("req_client_latency") > 0:
                #print("get lat:", s)
                lat.append(float(s[-1].split(":")[-1]))
    #print("execs:", tps)
    #print("lat:", lat)
    return tps, lat, execute_latencies, commit_tps, arrivals


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
    if not tps_sum:
        return 0.0, 0.0
    return tps_max, sum(tps_sum) / len(tps_sum)


def sustained_tps(per_file_series, per_file_arrivals, red, time=5):
    """Sustained goodput = total events / OFFERED-LOAD span, per file, then the
    mean across files.

    The denominator is the arrival span -- the first to the last monitoring
    window with request arrivals ("txn:" > 0) -- NOT the execute/commit span.
    This matches Pearl, which divides total executed by the full runtime window
    (das.py::_compute_tps_latency): empty slots keep emitting up to runtime, so
    Pearl's denominator is the load duration and its goodput tracks the offered
    rate below saturation (500->495, 1000->999).

    Dividing by the EXECUTION span instead would report the drain rate: at low
    rate, fair ordering buffers arrivals and flushes them in a shorter burst, so
    execution finishes over fewer windows than it arrived -> throughput > input
    (500->783, 1000->1573). Using the arrival span recovers ~offered load. At
    saturation the two spans coincide, so the plateau is unchanged.

    Numerator is the total over the whole log (all eventually-executed work,
    including a short post-load drain) -- each replica executes/commits the full
    stream, so per-replica == system-wide and the mean across files is a
    representative replica view. red=1 for execution; the consensus redundancy
    divisor is supplied by the caller.
    """
    max_rate = 0.0
    file_rates = []
    for series, arrivals in zip(per_file_series, per_file_arrivals):
        active = [k for k, v in enumerate(series) if v > 0]
        if not active:
            # This log did no execution/commit work -- a client machine or an
            # idle/silent replica. Skip it so it doesn't dilute the mean (this
            # is what the original cal_tps did by skipping zero windows).
            continue
        load = [k for k, v in enumerate(arrivals) if v > 0]
        if not load:
            load = active   # no arrivals logged -> fall back to the work span
        span_windows = load[-1] - load[0] + 1
        total = sum(series)
        file_rates.append(total / (span_windows * time * red))
        for v in series:
            max_rate = max(max_rate, v / time / red)
    if not file_rates:
        return 0.0, 0.0
    return max_rate, sum(file_rates) / len(file_rates)


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
    if not lat_sum:
        return 0.0, 0.0
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

    exec_series = []     # per-file ordered execute counts (one list per replica log)
    commit_series = []   # per-file ordered commit_txn counts
    arrival_series = []  # per-file ordered "txn:" arrival indicators
    lat = []
    els = []
    total = len(files)
    for f in files:
        t, l, el, ct, ar = read_tps(f)
        exec_series.append(t)
        commit_series.append(ct)
        arrival_series.append(ar)
        lat += l
        els += el

    # Sustained goodput (total / offered-load span), consistent with Pearl. red=1
    # for execution; consensus keeps the historical /(N) redundancy divisor.
    max_tps, avg_tps = sustained_tps(exec_series, arrival_series, 1)
    max_lat, avg_lat = cal_lat(lat)
    # print(els)
    avg_clat = cal_clat(avg_lat, els)
    max_cput, avg_cput = sustained_tps(commit_series, arrival_series, len(files) / 2) # redundancy is amount of nodes, we have clients == nodes so /2

    print(
        f"max throughput: {max_tps} average throughput: {avg_tps} max latency: {max_lat} average latency: {avg_lat}\n"
        f"average consensus latency: {avg_clat} average consensus throughput: {avg_cput}"
    )
