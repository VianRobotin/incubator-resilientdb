#!/usr/bin/env python3
# CLI wrapper around das.run() for the rate-sweep repetitions orchestrator.
# Runs ONE point: (n, rate, rl) with no induced faults and prints a single-
# line JSON result on stdout.  Stderr carries the underlying script's chatter.
#
# Usage (from baselines-fairdag/scripts/deploy):
#   python3 performance/das_rate.py --n 10 --rate 500 --rl 0
#   python3 performance/das_rate.py --n 16 --rate 1000 --rl 1

import argparse
import json
import sys

from das import run


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, required=True, help="committee size N")
    p.add_argument("--rate", type=int, required=True, help="target tx/s")
    p.add_argument("--duration", type=int, default=60, help="run duration in seconds")
    p.add_argument("--client-num", type=int, default=None,
                   help="number of client machines (default: same as n)")
    p.add_argument("--rl", type=int, choices=[0, 1], default=0,
                   help="0 = OL/AB (fair_performance.sh), 1 = BOF/RL (fairrl_performance.sh)")
    p.add_argument("--system", choices=["fairdag", "tusk"], default="fairdag",
                   help="'fairdag' (OL/BOF via --rl) or 'tusk' (plain DAG-BFT; ignores --rl)")
    args = p.parse_args()

    client_num = args.client_num if args.client_num is not None else args.n
    result = run(
        amount_replicas=args.n,
        duration=args.duration,
        client_num=client_num,
        target_tps=args.rate,
        faults=0,
        rl=bool(args.rl),
        system=args.system,
    )
    if not isinstance(result, dict):
        # Older run() return path — couldn't parse results.log
        result = {"throughput": 0.0, "latency_ms": 0.0,
                  "consensus_throughput": 0.0, "consensus_latency_ms": 0.0}
    print("RESULT_JSON " + json.dumps(result))


if __name__ == "__main__":
    sys.exit(main())
