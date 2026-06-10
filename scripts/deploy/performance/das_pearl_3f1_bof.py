#!/usr/bin/env python3
# Pearl BOF rate sweep at 3f+1 committee sizes (apples-to-apples N with the
# fairdag-bof / tusk / pompe / themis baselines, which all run at 3f+1).
#
# Pearl's claim is 2f+1 sufficiency, so the existing `tput_latency.csv` was
# produced at n ∈ {7, 11, 15} (2f+1 for f ∈ {3, 5, 7}). This wrapper instead
# runs Pearl BOF at n ∈ {10, 16, 22} (the matched 3f+1 sizes) so the rows can
# be plotted side-by-side with baselines at identical N.
#
# IMPORTANT: f must be PINNED to the baseline value (3/5/7), not left to the
# protocol's default f=(n-1)/2. Without pinning, Pearl at n=10 picks f=4 and a
# 2f+1=9-of-10 quorum — still its native near-unanimous regime, so it performs
# like the 2f+1 runs (the two curves overlapped, which is wrong). With f pinned
# to 3, n=10 is a genuine 3f+1 committee: commit quorum 2f+1=7-of-10, more nodes
# at the SAME fault tolerance → the expected degradation vs the 2f+1 committee.
# The pin is carried by the `fault_num` config field (consensus.cpp clamps it to
# [0,(n-1)/2]); f = (n-1)//3 gives 3/5/7 for n = 10/16/22.
#
# Schema mirrors `tput_latency.csv` verbatim so existing plotters can ingest
# it unchanged. Output goes to a SEPARATE CSV (`tput_latency_pearl_3f1.csv`)
# so the 2f+1 results stay untouched.
#
# Usage (from incubator-resilientdb/scripts/deploy/):
#   python3 performance/das_pearl_3f1_bof.py            # full sweep
#   python3 performance/das_pearl_3f1_bof.py --dry-run  # plan only
#   python3 performance/das_pearl_3f1_bof.py --nodes 10 # subset of N
#   python3 performance/das_pearl_3f1_bof.py --rates 500 2000  # subset of rates

import argparse
import sys
import time
from pathlib import Path

THIS_DIR    = Path(__file__).resolve().parent
PEARL_ROOT  = THIS_DIR.parents[2]
RESULTS_DIR = PEARL_ROOT / "das_results"
OUT_CSV     = RESULTS_DIR / "tput_latency_pearl_3f1.csv"

# 3f+1 committee sizes matched to baselines (f ∈ {3, 5, 7}).
N_VALUES = [10, 16, 22]

# Same 14-point ladder as the existing 2f+1 tput_latency.csv so rows are
# directly comparable.
RATES = [500, 1000, 2000, 4000, 6000, 8000, 10000, 12000,
         15000, 20000, 25000, 30000, 40000, 50000]

MODE        = "bof"
BLOCK_SIZE  = 100
RUNTIME     = 60
WARMUP      = 15
REPETITIONS = 5

# Cap load-generator clients so the reservation stays grantable on a busy DAS5.
# Replicas (n) are fixed; clients are just injectors and need not be 1-per-replica.
# Demand = n + min(n, MAX_CLIENTS): 17/23/29 for n=10/16/22 (vs 2n = 20/32/44 —
# 44 is never available while long-term reservations hold ~16 nodes). At 7 clients
# the peak per-client rate is 50000/7 ≈ 7.1k tx/s, the top of the range the 2f+1
# runs (num_clients=n, peak ~3.3k–7.1k) already validated, so clients stay below
# the bottleneck and throughput remains comparable.
MAX_CLIENTS = 7

HEADER = ("n,mode,input_rate,rep,"
          "consensus_tps,consensus_latency_ms,"
          "execution_tps,execution_latency_ms,"
          "slots_committed")


def init_csv(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(HEADER + "\n")
        return
    content = path.read_text()
    if not content.startswith(HEADER):
        path.write_text(HEADER + "\n" + content)


def load_completed(path: Path):
    """(n, mode, rate, rep) tuples already present with consensus_tps > 0."""
    done = set()
    if not path.exists():
        return done
    with open(path) as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 5:
                continue
            try:
                n, mode, rate, rep = int(parts[0]), parts[1], int(parts[2]), int(parts[3])
                if float(parts[4]) > 0:
                    done.add((n, mode, rate, rep))
            except (ValueError, IndexError):
                continue
    return done


def append_row(path: Path, row: str):
    with open(path, "a") as f:
        f.write(row + "\n")


def main():
    ap = argparse.ArgumentParser(
        description="Pearl BOF rate sweep at 3f+1 (n ∈ {10, 16, 22})")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print plan without reserving machines or running SSH")
    ap.add_argument("--nodes", type=int, nargs="+",
                    help="Subset of N to run (default: 10 16 22)")
    ap.add_argument("--rates", type=int, nargs="+",
                    help="Subset of input rates to run")
    ap.add_argument("--reps", type=int, default=REPETITIONS,
                    help=f"Repetitions per (n, rate) point (default: {REPETITIONS})")
    ap.add_argument("--max-clients", type=int, default=MAX_CLIENTS,
                    help=f"Cap on load-generator clients; num_clients=min(n, cap) "
                         f"(default: {MAX_CLIENTS}). Lower => fewer machines but "
                         f"higher per-client injection rate.")
    args = ap.parse_args()

    # Import das.py from the same directory.
    sys.path.insert(0, str(THIS_DIR))
    from das import run_experiment, check_binaries  # type: ignore

    if not args.dry_run:
        check_binaries()

    n_values = [n for n in N_VALUES if (not args.nodes or n in args.nodes)]
    rates    = [r for r in RATES    if (not args.rates or r in args.rates)]

    init_csv(OUT_CSV)
    done = load_completed(OUT_CSV)
    if done:
        print(f"Resuming: {len(done)} run(s) already complete in {OUT_CSV.name}; will skip.")

    total = len(n_values) * len(rates) * args.reps
    print(f"\n=== Pearl BOF 3f+1 rate sweep ===")
    print(f"  N values : {n_values}")
    print(f"  rates    : {rates}")
    print(f"  reps     : {args.reps}")
    print(f"  total    : {total} runs")
    print(f"  CSV      : {OUT_CSV}")

    failures = []
    for n in n_values:
        # Pin f to the 3f+1 fault tolerance (f=3/5/7 for n=10/16/22) so this is a
        # real 3f+1 committee, not Pearl's default f=(n-1)/2 near-unanimous regime.
        f_pin = (n - 1) // 3
        n_clients = min(n, args.max_clients)
        for rate in rates:
            for rep in range(1, args.reps + 1):
                if (n, MODE, rate, rep) in done:
                    print(f"  skip n={n} mode={MODE} rate={rate} rep={rep} (done)")
                    continue
                print(f"\n--- n={n} (f={f_pin}, 3f+1)  mode={MODE}  "
                      f"rate={rate}  rep={rep}/{args.reps}  "
                      f"clients={n_clients} (machines={n + n_clients}) ---")
                # One slow/short DAS5 reservation (e.g. 2n=44 machines at n=22 not
                # granted within the wait) raises from run_experiment. Catch it so a
                # single bad point is logged and skipped instead of unwinding the
                # whole sweep — load_completed() lets a later rerun fill the gap.
                try:
                    con_tps, con_lat_s, exec_tps, exec_lat_s, slots = run_experiment(
                        num_replicas=n,
                        mode=MODE,
                        block_size=BLOCK_SIZE,
                        runtime=RUNTIME,
                        warmup=WARMUP,
                        dry_run=args.dry_run,
                        num_clients=n_clients,   # capped (see MAX_CLIENTS)
                        target_tps=rate,
                        silent_faults=0,
                        fault_num=f_pin,
                    )
                except Exception as e:  # noqa: BLE001 — keep the sweep alive
                    failures.append((n, rate, rep, str(e)))
                    print(f"  !! FAILED n={n} rate={rate} rep={rep}: {e} "
                          f"— skipping (rerun to retry)")
                    continue
                if args.dry_run:
                    continue  # planning only — never write placeholder rows
                append_row(OUT_CSV,
                    f"{n},{MODE},{rate},{rep},"
                    f"{con_tps},{round(con_lat_s * 1000, 2)},"
                    f"{exec_tps},{round(exec_lat_s * 1000, 2)},"
                    f"{slots}")
                time.sleep(10)

    print(f"\nDone. Results in: {OUT_CSV}")
    if failures:
        print(f"\n{len(failures)} point(s) failed and were skipped "
              f"(rerun to retry):")
        for n, rate, rep, err in failures:
            print(f"  n={n} rate={rate} rep={rep}: {err}")


if __name__ == "__main__":
    main()
