#!/usr/bin/env python3
# Additional rate-sweep repetitions for the baseline systems.
#
# The existing das_results/baselines/{fairdag-ol,fairdag-bof,pompe,themis}.csv
# CSVs each hold rep=1 for the full (n, input_rate) grid. This orchestrator
# adds further reps (default 2..5) over the same grid with no induced
# faults, appending rows to those CSVs verbatim (same 11-column schema).
#
# Per-point entry points match what produced rep=1:
#   fairdag-ol/bof -> baselines-fairdag/scripts/deploy/performance/das_rate.py
#                     (thin CLI around das.run(faults=0))
#   pompe/themis   -> fab of-rate --flavor=... --n=... --rate=... (new single-
#                     point task that mirrors the inner body of fab `of`)
#
# Usage (from incubator-resilientdb/scripts/deploy/):
#   python3 performance/das_baselines_reps.py                 # all systems, reps 2..5
#   python3 performance/das_baselines_reps.py --systems pompe themis
#   python3 performance/das_baselines_reps.py --reps 2        # just rep 2
#   python3 performance/das_baselines_reps.py --n 10 --rates 500 1000
#   python3 performance/das_baselines_reps.py --dry-run

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

THIS_DIR    = Path(__file__).resolve().parent
PEARL_ROOT  = THIS_DIR.parents[2]
SCRATCH     = PEARL_ROOT.parent

FAIRDAG_ROOT  = SCRATCH / "baselines-fairdag"
NARWHAL_ROOT  = SCRATCH / "narwhal"

FAIRDAG_DEPLOY = FAIRDAG_ROOT / "scripts" / "deploy"
NARWHAL_BENCH  = NARWHAL_ROOT / "benchmark"

RESULTS_DIR = PEARL_ROOT / "das_results" / "baselines"

CONDA_PROFILE         = SCRATCH / "miniconda3" / "etc" / "profile.d" / "conda.sh"
NARWHAL_CONDA_PROFILE = NARWHAL_ROOT / "miniconda" / "etc" / "profile.d" / "conda.sh"
CONDA_ENV = "workenv"

# ---------------------------------------------------------------------------
# Per-system sweep grids (read off the existing baseline CSVs).
# ---------------------------------------------------------------------------

N_GRID = {
    "fairdag-ol":  [10, 16, 22],
    "fairdag-bof": [10, 16, 22],
    "pompe":       [10, 16, 22],
    "themis":      [13, 21, 29],
}

# Protocol max fault tolerance at each n (matches the `f` column in the
# existing baseline CSVs).  FairDAG and pompe are 3f+1; themis is 4f+1.
F_FOR_N = {
    "fairdag-ol":  {10: 3, 16: 5, 22: 7},
    "fairdag-bof": {10: 3, 16: 5, 22: 7},
    "pompe":       {10: 3, 16: 5, 22: 7},
    "themis":      {13: 3, 21: 5, 29: 7},
}

SYSTEM_MODE = {
    "fairdag-ol":  "ol",
    "fairdag-bof": "bof",
    "pompe":       "ol",
    "themis":      "bof",
}

RATE_GRID = [500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000]

# Existing baseline CSV schema (verbatim).  Trailing slots_committed column
# is left empty per the existing files' convention.
CSV_HEADER = ("baseline,n,f,mode,input_rate,rep,"
              "consensus_tps,consensus_latency_ms,"
              "execution_tps,execution_latency_ms,slots_committed")

# ---------------------------------------------------------------------------
# CSV utilities
# ---------------------------------------------------------------------------

def csv_path_for(system: str) -> Path:
    return RESULTS_DIR / f"{system}.csv"


def init_csv(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(CSV_HEADER + "\n")


def load_completed(path: Path) -> set:
    """Set of (n, mode, input_rate, rep) already recorded with execution_tps>0.

    Column layout:
        baseline, n, f, mode, input_rate, rep,
        c_tps, c_lat_ms, e_tps, e_lat_ms, slots_committed
            0   1  2   3     4         5    6     7        8     9       10
    """
    if not path.exists():
        return set()
    done = set()
    with open(path) as fh:
        next(fh, None)
        for line in fh:
            parts = line.strip().split(",")
            if len(parts) < 10:
                continue
            try:
                e_tps = float(parts[8])
                key = (int(parts[1]), parts[3].strip(),
                       int(parts[4]), int(parts[5]))
            except ValueError:
                continue
            if e_tps > 0:
                done.add(key)
    return done


def append_row(path: Path, system: str, n: int, rep: int, input_rate: int,
               exec_tps: float, exec_lat_ms: float,
               c_tps: float, c_lat_ms: float,
               mode: Optional[str] = None):
    if mode is None:
        mode = SYSTEM_MODE[system]
    f_max = F_FOR_N[system][n]
    # Trailing comma leaves slots_committed empty (matches existing rows).
    row = (f"{system},{n},{f_max},{mode},{input_rate},{rep},"
           f"{c_tps:.4f},{c_lat_ms:.4f},"
           f"{exec_tps:.4f},{exec_lat_ms:.4f},")
    with open(path, "a") as fh:
        fh.write(row + "\n")


# ---------------------------------------------------------------------------
# Runners — each returns (exec_tps, exec_lat_ms, c_tps, c_lat_ms).
# A return of all zeros signals failure; the row is still written so the
# orchestrator can be re-run to retry (dedup keys off tps>0).
# ---------------------------------------------------------------------------

def run_fairdag(rl: bool, n: int, rate: int, dry_run: bool):
    cwd = FAIRDAG_DEPLOY
    cmd = [
        sys.executable, "performance/das_rate.py",
        "--n", str(n), "--rate", str(rate),
        "--rl", "1" if rl else "0",
        "--duration", "300",
    ]
    if dry_run:
        print(f"[DRY-RUN] cd {cwd} && {' '.join(cmd)}")
        return 0.0, 0.0, 0.0, 0.0
    print(f"\n[fairdag] cd {cwd}")
    print(f"[fairdag] {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(cwd),
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print(f"[fairdag] non-zero exit ({proc.returncode}); recording zero")
        return 0.0, 0.0, 0.0, 0.0
    m = re.search(r"^RESULT_JSON (\{.*\})$", proc.stdout, re.MULTILINE)
    if not m:
        print("[fairdag] no RESULT_JSON line found; recording zero")
        return 0.0, 0.0, 0.0, 0.0
    try:
        d = json.loads(m.group(1))
    except json.JSONDecodeError:
        return 0.0, 0.0, 0.0, 0.0
    return (float(d.get("throughput", 0)),
            float(d.get("latency_ms", 0)),
            float(d.get("consensus_throughput", 0)),
            float(d.get("consensus_latency_ms", 0)))


def run_narwhal(flavor: str, n: int, rate: int, dry_run: bool):
    """Call `fab of-rate --flavor=... --n=... --rate=...` and parse the
    single new line appended to local-0-0-0-{wks}-{n}.txt.
    """
    workers = 1
    result_file = NARWHAL_BENCH / "results" / f"local-0-0-0-{workers}-{n}.txt"
    pre_lines = result_file.read_text().splitlines() if result_file.exists() else []

    conda_profile = NARWHAL_CONDA_PROFILE if NARWHAL_CONDA_PROFILE.exists() else CONDA_PROFILE
    shell_cmd = (
        f"source {conda_profile} && conda activate {CONDA_ENV} && "
        f"fab of-rate --flavor={flavor} -n={n} --rate={rate} --runs=1"
    )
    if dry_run:
        print(f"[DRY-RUN] cd {NARWHAL_BENCH} && bash -lc \"{shell_cmd}\"")
        return 0.0, 0.0, 0.0, 0.0
    print(f"\n[narwhal/{flavor}] cd {NARWHAL_BENCH}")
    print(f"[narwhal/{flavor}] {shell_cmd}")
    proc = subprocess.run(["bash", "-lc", shell_cmd], cwd=str(NARWHAL_BENCH),
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print(f"[narwhal/{flavor}] non-zero exit ({proc.returncode}); recording zero")
        return 0.0, 0.0, 0.0, 0.0

    if not result_file.exists():
        print(f"[narwhal/{flavor}] result file missing: {result_file}")
        return 0.0, 0.0, 0.0, 0.0

    data_line = None
    for ln in result_file.read_text().splitlines()[len(pre_lines):]:
        s = ln.strip()
        if not s or s.startswith("#") or s.startswith("ADVERSARIAL"):
            continue
        toks = s.split()
        if len(toks) < 3:
            continue
        try:
            r = float(toks[0])
            tps_v = float(toks[1])
            lat_v = float(toks[2])
        except ValueError:
            continue
        if int(r) != int(rate):
            continue
        data_line = (tps_v, lat_v)
        break
    if data_line is None:
        print(f"[narwhal/{flavor}] no new result line in {result_file}")
        return 0.0, 0.0, 0.0, 0.0
    tps, lat_ms = data_line
    # Pompe TPS is reported inflated 100x by narwhal.
    if flavor == "pompe":
        tps = tps / 100.0
    # Single-stage protocols: execution and consensus columns mirror.
    return tps, lat_ms, tps, lat_ms


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_one(system: str, n: int, rate: int, dry_run: bool):
    print(f"\n=========================")
    print(f" {system}  n={n}  rate={rate}")
    print(f"=========================")
    if system == "fairdag-ol":
        return run_fairdag(rl=False, n=n, rate=rate, dry_run=dry_run)
    if system == "fairdag-bof":
        return run_fairdag(rl=True,  n=n, rate=rate, dry_run=dry_run)
    if system == "pompe":
        return run_narwhal("pompe",  n, rate, dry_run)
    if system == "themis":
        return run_narwhal("themis", n, rate, dry_run)
    raise ValueError(f"unknown system: {system}")


def main():
    p = argparse.ArgumentParser(description="Add rate-sweep reps to baseline CSVs")
    p.add_argument("--systems", nargs="+", choices=list(N_GRID.keys()),
                   default=list(N_GRID.keys()),
                   help="subset of systems to run (default: all baselines)")
    p.add_argument("--n", type=int, nargs="+", default=None,
                   help="subset of n values (default: per-system grid)")
    p.add_argument("--rates", type=int, nargs="+", default=RATE_GRID,
                   help=f"input rates to sweep (default: {RATE_GRID})")
    p.add_argument("--reps", type=int, nargs="+", default=[2, 3, 4, 5],
                   help="rep numbers to record (default: 2 3 4 5)")
    p.add_argument("--dry-run", action="store_true",
                   help="print plan, don't reserve or run")
    args = p.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Iterate rep > system > n > rate so an early abort still leaves whole
    # reps completed across systems rather than partial reps everywhere.
    for rep in args.reps:
        for system in args.systems:
            path = csv_path_for(system)
            init_csv(path)
            completed = load_completed(path)
            mode = SYSTEM_MODE[system]
            n_list = args.n if args.n is not None else N_GRID[system]
            for n in n_list:
                if n not in F_FOR_N[system]:
                    print(f"  skip {system} n={n} (no f-mapping)")
                    continue
                for rate in args.rates:
                    if (n, mode, rate, rep) in completed:
                        print(f"  skip {system} n={n} mode={mode} rate={rate} rep={rep} (done)")
                        continue
                    t0 = time.time()
                    try:
                        exec_tps, exec_lat_ms, c_tps, c_lat_ms = run_one(
                            system, n, rate, args.dry_run)
                    except Exception as e:
                        print(f"  ERROR {system} n={n} rate={rate} rep={rep}: {e}")
                        exec_tps = exec_lat_ms = c_tps = c_lat_ms = 0.0
                    if not args.dry_run:
                        append_row(path, system, n, rep, rate,
                                   exec_tps, exec_lat_ms, c_tps, c_lat_ms,
                                   mode=mode)
                        print(f"  {system} n={n} mode={mode} rate={rate} rep={rep} -> "
                              f"exec_tps={exec_tps:.1f} exec_lat={exec_lat_ms:.1f}ms "
                              f"({time.time() - t0:.0f}s)")
                        time.sleep(10)

    print("\nDone. Results in:", RESULTS_DIR)


if __name__ == "__main__":
    main()
