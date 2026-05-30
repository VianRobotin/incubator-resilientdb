#!/usr/bin/env python3
# Faulty-node sweep across all systems for the thesis paper.
#
# Holds n fixed per protocol (smallest committee that tolerates f=5 silent
# peers), holds rate fixed at 500 tx/s, sweeps faults f in {1..5} with one
# rep each.  Writes one CSV per system into das_results/faulty/ and never
# touches the existing CSVs/plots.
#
# Usage (from incubator-resilientdb/scripts/deploy/):
#   python3 performance/das_faulty.py                 # full sweep, all systems
#   python3 performance/das_faulty.py --systems pearl # subset
#   python3 performance/das_faulty.py --faults 1 3 5  # subset of f values
#   python3 performance/das_faulty.py --dry-run

import argparse
import csv
import datetime
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

THIS_DIR    = Path(__file__).resolve().parent
PEARL_ROOT  = THIS_DIR.parents[2]                       # incubator-resilientdb
SCRATCH     = PEARL_ROOT.parent                          # /var/scratch/vrobotin

# Baselines now live inside the repo (incubator-resilientdb/baselines/) so
# their changes can be committed alongside Pearl. The old out-of-repo copies
# at SCRATCH/{baselines-fairdag,narwhal} are retired.
FAIRDAG_ROOT  = PEARL_ROOT / "baselines" / "fairdag"
NARWHAL_ROOT  = PEARL_ROOT / "baselines" / "narwhal"

FAIRDAG_DEPLOY = FAIRDAG_ROOT / "scripts" / "deploy"
NARWHAL_BENCH  = NARWHAL_ROOT / "benchmark"

RESULTS_DIR = PEARL_ROOT / "das_results" / "faulty"
RUNTIME_BASE = PEARL_ROOT / "das_runtime" / "faulty"

# Conda activation snippet for narwhal (workenv environment).
CONDA_PROFILE = SCRATCH / "miniconda3" / "etc" / "profile.d" / "conda.sh"
NARWHAL_CONDA_PROFILE = NARWHAL_ROOT / "miniconda" / "etc" / "profile.d" / "conda.sh"
CONDA_ENV = "workenv"

# ---------------------------------------------------------------------------
# Per-system fixed config
# ---------------------------------------------------------------------------
#
# Smallest committee size that tolerates f=5 silent peers per protocol's
# fault-tolerance threshold:
#   Pearl:    2f+1  -> N=11 (f_t=5)
#   FairDAG:  3f+1  -> N=16 (f_t=5)
#   Pompe:    3f+1  -> N=16 (f_t=5)
#   Themis:   4f+1  -> N=21 (f_t=5)
SYSTEMS = {
    "pearl":       {"n": 11, "rate": 500},
    "fairdag-ol":  {"n": 16, "rate": 500},
    "fairdag-bof": {"n": 16, "rate": 500},
    "tusk":        {"n": 16, "rate": 500},   # 3f+1, same as FairDAG
    "pompe":       {"n": 16, "rate": 500},
    "themis":      {"n": 21, "rate": 500},
}

# Per-system CSV schema is the existing result CSV's schema VERBATIM, with
# a `silent_faults` column appended.  In the existing baseline CSVs, the
# `f` column is the protocol's max fault tolerance (a function of n, fixed
# for a given protocol+n); the swept variable in this experiment is the
# number of silent replicas injected, recorded separately in
# `silent_faults`.  Pearl's tput_latency.csv has no `f` column, so its
# faulty CSV likewise has none.
PEARL_HEADER = ("n,mode,input_rate,rep,"
                "consensus_tps,consensus_latency_ms,"
                "execution_tps,execution_latency_ms,slots_committed,"
                "silent_faults")
BASELINE_HEADER = ("baseline,n,f,mode,input_rate,rep,"
                   "consensus_tps,consensus_latency_ms,"
                   "execution_tps,execution_latency_ms,slots_committed,"
                   "silent_faults")

# Protocol max fault tolerance for each baseline at its chosen n (used to
# fill the existing `f` column verbatim).  Pearl has no f column.
BASELINE_F = {
    "fairdag-ol":  5,   # 3f+1 with n=16 -> f=5
    "fairdag-bof": 5,   # 3f+1 with n=16 -> f=5
    "tusk":        5,   # 3f+1 with n=16 -> f=5
    "pompe":       5,   # 3f+1 with n=16 -> f=5
    "themis":      5,   # 4f+1 with n=21 -> f=5
}

# Each system's mode label matches what the existing baseline CSVs use:
#   pearl       -> "ol" (Pearl's TEE-fair-ordering contribution)
#   fairdag-ol  -> "ol"
#   fairdag-bof -> "bof"
#   pompe       -> "ol" (matches existing pompe.csv)
#   themis      -> "bof" (matches existing themis.csv)
SYSTEM_MODE = {
    "pearl":       "ol",
    "fairdag-ol":  "ol",
    "fairdag-bof": "bof",
    "tusk":        "none",   # no fair-ordering layer
    "pompe":       "ol",
    "themis":      "bof",
}

# ---------------------------------------------------------------------------
# CSV utilities
# ---------------------------------------------------------------------------

def csv_path_for(system: str) -> Path:
    return RESULTS_DIR / f"{system}.csv"


def header_for(system: str) -> str:
    return PEARL_HEADER if system == "pearl" else BASELINE_HEADER


def load_completed(path: Path, system: str) -> set:
    """Return the set of (n, mode, silent_faults, rep) already recorded with
    execution_tps>0. Including mode lets Pearl run OL and BOF for the same
    (n, sf, rep) without colliding.

    Pearl row layout (10 cols):
        n, mode, input_rate, rep,
        c_tps, c_lat, e_tps, e_lat, slots, silent_faults
    Baseline row layout (12 cols):
        baseline, n, f, mode, input_rate, rep,
        c_tps, c_lat, e_tps, e_lat, slots, silent_faults
    """
    if not path.exists():
        return set()
    done = set()
    if system == "pearl":
        n_col, mode_col, rep_col, tps_col, sf_col = 0, 1, 3, 6, 9
    else:
        n_col, mode_col, rep_col, tps_col, sf_col = 1, 3, 5, 8, 11
    with open(path) as fh:
        next(fh, None)  # header
        for line in fh:
            parts = line.strip().split(",")
            if len(parts) <= sf_col:
                continue
            try:
                tps = float(parts[tps_col])
                key = (int(parts[n_col]), parts[mode_col].strip(),
                       int(parts[sf_col]), int(parts[rep_col]))
            except ValueError:
                continue
            if tps > 0:
                done.add(key)
    return done


def init_csv(path: Path, system: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(header_for(system) + "\n")


def append_row(path: Path, system: str, n: int, silent_faults: int, rep: int,
               input_rate: int,
               exec_tps: float, exec_lat_ms: float,
               c_tps: float, c_lat_ms: float,
               slots: int = 0,
               mode: Optional[str] = None):
    if mode is None:
        mode = SYSTEM_MODE[system]
    if system == "pearl":
        row = (f"{n},{mode},{input_rate},{rep},"
               f"{c_tps:.2f},{c_lat_ms:.2f},"
               f"{exec_tps:.2f},{exec_lat_ms:.2f},{slots},"
               f"{silent_faults}")
    else:
        f_max = BASELINE_F[system]
        row = (f"{system},{n},{f_max},{mode},{input_rate},{rep},"
               f"{c_tps:.2f},{c_lat_ms:.2f},"
               f"{exec_tps:.2f},{exec_lat_ms:.2f},{slots},"
               f"{silent_faults}")
    with open(path, "a") as fh:
        fh.write(row + "\n")


# ---------------------------------------------------------------------------
# Per-system runners — each returns (tps, latency_ms, c_tps, c_lat_ms).
# A return of (0.0, 0.0, 0.0, 0.0) signals failure; the row is still written
# so the orchestrator can be re-run to retry.
# ---------------------------------------------------------------------------

def run_pearl(n: int, f: int, rate: int, dry_run: bool, mode: str = "ol"):
    """Pearl's das.run_experiment is in this same package — import directly.

    Returns (exec_tps, exec_lat_ms, con_tps, con_lat_ms, slots).
    """
    sys.path.insert(0, str(THIS_DIR))
    from das import run_experiment, check_binaries  # type: ignore

    if not dry_run:
        check_binaries()
    con_tps, con_lat_s, exec_tps, exec_lat_s, slots = run_experiment(
        num_replicas=n,
        mode=mode,
        block_size=100,
        runtime=60,
        warmup=15,
        dry_run=dry_run,
        num_clients=n,
        target_tps=rate,
        silent_faults=f,
    )
    return exec_tps, exec_lat_s * 1000, con_tps, con_lat_s * 1000, slots


def run_fairdag(rl: bool, n: int, f: int, rate: int, dry_run: bool, system: str = "fairdag"):
    """Subprocess into baselines/fairdag/scripts/deploy/performance/das_faulty.py.
    `system="tusk"` runs the plain DAG-BFT substrate (ignores `rl`).
    """
    cwd = FAIRDAG_DEPLOY
    cmd = [
        sys.executable, "performance/das_faulty.py",
        "--n", str(n), "--faults", str(f), "--rate", str(rate),
        "--rl", "1" if rl else "0",
        "--system", system,
        "--duration", "300",
    ]
    if dry_run:
        print(f"[DRY-RUN] cd {cwd} && {' '.join(cmd)}")
        return 0.0, 0.0, 0.0, 0.0, 0
    print(f"\n[{system}] cd {cwd}")
    print(f"[{system}] {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(cwd),
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print(f"[{system}] non-zero exit ({proc.returncode}); recording zero")
        return 0.0, 0.0, 0.0, 0.0, 0
    m = re.search(r"^RESULT_JSON (\{.*\})$", proc.stdout, re.MULTILINE)
    if not m:
        print(f"[{system}] no RESULT_JSON line found; recording zero")
        return 0.0, 0.0, 0.0, 0.0, 0
    try:
        d = json.loads(m.group(1))
    except json.JSONDecodeError:
        return 0.0, 0.0, 0.0, 0.0, 0
    return (float(d.get("throughput", 0)),
            float(d.get("latency_ms", 0)),
            float(d.get("consensus_throughput", 0)),
            float(d.get("consensus_latency_ms", 0)),
            0)


def run_narwhal(flavor: str, n: int, f: int, rate: int, dry_run: bool):
    """Subprocess `fab of_faulty` under conda workenv, parse the result file."""
    # PathMaker.local_result_file(0, 0, faults, workers, nodes)
    workers = 1
    result_file = NARWHAL_BENCH / "results" / f"local-0-0-{f}-{workers}-{n}.txt"
    runs = 1

    # Note pre-existing line count so we can grab only the new line(s).
    pre_lines = result_file.read_text().splitlines() if result_file.exists() else []

    conda_profile = NARWHAL_CONDA_PROFILE if NARWHAL_CONDA_PROFILE.exists() else CONDA_PROFILE
    shell_cmd = (
        f"source {conda_profile} && conda activate {CONDA_ENV} && "
        f"fab of-faulty --flavor={flavor} -n={n} --rate={rate} "
        f"--runs={runs} --faults={f}"
    )
    if dry_run:
        print(f"[DRY-RUN] cd {NARWHAL_BENCH} && bash -lc \"{shell_cmd}\"")
        return 0.0, 0.0, 0.0, 0.0, 0
    print(f"\n[narwhal/{flavor}] cd {NARWHAL_BENCH}")
    print(f"[narwhal/{flavor}] {shell_cmd}")
    proc = subprocess.run(["bash", "-lc", shell_cmd], cwd=str(NARWHAL_BENCH),
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print(f"[narwhal/{flavor}] non-zero exit ({proc.returncode}); recording zero")
        return 0.0, 0.0, 0.0, 0.0, 0

    if not result_file.exists():
        print(f"[narwhal/{flavor}] result file missing: {result_file}")
        return 0.0, 0.0, 0.0, 0.0, 0
    # ret.result() in OFBench writes a multi-line block per run: a leading
    # data line `<rate> <tps> <lat_ms> <avg_scc>` followed by the adversarial
    # reordering report. The report header starts with "ADVERSARIAL" but the
    # subsequent rows start with whitespace + "Dist=...", which previously
    # leaked through and tripped float() on parts[1] -> 0.0 for every themis
    # run. Match the data line by shape: 4 whitespace-separated tokens, all
    # numeric, first equal to the configured input rate.
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
        return 0.0, 0.0, 0.0, 0.0, 0
    tps, lat_ms = data_line
    # Pompe TPS is reported inflated 100x by narwhal — divide.
    if flavor == "pompe":
        tps = tps / 100.0
    # The existing baseline CSVs use the same value for execution and
    # consensus columns for pompe/themis (single-stage protocols), so
    # mirror that here.
    return tps, lat_ms, tps, lat_ms, 0


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_one(system: str, f: int, dry_run: bool, pearl_mode: str = "ol"):
    cfg = SYSTEMS[system]
    n, rate = cfg["n"], cfg["rate"]
    print(f"\n=========================")
    label = f"{system} ({pearl_mode})" if system == "pearl" else system
    print(f" {label}  n={n}  f={f}  rate={rate}")
    print(f"=========================")
    if system == "pearl":
        return run_pearl(n, f, rate, dry_run, mode=pearl_mode)
    if system == "fairdag-ol":
        return run_fairdag(rl=False, n=n, f=f, rate=rate, dry_run=dry_run)
    if system == "fairdag-bof":
        return run_fairdag(rl=True, n=n, f=f, rate=rate, dry_run=dry_run)
    if system == "tusk":
        return run_fairdag(rl=False, n=n, f=f, rate=rate, dry_run=dry_run, system="tusk")
    if system == "pompe":
        return run_narwhal("pompe", n, f, rate, dry_run)
    if system == "themis":
        return run_narwhal("themis", n, f, rate, dry_run)
    raise ValueError(f"unknown system: {system}")


def main():
    p = argparse.ArgumentParser(description="Faulty-node sweep across systems")
    p.add_argument("--systems", nargs="+", choices=list(SYSTEMS.keys()),
                   default=list(SYSTEMS.keys()),
                   help="subset of systems to run (default: all)")
    p.add_argument("--faults", type=int, nargs="+", default=[1, 2, 3, 4, 5],
                   help="fault counts to sweep")
    p.add_argument("--reps", type=int, default=1, help="repetitions per (system, f)")
    p.add_argument("--pearl-mode", choices=["ol", "bof"], default="ol",
                   help="Pearl mode for this run (default: ol). Ignored for non-pearl systems.")
    p.add_argument("--dry-run", action="store_true",
                   help="print plan, don't reserve or run")
    args = p.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_BASE.mkdir(parents=True, exist_ok=True)

    for system in args.systems:
        path = csv_path_for(system)
        init_csv(path, system)
        completed = load_completed(path, system)
        n = SYSTEMS[system]["n"]
        rate = SYSTEMS[system]["rate"]
        mode = args.pearl_mode if system == "pearl" else SYSTEM_MODE[system]
        for sf in args.faults:
            for rep in range(1, args.reps + 1):
                if (n, mode, sf, rep) in completed:
                    print(f"  skip {system} n={n} mode={mode} silent_faults={sf} rep={rep} (done)")
                    continue
                t0 = time.time()
                try:
                    exec_tps, exec_lat_ms, c_tps, c_lat_ms, slots = run_one(
                        system, sf, args.dry_run, pearl_mode=args.pearl_mode)
                except Exception as e:
                    print(f"  ERROR {system} mode={mode} silent_faults={sf} rep={rep}: {e}")
                    exec_tps = exec_lat_ms = c_tps = c_lat_ms = 0.0
                    slots = 0
                if not args.dry_run:
                    append_row(path, system, n, sf, rep, rate,
                               exec_tps, exec_lat_ms, c_tps, c_lat_ms, slots,
                               mode=mode)
                    print(f"  {system} mode={mode} silent_faults={sf} rep={rep} -> "
                          f"exec_tps={exec_tps:.1f} exec_lat={exec_lat_ms:.1f}ms "
                          f"({time.time() - t0:.0f}s)")
                # cooldown between runs (different systems may share the cluster)
                if not args.dry_run:
                    time.sleep(10)

    print("\nDone. Results in:", RESULTS_DIR)


if __name__ == "__main__":
    main()
