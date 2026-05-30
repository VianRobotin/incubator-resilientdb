#!/usr/bin/env python3
# Batch-size (block-size) sweep across all systems for the thesis paper.
#
# Holds n fixed per protocol (same as the faulty sweep), holds rate fixed
# at 500 tx/s, no induced faults, sweeps batch_size in {25,50,100,200,400}
# with `--reps` repetitions per point.  Writes one CSV per system into
# das_results/batch/ and never touches the existing CSVs/plots.
#
# Each system's "batch size" is a different physical knob; this is the
# closest comparable parameter per protocol:
#   pearl       -> block_size JSON config (# txns per block)
#   fairdag-ol  -> FAIRDAG_BLOCK_SIZE env var (patched into tusk.cpp)
#   fairdag-bof -> FAIRDAG_BLOCK_SIZE env var (patched into tusk.cpp)
#   pompe       -> node_params['batch_size'] in bytes = batch * tx_size
#   themis      -> same as pompe
#
# Usage (from incubator-resilientdb/scripts/deploy/):
#   python3 performance/das_batch.py                  # full sweep, all systems
#   python3 performance/das_batch.py --systems pearl  # subset
#   python3 performance/das_batch.py --batches 25 100 # subset of batch values
#   python3 performance/das_batch.py --pearl-mode bof
#   python3 performance/das_batch.py --dry-run

import argparse
import csv
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

# Baselines now live inside the repo (incubator-resilientdb/baselines/) so
# their changes can be committed alongside Pearl. The old out-of-repo copies
# at SCRATCH/{baselines-fairdag,narwhal} are retired.
FAIRDAG_ROOT  = PEARL_ROOT / "baselines" / "fairdag"
NARWHAL_ROOT  = PEARL_ROOT / "baselines" / "narwhal"

FAIRDAG_DEPLOY = FAIRDAG_ROOT / "scripts" / "deploy"
NARWHAL_BENCH  = NARWHAL_ROOT / "benchmark"

RESULTS_DIR  = PEARL_ROOT / "das_results" / "batch"
RUNTIME_BASE = PEARL_ROOT / "das_runtime" / "batch"

CONDA_PROFILE = SCRATCH / "miniconda3" / "etc" / "profile.d" / "conda.sh"
NARWHAL_CONDA_PROFILE = NARWHAL_ROOT / "miniconda" / "etc" / "profile.d" / "conda.sh"
CONDA_ENV = "workenv"

# ---------------------------------------------------------------------------
# Per-system fixed config — same n's as the faulty sweep so hardware setup
# and committee-size effects are held constant across experiments.
# ---------------------------------------------------------------------------
SYSTEMS = {
    "pearl":       {"n": 11, "rate": 500},
    "fairdag-ol":  {"n": 16, "rate": 500},
    "fairdag-bof": {"n": 16, "rate": 500},
    "tusk":        {"n": 16, "rate": 500},   # 3f+1, same as FairDAG
    "pompe":       {"n": 16, "rate": 500},
    "themis":      {"n": 21, "rate": 500},
}

# CSV schema: existing result CSV's schema VERBATIM, with `batch_size`
# appended in place of `silent_faults`.
PEARL_HEADER = ("n,mode,input_rate,rep,"
                "consensus_tps,consensus_latency_ms,"
                "execution_tps,execution_latency_ms,slots_committed,"
                "batch_size")
BASELINE_HEADER = ("baseline,n,f,mode,input_rate,rep,"
                   "consensus_tps,consensus_latency_ms,"
                   "execution_tps,execution_latency_ms,slots_committed,"
                   "batch_size")

BASELINE_F = {
    "fairdag-ol":  5,
    "fairdag-bof": 5,
    "tusk":        5,
    "pompe":       5,
    "themis":      5,
}

SYSTEM_MODE = {
    "pearl":       "ol",
    "fairdag-ol":  "ol",
    "fairdag-bof": "bof",
    "tusk":        "none",   # no fair-ordering layer
    "pompe":       "ol",
    "themis":      "bof",
}

# Narwhal accumulates batches by bytes; convert via this fixed tx_size.
NARWHAL_TX_SIZE = 128

# ---------------------------------------------------------------------------
# CSV utilities
# ---------------------------------------------------------------------------

def csv_path_for(system: str) -> Path:
    return RESULTS_DIR / f"{system}.csv"


def header_for(system: str) -> str:
    return PEARL_HEADER if system == "pearl" else BASELINE_HEADER


def load_completed(path: Path, system: str) -> set:
    """Set of (n, mode, batch_size, rep) already recorded with exec_tps>0."""
    if not path.exists():
        return set()
    done = set()
    if system == "pearl":
        n_col, mode_col, rep_col, tps_col, bs_col = 0, 1, 3, 6, 9
    else:
        n_col, mode_col, rep_col, tps_col, bs_col = 1, 3, 5, 8, 11
    with open(path) as fh:
        next(fh, None)
        for line in fh:
            parts = line.strip().split(",")
            if len(parts) <= bs_col:
                continue
            try:
                tps = float(parts[tps_col])
                key = (int(parts[n_col]), parts[mode_col].strip(),
                       int(parts[bs_col]), int(parts[rep_col]))
            except ValueError:
                continue
            if tps > 0:
                done.add(key)
    return done


def init_csv(path: Path, system: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(header_for(system) + "\n")


def append_row(path: Path, system: str, n: int, batch_size: int, rep: int,
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
               f"{batch_size}")
    else:
        f_max = BASELINE_F[system]
        row = (f"{system},{n},{f_max},{mode},{input_rate},{rep},"
               f"{c_tps:.2f},{c_lat_ms:.2f},"
               f"{exec_tps:.2f},{exec_lat_ms:.2f},{slots},"
               f"{batch_size}")
    with open(path, "a") as fh:
        fh.write(row + "\n")


# ---------------------------------------------------------------------------
# Per-system runners
# ---------------------------------------------------------------------------

def run_pearl(n: int, rate: int, batch_size: int, dry_run: bool, mode: str = "ol"):
    sys.path.insert(0, str(THIS_DIR))
    from das import run_experiment, check_binaries  # type: ignore

    if not dry_run:
        check_binaries()
    con_tps, con_lat_s, exec_tps, exec_lat_s, slots = run_experiment(
        num_replicas=n,
        mode=mode,
        block_size=batch_size,
        runtime=60,
        warmup=15,
        dry_run=dry_run,
        num_clients=n,
        target_tps=rate,
        silent_faults=0,
    )
    return exec_tps, exec_lat_s * 1000, con_tps, con_lat_s * 1000, slots


def run_fairdag(rl: bool, n: int, rate: int, batch_size: int, dry_run: bool, system: str = "fairdag"):
    """`system="tusk"` runs the plain DAG-BFT substrate (ignores `rl`).
    The block size is propagated to both via the FAIRDAG_BLOCK_SIZE env var.
    """
    cwd = FAIRDAG_DEPLOY
    cmd = [
        sys.executable, "performance/das_batch.py",
        "--n", str(n), "--batch", str(batch_size), "--rate", str(rate),
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


def run_narwhal(flavor: str, n: int, rate: int, batch_size: int, dry_run: bool):
    workers = 1
    # of_batch writes to local-batch{blk}-0-0-{wks}-{nodes}.txt
    result_file = NARWHAL_BENCH / "results" / f"local-batch{batch_size}-0-0-{workers}-{n}.txt"
    pre_lines = result_file.read_text().splitlines() if result_file.exists() else []
    runs = 1

    conda_profile = NARWHAL_CONDA_PROFILE if NARWHAL_CONDA_PROFILE.exists() else CONDA_PROFILE
    shell_cmd = (
        f"source {conda_profile} && conda activate {CONDA_ENV} && "
        f"fab of-batch --flavor={flavor} -n={n} --rate={rate} "
        f"--runs={runs} --batch={batch_size} --tx-size={NARWHAL_TX_SIZE}"
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
    if flavor == "pompe":
        tps = tps / 100.0
    return tps, lat_ms, tps, lat_ms, 0


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_one(system: str, batch_size: int, dry_run: bool, pearl_mode: str = "ol"):
    cfg = SYSTEMS[system]
    n, rate = cfg["n"], cfg["rate"]
    print(f"\n=========================")
    label = f"{system} ({pearl_mode})" if system == "pearl" else system
    print(f" {label}  n={n}  batch={batch_size}  rate={rate}")
    print(f"=========================")
    if system == "pearl":
        return run_pearl(n, rate, batch_size, dry_run, mode=pearl_mode)
    if system == "fairdag-ol":
        return run_fairdag(rl=False, n=n, rate=rate, batch_size=batch_size, dry_run=dry_run)
    if system == "fairdag-bof":
        return run_fairdag(rl=True,  n=n, rate=rate, batch_size=batch_size, dry_run=dry_run)
    if system == "tusk":
        return run_fairdag(rl=False, n=n, rate=rate, batch_size=batch_size, dry_run=dry_run, system="tusk")
    if system == "pompe":
        return run_narwhal("pompe",  n, rate, batch_size, dry_run)
    if system == "themis":
        return run_narwhal("themis", n, rate, batch_size, dry_run)
    raise ValueError(f"unknown system: {system}")


def main():
    p = argparse.ArgumentParser(description="Batch-size sweep across systems")
    p.add_argument("--systems", nargs="+", choices=list(SYSTEMS.keys()),
                   default=list(SYSTEMS.keys()),
                   help="subset of systems to run (default: all)")
    p.add_argument("--batches", type=int, nargs="+",
                   default=[25, 50, 100, 200, 400],
                   help="batch sizes to sweep (default: 25 50 100 200 400)")
    p.add_argument("--reps", type=int, default=1, help="repetitions per (system, batch)")
    p.add_argument("--pearl-modes", nargs="+", choices=["ol", "bof"],
                   default=["ol", "bof"],
                   help="Pearl modes to run (default: ol bof). Ignored for non-pearl systems.")
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
        # Pearl iterates over each requested mode; baselines run once with
        # their fixed mode (one entry list).
        modes = args.pearl_modes if system == "pearl" else [SYSTEM_MODE[system]]
        for mode in modes:
            for bs in args.batches:
                for rep in range(1, args.reps + 1):
                    if (n, mode, bs, rep) in completed:
                        print(f"  skip {system} n={n} mode={mode} batch={bs} rep={rep} (done)")
                        continue
                    t0 = time.time()
                    try:
                        exec_tps, exec_lat_ms, c_tps, c_lat_ms, slots = run_one(
                            system, bs, args.dry_run, pearl_mode=mode)
                    except Exception as e:
                        print(f"  ERROR {system} mode={mode} batch={bs} rep={rep}: {e}")
                        exec_tps = exec_lat_ms = c_tps = c_lat_ms = 0.0
                        slots = 0
                    if not args.dry_run:
                        append_row(path, system, n, bs, rep, rate,
                                   exec_tps, exec_lat_ms, c_tps, c_lat_ms, slots,
                                   mode=mode)
                        print(f"  {system} mode={mode} batch={bs} rep={rep} -> "
                              f"exec_tps={exec_tps:.1f} exec_lat={exec_lat_ms:.1f}ms "
                              f"({time.time() - t0:.0f}s)")
                    if not args.dry_run:
                        time.sleep(10)

    print("\nDone. Results in:", RESULTS_DIR)


if __name__ == "__main__":
    main()
