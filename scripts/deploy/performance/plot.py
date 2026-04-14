#!/usr/bin/env python3
"""
plot.py — Generate OL vs BOF comparison plots from CSV.

Produces three figures:
  1. End-to-End Throughput (tx/s) vs n
  2. Consensus Throughput (slots/s) vs n
  3. Consensus Latency (ms) vs n
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

PROJ_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CSV = PROJ_ROOT / "das_results" / "n_scaling.csv"
DEFAULT_OUT = PROJ_ROOT / "das_results" / "plots"

MODES = ["ol", "bof"]
MODE_LABELS = {"ol": "OL", "bof": "BOF"}
MODE_COLORS = {"ol": "#2196F3", "bof": "#F44336"}
MODE_MARKERS = {"ol": "o", "bof": "s"}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> dict:
    """
    data[mode][n] = {
        "e2e_tps": [],
        "con_tps": [],
        "con_lat_ms": []
    }
    """
    data = defaultdict(lambda: defaultdict(lambda: {
        "e2e_tps": [], "con_tps": [], "con_lat_ms": []
    }))

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                mode = row["mode"].strip()
                n = int(row["n"])

                e2e_tps = float(row["e2e_tps"])
                con_tps = float(row["con_tps"])
                con_lat = float(row["con_lat_s"]) * 1000.0  # → ms

            except (KeyError, ValueError):
                continue

            data[mode][n]["e2e_tps"].append(e2e_tps)
            data[mode][n]["con_tps"].append(con_tps)
            data[mode][n]["con_lat_ms"].append(con_lat)

    return data


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def stats(values: list, drop: int = 1):
    v = sorted(x for x in values if x >= 0)
    if len(v) > 2 * drop:
        v = v[drop:-drop]
    if not v:
        return 0.0, 0.0
    a = np.array(v, dtype=float)
    return float(a.mean()), float(a.std(ddof=0))


def extract_series(data: dict, mode: str, metric: str):
    ns_raw = sorted(data[mode].keys())
    ns, means, stds = [], [], []

    for n in ns_raw:
        m, s = stats(data[mode][n][metric])
        if m > 0:
            ns.append(n)
            means.append(m)
            stds.append(s)

    return np.array(ns), np.array(means), np.array(stds)


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def style_ax(ax, xlabel, ylabel, title):
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontweight="bold")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.5)


def plot_metric(data, metric, ylabel, title, out_path):
    fig, ax = plt.subplots(figsize=(7, 4.5))

    for mode in MODES:
        if mode not in data:
            continue

        ns, means, stds = extract_series(data, mode, metric)
        if len(ns) == 0:
            continue

        ax.errorbar(
            ns, means, yerr=stds,
            label=MODE_LABELS[mode],
            color=MODE_COLORS[mode],
            marker=MODE_MARKERS[mode],
            linewidth=2,
            capsize=4,
        )

    style_ax(ax, "Number of replicas (n)", ylabel, title)
    ax.set_xticks(sorted({n for m in MODES if m in data for n in data[m]}))

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"ERROR: CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    data = load_csv(args.csv)

    if not data:
        print("ERROR: no data parsed", file=sys.stderr)
        sys.exit(1)

    # 1. End-to-end throughput
    plot_metric(
        data, "e2e_tps",
        "Throughput (tx/s)",
        "End-to-End Throughput vs n",
        args.out_dir / "e2e_throughput.png",
    )

    # 2. Consensus throughput
    plot_metric(
        data, "con_tps",
        "Consensus throughput (slots/s)",
        "Consensus Throughput vs n",
        args.out_dir / "consensus_throughput.png",
    )

    # 3. Consensus latency
    plot_metric(
        data, "con_lat_ms",
        "Consensus latency (ms)",
        "Consensus Latency vs n",
        args.out_dir / "consensus_latency.png",
    )

    print("\nAll plots saved in:", args.out_dir)


if __name__ == "__main__":
    main()