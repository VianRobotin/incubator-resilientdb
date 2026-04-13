#!/usr/bin/env python3
"""
plot.py — Generate OL vs BOF comparison plots from n_scaling.csv.

Produces three figures:
  1. Application Throughput (tx/s) vs n
  2. Consensus Throughput (slots/s) vs n
  3. Consensus Latency (ms) vs n

Each figure shows two lines (OL, BOF) with error bars (±1 std dev across 5 reps).

Usage:
    python3 performance/plot.py [--csv PATH] [--out-dir DIR]
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")          # headless — no display needed on DAS5
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
MODE_COLORS = {"ol": "#2196F3", "bof": "#F44336"}   # blue / red
MODE_MARKERS = {"ol": "o", "bof": "s"}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> dict:
    """
    Returns nested dict: data[mode][n] = dict(tps=[..], lat=[..], con_tps=[..], con_lat=[..])
    lat is stored in milliseconds; con_lat is stored in milliseconds.
    """
    data: dict = defaultdict(lambda: defaultdict(lambda: {
        "tps": [], "con_tps": [], "con_lat_ms": []
    }))

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                mode = row["mode"].strip()
                n    = int(row["n"])
                tps  = float(row["tps"])
                ctp  = float(row["consensus_tps"])
                clat = float(row["consensus_latency_s"]) * 1000.0  # → ms
            except (KeyError, ValueError):
                continue
            data[mode][n]["tps"].append(tps)
            data[mode][n]["con_tps"].append(ctp)
            data[mode][n]["con_lat_ms"].append(clat)

    return data


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def stats(values: list, drop: int = 1):
    """
    Trimmed mean and std-dev after dropping `drop` lowest and highest.
    Returns (mean, std).  Returns (0, 0) if empty.
    """
    v = sorted(x for x in values if x >= 0)
    if len(v) > 2 * drop:
        v = v[drop:-drop]
    if not v:
        return 0.0, 0.0
    a = np.array(v, dtype=float)
    return float(a.mean()), float(a.std(ddof=0))


def extract_series(data: dict, mode: str, metric: str):
    """
    Returns (ns, means, stds) sorted by n for the given mode and metric key.
    """
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
# Plotting helpers
# ---------------------------------------------------------------------------

def style_ax(ax, xlabel: str, ylabel: str, title: str):
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.grid(axis="x", linestyle=":", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def plot_metric(data: dict, metric: str, ylabel: str, title: str, out_path: Path):
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
            markersize=7,
            linewidth=2,
            capsize=4,
            capthick=1.5,
        )
    style_ax(ax, xlabel="Number of replicas (n)", ylabel=ylabel, title=title)
    ax.set_xticks(sorted({n for mode in MODES if mode in data for n in data[mode]}))
    fig.tight_layout()
    fig.savefig(str(out_path), dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Plot OL vs BOF n-scaling results")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV,
                        help=f"Path to n_scaling.csv  (default: {DEFAULT_CSV})")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT,
                        help=f"Directory for output PNG files  (default: {DEFAULT_OUT})")
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"ERROR: CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    data = load_csv(args.csv)

    if not data:
        print("ERROR: no data parsed from CSV", file=sys.stderr)
        sys.exit(1)

    # ---- Figure 1: Application throughput ----
    plot_metric(
        data, metric="tps",
        ylabel="Throughput (tx/s)",
        title="Application Throughput vs Number of Replicas",
        out_path=args.out_dir / "throughput_vs_n.png",
    )

    # ---- Figure 2: Consensus throughput ----
    plot_metric(
        data, metric="con_tps",
        ylabel="Consensus throughput (slots/s)",
        title="Consensus Throughput vs Number of Replicas",
        out_path=args.out_dir / "consensus_throughput_vs_n.png",
    )

    # ---- Figure 3: Consensus latency ----
    plot_metric(
        data, metric="con_lat_ms",
        ylabel="Consensus latency (ms)",
        title="Consensus Latency vs Number of Replicas",
        out_path=args.out_dir / "consensus_latency_vs_n.png",
    )

    print("\nAll plots written to:", args.out_dir)


if __name__ == "__main__":
    main()
