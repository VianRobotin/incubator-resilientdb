#!/usr/bin/env python3
"""
plot.py — Generate throughput-vs-latency L-curve from tput_latency.csv.

The classical BFT benchmark plot: throughput (x-axis) vs latency (y-axis).
Points are obtained by varying the injection rate; the curve traces an L-shape
(flat low-latency region → saturation knee → steep latency rise).

One curve per (N, mode) combination.  Run plot.py after das.py completes.

Usage:
  python3 performance/plot.py                  # use default tput_latency.csv
  python3 performance/plot.py --csv path.csv   # use a different CSV
  python3 performance/plot.py --out-dir /tmp   # different output directory
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

PROJ_ROOT   = Path(__file__).resolve().parents[3]
DEFAULT_CSV = PROJ_ROOT / "das_results" / "tput_latency.csv"
DEFAULT_OUT = PROJ_ROOT / "das_results" / "plots"

# Colours and markers: one style per (mode, metric)
# "consensus" = block certification layer; "execution" = end-to-end commit
STYLE = {
    ("ol",  "execution"): dict(color="#1565C0", marker="o",
                                label="OL (end-to-end)", linestyle="-"),
    ("bof", "execution"): dict(color="#B71C1C", marker="s",
                                label="BOF (end-to-end)", linestyle="-"),
    ("ol",  "consensus"): dict(color="#42A5F5", marker="^",
                                label="OL (consensus)", linestyle="--"),
    ("bof", "consensus"): dict(color="#EF9A9A", marker="D",
                                label="BOF (consensus)", linestyle="--"),
}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> dict:
    """
    Returns data[(n, mode, metric)][input_rate] = {"tps": [...], "lat_ms": [...]}

    CSV columns:
      n, mode, input_rate, rep,
      consensus_tps, consensus_latency_ms,
      execution_tps, execution_latency_ms,
      slots_committed
    """
    data: dict = defaultdict(lambda: defaultdict(lambda: {"tps": [], "lat_ms": []}))

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                n    = int(row["n"])
                mode = row["mode"].strip()
                rate = int(row["input_rate"])
                con_tps = float(row["consensus_tps"])
                con_lat = float(row["consensus_latency_ms"])
                exe_tps = float(row["execution_tps"])
                exe_lat = float(row["execution_latency_ms"])
            except (KeyError, ValueError):
                continue

            data[(n, mode, "consensus")][rate]["tps"].append(con_tps)
            data[(n, mode, "consensus")][rate]["lat_ms"].append(con_lat)
            data[(n, mode, "execution")][rate]["tps"].append(exe_tps)
            data[(n, mode, "execution")][rate]["lat_ms"].append(exe_lat)

    return data


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def trimmed_stats(values: list, drop: int = 1):
    """(mean, std) after dropping `drop` lowest and highest values."""
    v = sorted(x for x in values if x > 0)
    if len(v) > 2 * drop:
        v = v[drop:-drop]
    if not v:
        return 0.0, 0.0
    a = np.array(v, dtype=float)
    return float(a.mean()), float(a.std(ddof=0))


def series_for_key(data: dict, key: tuple):
    """
    For a given (n, mode, metric) key, return arrays sorted by input_rate:
      (rates, mean_tps, std_tps, mean_lat, std_lat)
    """
    if key not in data:
        return [np.array([])] * 5

    rates_raw = sorted(data[key].keys())
    rates, m_tps, s_tps, m_lat, s_lat = [], [], [], [], []
    for rate in rates_raw:
        d = data[key][rate]
        mt, st = trimmed_stats(d["tps"])
        ml, sl = trimmed_stats(d["lat_ms"])
        if mt > 0 or ml > 0:   # skip empty rows
            rates.append(rate)
            m_tps.append(mt); s_tps.append(st)
            m_lat.append(ml); s_lat.append(sl)
    return (np.array(rates), np.array(m_tps), np.array(s_tps),
            np.array(m_lat), np.array(s_lat))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def style_ax(ax, xlabel, ylabel, title):
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_title(title, fontweight="bold")
    ax.legend(fontsize=8)
    ax.grid(True, linestyle="--", alpha=0.4)


def all_n_values(data: dict) -> list:
    return sorted({k[0] for k in data})


def all_modes(data: dict) -> list:
    return sorted({k[1] for k in data})


# ---------------------------------------------------------------------------
# Plot 1 — L-curve: Throughput (x) vs Latency (y)  [MAIN PLOT]
#
# Points trace the L-curve as injection rate increases:
#   left-bottom = unsaturated (low rate, low latency, throughput ≈ rate)
#   right-bottom = approaching saturation
#   right-top    = saturated (throughput plateaus, latency explodes)
# ---------------------------------------------------------------------------

def plot_l_curve(data: dict, out_path: Path, metric: str = "execution"):
    """
    One curve per (N, mode).  Points sorted by injection rate.
    metric: "execution" (end-to-end, default) or "consensus" (protocol layer).
    """
    ns    = all_n_values(data)
    modes = all_modes(data)

    # Assign a distinct colour per N; linestyle per mode
    cmap   = plt.cm.get_cmap("tab10", max(len(ns), 1))
    ls_map = {"ol": "-", "bof": "--"}
    mk_map = {"ol": "o", "bof": "s"}

    fig, ax = plt.subplots(figsize=(8, 5))

    for n_idx, n in enumerate(ns):
        color = cmap(n_idx)
        for mode in modes:
            key = (n, mode, metric)
            rates, m_tps, s_tps, m_lat, s_lat = series_for_key(data, key)
            if len(rates) == 0:
                continue

            label = f"N={n} {mode.upper()}"
            ax.errorbar(
                m_tps, m_lat,
                xerr=s_tps, yerr=s_lat,
                label=label,
                color=color,
                linestyle=ls_map.get(mode, "-"),
                marker=mk_map.get(mode, "o"),
                linewidth=1.8,
                markersize=6,
                capsize=3,
            )
            # Annotate each point with its injection rate (in k tx/s)
            for rate, x, y in zip(rates, m_tps, m_lat):
                ax.annotate(
                    f"{rate//1000}k" if rate >= 1000 else str(rate),
                    (x, y),
                    textcoords="offset points",
                    xytext=(5, 4),
                    fontsize=6,
                    color=color,
                )

    metric_label = "End-to-end" if metric == "execution" else "Consensus"
    style_ax(ax, "Throughput (tx/s)", f"{metric_label} Latency (ms)",
             f"Throughput vs Latency — OL vs BOF")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Plot 2 — Throughput vs Injection Rate (saturation diagnostic)
# ---------------------------------------------------------------------------

def plot_tput_vs_rate(data: dict, out_path: Path, metric: str = "execution"):
    ns    = all_n_values(data)
    modes = all_modes(data)
    cmap  = plt.cm.get_cmap("tab10", max(len(ns), 1))
    ls_map = {"ol": "-", "bof": "--"}
    mk_map = {"ol": "o", "bof": "s"}

    fig, ax = plt.subplots(figsize=(8, 5))

    all_rates: list = []
    for n_idx, n in enumerate(ns):
        color = cmap(n_idx)
        for mode in modes:
            key = (n, mode, metric)
            rates, m_tps, s_tps, _, _ = series_for_key(data, key)
            if len(rates) == 0:
                continue
            all_rates.extend(rates.tolist())
            ax.errorbar(
                rates, m_tps, yerr=s_tps,
                label=f"N={n} {mode.upper()}",
                color=color,
                linestyle=ls_map.get(mode, "-"),
                marker=mk_map.get(mode, "o"),
                linewidth=1.5,
                capsize=3,
            )

    if all_rates:
        lim = max(all_rates) * 1.05
        ax.plot([0, lim], [0, lim], "k--", linewidth=1, alpha=0.3,
                label="y = x (perfect)")

    metric_label = "End-to-end" if metric == "execution" else "Consensus"
    style_ax(ax, "Injection Rate (tx/s)", f"{metric_label} Throughput (tx/s)",
             "Throughput vs Injection Rate")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Plot 3 — Latency vs Injection Rate (latency diagnostic)
# ---------------------------------------------------------------------------

def plot_lat_vs_rate(data: dict, out_path: Path, metric: str = "execution"):
    ns    = all_n_values(data)
    modes = all_modes(data)
    cmap  = plt.cm.get_cmap("tab10", max(len(ns), 1))
    ls_map = {"ol": "-", "bof": "--"}
    mk_map = {"ol": "o", "bof": "s"}

    fig, ax = plt.subplots(figsize=(8, 5))

    for n_idx, n in enumerate(ns):
        color = cmap(n_idx)
        for mode in modes:
            key = (n, mode, metric)
            rates, _, _, m_lat, s_lat = series_for_key(data, key)
            if len(rates) == 0:
                continue
            ax.errorbar(
                rates, m_lat, yerr=s_lat,
                label=f"N={n} {mode.upper()}",
                color=color,
                linestyle=ls_map.get(mode, "-"),
                marker=mk_map.get(mode, "o"),
                linewidth=1.5,
                capsize=3,
            )

    metric_label = "End-to-end" if metric == "execution" else "Consensus"
    style_ax(ax, "Injection Rate (tx/s)", f"{metric_label} Latency (ms)",
             "Latency vs Injection Rate")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate L-curve throughput-vs-latency plot from tput_latency.csv"
    )
    parser.add_argument("--csv",     type=Path, default=DEFAULT_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"ERROR: CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    data = load_csv(args.csv)
    if not data:
        print("ERROR: no data parsed from CSV", file=sys.stderr)
        sys.exit(1)

    # Main L-curve using end-to-end (execution) metrics
    plot_l_curve(data, args.out_dir / "tput_vs_latency.png", metric="execution")

    # Same L-curve but for the consensus (block certification) layer
    plot_l_curve(data, args.out_dir / "tput_vs_latency_consensus.png",
                 metric="consensus")

    # Diagnostic: throughput vs injection rate (shows saturation point)
    plot_tput_vs_rate(data, args.out_dir / "tput_vs_rate.png", metric="execution")

    # Diagnostic: latency vs injection rate (shows where latency spikes)
    plot_lat_vs_rate(data, args.out_dir / "lat_vs_rate.png", metric="execution")

    print("\nAll plots saved in:", args.out_dir)


if __name__ == "__main__":
    main()
