#!/usr/bin/env python3
"""
plot_paper_faulty_curated.py — Silent-fault sweep figures for the paper.

Reads das_results/faulty/{pearl,fairdag-ol,fairdag-bof,pompe,themis}.csv and
emits two figures into das_results/paper_plots/curated/:

  pearl_vs_baselines_lat_faults.png
      Execution latency (ms, log) vs number of silent faulty replicas.

  pearl_vs_baselines_tput_faults.png
      Execution throughput (tx/s, log) vs number of silent faulty replicas.

Series styling matches plot_paper.py exactly: same colours, markers, and
display labels (Pearl, Pompe, Themis, FairDAG-AB, FairDAG-RL).
"""

import csv
import os
from collections import defaultdict
from pathlib import Path

import numpy as np
if not hasattr(np, "Inf"):
    np.Inf = np.inf

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

LINEAR = os.environ.get("PLOT_LINEAR") == "1"
_OUT_SUBDIR = "curated_linear" if LINEAR else "curated"

PROJ_ROOT  = Path(__file__).resolve().parents[3]
FAULTY_DIR = PROJ_ROOT / "das_results" / "faulty"
OUT_DIR    = PROJ_ROOT / "das_results" / "paper_plots" / _OUT_SUBDIR

# The x=0 (no-fault) reference comes from the rate sweep at the same
# configuration the faulty sweep used: f=5 (Pearl n=11, baselines 3f+1=16/22),
# rate=500 tx/s. Lets each line start at zero faults instead of one.
PEARL_RATE_CSV     = PROJ_ROOT / "das_results" / "tput_latency.csv"
BASELINE_RATE_DIR  = PROJ_ROOT / "das_results" / "baselines"
ZERO_FAULT_RATE       = 500
ZERO_FAULT_PEARL_N    = 11
ZERO_FAULT_BASELINE_F = 5


def _log_label(text):
    return text if not LINEAR else text.replace(", log", "").replace(" (log)", "")

# ---------------------------------------------------------------------------
# Visual identity — kept identical to plot_paper.py
# ---------------------------------------------------------------------------

PEARL_OL_COLOR  = "#0D47A1"
PEARL_BOF_COLOR = "#E65100"

SERIES_STYLE = {
    "pearl-ol":    dict(color=PEARL_OL_COLOR,  marker="o", linestyle="-", label="Pearl (OL)"),
    "pearl-bof":   dict(color=PEARL_BOF_COLOR, marker="s", linestyle="-", label="Pearl (BOF)"),
    "fairdag-ol":  dict(color="#6A1B9A",       marker="v", linestyle="-", label="FairDAG-AB"),
    "fairdag-bof": dict(color="#C2185B",       marker="P", linestyle="-", label="FairDAG-RL"),
    "tusk":        dict(color="#00838F",       marker="X", linestyle="-", label="Tusk"),
    "pompe":       dict(color="#2E7D32",       marker="D", linestyle="-", label="Pompe"),
    "themis":      dict(color="#FBC02D",       marker="^", linestyle="-", label="Themis"),
}
SERIES_ORDER = ["pearl-ol", "pearl-bof", "fairdag-ol", "fairdag-bof", "tusk", "pompe", "themis"]

# Each series maps to (csv_filename, mode_filter). mode_filter=None matches
# any row (baselines have a single mode and the column carries no extra info).
SERIES_SOURCE = {
    "pearl-ol":    ("pearl.csv",       "ol"),
    "pearl-bof":   ("pearl.csv",       "bof"),
    "fairdag-ol":  ("fairdag-ol.csv",  None),
    "fairdag-bof": ("fairdag-bof.csv", None),
    "tusk":        ("tusk.csv",        None),
    "pompe":       ("pompe.csv",       None),
    "themis":      ("themis.csv",      None),
}

FULL_W  = 7.0
PANEL_H = 4.5

# Combined 1x2 panel (throughput | latency) sized for an IEEE full-width
# figure*: one shared legend on top, both panels rendered at \textwidth.
COMBINED_W = 11.5
COMBINED_H = 4.8

plt.rcParams.update({
    "font.size":        11.0,
    "axes.titlesize":   13.0,
    "axes.labelsize":   12.0,
    "xtick.labelsize":  10.0,
    "ytick.labelsize":  10.0,
    "legend.fontsize":  10.0,
    "lines.linewidth":  1.8,
    "lines.markersize": 6.0,
    "axes.grid":        True,
    "grid.linestyle":   "--",
    "grid.alpha":       0.35,
    "savefig.dpi":      200,
})


# ---------------------------------------------------------------------------
# Load + aggregate
# ---------------------------------------------------------------------------

def load_csv(path: Path, mode_filter=None):
    """{silent_faults: [(exec_tps, exec_latency_ms), ...]}, skipping zero rows.
    If mode_filter is set, only rows with matching `mode` column are kept."""
    points = defaultdict(list)
    if not path.exists():
        return points
    with path.open(newline="") as fh:
        for row in csv.DictReader(fh):
            if mode_filter is not None and row.get("mode", "").strip() != mode_filter:
                continue
            try:
                sf  = int(row["silent_faults"])
                tps = float(row["execution_tps"])
                lat = float(row["execution_latency_ms"])
            except (KeyError, ValueError):
                continue
            if tps <= 0 and lat <= 0:
                continue
            points[sf].append((tps, lat))
    return points


def aggregate(points):
    """Sorted (f, mean_tps, std_tps, mean_lat, std_lat) tuples."""
    out = []
    for f in sorted(points):
        pts = points[f]
        if not pts:
            continue
        tps = np.array([p[0] for p in pts], dtype=float)
        lat = np.array([p[1] for p in pts], dtype=float)
        out.append((f, tps.mean(), tps.std(ddof=0), lat.mean(), lat.std(ddof=0)))
    return out


def load_zero_fault(system):
    """No-fault (silent_faults=0) samples for `system`, pulled from the rate
    sweep at the faulty-sweep configuration (f=5, rate=500)."""
    fname, mode = SERIES_SOURCE[system]
    pts = []
    if system.startswith("pearl"):
        path = PEARL_RATE_CSV
        if not path.exists():
            return pts
        with path.open(newline="") as fh:
            for row in csv.DictReader(fh):
                try:
                    if int(row["n"]) != ZERO_FAULT_PEARL_N:
                        continue
                    if int(row["input_rate"]) != ZERO_FAULT_RATE:
                        continue
                    if row.get("mode", "").strip() != mode:
                        continue
                    tps = float(row["execution_tps"])
                    lat = float(row["execution_latency_ms"])
                except (KeyError, ValueError):
                    continue
                if tps <= 0 and lat <= 0:
                    continue
                pts.append((tps, lat))
    else:
        path = BASELINE_RATE_DIR / fname
        if not path.exists():
            return pts
        with path.open(newline="") as fh:
            for row in csv.DictReader(fh):
                try:
                    if int(row["f"]) != ZERO_FAULT_BASELINE_F:
                        continue
                    if int(row["input_rate"]) != ZERO_FAULT_RATE:
                        continue
                    tps = float(row["execution_tps"])
                    lat = float(row["execution_latency_ms"])
                except (KeyError, ValueError):
                    continue
                if tps <= 0 and lat <= 0:
                    continue
                pts.append((tps, lat))
    return pts


def aggregated_series(system):
    """Per-fault aggregates for `system`, including the x=0 no-fault point."""
    fname, mode = SERIES_SOURCE[system]
    points = load_csv(FAULTY_DIR / fname, mode_filter=mode)
    zf = load_zero_fault(system)
    if zf:
        points[0] = zf
    return aggregate(points)


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _save(fig, stem):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pdf = OUT_DIR / f"{stem}.pdf"
    fig.savefig(pdf)
    plt.close(fig)
    print(f"  wrote {pdf}")


def plot_metric(metric: str, ylabel: str, out_stem: str):
    """metric: 'tps' or 'lat'."""
    fig, ax = plt.subplots(figsize=(FULL_W, PANEL_H))
    any_data = False
    for system in SERIES_ORDER:
        agg = aggregated_series(system)
        if not agg:
            continue
        xs   = [t[0] for t in agg]
        if metric == "tps":
            ys   = [t[1] for t in agg]
            errs = [t[2] for t in agg]
        else:
            ys   = [t[3] for t in agg]
            errs = [t[4] for t in agg]
        st = SERIES_STYLE[system]
        ax.errorbar(xs, ys, yerr=errs,
                    color=st["color"], marker=st["marker"],
                    linestyle=st["linestyle"], label=st["label"],
                    capsize=3)
        any_data = True

    if not any_data:
        print(f"  no data for {out_stem}; skipping")
        plt.close(fig)
        return

    ax.set_xlabel("Number of silent faulty replicas")
    ax.set_ylabel(_log_label(ylabel))
    ax.set_xticks([0, 1, 2, 3, 4, 5])
    if not LINEAR:
        ax.set_yscale("log")
    ax.legend(loc="best", framealpha=0.9)
    # Manual margins — avoid tight_layout/get_tightbbox which trips the
    # np.Inf removal in newer numpy on this env's matplotlib 3.3.
    fig.subplots_adjust(left=0.13, right=0.97, bottom=0.13, top=0.95)
    _save(fig, out_stem)


def plot_combined(out_stem: str):
    """Throughput | latency side by side in a single full-width figure with
    one shared legend on top — the version that goes in the paper."""
    fig, (ax_t, ax_l) = plt.subplots(1, 2, figsize=(COMBINED_W, COMBINED_H))
    any_data = False
    for ax, metric, ylabel in ((ax_t, "tps", "Throughput (tx/s, log)"),
                               (ax_l, "lat", "Latency (ms, log)")):
        for system in SERIES_ORDER:
            agg = aggregated_series(system)
            if not agg:
                continue
            xs = [t[0] for t in agg]
            if metric == "tps":
                ys, errs = [t[1] for t in agg], [t[2] for t in agg]
            else:
                ys, errs = [t[3] for t in agg], [t[4] for t in agg]
            st = SERIES_STYLE[system]
            ax.errorbar(xs, ys, yerr=errs,
                        color=st["color"], marker=st["marker"],
                        linestyle=st["linestyle"], label=st["label"], capsize=3)
            any_data = True
        ax.set_xlabel("Number of silent faulty replicas")
        ax.set_ylabel(_log_label(ylabel))
        ax.set_xticks([0, 1, 2, 3, 4, 5])
        if not LINEAR:
            ax.set_yscale("log")

    if not any_data:
        print(f"  no data for {out_stem}; skipping")
        plt.close(fig)
        return

    handles, labels = ax_t.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels),
               framealpha=0.9, bbox_to_anchor=(0.5, 1.0),
               columnspacing=1.1, handletextpad=0.4)
    fig.subplots_adjust(left=0.07, right=0.99, bottom=0.13, top=0.83, wspace=0.20)
    _save(fig, out_stem)


def main():
    print("Plotting curated faulty-node figures...")
    # Combined figure used in the paper (throughput | latency, shared legend).
    plot_combined("pearl_vs_baselines_faults")
    # Single-metric variants kept for backwards compatibility / other uses.
    plot_metric("lat", "Latency (ms, log)",
                "pearl_vs_baselines_lat_faults")
    plot_metric("tps", "Throughput (tx/s, log)",
                "pearl_vs_baselines_tput_faults")
    print("Done.")


if __name__ == "__main__":
    main()
