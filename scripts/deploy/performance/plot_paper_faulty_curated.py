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

PROJ_ROOT  = Path(__file__).resolve().parents[3]
FAULTY_DIR = PROJ_ROOT / "das_results" / "faulty"
OUT_DIR    = PROJ_ROOT / "das_results" / "paper_plots" / "curated"

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
    "pompe":       dict(color="#2E7D32",       marker="D", linestyle="-", label="Pompe"),
    "themis":      dict(color="#FBC02D",       marker="^", linestyle="-", label="Themis"),
}
SERIES_ORDER = ["pearl-ol", "pearl-bof", "fairdag-ol", "fairdag-bof", "pompe", "themis"]

# Each series maps to (csv_filename, mode_filter). mode_filter=None matches
# any row (baselines have a single mode and the column carries no extra info).
SERIES_SOURCE = {
    "pearl-ol":    ("pearl.csv",       "ol"),
    "pearl-bof":   ("pearl.csv",       "bof"),
    "fairdag-ol":  ("fairdag-ol.csv",  None),
    "fairdag-bof": ("fairdag-bof.csv", None),
    "pompe":       ("pompe.csv",       None),
    "themis":      ("themis.csv",      None),
}

FULL_W  = 7.0
PANEL_H = 4.5

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


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _save(fig, stem):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    png = OUT_DIR / f"{stem}.png"
    fig.savefig(png)
    plt.close(fig)
    print(f"  wrote {png}")


def plot_metric(metric: str, ylabel: str, out_stem: str):
    """metric: 'tps' or 'lat'."""
    fig, ax = plt.subplots(figsize=(FULL_W, PANEL_H))
    any_data = False
    for system in SERIES_ORDER:
        fname, mode = SERIES_SOURCE[system]
        agg = aggregate(load_csv(FAULTY_DIR / fname, mode_filter=mode))
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
    ax.set_ylabel(ylabel)
    ax.set_xticks([1, 2, 3, 4, 5])
    ax.set_yscale("log")
    ax.legend(loc="best", framealpha=0.9)
    # Manual margins — avoid tight_layout/get_tightbbox which trips the
    # np.Inf removal in newer numpy on this env's matplotlib 3.3.
    fig.subplots_adjust(left=0.13, right=0.97, bottom=0.13, top=0.95)
    _save(fig, out_stem)


def main():
    print("Plotting curated faulty-node figures...")
    plot_metric("lat", "Latency (ms, log)",
                "pearl_vs_baselines_lat_faults")
    plot_metric("tps", "Throughput (tx/s, log)",
                "pearl_vs_baselines_tput_faults")
    print("Done.")


if __name__ == "__main__":
    main()
