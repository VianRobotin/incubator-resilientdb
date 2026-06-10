#!/usr/bin/env python3
"""
plot_pearl_bof_3f1_compare.py — Committee-size control for the BOF path:
Pearl-BOF run unchanged at 2f+1 vs 3f+1 at a single fault level f, on a
single latency-vs-throughput panel (no title).

Series (Pearl only — the Themis / FairDAG-RL baselines are discussed in the
paper text but deliberately left off this control figure):
    pearl-bof            das_results/tput_latency.csv           mode=bof  (2f+1)
    pearl-bof (3f+1)     das_results/tput_latency_pearl_3f1.csv mode=bof  (3f+1)

Two files are written per run, identical content on different axis scales:
    <out>_f{f}_log.pdf       log axes (rate log; tput-vs-lat log-log)
    <out>_f{f}_linear.pdf    linear axes

Usage:
    python3 plot_pearl_bof_3f1_compare.py [--f F [F ...]] [--metric exec|consensus]

By default it plots f = 3, 5, 7 (one log + one linear file each). The Pearl-BOF
(3f+1) curve at a given f needs its n = 3f+1 sweep to be present
(n=10/16/22 for f=3/5/7); any series with no rows at an f is skipped with a
warning, so this is safe to re-run as the 3f+1 sweep fills in.
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ---------------------------------------------------------------------------
# Paths (resolved relative to this script, like plot_paper.py).
# ---------------------------------------------------------------------------
PROJ_ROOT     = Path(__file__).resolve().parents[3]
PEARL_CSV     = PROJ_ROOT / "das_results" / "tput_latency.csv"
PEARL_3F1_CSV = PROJ_ROOT / "das_results" / "tput_latency_pearl_3f1.csv"
BASELINES_DIR = PROJ_ROOT / "das_results" / "baselines"
OUT_DIR       = PROJ_ROOT / "das_results" / "paper_plots"

# Pearl (3f+1) n per f -- this CSV uses a 3f+1 committee (n = 3f+1).
PEARL_3F1_N_FOR_F = {3: 10, 5: 16, 7: 22}

# ---------------------------------------------------------------------------
# Visual identity. Pearl-BOF stays amber (matching plot_paper.py); the 3f+1
# variant is a distinct dark red, dashed, so the same protocol at a larger
# committee reads as a sibling rather than a separate system.
# ---------------------------------------------------------------------------
SERIES_STYLE = {
    "pearl-bof":     dict(color="#E65100", marker="s", linestyle="-",  label="Pearl-BOF (2f+1)"),
    "pearl-bof-3f1": dict(color="#B71C1C", marker="D", linestyle="--", label="Pearl-BOF (3f+1)"),
}
# Only the two Pearl committee sizes are plotted; the Themis / FairDAG-RL
# baselines are discussed in the text but kept off this control figure so the
# committee-size effect reads cleanly.
SERIES_ORDER = ["pearl-bof", "pearl-bof-3f1"]

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
# CSV loading -- one per_rate dict {rate -> {"tps": [...], "lat_ms": [...]}}
# per series, already filtered to the requested f.
# ---------------------------------------------------------------------------
def _empty_per_rate():
    return defaultdict(lambda: {"tps": [], "lat_ms": []})


def _tps_lat_cols(metric):
    if metric == "consensus":
        return "consensus_tps", "consensus_latency_ms"
    return "execution_tps", "execution_latency_ms"


def load_pearl(path, want_n, metric):
    """Pearl rate-sweep CSV (no leading 'baseline' column), mode=bof, n==want_n."""
    tps_col, lat_col = _tps_lat_cols(metric)
    per_rate = _empty_per_rate()
    if not path.exists():
        return per_rate
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                if int(row["n"]) != want_n or row["mode"].strip() != "bof":
                    continue
                rate = int(row["input_rate"])
                tps = float(row[tps_col])
                lat = float(row[lat_col])
            except (KeyError, ValueError):
                continue
            per_rate[rate]["tps"].append(tps)
            per_rate[rate]["lat_ms"].append(lat)
    return per_rate


def load_baseline(path, want_f, metric):
    """Baseline CSV (leading 'baseline' column, explicit 'f' column)."""
    tps_col, lat_col = _tps_lat_cols(metric)
    per_rate = _empty_per_rate()
    if not path.exists():
        return per_rate
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                if int(row["f"]) != want_f:
                    continue
                rate = int(row["input_rate"])
                tps_raw, lat_raw = row.get(tps_col, ""), row.get(lat_col, "")
                tps = float(tps_raw) if tps_raw else 0.0
                lat = float(lat_raw) if lat_raw else 0.0
            except (KeyError, ValueError):
                continue
            per_rate[rate]["tps"].append(tps)
            per_rate[rate]["lat_ms"].append(lat)
    return per_rate


# ---------------------------------------------------------------------------
# Aggregation -- trimmed mean/std per rate (drop 1 high + 1 low like plot_paper).
# ---------------------------------------------------------------------------
def _trimmed_stats(values, drop=1):
    v = sorted(x for x in values if x > 0)
    if len(v) > 2 * drop:
        v = v[drop:-drop]
    if not v:
        return 0.0, 0.0
    a = np.array(v, dtype=float)
    return float(a.mean()), float(a.std(ddof=0))


def series_arrays(per_rate):
    rates = sorted(per_rate.keys())
    out_rates, m_tps, s_tps, m_lat, s_lat = [], [], [], [], []
    for r in rates:
        mt, st = _trimmed_stats(per_rate[r]["tps"])
        ml, sl = _trimmed_stats(per_rate[r]["lat_ms"])
        if mt > 0 or ml > 0:
            out_rates.append(r)
            m_tps.append(mt); s_tps.append(st)
            m_lat.append(ml); s_lat.append(sl)
    return (np.array(out_rates), np.array(m_tps), np.array(s_tps),
            np.array(m_lat), np.array(s_lat))


# ---------------------------------------------------------------------------
# Figure
# ---------------------------------------------------------------------------
def build_figure(data, f, log_scale, metric):
    fig, ax_lt = plt.subplots(1, 1, figsize=(6.4, 4.8))

    for key in SERIES_ORDER:
        rates, m_tps, s_tps, m_lat, s_lat = data[key]
        if len(rates) == 0:
            continue
        st = SERIES_STYLE[key]
        # Latency vs *achieved* throughput (the L-T / hockey-stick curve).
        # x is delivered load, not offered rate, so the curves are compared at
        # equal work -- this removes the saturation crossover that appears when
        # both systems are pinned at their ceiling at a fixed offered rate.
        # Sort by throughput so the connecting line reads left-to-right.
        order = np.argsort(m_tps)
        ax_lt.errorbar(m_tps[order], m_lat[order],
                       yerr=s_lat[order], xerr=s_tps[order],
                       color=st["color"], marker=st["marker"],
                       linestyle=st["linestyle"], label=st["label"], capsize=3)

    ax_lt.set_xlabel("Throughput (tx/s)")
    ax_lt.set_ylabel("Latency (ms)")

    if log_scale:
        ax_lt.set_xscale("log")
        ax_lt.set_yscale("log")
    else:
        ax_lt.set_xlim(left=0)
        # Only the two Pearl curves remain, so let the y-axis autoscale to them.
        ax_lt.set_ylim(bottom=0)

    handles, labels = ax_lt.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels),
               frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return fig


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--f", type=int, nargs="+", default=[3, 5, 7], choices=(3, 5, 7),
                    help="fault level(s) to plot; one log+linear pair per f "
                         "(default: 3 5 7). A series with no rows at a given f is "
                         "skipped with a warning -- handy while the 3f+1 sweep "
                         "(n=16 for f=5, n=22 for f=7) is still filling in.")
    ap.add_argument("--metric", choices=("exec", "consensus"), default="exec",
                    help="which throughput/latency columns to use (default: exec)")
    ap.add_argument("--out", default=None,
                    help="output stem (default: das_results/paper_plots/"
                         "pearl_bof_3f1_compare)")
    args = ap.parse_args()

    out_stem = Path(args.out) if args.out else (OUT_DIR / "pearl_bof_3f1_compare")
    out_stem.parent.mkdir(parents=True, exist_ok=True)

    for f in args.f:
        pearl_2f1_n = 2 * f + 1
        pearl_3f1_n = PEARL_3F1_N_FOR_F[f]

        data = {
            "pearl-bof":     series_arrays(load_pearl(PEARL_CSV, pearl_2f1_n, args.metric)),
            "pearl-bof-3f1": series_arrays(load_pearl(PEARL_3F1_CSV, pearl_3f1_n, args.metric)),
        }

        print(f"f = {f}  (pearl 2f+1 n={pearl_2f1_n}, pearl 3f+1 n={pearl_3f1_n})")
        for key in SERIES_ORDER:
            n_pts = len(data[key][0])
            print(f"  {SERIES_STYLE[key]['label']:<20} {n_pts} rate points")
            if n_pts == 0:
                print(f"    WARNING: no data for {key} at f={f}")

        for log_scale, tag in ((True, "log"), (False, "linear")):
            fig = build_figure(data, f, log_scale, args.metric)
            out = Path(f"{out_stem}_f{f}_{tag}.pdf")
            fig.savefig(out, bbox_inches="tight")
            plt.close(fig)
            print(f"  wrote {out}")


if __name__ == "__main__":
    main()
