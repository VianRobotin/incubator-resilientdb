#!/usr/bin/env python3
"""
plot_paper.py — Generate the curated set of paper figures for the
"Fair Ordering with TEEs" thesis (system name: Pearl).

Outputs PNG to das_results/paper_plots/curated/:

  pearl_scaling_l_curve.png
      Full-width 1x3: per-N L-curve (N=7, N=11, N=15), Pearl OL vs BOF,
      execution latency only. Story: 2f+1 sustains throughput as N grows.

  pearl_lat_vs_rate.png
      Full-width 1x3: per-N latency vs injection rate, OL vs BOF.
      Story: latency response across the rate sweep, including
      post-saturation behaviour.

  pearl_vs_baselines.png
      Full-width 1x3: per-f overlay (f=3, f=5, f=7), Pearl OL+BOF vs
      Pompe / Themis / FairDAG-OL / FairDAG-BOF, execution latency on a
      log-scale y-axis. Story: Pearl dominates at every f.

  pearl_saturation.png
      Full-width 1x3: per-N throughput vs injection rate, OL vs BOF, with
      a y=x reference per panel. Story: linear up to ~10k tx/s, then a
      saturation knee whose location shifts with N.

These four plots are the locked paper set as of 2026-05-04. The existing
scripts (plot.py, plot_baselines.py) are unchanged and keep producing the
broader plot set in das_results/plots/ and das_results/baselines/plots/.
"""

import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ---------------------------------------------------------------------------
# Paths -- resolved relative to this script so it runs on WSL and DAS5 alike.
# ---------------------------------------------------------------------------

LINEAR = os.environ.get("PLOT_LINEAR") == "1"
_OUT_SUBDIR = "curated_linear" if LINEAR else "curated"

PROJ_ROOT      = Path(__file__).resolve().parents[3]
PEARL_CSV      = PROJ_ROOT / "das_results" / "tput_latency.csv"
BASELINES_DIR  = PROJ_ROOT / "das_results" / "baselines"
OUT_DIR        = PROJ_ROOT / "das_results" / "paper_plots" / _OUT_SUBDIR


def _log_label(text):
    return text if not LINEAR else text.replace(", log", "").replace(" (log)", "")

# ---------------------------------------------------------------------------
# Visual identity for the paper. One palette, used consistently across figs.
# ---------------------------------------------------------------------------

# Pearl has two modes that always need to be distinguishable; give each
# a fully distinct colour, not just a linestyle. OL = deep navy, BOF = amber.
PEARL_OL_COLOR  = "#0D47A1"
PEARL_BOF_COLOR = "#E65100"

SERIES_STYLE = {
    "pearl-ol":    dict(color=PEARL_OL_COLOR,  marker="o", linestyle="-",  label="Pearl (OL)"),
    "pearl-bof":   dict(color=PEARL_BOF_COLOR, marker="s", linestyle="-",  label="Pearl (BOF)"),
    "pompe":       dict(color="#2E7D32",       marker="D", linestyle="-",  label="Pompe"),
    "themis":      dict(color="#FBC02D",       marker="^", linestyle="-",  label="Themis"),
    "fairdag-ol":  dict(color="#6A1B9A",       marker="v", linestyle="-",  label="FairDAG-AB"),
    "fairdag-bof": dict(color="#C2185B",       marker="P", linestyle="-",  label="FairDAG-RL"),
}

# Bigger sizes than IEEE-minimum so individual elements are easy to read.
FULL_W  = 11.0  # inches; cross-column 1x3 figures
PANEL_H = 4.0

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
# CSV loading
# ---------------------------------------------------------------------------

def _trimmed_stats(values, drop=1):
    v = sorted(x for x in values if x > 0)
    if len(v) > 2 * drop:
        v = v[drop:-drop]
    if not v:
        return 0.0, 0.0
    a = np.array(v, dtype=float)
    return float(a.mean()), float(a.std(ddof=0))


def _series_sorted_by_rate(per_rate):
    rates = sorted(per_rate.keys())
    out_rates, m_tps, s_tps, m_lat, s_lat = [], [], [], [], []
    for r in rates:
        d = per_rate[r]
        mt, st = _trimmed_stats(d["tps"])
        ml, sl = _trimmed_stats(d["lat_ms"])
        if mt > 0 or ml > 0:
            out_rates.append(r)
            m_tps.append(mt); s_tps.append(st)
            m_lat.append(ml); s_lat.append(sl)
    return (np.array(out_rates), np.array(m_tps), np.array(s_tps),
            np.array(m_lat), np.array(s_lat))


def load_pearl_by_n(path):
    """Pearl-TEE results indexed by (n, mode) -> per_rate dict (execution only)."""
    out = defaultdict(lambda: defaultdict(lambda: {"tps": [], "lat_ms": []}))
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                n = int(row["n"])
                mode = row["mode"].strip()
                rate = int(row["input_rate"])
                tps = float(row["execution_tps"])
                lat = float(row["execution_latency_ms"])
            except (KeyError, ValueError):
                continue
            out[(n, mode)][rate]["tps"].append(tps)
            out[(n, mode)][rate]["lat_ms"].append(lat)
    return out


def load_pearl_by_f(path):
    """Pearl-TEE results indexed by (f, mode) -- f = (n-1)//2 since 2f+1."""
    out = defaultdict(lambda: defaultdict(lambda: {"tps": [], "lat_ms": []}))
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                n = int(row["n"])
                mode = row["mode"].strip()
                rate = int(row["input_rate"])
                tps = float(row["execution_tps"])
                lat = float(row["execution_latency_ms"])
            except (KeyError, ValueError):
                continue
            fault = (n - 1) // 2
            out[(fault, mode)][rate]["tps"].append(tps)
            out[(fault, mode)][rate]["lat_ms"].append(lat)
    return out


def load_baseline_by_f(path):
    """Baseline CSV indexed by (f, mode)."""
    out = defaultdict(lambda: defaultdict(lambda: {"tps": [], "lat_ms": []}))
    if not path.exists():
        return out
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                fault = int(row["f"])
                mode = row["mode"].strip()
                rate = int(row["input_rate"])
                tps_raw = row.get("execution_tps", "")
                lat_raw = row.get("execution_latency_ms", "")
                tps = float(tps_raw) if tps_raw else 0.0
                lat = float(lat_raw) if lat_raw else 0.0
            except (KeyError, ValueError):
                continue
            out[(fault, mode)][rate]["tps"].append(tps)
            out[(fault, mode)][rate]["lat_ms"].append(lat)
    return out


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

def _draw_pearl_panel(ax, pearl_data, n, show_legend=True):
    """One panel of the scaling L-curve: Pearl OL vs BOF for a single N.
    OL and BOF use distinct colours so they never blur together."""
    for mode, color in (("ol", PEARL_OL_COLOR), ("bof", PEARL_BOF_COLOR)):
        st = SERIES_STYLE[f"pearl-{mode}"]
        per_rate = pearl_data.get((n, mode))
        if not per_rate:
            continue
        rates, m_tps, s_tps, m_lat, s_lat = _series_sorted_by_rate(per_rate)
        if len(rates) == 0:
            continue
        ax.errorbar(m_tps, m_lat, xerr=s_tps, yerr=s_lat,
                    color=color, marker=st["marker"], linestyle=st["linestyle"],
                    label=st["label"], capsize=3)
    ax.set_title(f"N = {n}  (f = {(n - 1) // 2})")
    ax.set_xlabel("Throughput (tx/s)")
    ax.set_ylabel("Latency (ms)")
    if show_legend:
        ax.legend(loc="upper right", framealpha=0.9)


def _draw_lat_vs_rate_panel(ax, pearl_data, n, show_legend=True):
    """One panel of latency vs injection rate: OL vs BOF for a single N."""
    for mode, color in (("ol", PEARL_OL_COLOR), ("bof", PEARL_BOF_COLOR)):
        st = SERIES_STYLE[f"pearl-{mode}"]
        per_rate = pearl_data.get((n, mode))
        if not per_rate:
            continue
        rates, _m_tps, _s_tps, m_lat, s_lat = _series_sorted_by_rate(per_rate)
        if len(rates) == 0:
            continue
        ax.errorbar(rates, m_lat, yerr=s_lat,
                    color=color, marker=st["marker"], linestyle=st["linestyle"],
                    label=st["label"], capsize=3)
    if not LINEAR:
        ax.set_xscale("log")
    ax.set_title(f"N = {n}  (f = {(n - 1) // 2})")
    ax.set_xlabel("Injection rate (tx/s)")
    ax.set_ylabel("Latency (ms)")
    if show_legend:
        ax.legend(loc="upper left", framealpha=0.9)


def _draw_saturation_panel(ax, pearl_data, n, show_legend=True):
    """One panel of the saturation diagnostic: throughput vs injection rate,
    OL vs BOF for a single N, with a y=x reference line."""
    max_rate = 0
    for mode, color in (("ol", PEARL_OL_COLOR), ("bof", PEARL_BOF_COLOR)):
        st = SERIES_STYLE[f"pearl-{mode}"]
        per_rate = pearl_data.get((n, mode))
        if not per_rate:
            continue
        rates, m_tps, s_tps, _, _ = _series_sorted_by_rate(per_rate)
        if len(rates) == 0:
            continue
        ax.errorbar(rates, m_tps, yerr=s_tps,
                    color=color, marker=st["marker"], linestyle=st["linestyle"],
                    label=st["label"], capsize=3)
        if len(rates) > 0:
            max_rate = max(max_rate, float(rates.max()))
    if max_rate > 0:
        lim = max_rate * 1.05
        ax.plot([0, lim], [0, lim], color="black", linestyle=":",
                linewidth=1.2, alpha=0.5, label="y = x")
    ax.set_title(f"N = {n}  (f = {(n - 1) // 2})")
    ax.set_xlabel("Injection rate (tx/s)")
    ax.set_ylabel("Throughput (tx/s)")
    if show_legend:
        ax.legend(loc="upper left", framealpha=0.9)


def _draw_overlay_panel(ax, sources, fault, show_legend=False):
    """One panel of the baselines overlay: all sources at a given f, log-scale y."""
    for key in ("pearl-ol", "pearl-bof",
                "pompe", "themis", "fairdag-ol", "fairdag-bof"):
        per_rate = sources.get(key, {}).get(fault)
        if not per_rate:
            continue
        rates, m_tps, s_tps, m_lat, s_lat = _series_sorted_by_rate(per_rate)
        if len(rates) == 0:
            continue
        st = SERIES_STYLE[key]
        ax.errorbar(m_tps, m_lat, xerr=s_tps, yerr=s_lat,
                    color=st["color"], marker=st["marker"],
                    linestyle=st["linestyle"], label=st["label"],
                    capsize=2.5)
    if not LINEAR:
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_ylim(1e2, 1e5)
    ax.set_title(f"f = {fault}")
    ax.set_xlabel(_log_label("Throughput (tx/s, log)"))
    ax.set_ylabel(_log_label("Latency (ms, log)"))
    if show_legend:
        ax.legend(loc="upper right", framealpha=0.9, ncol=1)


def _draw_overlay_tput_vs_rate_panel(ax, sources, fault, show_legend=False):
    """Throughput vs injection rate, all sources for a given f, log axes.
    Log y-axis because Pompe collapses to <10 tx/s at f >= 5 while Pearl
    sustains ~10k tx/s at the same point."""
    for key in ("pearl-ol", "pearl-bof",
                "pompe", "themis", "fairdag-ol", "fairdag-bof"):
        per_rate = sources.get(key, {}).get(fault)
        if not per_rate:
            continue
        rates, m_tps, s_tps, _, _ = _series_sorted_by_rate(per_rate)
        if len(rates) == 0:
            continue
        st = SERIES_STYLE[key]
        ax.errorbar(rates, m_tps, yerr=s_tps,
                    color=st["color"], marker=st["marker"],
                    linestyle=st["linestyle"], label=st["label"],
                    capsize=2.5)
    if not LINEAR:
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_ylim(1e0, 2e4)
    ax.set_title(f"f = {fault}")
    ax.set_xlabel("Injection rate (tx/s)")
    ax.set_ylabel(_log_label("Throughput (tx/s, log)"))
    if show_legend:
        ax.legend(loc="lower right", framealpha=0.9, ncol=1)


def _draw_overlay_lat_vs_rate_panel(ax, sources, fault, show_legend=False):
    """Latency vs injection rate, all sources for a given f, log y."""
    for key in ("pearl-ol", "pearl-bof",
                "pompe", "themis", "fairdag-ol", "fairdag-bof"):
        per_rate = sources.get(key, {}).get(fault)
        if not per_rate:
            continue
        rates, _, _, m_lat, s_lat = _series_sorted_by_rate(per_rate)
        if len(rates) == 0:
            continue
        st = SERIES_STYLE[key]
        ax.errorbar(rates, m_lat, yerr=s_lat,
                    color=st["color"], marker=st["marker"],
                    linestyle=st["linestyle"], label=st["label"],
                    capsize=2.5)
    if not LINEAR:
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_ylim(1e2, 1e5)
    ax.set_title(f"f = {fault}")
    ax.set_xlabel("Injection rate (tx/s)")
    ax.set_ylabel(_log_label("Latency (ms, log)"))
    if show_legend:
        ax.legend(loc="upper right", framealpha=0.9, ncol=1)


# ---------------------------------------------------------------------------
# Top-level figure builders
# ---------------------------------------------------------------------------

def fig_scaling_l_curve(pearl_data, out_stem):
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, PANEL_H + 0.6), sharey=True)
    for ax, n in zip(axes, (7, 11, 15)):
        _draw_pearl_panel(ax, pearl_data, n, show_legend=False)
    axes[1].set_ylabel("")
    axes[2].set_ylabel("")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels),
               frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    _save(fig, out_stem)


def fig_lat_vs_rate(pearl_data, out_stem):
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, PANEL_H + 0.6), sharey=True)
    for ax, n in zip(axes, (7, 11, 15)):
        _draw_lat_vs_rate_panel(ax, pearl_data, n, show_legend=False)
    axes[1].set_ylabel("")
    axes[2].set_ylabel("")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels),
               frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    _save(fig, out_stem)


def fig_overlay(sources, out_stem):
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, PANEL_H + 0.7), sharey=True)
    for ax, fault in zip(axes, (3, 5, 7)):
        _draw_overlay_panel(ax, sources, fault, show_legend=False)
    axes[1].set_ylabel("")
    axes[2].set_ylabel("")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels),
               frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=[0, 0, 1, 0.90])
    _save(fig, out_stem)


def fig_overlay_tput_vs_rate(sources, out_stem):
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, PANEL_H + 0.7), sharey=True)
    for ax, fault in zip(axes, (3, 5, 7)):
        _draw_overlay_tput_vs_rate_panel(ax, sources, fault, show_legend=False)
    axes[1].set_ylabel("")
    axes[2].set_ylabel("")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels),
               frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=[0, 0, 1, 0.90])
    _save(fig, out_stem)


def fig_overlay_lat_vs_rate(sources, out_stem):
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, PANEL_H + 0.7), sharey=True)
    for ax, fault in zip(axes, (3, 5, 7)):
        _draw_overlay_lat_vs_rate_panel(ax, sources, fault, show_legend=False)
    axes[1].set_ylabel("")
    axes[2].set_ylabel("")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels),
               frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=[0, 0, 1, 0.90])
    _save(fig, out_stem)


def fig_saturation(pearl_data, out_stem):
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, PANEL_H + 0.6), sharey=True)
    for ax, n in zip(axes, (7, 11, 15)):
        _draw_saturation_panel(ax, pearl_data, n, show_legend=False)
    axes[1].set_ylabel("")
    axes[2].set_ylabel("")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(labels),
               frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    _save(fig, out_stem)


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------

def _save(fig, stem):
    pdf = Path(f"{stem}.pdf")
    fig.savefig(pdf)
    plt.close(fig)
    print(f"  wrote {pdf.name}")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not PEARL_CSV.exists():
        print(f"ERROR: missing Pearl CSV at {PEARL_CSV}", file=sys.stderr)
        sys.exit(1)

    print("Loading data...")
    pearl_by_n = load_pearl_by_n(PEARL_CSV)
    pearl_by_f = load_pearl_by_f(PEARL_CSV)

    sources = {
        "pearl-ol":  {f: pearl_by_f.get((f, "ol"), {})  for f in (3, 5, 7)},
        "pearl-bof": {f: pearl_by_f.get((f, "bof"), {}) for f in (3, 5, 7)},
    }
    for name in ("pompe", "themis", "fairdag-ol", "fairdag-bof"):
        d = load_baseline_by_f(BASELINES_DIR / f"{name}.csv")
        # Each baseline has at most one mode -- collapse (f, mode) -> f.
        per_f = defaultdict(lambda: defaultdict(lambda: {"tps": [], "lat_ms": []}))
        for (fault, _mode), per_rate in d.items():
            for r, samples in per_rate.items():
                per_f[fault][r]["tps"].extend(samples["tps"])
                per_f[fault][r]["lat_ms"].extend(samples["lat_ms"])
        sources[name] = per_f

    print(f"Writing curated paper plots to {OUT_DIR}")

    fig_scaling_l_curve(pearl_by_n, OUT_DIR / "pearl_scaling_l_curve")
    fig_lat_vs_rate(pearl_by_n, OUT_DIR / "pearl_lat_vs_rate")
    fig_overlay(sources, OUT_DIR / "pearl_vs_baselines")
    fig_saturation(pearl_by_n, OUT_DIR / "pearl_saturation")
    fig_overlay_tput_vs_rate(sources, OUT_DIR / "pearl_vs_baselines_tput_rate")
    fig_overlay_lat_vs_rate(sources, OUT_DIR / "pearl_vs_baselines_lat_rate")

    print("Done.")


if __name__ == "__main__":
    main()
