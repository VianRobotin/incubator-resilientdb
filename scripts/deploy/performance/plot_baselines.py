#!/usr/bin/env python3
# Plot baselines: per-baseline replicas of the autobahn plots, plus
# overlay-by-f figures comparing all baselines (and optionally autobahn).
#
# Reuses load_csv / series_for_key / style_ax from
# scripts/deploy/performance/plot.py so styling stays consistent.
#
# Outputs:
#   das_results/baselines/plots/{baseline}_lat_vs_rate_{metric}.png
#   das_results/baselines/plots/{baseline}_tput_vs_rate_{metric}.png
#   das_results/baselines/plots/{baseline}_tput_vs_latency_{metric}.png
#   das_results/baselines/plots/overlay_lat_vs_rate_f{f}_{metric}.png
#   das_results/baselines/plots/overlay_tput_vs_rate_f{f}_{metric}.png
#   das_results/baselines/plots/overlay_tput_vs_latency_f{f}_{metric}.png

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

PROJ_ROOT = Path("/var/scratch/vrobotin/incubator-resilientdb")
sys.path.insert(0, str(PROJ_ROOT / "scripts" / "deploy" / "performance"))
from plot import series_for_key, trimmed_stats, style_ax  # noqa: E402

BASELINES_CSV_DIR = PROJ_ROOT / "das_results" / "baselines"
AUTOBAHN_CSV      = PROJ_ROOT / "das_results" / "tput_latency.csv"
DEFAULT_OUT       = BASELINES_CSV_DIR / "plots"

# Visual identity — one colour per baseline + autobahn.
SERIES_STYLE = {
    "autobahn":    dict(color="#1565C0", marker="o", linestyle="-"),
    "pompe":       dict(color="#2E7D32", marker="s", linestyle="-"),
    "themis":      dict(color="#EF6C00", marker="D", linestyle="--"),
    "fairdag-ol":  dict(color="#6A1B9A", marker="^", linestyle="-"),
    "fairdag-bof": dict(color="#AD1457", marker="v", linestyle="--"),
}


def load_baseline_csv(path: Path):
    """data[(n, mode, metric)][input_rate] = {"tps": [...], "lat_ms": [...]}.

    Same shape as plot.load_csv but tolerant of the extra `baseline`/`f`
    columns up front in baseline CSVs.
    """
    data = defaultdict(lambda: defaultdict(lambda: {"tps": [], "lat_ms": []}))
    if not path.exists():
        return data
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                n = int(row["n"])
                mode = row["mode"].strip()
                rate = int(row["input_rate"])
                con_tps = float(row["consensus_tps"]) if row["consensus_tps"] else 0.0
                con_lat = float(row["consensus_latency_ms"]) if row["consensus_latency_ms"] else 0.0
                exe_tps = float(row["execution_tps"]) if row["execution_tps"] else 0.0
                exe_lat = float(row["execution_latency_ms"]) if row["execution_latency_ms"] else 0.0
            except (KeyError, ValueError):
                continue
            data[(n, mode, "consensus")][rate]["tps"].append(con_tps)
            data[(n, mode, "consensus")][rate]["lat_ms"].append(con_lat)
            data[(n, mode, "execution")][rate]["tps"].append(exe_tps)
            data[(n, mode, "execution")][rate]["lat_ms"].append(exe_lat)
    return data


def load_baseline_csv_with_f(path: Path):
    """Like load_baseline_csv but indexed by (f, mode, metric) for overlay plots."""
    data = defaultdict(lambda: defaultdict(lambda: {"tps": [], "lat_ms": []}))
    if not path.exists():
        return data
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                fault = int(row["f"])
                mode = row["mode"].strip()
                rate = int(row["input_rate"])
                con_tps = float(row["consensus_tps"]) if row["consensus_tps"] else 0.0
                con_lat = float(row["consensus_latency_ms"]) if row["consensus_latency_ms"] else 0.0
                exe_tps = float(row["execution_tps"]) if row["execution_tps"] else 0.0
                exe_lat = float(row["execution_latency_ms"]) if row["execution_latency_ms"] else 0.0
            except (KeyError, ValueError):
                continue
            data[(fault, mode, "consensus")][rate]["tps"].append(con_tps)
            data[(fault, mode, "consensus")][rate]["lat_ms"].append(con_lat)
            data[(fault, mode, "execution")][rate]["tps"].append(exe_tps)
            data[(fault, mode, "execution")][rate]["lat_ms"].append(exe_lat)
    return data


def load_autobahn_with_f(path: Path):
    """Autobahn uses 2f+1 ⇒ f = (n-1)//2.  Same shape as load_baseline_csv_with_f."""
    data = defaultdict(lambda: defaultdict(lambda: {"tps": [], "lat_ms": []}))
    if not path.exists():
        return data
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                n = int(row["n"])
                mode = row["mode"].strip()
                rate = int(row["input_rate"])
                con_tps = float(row["consensus_tps"])
                con_lat = float(row["consensus_latency_ms"])
                exe_tps = float(row["execution_tps"])
                exe_lat = float(row["execution_latency_ms"])
            except (KeyError, ValueError):
                continue
            fault = (n - 1) // 2
            data[(fault, mode, "consensus")][rate]["tps"].append(con_tps)
            data[(fault, mode, "consensus")][rate]["lat_ms"].append(con_lat)
            data[(fault, mode, "execution")][rate]["tps"].append(exe_tps)
            data[(fault, mode, "execution")][rate]["lat_ms"].append(exe_lat)
    return data


# ---------------------------------------------------------------------------
# Per-baseline plots (your three figures, scoped to one baseline)
# ---------------------------------------------------------------------------


def _per_baseline_figure(baseline: str, data: dict, metric: str,
                         kind: str, out_path: Path):
    """kind ∈ {"lat_vs_rate", "tput_vs_rate", "tput_vs_latency"}."""
    fig, ax = plt.subplots(figsize=(8, 5))
    ns = sorted({k[0] for k in data if k[2] == metric})
    if not ns:
        plt.close(fig); return
    cmap = plt.cm.get_cmap("tab10", max(len(ns), 1))

    for i, n in enumerate(ns):
        modes = sorted({k[1] for k in data if k[0] == n and k[2] == metric})
        for mode in modes:
            rates, m_tps, s_tps, m_lat, s_lat = series_for_key(
                data, (n, mode, metric))
            if len(rates) == 0:
                continue
            label = f"N={n}" if len(modes) == 1 else f"N={n} {mode.upper()}"
            ls = "-" if mode == "ol" else "--"
            mk = "o" if mode == "ol" else "s"
            color = cmap(i)
            if kind == "lat_vs_rate":
                ax.errorbar(rates, m_lat, yerr=s_lat, label=label,
                            color=color, linestyle=ls, marker=mk,
                            linewidth=1.5, capsize=3)
            elif kind == "tput_vs_rate":
                ax.errorbar(rates, m_tps, yerr=s_tps, label=label,
                            color=color, linestyle=ls, marker=mk,
                            linewidth=1.5, capsize=3)
            elif kind == "tput_vs_latency":
                ax.errorbar(m_tps, m_lat, xerr=s_tps, yerr=s_lat, label=label,
                            color=color, linestyle=ls, marker=mk,
                            linewidth=1.8, capsize=3, markersize=6)
                for r, x, y in zip(rates, m_tps, m_lat):
                    ax.annotate(f"{r//1000}k" if r >= 1000 else str(r),
                                (x, y), textcoords="offset points",
                                xytext=(5, 4), fontsize=6, color=color)

    metric_label = "Consensus" if metric == "consensus" else "Execution"
    if kind == "lat_vs_rate":
        style_ax(ax, "Injection Rate (tx/s)",
                 f"{metric_label} Latency (ms)",
                 f"{baseline}: Latency vs Injection Rate — {metric_label}")
    elif kind == "tput_vs_rate":
        style_ax(ax, "Injection Rate (tx/s)",
                 f"{metric_label} Throughput (tx/s)",
                 f"{baseline}: Throughput vs Injection Rate — {metric_label}")
    else:
        style_ax(ax, f"{metric_label} Throughput (tx/s)",
                 f"{metric_label} Latency (ms)",
                 f"{baseline}: Throughput vs Latency — {metric_label}")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


def per_baseline_plots(baseline: str, csv_path: Path, out_dir: Path):
    data = load_baseline_csv(csv_path)
    if not data:
        print(f"[skip] no data for {baseline}")
        return
    for metric in ("execution", "consensus"):
        _per_baseline_figure(baseline, data, metric, "lat_vs_rate",
                             out_dir / f"{baseline}_lat_vs_rate_{metric}.png")
        _per_baseline_figure(baseline, data, metric, "tput_vs_rate",
                             out_dir / f"{baseline}_tput_vs_rate_{metric}.png")
        _per_baseline_figure(baseline, data, metric, "tput_vs_latency",
                             out_dir / f"{baseline}_tput_vs_latency_{metric}.png")


# ---------------------------------------------------------------------------
# Overlay plots (all baselines + optionally autobahn) at fixed f
# ---------------------------------------------------------------------------


def _collect_overlay_series(
    sources: dict, fault: int, mode_filter: list, metric: str
):
    """sources: {label: data_with_f}.  Returns ordered list of
    (label, rates, m_tps, s_tps, m_lat, s_lat) for non-empty series."""
    out = []
    for label, data in sources.items():
        modes_for_f = sorted({
            k[1] for k in data if k[0] == fault and k[2] == metric
        })
        for mode in modes_for_f:
            if mode_filter and mode not in mode_filter:
                continue
            rates, m_tps, s_tps, m_lat, s_lat = series_for_key(
                data, (fault, mode, metric))
            if len(rates) == 0:
                continue
            display = label if len(modes_for_f) == 1 else f"{label} ({mode.upper()})"
            out.append((display, label, mode, rates, m_tps, s_tps, m_lat, s_lat))
    return out


def overlay_figure(sources: dict, fault: int, metric: str, kind: str,
                   out_path: Path, mode_filter: list):
    series = _collect_overlay_series(sources, fault, mode_filter, metric)
    if not series:
        return
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for display, label, mode, rates, m_tps, s_tps, m_lat, s_lat in series:
        style = SERIES_STYLE.get(label, dict(color="gray", marker="x", linestyle=":"))
        ls = style["linestyle"] if mode == "ol" else (
            "--" if style["linestyle"] == "-" else style["linestyle"])
        if kind == "lat_vs_rate":
            ax.errorbar(rates, m_lat, yerr=s_lat, label=display,
                        color=style["color"], linestyle=ls,
                        marker=style["marker"], linewidth=1.6, capsize=3)
        elif kind == "tput_vs_rate":
            ax.errorbar(rates, m_tps, yerr=s_tps, label=display,
                        color=style["color"], linestyle=ls,
                        marker=style["marker"], linewidth=1.6, capsize=3)
        else:  # tput_vs_latency
            ax.errorbar(m_tps, m_lat, xerr=s_tps, yerr=s_lat, label=display,
                        color=style["color"], linestyle=ls,
                        marker=style["marker"], linewidth=1.8, capsize=3,
                        markersize=6)

    metric_label = "Consensus" if metric == "consensus" else "Execution"
    title_suffix = f"f={fault} — {metric_label}"
    if kind == "lat_vs_rate":
        style_ax(ax, "Injection Rate (tx/s)",
                 f"{metric_label} Latency (ms)",
                 f"Latency vs Injection Rate ({title_suffix})")
    elif kind == "tput_vs_rate":
        style_ax(ax, "Injection Rate (tx/s)",
                 f"{metric_label} Throughput (tx/s)",
                 f"Throughput vs Injection Rate ({title_suffix})")
    else:
        style_ax(ax, f"{metric_label} Throughput (tx/s)",
                 f"{metric_label} Latency (ms)",
                 f"Throughput vs Latency ({title_suffix})")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


def overlay_plots(out_dir: Path, include_autobahn: bool, f_values: list,
                  mode_filter: list):
    sources = {}
    for baseline in ("pompe", "themis", "fairdag-ol", "fairdag-bof"):
        path = BASELINES_CSV_DIR / f"{baseline}.csv"
        d = load_baseline_csv_with_f(path)
        if d:
            sources[baseline] = d
    if include_autobahn:
        d = load_autobahn_with_f(AUTOBAHN_CSV)
        if d:
            sources["autobahn"] = d

    if not sources:
        print("[skip] no overlay data")
        return

    for fault in f_values:
        for metric in ("execution", "consensus"):
            overlay_figure(sources, fault, metric, "lat_vs_rate",
                           out_dir / f"overlay_lat_vs_rate_f{fault}_{metric}.png",
                           mode_filter)
            overlay_figure(sources, fault, metric, "tput_vs_rate",
                           out_dir / f"overlay_tput_vs_rate_f{fault}_{metric}.png",
                           mode_filter)
            overlay_figure(sources, fault, metric, "tput_vs_latency",
                           out_dir / f"overlay_tput_vs_latency_f{fault}_{metric}.png",
                           mode_filter)


# ---------------------------------------------------------------------------


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    p.add_argument("--baselines", nargs="+",
                   default=["pompe", "themis", "fairdag-ol", "fairdag-bof"])
    p.add_argument("--no-overlay", action="store_true")
    p.add_argument("--no-autobahn", action="store_true",
                   help="skip overlaying autobahn data on overlay plots")
    p.add_argument("--f-values", nargs="+", type=int, default=[3, 5, 7])
    p.add_argument("--modes", nargs="+", default=[],
                   help="filter overlay to specific modes (e.g. --modes ol)")
    args = p.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    for baseline in args.baselines:
        per_baseline_plots(
            baseline, BASELINES_CSV_DIR / f"{baseline}.csv", args.out_dir)

    if not args.no_overlay:
        overlay_plots(args.out_dir,
                      include_autobahn=not args.no_autobahn,
                      f_values=args.f_values,
                      mode_filter=args.modes)


if __name__ == "__main__":
    main()
