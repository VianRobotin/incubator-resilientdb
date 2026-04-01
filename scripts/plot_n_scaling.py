#!/usr/bin/env python3
"""
N-Scaling Plotter: OL vs BOF throughput and latency as n grows.
3 replicas (f=1), 5 replicas (f=2), 7 replicas (f=3).
Block size fixed at 100, Δ=1s.

Usage: python3 plot_n_scaling.py [results_csv] [output_dir]
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OL_COLOR  = '#2196F3'
BOF_COLOR = '#FF5722'

plt.rcParams.update({
    'figure.facecolor': 'white',
    'axes.facecolor':   '#fafafa',
    'axes.grid':        True,
    'grid.alpha':       0.35,
    'grid.linestyle':   '--',
    'font.size':        12,
    'axes.titlesize':   13,
    'axes.labelsize':   12,
    'legend.fontsize':  11,
    'figure.dpi':       150,
})


def load_csv(path):
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df['mode'] = df['mode'].str.strip().str.lower()
    return df


def latency_s(row):
    """Average commit latency per slot (seconds)."""
    if row['slots_committed'] == 0:
        return float('nan')
    return row['runtime_s'] / row['slots_committed']


def plot_throughput_line(df_ol, df_bof, out_path):
    ns = sorted(df_ol['n'].unique())
    ol_tps  = [df_ol[df_ol['n'] == n]['tps'].values[0]  for n in ns]
    bof_tps = [df_bof[df_bof['n'] == n]['tps'].values[0] for n in ns]

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.plot(ns, ol_tps,  'o-', color=OL_COLOR,  linewidth=2.5, markersize=9,
            label='OL (Ordering Linearizability)', markerfacecolor='white', markeredgewidth=2.5)
    ax.plot(ns, bof_tps, 's-', color=BOF_COLOR, linewidth=2.5, markersize=9,
            label='BOF (Batch-Order Fairness)', markerfacecolor='white', markeredgewidth=2.5)

    for n, v in zip(ns, ol_tps):
        ax.annotate(f'{v:.1f}', (n, v), textcoords='offset points', xytext=(0, 10),
                    ha='center', fontsize=10, color=OL_COLOR, fontweight='bold')
    for n, v in zip(ns, bof_tps):
        ax.annotate(f'{v:.1f}', (n, v), textcoords='offset points', xytext=(0, -18),
                    ha='center', fontsize=10, color=BOF_COLOR, fontweight='bold')

    ax.set_xlabel('Number of Replicas (n = 2f+1)')
    ax.set_ylabel('Throughput (transactions / second)')
    ax.set_title('Throughput vs Number of Replicas: OL vs BOF\n'
                 '(block size = 100 · Δ = 1s · 1 client per replica)',
                 fontweight='bold')
    ax.legend(loc='upper right')
    ax.set_xticks(ns)
    ax.set_xticklabels([f'n={n}\n(f={(n-1)//2})' for n in ns])
    ax.set_ylim(0, max(ol_tps + bof_tps) * 1.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def plot_latency_line(df_ol, df_bof, out_path):
    ns = sorted(df_ol['n'].unique())
    ol_lat  = [latency_s(df_ol[df_ol['n'] == n].iloc[0])  for n in ns]
    bof_lat = [latency_s(df_bof[df_bof['n'] == n].iloc[0]) for n in ns]

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.plot(ns, ol_lat,  'o-', color=OL_COLOR,  linewidth=2.5, markersize=9,
            label='OL (Ordering Linearizability)', markerfacecolor='white', markeredgewidth=2.5)
    ax.plot(ns, bof_lat, 's-', color=BOF_COLOR, linewidth=2.5, markersize=9,
            label='BOF (Batch-Order Fairness)', markerfacecolor='white', markeredgewidth=2.5)

    for n, v in zip(ns, ol_lat):
        if not np.isnan(v):
            ax.annotate(f'{v:.1f}s', (n, v), textcoords='offset points', xytext=(0, 10),
                        ha='center', fontsize=10, color=OL_COLOR, fontweight='bold')
    for n, v in zip(ns, bof_lat):
        if not np.isnan(v):
            ax.annotate(f'{v:.1f}s', (n, v), textcoords='offset points', xytext=(0, -18),
                        ha='center', fontsize=10, color=BOF_COLOR, fontweight='bold')

    ax.set_xlabel('Number of Replicas (n = 2f+1)')
    ax.set_ylabel('Avg Commit Latency (seconds per slot)')
    ax.set_title('Latency vs Number of Replicas: OL vs BOF\n'
                 '(block size = 100 · Δ = 1s · 1 client per replica)',
                 fontweight='bold')
    ax.legend(loc='upper left')
    ax.set_xticks(ns)
    ax.set_xticklabels([f'n={n}\n(f={(n-1)//2})' for n in ns])
    valid = [v for v in ol_lat + bof_lat if not np.isnan(v)]
    ax.set_ylim(0, max(valid) * 1.3 if valid else 1)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def plot_slots_line(df_ol, df_bof, out_path):
    ns = sorted(df_ol['n'].unique())
    ol_slots  = [df_ol[df_ol['n'] == n]['slots_committed'].values[0]  for n in ns]
    bof_slots = [df_bof[df_bof['n'] == n]['slots_committed'].values[0] for n in ns]

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.plot(ns, ol_slots,  'o-', color=OL_COLOR,  linewidth=2.5, markersize=9,
            label='OL', markerfacecolor='white', markeredgewidth=2.5)
    ax.plot(ns, bof_slots, 's-', color=BOF_COLOR, linewidth=2.5, markersize=9,
            label='BOF', markerfacecolor='white', markeredgewidth=2.5)

    for n, v in zip(ns, ol_slots):
        ax.annotate(str(int(v)), (n, v), textcoords='offset points', xytext=(0, 8),
                    ha='center', fontsize=10, color=OL_COLOR, fontweight='bold')
    for n, v in zip(ns, bof_slots):
        ax.annotate(str(int(v)), (n, v), textcoords='offset points', xytext=(0, -16),
                    ha='center', fontsize=10, color=BOF_COLOR, fontweight='bold')

    ax.set_xlabel('Number of Replicas (n = 2f+1)')
    ax.set_ylabel('Consensus Slots Committed (90s run)')
    ax.set_title('Slots Committed vs Number of Replicas: OL vs BOF\n'
                 '(block size = 100 · Δ = 1s)',
                 fontweight='bold')
    ax.legend(loc='upper right')
    ax.set_xticks(ns)
    ax.set_xticklabels([f'n={n}\n(f={(n-1)//2})' for n in ns])
    ax.set_ylim(0, max(ol_slots + bof_slots) * 1.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def plot_combined(df_ol, df_bof, out_path):
    ns = sorted(df_ol['n'].unique())
    ol_tps   = [df_ol[df_ol['n'] == n]['tps'].values[0]              for n in ns]
    bof_tps  = [df_bof[df_bof['n'] == n]['tps'].values[0]             for n in ns]
    ol_lat   = [latency_s(df_ol[df_ol['n'] == n].iloc[0])             for n in ns]
    bof_lat  = [latency_s(df_bof[df_bof['n'] == n].iloc[0])           for n in ns]
    ol_slots = [df_ol[df_ol['n'] == n]['slots_committed'].values[0]   for n in ns]
    bof_slots= [df_bof[df_bof['n'] == n]['slots_committed'].values[0] for n in ns]
    ol_txns  = [df_ol[df_ol['n'] == n]['txns_committed'].values[0]    for n in ns]
    bof_txns = [df_bof[df_bof['n'] == n]['txns_committed'].values[0]  for n in ns]
    xlabels  = [f'n={n}\n(f={(n-1)//2})' for n in ns]

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        'Fair Ordering Performance vs Number of Replicas: OL vs BOF\n'
        'block size = 100 · Δ = 1s · 90-second runs',
        fontsize=14, fontweight='bold', y=1.01)

    def annotate(ax, xs, ys, color, dy):
        for x, y in zip(xs, ys):
            if not (isinstance(y, float) and np.isnan(y)):
                ax.annotate(f'{y:.1f}', (x, y), textcoords='offset points',
                            xytext=(0, dy), ha='center', fontsize=9,
                            color=color, fontweight='bold')

    # Throughput
    ax = axes[0, 0]
    ax.plot(ns, ol_tps,  'o-', color=OL_COLOR,  linewidth=2, markersize=8,
            label='OL', markerfacecolor='white', markeredgewidth=2)
    ax.plot(ns, bof_tps, 's-', color=BOF_COLOR, linewidth=2, markersize=8,
            label='BOF', markerfacecolor='white', markeredgewidth=2)
    annotate(ax, ns, ol_tps,  OL_COLOR,  9)
    annotate(ax, ns, bof_tps, BOF_COLOR, -16)
    ax.set_xticks(ns); ax.set_xticklabels(xlabels)
    ax.set_ylabel('Throughput (TPS)'); ax.set_title('(A) Throughput', fontweight='bold')
    ax.set_ylim(0, max(ol_tps + bof_tps) * 1.3); ax.legend()

    # Latency
    ax = axes[0, 1]
    ax.plot(ns, ol_lat,  'o-', color=OL_COLOR,  linewidth=2, markersize=8,
            label='OL', markerfacecolor='white', markeredgewidth=2)
    ax.plot(ns, bof_lat, 's-', color=BOF_COLOR, linewidth=2, markersize=8,
            label='BOF', markerfacecolor='white', markeredgewidth=2)
    annotate(ax, ns, ol_lat,  OL_COLOR,  9)
    annotate(ax, ns, bof_lat, BOF_COLOR, -16)
    ax.set_xticks(ns); ax.set_xticklabels(xlabels)
    ax.set_ylabel('Avg Commit Latency (s/slot)'); ax.set_title('(B) Commit Latency', fontweight='bold')
    valid = [v for v in ol_lat + bof_lat if not (isinstance(v, float) and np.isnan(v))]
    ax.set_ylim(0, max(valid) * 1.3 if valid else 1); ax.legend()

    # Slots
    ax = axes[1, 0]
    ax.plot(ns, ol_slots,  'o-', color=OL_COLOR,  linewidth=2, markersize=8,
            label='OL', markerfacecolor='white', markeredgewidth=2)
    ax.plot(ns, bof_slots, 's-', color=BOF_COLOR, linewidth=2, markersize=8,
            label='BOF', markerfacecolor='white', markeredgewidth=2)
    for n, v in zip(ns, ol_slots):
        ax.annotate(str(int(v)), (n, v), textcoords='offset points',
                    xytext=(0, 8), ha='center', fontsize=9, color=OL_COLOR, fontweight='bold')
    for n, v in zip(ns, bof_slots):
        ax.annotate(str(int(v)), (n, v), textcoords='offset points',
                    xytext=(0, -15), ha='center', fontsize=9, color=BOF_COLOR, fontweight='bold')
    ax.set_xticks(ns); ax.set_xticklabels(xlabels)
    ax.set_ylabel('Slots Committed'); ax.set_title('(C) Consensus Slots (90s)', fontweight='bold')
    ax.set_ylim(0, max(ol_slots + bof_slots) * 1.3); ax.legend()

    # Transactions
    ax = axes[1, 1]
    ax.plot(ns, ol_txns,  'o-', color=OL_COLOR,  linewidth=2, markersize=8,
            label='OL', markerfacecolor='white', markeredgewidth=2)
    ax.plot(ns, bof_txns, 's-', color=BOF_COLOR, linewidth=2, markersize=8,
            label='BOF', markerfacecolor='white', markeredgewidth=2)
    for n, v in zip(ns, ol_txns):
        ax.annotate(f'{int(v):,}', (n, v), textcoords='offset points',
                    xytext=(0, 8), ha='center', fontsize=8, color=OL_COLOR, fontweight='bold')
    for n, v in zip(ns, bof_txns):
        ax.annotate(f'{int(v):,}', (n, v), textcoords='offset points',
                    xytext=(0, -15), ha='center', fontsize=8, color=BOF_COLOR, fontweight='bold')
    ax.set_xticks(ns); ax.set_xticklabels(xlabels)
    ax.set_ylabel('Total Txns Committed'); ax.set_title('(D) Transactions Committed', fontweight='bold')
    ax.set_ylim(0, max(ol_txns + bof_txns) * 1.2); ax.legend()

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else 'benchmark_results/n_scaling.csv'
    out_dir  = sys.argv[2] if len(sys.argv) > 2 else 'benchmark_results/plots'
    os.makedirs(out_dir, exist_ok=True)

    df = load_csv(csv_path)
    df_ol  = df[df['mode'] == 'ol'].reset_index(drop=True)
    df_bof = df[df['mode'] == 'bof'].reset_index(drop=True)

    if df_ol.empty or df_bof.empty:
        print("ERROR: CSV must contain rows with mode='ol' and mode='bof'")
        sys.exit(1)

    print(f"\nLoaded {len(df_ol)} OL rows, {len(df_bof)} BOF rows")
    print("\nOL:\n", df_ol.to_string(index=False))
    print("\nBOF:\n", df_bof.to_string(index=False))

    print("\nCommit latency (runtime / slots):")
    for _, row in df.iterrows():
        lat = latency_s(row)
        print(f"  {row['mode'].upper()} n={row['n']}: {lat:.1f}s  [{row['slots_committed']} slots]")

    print(f"\nGenerating plots in: {out_dir}/")
    plot_throughput_line(df_ol, df_bof,
                         os.path.join(out_dir, 'n_scaling_throughput.png'))
    plot_latency_line(df_ol, df_bof,
                      os.path.join(out_dir, 'n_scaling_latency.png'))
    plot_slots_line(df_ol, df_bof,
                    os.path.join(out_dir, 'n_scaling_slots.png'))
    plot_combined(df_ol, df_bof,
                  os.path.join(out_dir, 'n_scaling_dashboard.png'))
    print("\nDone.")


if __name__ == '__main__':
    main()
