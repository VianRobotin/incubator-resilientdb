#!/usr/bin/env python3
"""
OL vs BOF Comparison Plotter
3 replicas (n=2f+1, f=1), 1 client
Generates throughput and latency comparison plots for the paper.

Usage: python3 plot_ol_vs_bof.py [results_csv] [output_dir]
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# ── Style ─────────────────────────────────────────────────────────────────────
OL_COLOR  = '#2196F3'   # blue
BOF_COLOR = '#FF5722'   # orange-red
HATCHES   = ['', '///']

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


def compute_latency_ms(row):
    """
    Estimate end-to-end commit latency from the benchmark run.

    Commit latency = time between a transaction being included in a slot
                     and that slot being committed.

    We approximate this as:
        latency_s = runtime_s / slots_committed

    This is the average slot duration, which is the dominant term
    in the 3Δ formula from the paper (Δ=1s → 3s minimum).
    Transactions committed per slot ≈ txns_committed / slots_committed.
    """
    if row['slots_committed'] == 0:
        return np.nan
    return (row['runtime_s'] / row['slots_committed']) * 1000  # → ms


def plot_throughput_bar(df_ol, df_bof, out_path):
    """Bar chart: TPS for OL vs BOF at each block size."""
    block_sizes = sorted(df_ol['block_size'].unique())
    x = np.arange(len(block_sizes))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))

    ol_tps  = [df_ol[df_ol['block_size'] == bs]['tps'].values[0]  for bs in block_sizes]
    bof_tps = [df_bof[df_bof['block_size'] == bs]['tps'].values[0] for bs in block_sizes]

    bars_ol  = ax.bar(x - width/2, ol_tps,  width, color=OL_COLOR,  alpha=0.85,
                      label='OL (Ordering Linearizability)', edgecolor='white', linewidth=1.2)
    bars_bof = ax.bar(x + width/2, bof_tps, width, color=BOF_COLOR, alpha=0.85,
                      label='BOF (Batch-Order Fairness)', edgecolor='white', linewidth=1.2)

    # Value labels on top of each bar
    for bar in bars_ol:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 1.5, f'{h:.1f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold', color=OL_COLOR)
    for bar in bars_bof:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 1.5, f'{h:.1f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold', color=BOF_COLOR)

    ax.set_xticks(x)
    ax.set_xticklabels([f'Block size = {bs}' for bs in block_sizes])
    ax.set_ylabel('Throughput (transactions / second)')
    ax.set_title('Throughput: OL vs BOF\n(3 replicas, n=2f+1, f=1 | 1 client)', fontweight='bold')
    ax.legend(loc='upper right')
    ax.set_ylim(0, max(ol_tps + bof_tps) * 1.25)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def plot_latency_bar(df_ol, df_bof, out_path):
    """Bar chart: estimated commit latency for OL vs BOF at each block size."""
    block_sizes = sorted(df_ol['block_size'].unique())
    x = np.arange(len(block_sizes))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))

    ol_lat  = [compute_latency_ms(df_ol[df_ol['block_size'] == bs].iloc[0])
               for bs in block_sizes]
    bof_lat = [compute_latency_ms(df_bof[df_bof['block_size'] == bs].iloc[0])
               for bs in block_sizes]

    bars_ol  = ax.bar(x - width/2, ol_lat,  width, color=OL_COLOR,  alpha=0.85,
                      label='OL (Ordering Linearizability)', edgecolor='white', linewidth=1.2)
    bars_bof = ax.bar(x + width/2, bof_lat, width, color=BOF_COLOR, alpha=0.85,
                      label='BOF (Batch-Order Fairness)', edgecolor='white', linewidth=1.2)

    for bar in bars_ol:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + max(ol_lat+bof_lat)*0.015,
                f'{h/1000:.1f}s', ha='center', va='bottom',
                fontsize=10, fontweight='bold', color=OL_COLOR)
    for bar in bars_bof:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + max(ol_lat+bof_lat)*0.015,
                f'{h/1000:.1f}s', ha='center', va='bottom',
                fontsize=10, fontweight='bold', color=BOF_COLOR)

    ax.set_xticks(x)
    ax.set_xticklabels([f'Block size = {bs}' for bs in block_sizes])
    ax.set_ylabel('Avg Commit Latency (ms)')
    ax.set_title('Latency: OL vs BOF\n(3 replicas, n=2f+1, f=1 | 1 client)', fontweight='bold')
    ax.legend(loc='upper left')
    ax.set_ylim(0, max(ol_lat + bof_lat) * 1.25)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def plot_txns_committed(df_ol, df_bof, out_path):
    """Bar chart: total transactions committed for OL vs BOF."""
    block_sizes = sorted(df_ol['block_size'].unique())
    x = np.arange(len(block_sizes))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))

    ol_txns  = [df_ol[df_ol['block_size'] == bs]['txns_committed'].values[0]
                for bs in block_sizes]
    bof_txns = [df_bof[df_bof['block_size'] == bs]['txns_committed'].values[0]
                for bs in block_sizes]

    bars_ol  = ax.bar(x - width/2, ol_txns,  width, color=OL_COLOR,  alpha=0.85,
                      label='OL', edgecolor='white')
    bars_bof = ax.bar(x + width/2, bof_txns, width, color=BOF_COLOR, alpha=0.85,
                      label='BOF', edgecolor='white')

    for bar in bars_ol:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 50,
                f'{int(h):,}', ha='center', va='bottom', fontsize=9, color=OL_COLOR)
    for bar in bars_bof:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 50,
                f'{int(h):,}', ha='center', va='bottom', fontsize=9, color=BOF_COLOR)

    ax.set_xticks(x)
    ax.set_xticklabels([f'Block size = {bs}' for bs in block_sizes])
    ax.set_ylabel('Total Transactions Committed')
    ax.set_title('Total Committed Transactions: OL vs BOF\n(3 replicas, 90-second run)',
                 fontweight='bold')
    ax.legend()
    ax.set_ylim(0, max(ol_txns + bof_txns) * 1.2)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def plot_combined_dashboard(df_ol, df_bof, out_path):
    """2x2 dashboard: throughput, latency, slots, txns committed."""
    block_sizes = sorted(df_ol['block_size'].unique())
    x = np.arange(len(block_sizes))
    width = 0.32
    xlabels = [str(bs) for bs in block_sizes]

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        'Fair Ordering Performance: OL vs BOF\n'
        '3 replicas (n = 2f+1, f = 1) · 1 client · 90-second runs',
        fontsize=14, fontweight='bold', y=1.01)

    # ── Panel A: Throughput ────────────────────────────────────────────
    ax = axes[0, 0]
    ol_tps  = [df_ol[df_ol['block_size'] == bs]['tps'].values[0]  for bs in block_sizes]
    bof_tps = [df_bof[df_bof['block_size'] == bs]['tps'].values[0] for bs in block_sizes]

    ax.bar(x - width/2, ol_tps,  width, color=OL_COLOR,  alpha=0.85,
           label='OL', edgecolor='white', linewidth=0.8)
    ax.bar(x + width/2, bof_tps, width, color=BOF_COLOR, alpha=0.85,
           label='BOF', edgecolor='white', linewidth=0.8)
    ax.set_xticks(x); ax.set_xticklabels(xlabels)
    ax.set_xlabel('Block Size (transactions)')
    ax.set_ylabel('Throughput (TPS)')
    ax.set_title('(A) Throughput', fontweight='bold')
    ax.legend()
    ax.set_ylim(0, max(ol_tps + bof_tps) * 1.3)
    for i, (v1, v2) in enumerate(zip(ol_tps, bof_tps)):
        ax.text(i - width/2, v1 + 1, f'{v1:.0f}', ha='center', va='bottom',
                fontsize=9, color=OL_COLOR)
        ax.text(i + width/2, v2 + 1, f'{v2:.0f}', ha='center', va='bottom',
                fontsize=9, color=BOF_COLOR)

    # ── Panel B: Latency ───────────────────────────────────────────────
    ax = axes[0, 1]
    ol_lat  = [compute_latency_ms(df_ol[df_ol['block_size'] == bs].iloc[0])
               for bs in block_sizes]
    bof_lat = [compute_latency_ms(df_bof[df_bof['block_size'] == bs].iloc[0])
               for bs in block_sizes]

    ax.bar(x - width/2, ol_lat,  width, color=OL_COLOR,  alpha=0.85,
           label='OL', edgecolor='white', linewidth=0.8)
    ax.bar(x + width/2, bof_lat, width, color=BOF_COLOR, alpha=0.85,
           label='BOF', edgecolor='white', linewidth=0.8)
    ax.set_xticks(x); ax.set_xticklabels(xlabels)
    ax.set_xlabel('Block Size (transactions)')
    ax.set_ylabel('Avg Commit Latency (ms)')
    ax.set_title('(B) Commit Latency (runtime / slots)', fontweight='bold')
    ax.legend()
    ax.set_ylim(0, max(ol_lat + bof_lat) * 1.25)
    for i, (v1, v2) in enumerate(zip(ol_lat, bof_lat)):
        ax.text(i - width/2, v1 + max(ol_lat+bof_lat)*0.01, f'{v1/1000:.1f}s',
                ha='center', va='bottom', fontsize=9, color=OL_COLOR)
        ax.text(i + width/2, v2 + max(ol_lat+bof_lat)*0.01, f'{v2/1000:.1f}s',
                ha='center', va='bottom', fontsize=9, color=BOF_COLOR)

    # ── Panel C: Slots committed ───────────────────────────────────────
    ax = axes[1, 0]
    ol_slots  = [df_ol[df_ol['block_size'] == bs]['slots_committed'].values[0]
                 for bs in block_sizes]
    bof_slots = [df_bof[df_bof['block_size'] == bs]['slots_committed'].values[0]
                 for bs in block_sizes]

    ax.bar(x - width/2, ol_slots,  width, color=OL_COLOR,  alpha=0.85,
           label='OL', edgecolor='white', linewidth=0.8)
    ax.bar(x + width/2, bof_slots, width, color=BOF_COLOR, alpha=0.85,
           label='BOF', edgecolor='white', linewidth=0.8)
    ax.set_xticks(x); ax.set_xticklabels(xlabels)
    ax.set_xlabel('Block Size (transactions)')
    ax.set_ylabel('Consensus Slots Committed')
    ax.set_title('(C) Consensus Slots (90s run)', fontweight='bold')
    ax.legend()
    ax.set_ylim(0, max(ol_slots + bof_slots) * 1.3)
    for i, (v1, v2) in enumerate(zip(ol_slots, bof_slots)):
        ax.text(i - width/2, v1 + 0.2, str(int(v1)), ha='center', va='bottom',
                fontsize=9, color=OL_COLOR)
        ax.text(i + width/2, v2 + 0.2, str(int(v2)), ha='center', va='bottom',
                fontsize=9, color=BOF_COLOR)

    # ── Panel D: Transactions committed ───────────────────────────────
    ax = axes[1, 1]
    ol_txns  = [df_ol[df_ol['block_size'] == bs]['txns_committed'].values[0]
                for bs in block_sizes]
    bof_txns = [df_bof[df_bof['block_size'] == bs]['txns_committed'].values[0]
                for bs in block_sizes]

    ax.bar(x - width/2, ol_txns,  width, color=OL_COLOR,  alpha=0.85,
           label='OL', edgecolor='white', linewidth=0.8)
    ax.bar(x + width/2, bof_txns, width, color=BOF_COLOR, alpha=0.85,
           label='BOF', edgecolor='white', linewidth=0.8)
    ax.set_xticks(x); ax.set_xticklabels(xlabels)
    ax.set_xlabel('Block Size (transactions)')
    ax.set_ylabel('Transactions Committed')
    ax.set_title('(D) Total Transactions Committed', fontweight='bold')
    ax.legend()
    ax.set_ylim(0, max(ol_txns + bof_txns) * 1.2)
    for i, (v1, v2) in enumerate(zip(ol_txns, bof_txns)):
        ax.text(i - width/2, v1 + 50, f'{int(v1):,}', ha='center', va='bottom',
                fontsize=8, color=OL_COLOR)
        ax.text(i + width/2, v2 + 50, f'{int(v2):,}', ha='center', va='bottom',
                fontsize=8, color=BOF_COLOR)

    # Shared legend annotation
    ol_patch  = mpatches.Patch(color=OL_COLOR,  alpha=0.85, label='OL (Ordering Linearizability)')
    bof_patch = mpatches.Patch(color=BOF_COLOR, alpha=0.85, label='BOF (Batch-Order Fairness)')
    fig.legend(handles=[ol_patch, bof_patch], loc='lower center', ncol=2,
               fontsize=12, frameon=True, bbox_to_anchor=(0.5, -0.03))

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def plot_throughput_line(df_ol, df_bof, out_path):
    """Line plot: TPS vs block size for OL vs BOF."""
    block_sizes = sorted(df_ol['block_size'].unique())
    ol_tps  = [df_ol[df_ol['block_size'] == bs]['tps'].values[0]  for bs in block_sizes]
    bof_tps = [df_bof[df_bof['block_size'] == bs]['tps'].values[0] for bs in block_sizes]

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.plot(block_sizes, ol_tps,  'o-', color=OL_COLOR,  linewidth=2.5, markersize=9,
            label='OL (Ordering Linearizability)', markerfacecolor='white',
            markeredgewidth=2.5)
    ax.plot(block_sizes, bof_tps, 's-', color=BOF_COLOR, linewidth=2.5, markersize=9,
            label='BOF (Batch-Order Fairness)', markerfacecolor='white',
            markeredgewidth=2.5)

    for bs, v in zip(block_sizes, ol_tps):
        ax.annotate(f'{v:.1f}', (bs, v), textcoords='offset points', xytext=(0, 10),
                    ha='center', fontsize=10, color=OL_COLOR, fontweight='bold')
    for bs, v in zip(block_sizes, bof_tps):
        ax.annotate(f'{v:.1f}', (bs, v), textcoords='offset points', xytext=(0, -18),
                    ha='center', fontsize=10, color=BOF_COLOR, fontweight='bold')

    ax.set_xlabel('Block Size (transactions per block)')
    ax.set_ylabel('Throughput (transactions / second)')
    ax.set_title('Throughput vs Block Size: OL vs BOF\n'
                 '(3 replicas · n=2f+1, f=1 · 1 client)', fontweight='bold')
    ax.legend(loc='upper left')
    ax.set_xticks(block_sizes)
    ax.set_ylim(0, max(ol_tps + bof_tps) * 1.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


def plot_latency_line(df_ol, df_bof, out_path):
    """Line plot: commit latency vs block size for OL vs BOF."""
    block_sizes = sorted(df_ol['block_size'].unique())
    ol_lat  = [compute_latency_ms(df_ol[df_ol['block_size'] == bs].iloc[0])
               for bs in block_sizes]
    bof_lat = [compute_latency_ms(df_bof[df_bof['block_size'] == bs].iloc[0])
               for bs in block_sizes]

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.plot(block_sizes, [v/1000 for v in ol_lat],  'o-', color=OL_COLOR,  linewidth=2.5,
            markersize=9, label='OL (Ordering Linearizability)', markerfacecolor='white',
            markeredgewidth=2.5)
    ax.plot(block_sizes, [v/1000 for v in bof_lat], 's-', color=BOF_COLOR, linewidth=2.5,
            markersize=9, label='BOF (Batch-Order Fairness)', markerfacecolor='white',
            markeredgewidth=2.5)

    for bs, v in zip(block_sizes, ol_lat):
        ax.annotate(f'{v/1000:.1f}s', (bs, v/1000),
                    textcoords='offset points', xytext=(0, 10),
                    ha='center', fontsize=10, color=OL_COLOR, fontweight='bold')
    for bs, v in zip(block_sizes, bof_lat):
        ax.annotate(f'{v/1000:.1f}s', (bs, v/1000),
                    textcoords='offset points', xytext=(0, -20),
                    ha='center', fontsize=10, color=BOF_COLOR, fontweight='bold')

    ax.set_xlabel('Block Size (transactions per block)')
    ax.set_ylabel('Avg Commit Latency (seconds)')
    ax.set_title('Latency vs Block Size: OL vs BOF\n'
                 '(3 replicas · n=2f+1, f=1 · 1 client)', fontweight='bold')
    ax.legend(loc='upper left')
    ax.set_xticks(block_sizes)
    ax.set_ylim(0, max(ol_lat + bof_lat) / 1000 * 1.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else 'benchmark_results/comparison.csv'
    out_dir  = sys.argv[2] if len(sys.argv) > 2 else 'benchmark_results/plots'
    os.makedirs(out_dir, exist_ok=True)

    df = load_csv(csv_path)
    df_ol  = df[df['mode'] == 'ol'].reset_index(drop=True)
    df_bof = df[df['mode'] == 'bof'].reset_index(drop=True)

    if df_ol.empty or df_bof.empty:
        print("ERROR: CSV must contain rows with mode='ol' and mode='bof'")
        sys.exit(1)

    print(f"\nLoaded {len(df_ol)} OL rows and {len(df_bof)} BOF rows")
    print("\nOL data:")
    print(df_ol.to_string(index=False))
    print("\nBOF data:")
    print(df_bof.to_string(index=False))

    # Compute and display latency estimates
    print("\nEstimated commit latency (runtime / slots):")
    for _, row in df.iterrows():
        lat_ms = compute_latency_ms(row)
        print(f"  {row['mode'].upper()} block_size={row['block_size']:3d}: "
              f"{lat_ms/1000:.1f}s ({lat_ms:.0f}ms)  "
              f"[{row['slots_committed']} slots in {row['runtime_s']}s]")

    print(f"\nGenerating plots in: {out_dir}/")

    plot_throughput_line(df_ol, df_bof,
                         os.path.join(out_dir, 'ol_vs_bof_throughput_line.png'))
    plot_latency_line(df_ol, df_bof,
                      os.path.join(out_dir, 'ol_vs_bof_latency_line.png'))
    plot_throughput_bar(df_ol, df_bof,
                        os.path.join(out_dir, 'ol_vs_bof_throughput_bar.png'))
    plot_latency_bar(df_ol, df_bof,
                     os.path.join(out_dir, 'ol_vs_bof_latency_bar.png'))
    plot_txns_committed(df_ol, df_bof,
                        os.path.join(out_dir, 'ol_vs_bof_txns_committed.png'))
    plot_combined_dashboard(df_ol, df_bof,
                            os.path.join(out_dir, 'ol_vs_bof_dashboard.png'))

    print("\nDone.")


if __name__ == '__main__':
    main()
