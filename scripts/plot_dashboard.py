#!/usr/bin/env python3
"""
Autobahn Benchmark Dashboard — Single comprehensive plot.
Parses monitor logs and generates a publication-quality dashboard.
"""

import os
import re
import sys
import glob
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

COLORS = ['#2196F3', '#FF5722', '#4CAF50', '#9C27B0']

def parse_monitor_line(line):
    metrics = {}
    patterns = {
        'server_call': r'server call:(\d+)',
        'broad_cast': r'broad_cast:(\d+)',
        'propose': r'propose:(\d+)',
        'commit': r' commit:(\d+)',
        'execute_done': r'execute done:(\d+)',
        'time': r'time:(\d+)',
        'new_transactions': r'new transactions:(\d+)',
        'consumed_transactions': r'consumed transactions:(\d+)',
    }
    latency_patterns = {
        'execute_latency': r'execute latency :([0-9.e+\-]+)',
        'execute_queuing_latency': r'execute_queuing latency :([0-9.e+\-]+)',
        'commit_delay_latency': r'commit_delay latency :([0-9.e+\-]+)',
        'cpu_usage': r'cpu usage:([0-9.e+\-]+)',
    }

    for key, pat in patterns.items():
        m = re.search(pat, line)
        if m:
            try:
                metrics[key] = float(m.group(1))
            except ValueError:
                metrics[key] = 0.0

    for key, pat in latency_patterns.items():
        m = re.search(pat, line)
        if m:
            try:
                val = float(m.group(1))
                if np.isfinite(val) and val > 0 and val < 1e6:
                    metrics[key] = val
            except ValueError:
                pass

    return metrics


def parse_log_file(filepath):
    records = []
    fair_ordering = []

    with open(filepath, 'r', errors='replace') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        if 'monitor =========' in line and '--------------- monitor' not in line:
            data_line = ""
            j = i + 1
            while j < len(lines) and '--------------- monitor' not in lines[j] and 'monitor =========' not in lines[j]:
                data_line += lines[j].strip() + " "
                j += 1
            if data_line:
                m = parse_monitor_line(data_line)
                if m and 'time' in m:
                    records.append(m)
            i = j
            continue

        fo_match = re.search(r'SyncHS slot (\d+):.*?fairly ordered (\d+) txns', line)
        if fo_match:
            fair_ordering.append({
                'slot': int(fo_match.group(1)),
                'fair_txns': int(fo_match.group(2))
            })
        i += 1

    return pd.DataFrame(records), pd.DataFrame(fair_ordering) if fair_ordering else pd.DataFrame()


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "benchmark_results"
    plots_dir = os.path.join(results_dir, 'plots')
    os.makedirs(plots_dir, exist_ok=True)

    # Find all log directories
    log_dirs = sorted(glob.glob(os.path.join(results_dir, 'logs_*')))
    if not log_dirs:
        print("No log directories found")
        sys.exit(1)

    # Parse all runs
    all_runs = {}
    all_fo = {}
    for log_dir in log_dirs:
        run_label = os.path.basename(log_dir).replace('logs_', '')
        replica_data = {}
        fo_data = {}
        for lf in sorted(glob.glob(os.path.join(log_dir, 'server_*.log'))):
            rid = int(re.search(r'server_(\d+)', lf).group(1))
            df, fo_df = parse_log_file(lf)
            if not df.empty:
                replica_data[rid] = df
            if not fo_df.empty:
                fo_data[rid] = fo_df
        if replica_data:
            all_runs[run_label] = replica_data
            all_fo[run_label] = fo_data
            print(f"Parsed {run_label}: {len(replica_data)} replicas, "
                  f"{sum(len(df) for df in replica_data.values())} samples")

    # ─── Dashboard Plot ───────────────────────────────────────────────────

    plt.rcParams.update({
        'figure.facecolor': 'white',
        'axes.facecolor': '#f8f9fa',
        'axes.grid': True,
        'grid.alpha': 0.3,
        'grid.linestyle': '--',
        'font.size': 10,
        'axes.titlesize': 12,
        'axes.labelsize': 10,
        'legend.fontsize': 8,
    })

    fig = plt.figure(figsize=(18, 14))
    fig.suptitle('Autobahn Benchmark Dashboard\nSync HotStuff + Fair Ordering (TEE) · 4 Replicas · Local',
                 fontsize=16, fontweight='bold', y=0.98)

    gs = GridSpec(3, 3, figure=fig, hspace=0.35, wspace=0.3)

    # ─── Panel 1: Throughput over time (best run) ─────────────────────
    ax1 = fig.add_subplot(gs[0, 0:2])
    # Pick the run with most data points
    best_run = max(all_runs.items(), key=lambda x: sum(len(df) for df in x[1].values()))
    best_label, best_data = best_run

    for i, (rid, df) in enumerate(sorted(best_data.items())):
        if 'time' in df.columns and 'commit' in df.columns:
            ax1.plot(df['time'], df['commit'],
                    label=f'Replica {rid}', color=COLORS[i % 4],
                    linewidth=1.3, alpha=0.85)

    ax1.set_xlabel('Time (seconds)')
    ax1.set_ylabel('Committed Txns / second')
    ax1.set_title(f'Throughput Over Time ({best_label})')
    ax1.legend(loc='upper right')
    ax1.set_ylim(bottom=0)

    # ─── Panel 2: Cumulative commits ─────────────────────────────────
    ax2 = fig.add_subplot(gs[0, 2])
    for i, (rid, df) in enumerate(sorted(best_data.items())):
        if 'time' in df.columns and 'commit' in df.columns:
            cum = df['commit'].cumsum()
            ax2.plot(df['time'], cum,
                    label=f'R{rid}', color=COLORS[i % 4],
                    linewidth=1.5)

    ax2.set_xlabel('Time (s)')
    ax2.set_ylabel('Cumulative Committed Txns')
    ax2.set_title(f'Total Committed ({best_label})')
    ax2.legend()
    ax2.set_ylim(bottom=0)

    # ─── Panel 3: Throughput comparison across block sizes ────────────
    ax3 = fig.add_subplot(gs[1, 0])
    labels = []
    avg_tps = []
    peak_tps = []
    total_committed = []

    for run_label, replica_data in sorted(all_runs.items()):
        labels.append(run_label)
        all_commit = []
        total_c = 0
        for rid, df in replica_data.items():
            if 'commit' in df.columns:
                valid = df[df['commit'] > 0]['commit'].values
                all_commit.extend(valid)
                total_c += df['commit'].sum()

        avg_tps.append(np.mean(all_commit) if all_commit else 0)
        peak_tps.append(np.max(all_commit) if all_commit else 0)
        total_committed.append(total_c / len(replica_data))  # per replica avg

    x = np.arange(len(labels))
    width = 0.35
    ax3.bar(x - width/2, avg_tps, width, label='Avg TPS', color='#2196F3', alpha=0.85, edgecolor='white')
    ax3.bar(x + width/2, peak_tps, width, label='Peak TPS', color='#FF5722', alpha=0.85, edgecolor='white')
    ax3.set_xlabel('Block Size Config')
    ax3.set_ylabel('Transactions / second')
    ax3.set_title('Throughput by Block Size')
    ax3.set_xticks(x)
    ax3.set_xticklabels(labels)
    ax3.legend()

    # ─── Panel 4: Total committed txns comparison ─────────────────────
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.bar(labels, total_committed, color='#4CAF50', alpha=0.85, edgecolor='white')
    for i, v in enumerate(total_committed):
        ax4.text(i, v + max(total_committed)*0.02, f'{v:.0f}', ha='center', fontsize=9, fontweight='bold')
    ax4.set_xlabel('Block Size Config')
    ax4.set_ylabel('Total Committed Txns (per replica)')
    ax4.set_title('Total Committed Transactions')

    # ─── Panel 5: Execute Latency ────────────────────────────────────
    ax5 = fig.add_subplot(gs[1, 2])
    for i, (rid, df) in enumerate(sorted(best_data.items())):
        if 'time' in df.columns and 'execute_latency' in df.columns:
            valid = df[df['execute_latency'].notna() & (df['execute_latency'] > 0)]
            if not valid.empty:
                ax5.plot(valid['time'], valid['execute_latency'] * 1000,
                        label=f'R{rid}', color=COLORS[i % 4],
                        linewidth=1.0, alpha=0.8)

    ax5.set_xlabel('Time (s)')
    ax5.set_ylabel('Execute Latency (ms)')
    ax5.set_title(f'Execute Latency ({best_label})')
    ax5.legend()
    ax5.set_ylim(bottom=0)

    # ─── Panel 6: CPU Usage ───────────────────────────────────────────
    ax6 = fig.add_subplot(gs[2, 0])
    for i, (rid, df) in enumerate(sorted(best_data.items())):
        if 'time' in df.columns and 'cpu_usage' in df.columns:
            valid = df[df['cpu_usage'].notna() & (df['cpu_usage'] > 0) & (df['cpu_usage'] < 100)]
            if not valid.empty:
                ax6.plot(valid['time'], valid['cpu_usage'],
                        label=f'R{rid}', color=COLORS[i % 4],
                        linewidth=1.0, alpha=0.8)

    ax6.set_xlabel('Time (s)')
    ax6.set_ylabel('CPU Usage (%)')
    ax6.set_title(f'CPU Usage ({best_label})')
    ax6.legend()
    ax6.set_ylim(0, 50)

    # ─── Panel 7: Broadcasts per interval ─────────────────────────────
    ax7 = fig.add_subplot(gs[2, 1])
    for i, (rid, df) in enumerate(sorted(best_data.items())):
        if 'time' in df.columns and 'broad_cast' in df.columns:
            valid = df[df['broad_cast'] > 0]
            if not valid.empty:
                ax7.plot(valid['time'], valid['broad_cast'],
                        label=f'R{rid}', color=COLORS[i % 4],
                        linewidth=1.0, alpha=0.8)

    ax7.set_xlabel('Time (s)')
    ax7.set_ylabel('Broadcasts / interval')
    ax7.set_title(f'Network Broadcasts ({best_label})')
    ax7.legend()
    ax7.set_ylim(bottom=0)

    # ─── Panel 8: Fair Ordering (if available) or Summary stats ───────
    ax8 = fig.add_subplot(gs[2, 2])

    # Check if we have fair ordering data
    has_fo = False
    for run_label, fo_data in all_fo.items():
        if fo_data:
            has_fo = True
            first_rid = min(fo_data.keys())
            fo_df = fo_data[first_rid]
            if not fo_df.empty:
                ax8.bar(fo_df['slot'], fo_df['fair_txns'], color='#9C27B0', alpha=0.8, edgecolor='white')
                ax8.set_xlabel('Consensus Slot')
                ax8.set_ylabel('Fairly Ordered Txns')
                ax8.set_title(f'Fair Ordering per Slot ({run_label})')
            break

    if not has_fo:
        # Summary text instead
        ax8.axis('off')
        summary_lines = []
        for run_label, replica_data in sorted(all_runs.items()):
            all_commit = []
            for rid, df in replica_data.items():
                if 'commit' in df.columns:
                    valid = df[df['commit'] > 0]['commit'].values
                    all_commit.extend(valid)
            if all_commit:
                summary_lines.append(f"{run_label}:")
                summary_lines.append(f"  Avg: {np.mean(all_commit):.0f} txn/s")
                summary_lines.append(f"  Peak: {np.max(all_commit):.0f} txn/s")
                summary_lines.append(f"  Median: {np.median(all_commit):.0f} txn/s")
                summary_lines.append("")

        summary = "\n".join(summary_lines)
        ax8.text(0.1, 0.9, "Performance Summary\n" + "─" * 30 + "\n" + summary,
                transform=ax8.transAxes, fontsize=10, verticalalignment='top',
                fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='#e8f5e9', alpha=0.8))
        ax8.set_title('Summary Statistics')

    plt.savefig(os.path.join(plots_dir, 'dashboard.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\nDashboard saved: {os.path.join(plots_dir, 'dashboard.png')}")

    # ─── Per-run throughput comparison plot ─────────────────────────────
    if len(all_runs) >= 2:
        fig2, axes = plt.subplots(1, len(all_runs), figsize=(6 * len(all_runs), 5), sharey=True)
        fig2.suptitle('Throughput Comparison Across Block Sizes', fontsize=14, fontweight='bold')

        for ax_idx, (run_label, replica_data) in enumerate(sorted(all_runs.items())):
            ax = axes[ax_idx] if len(all_runs) > 1 else axes
            for i, (rid, df) in enumerate(sorted(replica_data.items())):
                if 'time' in df.columns and 'commit' in df.columns:
                    ax.plot(df['time'], df['commit'],
                           label=f'R{rid}', color=COLORS[i % 4],
                           linewidth=1.0, alpha=0.8)

            ax.set_xlabel('Time (s)')
            if ax_idx == 0:
                ax.set_ylabel('Committed Txns / s')
            ax.set_title(run_label)
            ax.legend(fontsize=7)
            ax.set_ylim(bottom=0)

        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'throughput_comparison.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Comparison saved: {os.path.join(plots_dir, 'throughput_comparison.png')}")

    print("\nDone!")


if __name__ == '__main__':
    main()
