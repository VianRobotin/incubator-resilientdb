#!/usr/bin/env python3
"""
Autobahn Benchmark Plotter
Parses monitor logs from ResilientDB's Stats module and generates performance plots.

Usage: python3 plot_benchmark.py <results_dir>
"""

import os
import re
import sys
import glob
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from collections import defaultdict

# ── Parsing ──────────────────────────────────────────────────────────────────

def parse_monitor_line(line):
    """Extract key metrics from a single monitor log line."""
    metrics = {}

    # Pattern: key:value pairs in the monitor output
    patterns = {
        'txn': r'txn:(\d+)',
        'propose': r'propose:(\d+)',
        'commit': r'commit:(\d+)',
        'execute': r'execute:(\d+)',
        'execute_done': r'execute done:(\d+)',
        'pending_execute': r'pending execute:(\d+)',
        'server_call': r'server call:(\d+)',
        'client_req': r'client req:(\d+)',
        'broad_cast': r'broad_cast:(\d+)',
        'new_transactions': r'new transactions:(\d+)',
        'consumed_transactions': r'consumed transactions:(\d+)',
        'total_request': r'total request:(\d+)',
        'time': r'time:(\d+)',
        'commit_txn': r'commit_txn :(\d+)',
        'commit_txn_num': r'commit_txn num:(\d+)',
        'block_size': r'block_size latency :([0-9.e+\-nan]+)',
        'cpu_usage': r'cpu usage:([0-9.e+\-nan]+)',
    }

    # Latency patterns (floating point, may be nan/inf)
    latency_patterns = {
        'queuing_latency': r'queuing latency :([0-9.e+\-naninf]+)',
        'round_latency': r'round latency :([0-9.e+\-naninf]+)',
        'commit_latency': r'commit latency :([0-9.e+\-naninf]+)',
        'verify_latency': r'verify latency :([0-9.e+\-naninf]+)',
        'execute_latency': r'execute latency :([0-9.e+\-naninf]+)',
        'execute_queuing_latency': r'execute_queuing latency :([0-9.e+\-naninf]+)',
        'commit_delay_latency': r'commit_delay latency :([0-9.e+\-naninf]+)',
        'commit_interval_latency': r'commit_interval latency :([0-9.e+\-naninf]+)',
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
                if np.isfinite(val) and val > 0:
                    metrics[key] = val
            except ValueError:
                pass

    return metrics


def parse_log_file(filepath):
    """Parse an entire server log file and extract time-series metrics.

    The Stats monitor output spans two lines:
      Line 1: "=========== monitor ========="
      Line 2: "server call:X server process:X ... time:X ... cpu usage:X"
    We need to grab the line AFTER the monitor header.
    """
    records = []
    fair_ordering = []

    with open(filepath, 'r', errors='replace') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i]

        # The monitor header is on one line, data on the next
        if 'monitor =========' in line and '--------------- monitor' not in line:
            # Data is on the next line(s) — grab up to the separator
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

        # Parse fair ordering lines
        fo_match = re.search(r'SyncHS slot (\d+):.*?fairly ordered (\d+) txns', line)
        if fo_match:
            fair_ordering.append({
                'slot': int(fo_match.group(1)),
                'fair_txns': int(fo_match.group(2))
            })

        i += 1

    df = pd.DataFrame(records)
    fo_df = pd.DataFrame(fair_ordering) if fair_ordering else pd.DataFrame()
    return df, fo_df


def parse_benchmark_run(log_dir):
    """Parse all replica logs from a single benchmark run."""
    replica_data = {}
    fair_ordering_data = {}

    log_files = sorted(glob.glob(os.path.join(log_dir, 'server_*.log')))

    for lf in log_files:
        replica_id = int(re.search(r'server_(\d+)', lf).group(1))
        df, fo_df = parse_log_file(lf)
        if not df.empty:
            replica_data[replica_id] = df
        if not fo_df.empty:
            fair_ordering_data[replica_id] = fo_df

    return replica_data, fair_ordering_data


# ── Plotting ─────────────────────────────────────────────────────────────────

COLORS = ['#2196F3', '#FF5722', '#4CAF50', '#9C27B0', '#FF9800', '#00BCD4']

def setup_style():
    """Set up clean plot styling."""
    plt.rcParams.update({
        'figure.facecolor': 'white',
        'axes.facecolor': '#fafafa',
        'axes.grid': True,
        'grid.alpha': 0.3,
        'grid.linestyle': '--',
        'font.size': 11,
        'axes.titlesize': 13,
        'axes.labelsize': 11,
        'legend.fontsize': 9,
        'figure.dpi': 150,
    })


def plot_throughput_over_time(replica_data, title_suffix, save_path):
    """Plot committed transactions per second over time for each replica."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    fig.suptitle(f'Throughput Over Time — {title_suffix}', fontsize=14, fontweight='bold')

    for i, (rid, df) in enumerate(sorted(replica_data.items())):
        if 'time' not in df.columns:
            continue

        # Plot committed txns/s (the primary throughput metric)
        if 'commit' in df.columns:
            valid = df[['time', 'commit']].dropna()
            ax1.plot(valid['time'], valid['commit'],
                    label=f'Replica {rid}', color=COLORS[i % len(COLORS)],
                    linewidth=1.5, alpha=0.85)

        # Plot executed txns/s
        if 'execute_done' in df.columns:
            valid = df[['time', 'execute_done']].dropna()
            ax2.plot(valid['time'], valid['execute_done'],
                    label=f'Replica {rid}', color=COLORS[i % len(COLORS)],
                    linewidth=1.5, alpha=0.85)

    ax1.set_ylabel('Committed Txns / interval')
    ax1.set_title('Committed Transactions')
    ax1.legend(loc='upper right')
    ax1.set_ylim(bottom=0)

    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('Executed Txns / interval')
    ax2.set_title('Executed Transactions')
    ax2.legend(loc='upper right')
    ax2.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {save_path}")


def plot_latency_breakdown(replica_data, title_suffix, save_path):
    """Plot latency breakdown (commit, execute, queuing, verify) over time."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    fig.suptitle(f'Latency Breakdown — {title_suffix}', fontsize=14, fontweight='bold')

    latency_keys = [
        ('execute_latency', 'Execute Latency (s)'),
        ('execute_queuing_latency', 'Execute Queuing Latency (s)'),
        ('commit_latency', 'Commit Latency (s)'),
        ('cpu_usage', 'CPU Usage (%)'),
    ]

    for ax_idx, (key, ylabel) in enumerate(latency_keys):
        ax = axes[ax_idx // 2][ax_idx % 2]
        has_data = False
        for i, (rid, df) in enumerate(sorted(replica_data.items())):
            if 'time' in df.columns and key in df.columns:
                valid = df[df[key].notna() & (df[key] > 0) & (df[key] < 100)]
                if not valid.empty:
                    ax.plot(valid['time'], valid[key],
                           label=f'Replica {rid}', color=COLORS[i % len(COLORS)],
                           linewidth=1.2, alpha=0.8)
                    has_data = True

        ax.set_xlabel('Time (s)')
        ax.set_ylabel(ylabel)
        ax.set_title(ylabel.replace(' (s)', ''))
        if has_data:
            ax.legend(fontsize=8)
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {save_path}")


def plot_commits_and_proposals(replica_data, title_suffix, save_path):
    """Plot cumulative commits and proposals over time."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(f'Consensus Progress — {title_suffix}', fontsize=14, fontweight='bold')

    for i, (rid, df) in enumerate(sorted(replica_data.items())):
        if 'time' in df.columns:
            if 'commit' in df.columns:
                cum_commit = df['commit'].cumsum()
                ax1.plot(df['time'], cum_commit,
                        label=f'Replica {rid}', color=COLORS[i % len(COLORS)],
                        linewidth=1.5)
            if 'propose' in df.columns:
                cum_propose = df['propose'].cumsum()
                ax2.plot(df['time'], cum_propose,
                        label=f'Replica {rid}', color=COLORS[i % len(COLORS)],
                        linewidth=1.5)

    ax1.set_xlabel('Time (s)')
    ax1.set_ylabel('Cumulative Commits')
    ax1.set_title('Commits Over Time')
    ax1.legend()
    ax1.set_ylim(bottom=0)

    ax2.set_xlabel('Time (s)')
    ax2.set_ylabel('Cumulative Proposals')
    ax2.set_title('Proposals Over Time')
    ax2.legend()
    ax2.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {save_path}")


def plot_fair_ordering(fair_ordering_data, title_suffix, save_path):
    """Plot fair ordering distribution across consensus slots."""
    if not fair_ordering_data:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(f'Fair Ordering Distribution — {title_suffix}', fontsize=14, fontweight='bold')

    # Per-slot fair ordering from one replica (they should all agree)
    first_rid = min(fair_ordering_data.keys())
    fo_df = fair_ordering_data[first_rid]

    if not fo_df.empty:
        # Bar chart of fair txns per slot
        ax1.bar(fo_df['slot'], fo_df['fair_txns'], color='#2196F3', alpha=0.8, edgecolor='white')
        ax1.set_xlabel('Consensus Slot')
        ax1.set_ylabel('Fairly Ordered Transactions')
        ax1.set_title(f'Fair Ordering per Slot (Replica {first_rid})')

        # Cumulative fair ordering
        fo_sorted = fo_df.sort_values('slot')
        cum_fair = fo_sorted['fair_txns'].cumsum()
        ax2.plot(fo_sorted['slot'], cum_fair, 'o-', color='#4CAF50', linewidth=2, markersize=5)
        ax2.fill_between(fo_sorted['slot'], cum_fair, alpha=0.2, color='#4CAF50')
        ax2.set_xlabel('Consensus Slot')
        ax2.set_ylabel('Cumulative Fairly Ordered Txns')
        ax2.set_title('Cumulative Fair Ordering')

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {save_path}")


def plot_cpu_usage(replica_data, title_suffix, save_path):
    """Plot CPU usage over time per replica."""
    fig, ax = plt.subplots(figsize=(12, 5))

    has_data = False
    for i, (rid, df) in enumerate(sorted(replica_data.items())):
        if 'time' in df.columns and 'cpu_usage' in df.columns:
            valid = df[df['cpu_usage'].notna() & (df['cpu_usage'] > 0) & (df['cpu_usage'] < 100)]
            if not valid.empty:
                ax.plot(valid['time'], valid['cpu_usage'],
                       label=f'Replica {rid}', color=COLORS[i % len(COLORS)],
                       linewidth=1.2, alpha=0.8)
                has_data = True

    if has_data:
        ax.set_xlabel('Time (seconds)')
        ax.set_ylabel('CPU Usage (%)')
        ax.set_title(f'CPU Usage Over Time — {title_suffix}')
        ax.legend()
        ax.set_ylim(0, 100)
        plt.tight_layout()
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Saved: {save_path}")
    else:
        plt.close()


def plot_block_dissemination(replica_data, title_suffix, save_path):
    """Plot block dissemination metrics (new transactions, consumed transactions, broadcast)."""
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle(f'Block Dissemination — {title_suffix}', fontsize=14, fontweight='bold')

    metrics = [
        ('new_transactions', 'New Transactions / interval'),
        ('consumed_transactions', 'Consumed Transactions / interval'),
        ('broad_cast', 'Broadcasts / interval'),
    ]

    for ax_idx, (key, ylabel) in enumerate(metrics):
        ax = axes[ax_idx]
        for i, (rid, df) in enumerate(sorted(replica_data.items())):
            if 'time' in df.columns and key in df.columns:
                valid = df[df[key] > 0]
                if not valid.empty:
                    ax.plot(valid['time'], valid[key],
                           label=f'R{rid}', color=COLORS[i % len(COLORS)],
                           linewidth=1.2, alpha=0.8)
        ax.set_xlabel('Time (s)')
        ax.set_ylabel(ylabel)
        ax.set_title(ylabel.split('/')[0].strip())
        ax.legend(fontsize=8)
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {save_path}")


def plot_comparison_bar(all_runs, save_path):
    """Compare throughput and latency across different block sizes."""
    if len(all_runs) < 2:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle('Performance Comparison Across Block Sizes', fontsize=14, fontweight='bold')

    labels = []
    avg_tps = []
    peak_tps = []
    avg_commit_lat = []

    for run_label, replica_data in sorted(all_runs.items()):
        labels.append(run_label)

        all_txn = []
        all_lat = []
        for rid, df in replica_data.items():
            # Use 'commit' as throughput metric (committed txns per interval)
            if 'commit' in df.columns:
                valid = df[df['commit'] > 0]['commit'].values
                all_txn.extend(valid)
            if 'commit_latency' in df.columns:
                valid = df[df['commit_latency'].notna() & (df['commit_latency'] > 0) & (df['commit_latency'] < 100)]
                all_lat.extend(valid['commit_latency'].values)

        if all_txn:
            avg_tps.append(np.mean(all_txn))
            peak_tps.append(np.max(all_txn))
        else:
            avg_tps.append(0)
            peak_tps.append(0)

        if all_lat:
            avg_commit_lat.append(np.mean(all_lat) * 1000)  # to ms
        else:
            avg_commit_lat.append(0)

    x = np.arange(len(labels))
    width = 0.35

    ax1.bar(x - width/2, avg_tps, width, label='Average TPS', color='#2196F3', alpha=0.8)
    ax1.bar(x + width/2, peak_tps, width, label='Peak TPS', color='#FF5722', alpha=0.8)
    ax1.set_xlabel('Configuration')
    ax1.set_ylabel('Transactions / second')
    ax1.set_title('Throughput Comparison')
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.legend()

    ax2.bar(x, avg_commit_lat, color='#4CAF50', alpha=0.8)
    ax2.set_xlabel('Configuration')
    ax2.set_ylabel('Avg Commit Latency (ms)')
    ax2.set_title('Commit Latency Comparison')
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {save_path}")


def generate_summary(all_runs, save_path):
    """Generate a text summary of all benchmark results."""
    lines = []
    lines.append("=" * 70)
    lines.append("  AUTOBAHN BENCHMARK RESULTS SUMMARY")
    lines.append("  Sync HotStuff + Fair Ordering (TEE)")
    lines.append("=" * 70)
    lines.append("")

    for run_label, replica_data in sorted(all_runs.items()):
        lines.append(f"--- {run_label} ---")

        all_txn = []
        all_commit_lat = []
        total_commits = 0
        total_proposals = 0

        for rid, df in sorted(replica_data.items()):
            if 'commit' in df.columns:
                valid = df[df['commit'] > 0]['commit'].values
                all_txn.extend(valid)
            if 'commit_latency' in df.columns:
                valid = df[df['commit_latency'].notna() & (df['commit_latency'] > 0) & (df['commit_latency'] < 100)]
                all_commit_lat.extend(valid['commit_latency'].values)
            if 'commit' in df.columns:
                total_commits += df['commit'].sum()
            if 'propose' in df.columns:
                total_proposals += df['propose'].sum()

        if all_txn:
            lines.append(f"  Throughput:  avg={np.mean(all_txn):.0f} txn/s  peak={np.max(all_txn):.0f} txn/s  median={np.median(all_txn):.0f} txn/s")
        else:
            lines.append(f"  Throughput:  no data")

        if all_commit_lat:
            lines.append(f"  Commit Lat:  avg={np.mean(all_commit_lat)*1000:.1f} ms  p50={np.percentile(all_commit_lat, 50)*1000:.1f} ms  p99={np.percentile(all_commit_lat, 99)*1000:.1f} ms")

        lines.append(f"  Total commits: {total_commits:.0f}  proposals: {total_proposals:.0f}")
        lines.append(f"  Replicas: {len(replica_data)}")
        lines.append("")

    summary = "\n".join(lines)
    with open(save_path, 'w') as f:
        f.write(summary)
    print(summary)
    return summary


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 plot_benchmark.py <results_dir>")
        sys.exit(1)

    results_dir = sys.argv[1]
    plots_dir = os.path.join(results_dir, 'plots')
    os.makedirs(plots_dir, exist_ok=True)

    setup_style()

    # Find all log directories
    log_dirs = sorted(glob.glob(os.path.join(results_dir, 'logs_*')))

    if not log_dirs:
        print(f"No log directories found in {results_dir}")
        print("Looking for server log files directly...")
        # Check if logs are directly in results_dir or nearby
        log_files = glob.glob(os.path.join(results_dir, '*.log'))
        if not log_files:
            log_files = glob.glob(os.path.join(os.path.dirname(results_dir), 'test_logs', '*.log'))
        if log_files:
            log_dirs = [os.path.dirname(log_files[0])]
            print(f"Found logs in: {log_dirs[0]}")
        else:
            print("No logs found anywhere. Exiting.")
            sys.exit(1)

    print(f"\nFound {len(log_dirs)} benchmark run(s)")

    all_runs = {}
    all_fair_ordering = {}

    for log_dir in log_dirs:
        run_label = os.path.basename(log_dir).replace('logs_', '')
        if not run_label or run_label == os.path.basename(log_dir):
            run_label = "default"

        print(f"\nParsing: {run_label} ({log_dir})")
        replica_data, fo_data = parse_benchmark_run(log_dir)

        if not replica_data:
            print(f"  No data found, skipping")
            continue

        for rid, df in replica_data.items():
            print(f"  Replica {rid}: {len(df)} monitor samples")

        all_runs[run_label] = replica_data
        all_fair_ordering[run_label] = fo_data

        # Per-run plots
        print(f"  Generating plots for {run_label}...")
        plot_throughput_over_time(replica_data, run_label,
                                  os.path.join(plots_dir, f'throughput_{run_label}.png'))
        plot_latency_breakdown(replica_data, run_label,
                                os.path.join(plots_dir, f'latency_{run_label}.png'))
        plot_commits_and_proposals(replica_data, run_label,
                                    os.path.join(plots_dir, f'consensus_{run_label}.png'))
        plot_block_dissemination(replica_data, run_label,
                                  os.path.join(plots_dir, f'dissemination_{run_label}.png'))
        plot_cpu_usage(replica_data, run_label,
                        os.path.join(plots_dir, f'cpu_{run_label}.png'))

        if fo_data:
            plot_fair_ordering(fo_data, run_label,
                                os.path.join(plots_dir, f'fair_ordering_{run_label}.png'))

    # Cross-run comparison
    if len(all_runs) >= 2:
        print("\nGenerating comparison plots...")
        plot_comparison_bar(all_runs, os.path.join(plots_dir, 'comparison.png'))

    # Summary
    print("\n")
    generate_summary(all_runs, os.path.join(results_dir, 'summary.txt'))

    print(f"\nAll plots saved to: {plots_dir}/")


if __name__ == '__main__':
    main()
