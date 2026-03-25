#!/usr/bin/env python3
"""
Autobahn vs FairDAG Benchmark Comparison
Parses monitor logs and generates comparison plots.

Usage: python3 benchmark_compare.py <results_dir>
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

COLORS = {
    'autobahn': '#2196F3',
    'fairdag': '#FF5722',
}
REPLICA_COLORS = ['#2196F3', '#FF5722', '#4CAF50', '#9C27B0']


def parse_monitor_line(line):
    """Extract key metrics from a monitor data line."""
    metrics = {}

    # Integer counters
    int_patterns = {
        'server_call': r'server call:(\d+)',
        'broad_cast': r'broad_cast:(\d+)',
        'propose': r'propose:(\d+)',
        'commit': r'(?<= )commit:(\d+)',
        'execute_done': r'execute done:(\d+)',
        'pending_execute': r'pending execute:(\d+)',
        'time': r'time:(\d+)',
        'total_request': r'total request:(\d+)',
        'txn': r'txn:(\d+)',
        'new_transactions': r'new transactions:(\d+)',
        'consumed_transactions': r'consumed transactions:(\d+)',
        'commit_txn_count': r'commit_txn num:(\d+)',
    }

    # Float patterns (may be -nan)
    float_patterns = {
        'execute_latency': r'execute latency :([0-9.e+\-]+)',
        'execute_queuing_latency': r'execute_queuing latency :([0-9.e+\-]+)',
        'commit_latency': r'commit latency :([0-9.e+\-]+)',
        'queuing_latency': r'queuing latency :([0-9.e+\-]+)',
        'round_latency': r'round latency :([0-9.e+\-]+)',
        'verify_latency': r'verify latency :([0-9.e+\-]+)',
        'commit_delay_latency': r'commit_delay latency :([0-9.e+\-]+)',
        'commit_interval_latency': r'commit_interval latency :([0-9.e+\-]+)',
        'cpu_usage': r'cpu usage:([0-9.e+\-]+)',
    }

    for key, pat in int_patterns.items():
        m = re.search(pat, line)
        if m:
            try:
                metrics[key] = int(m.group(1))
            except ValueError:
                pass

    for key, pat in float_patterns.items():
        m = re.search(pat, line)
        if m:
            try:
                val = float(m.group(1))
                if np.isfinite(val) and val >= 0 and val < 1e6:
                    metrics[key] = val
            except ValueError:
                pass

    return metrics


def parse_log_file(filepath):
    """Parse server log file extracting time-series and fair ordering data."""
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


def parse_run(log_dir):
    """Parse all replica logs from a benchmark run."""
    replica_data = {}
    fair_ordering_data = {}
    for lf in sorted(glob.glob(os.path.join(log_dir, 'server_*.log'))):
        rid = int(re.search(r'server_(\d+)', lf).group(1))
        df, fo_df = parse_log_file(lf)
        if not df.empty:
            replica_data[rid] = df
        if not fo_df.empty:
            fair_ordering_data[rid] = fo_df
    return replica_data, fair_ordering_data


def compute_stats(replica_data, warmup=12):
    """Compute aggregate stats from replica data, skipping warmup period."""
    all_commit = []
    all_exec_lat = []
    all_cpu = []
    total_committed = 0

    for rid, df in replica_data.items():
        if 'time' not in df.columns:
            continue
        steady = df[df['time'] >= warmup]

        if 'commit' in steady.columns:
            vals = steady[steady['commit'] > 0]['commit'].values
            all_commit.extend(vals)
            total_committed += steady['commit'].sum()

        if 'execute_latency' in steady.columns:
            vals = steady[steady['execute_latency'].notna() & (steady['execute_latency'] > 0)]['execute_latency'].values
            all_exec_lat.extend(vals)

        if 'cpu_usage' in steady.columns:
            vals = steady[steady['cpu_usage'].notna() & (steady['cpu_usage'] > 0)]['cpu_usage'].values
            all_cpu.extend(vals)

    n_replicas = len(replica_data)
    return {
        'avg_tps': np.mean(all_commit) if all_commit else 0,
        'peak_tps': np.max(all_commit) if all_commit else 0,
        'median_tps': np.median(all_commit) if all_commit else 0,
        'total_committed_per_replica': total_committed / n_replicas if n_replicas else 0,
        'avg_exec_lat_ms': np.mean(all_exec_lat) * 1000 if all_exec_lat else 0,
        'p99_exec_lat_ms': np.percentile(all_exec_lat, 99) * 1000 if all_exec_lat else 0,
        'avg_cpu': np.mean(all_cpu) if all_cpu else 0,
        'n_replicas': n_replicas,
    }


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "benchmark_results"
    plots_dir = os.path.join(results_dir, 'plots')
    os.makedirs(plots_dir, exist_ok=True)

    plt.rcParams.update({
        'figure.facecolor': 'white',
        'axes.facecolor': '#f8f9fa',
        'axes.grid': True,
        'grid.alpha': 0.3,
        'grid.linestyle': '--',
        'font.size': 11,
        'axes.titlesize': 13,
        'axes.labelsize': 11,
        'legend.fontsize': 9,
        'figure.dpi': 150,
    })

    # ── Parse all runs ────────────────────────────────────────────────

    protocols = {}  # {protocol_name: {replica_data, fair_ordering, stats}}

    for log_dir in sorted(glob.glob(os.path.join(results_dir, 'logs_*'))):
        label = os.path.basename(log_dir).replace('logs_', '')
        rd, fo = parse_run(log_dir)
        if rd:
            stats = compute_stats(rd)
            protocols[label] = {'replica_data': rd, 'fair_ordering': fo, 'stats': stats}
            print(f"Parsed {label}: {len(rd)} replicas, "
                  f"{sum(len(df) for df in rd.values())} samples, "
                  f"avg TPS={stats['avg_tps']:.0f}")

    if not protocols:
        print("No data found!")
        sys.exit(1)

    # ── 1) Per-protocol throughput plots ──────────────────────────────

    for label, pdata in protocols.items():
        rd = pdata['replica_data']
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        fig.suptitle(f'Throughput Over Time — {label}', fontsize=14, fontweight='bold')

        for i, (rid, df) in enumerate(sorted(rd.items())):
            if 'time' in df.columns and 'commit' in df.columns:
                ax1.plot(df['time'], df['commit'],
                        label=f'Replica {rid}', color=REPLICA_COLORS[i % 4],
                        linewidth=1.3, alpha=0.85)
            if 'time' in df.columns and 'execute_done' in df.columns:
                ax2.plot(df['time'], df['execute_done'],
                        label=f'Replica {rid}', color=REPLICA_COLORS[i % 4],
                        linewidth=1.3, alpha=0.85)

        ax1.set_ylabel('Committed Txns / s')
        ax1.set_title('Committed Transactions')
        ax1.legend(loc='upper right')
        ax1.set_ylim(bottom=0)
        ax2.set_xlabel('Time (seconds)')
        ax2.set_ylabel('Executed Txns / s')
        ax2.set_title('Executed Transactions')
        ax2.legend(loc='upper right')
        ax2.set_ylim(bottom=0)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f'throughput_{label}.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Saved throughput_{label}.png")

    # ── 2) Per-protocol latency + CPU plots ──────────────────────────

    for label, pdata in protocols.items():
        rd = pdata['replica_data']
        fig, axes = plt.subplots(2, 2, figsize=(14, 8))
        fig.suptitle(f'Latency & Resource Usage — {label}', fontsize=14, fontweight='bold')

        panels = [
            (axes[0, 0], 'execute_latency', 'Execute Latency (ms)', 1000),
            (axes[0, 1], 'execute_queuing_latency', 'Execute Queuing Latency (ms)', 1000),
            (axes[1, 0], 'cpu_usage', 'CPU Usage (%)', 1),
            (axes[1, 1], 'broad_cast', 'Broadcasts / interval', 1),
        ]

        for ax, key, ylabel, scale in panels:
            has_data = False
            for i, (rid, df) in enumerate(sorted(rd.items())):
                if 'time' in df.columns and key in df.columns:
                    valid = df[df[key].notna() & (df[key] > 0)]
                    if not valid.empty:
                        ax.plot(valid['time'], valid[key] * scale,
                               label=f'R{rid}', color=REPLICA_COLORS[i % 4],
                               linewidth=1.0, alpha=0.8)
                        has_data = True
            ax.set_xlabel('Time (s)')
            ax.set_ylabel(ylabel)
            ax.set_title(ylabel)
            if has_data:
                ax.legend(fontsize=8)
            ax.set_ylim(bottom=0)

        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f'latency_{label}.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Saved latency_{label}.png")

    # ── 3) Per-protocol consensus progress ───────────────────────────

    for label, pdata in protocols.items():
        rd = pdata['replica_data']
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        fig.suptitle(f'Consensus Progress — {label}', fontsize=14, fontweight='bold')

        for i, (rid, df) in enumerate(sorted(rd.items())):
            if 'time' in df.columns and 'commit' in df.columns:
                ax1.plot(df['time'], df['commit'].cumsum(),
                        label=f'R{rid}', color=REPLICA_COLORS[i % 4], linewidth=1.5)
            if 'time' in df.columns and 'broad_cast' in df.columns:
                ax2.plot(df['time'], df['broad_cast'].cumsum(),
                        label=f'R{rid}', color=REPLICA_COLORS[i % 4], linewidth=1.5)

        ax1.set_xlabel('Time (s)')
        ax1.set_ylabel('Cumulative Committed Txns')
        ax1.set_title('Commits')
        ax1.legend()
        ax1.set_ylim(bottom=0)
        ax2.set_xlabel('Time (s)')
        ax2.set_ylabel('Cumulative Broadcasts')
        ax2.set_title('Network Broadcasts')
        ax2.legend()
        ax2.set_ylim(bottom=0)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f'consensus_{label}.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Saved consensus_{label}.png")

    # ── 4) Fair ordering per-slot (if available) ─────────────────────

    for label, pdata in protocols.items():
        fo = pdata['fair_ordering']
        if not fo:
            continue
        first_rid = min(fo.keys())
        fo_df = fo[first_rid]
        if fo_df.empty:
            continue

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        fig.suptitle(f'Fair Ordering — {label}', fontsize=14, fontweight='bold')

        ax1.bar(fo_df['slot'], fo_df['fair_txns'], color=COLORS.get(label.split('_')[0], '#2196F3'),
                alpha=0.8, edgecolor='white')
        ax1.set_xlabel('Consensus Slot')
        ax1.set_ylabel('Fairly Ordered Txns')
        ax1.set_title('Per-Slot Fair Ordering')

        fo_sorted = fo_df.sort_values('slot')
        cum = fo_sorted['fair_txns'].cumsum()
        ax2.plot(fo_sorted['slot'], cum, 'o-', color='#4CAF50', linewidth=2, markersize=5)
        ax2.fill_between(fo_sorted['slot'], cum, alpha=0.2, color='#4CAF50')
        ax2.set_xlabel('Consensus Slot')
        ax2.set_ylabel('Cumulative')
        ax2.set_title('Cumulative Fair Ordering')

        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, f'fair_ordering_{label}.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Saved fair_ordering_{label}.png")

    # ── 5) COMPARISON DASHBOARD ──────────────────────────────────────

    # Separate autobahn and fairdag runs
    autobahn_runs = {k: v for k, v in protocols.items() if 'autobahn' in k.lower()}
    fairdag_runs = {k: v for k, v in protocols.items() if 'fairdag' in k.lower()}

    if autobahn_runs and fairdag_runs:
        print("\n=== Generating Comparison Dashboard ===")

        fig = plt.figure(figsize=(18, 14))
        fig.suptitle('Autobahn vs FairDAG — Fair Ordering Benchmark Comparison\n'
                     'Autobahn: Sync HotStuff (2f+1, 3 replicas) · FairDAG: Async DAG/Tusk (3f+1, 4 replicas)',
                     fontsize=15, fontweight='bold', y=0.99)
        gs = GridSpec(3, 3, figure=fig, hspace=0.4, wspace=0.35)

        # Get the main run for each protocol
        ab_label = list(autobahn_runs.keys())[0]
        fd_label = list(fairdag_runs.keys())[0]
        ab = autobahn_runs[ab_label]
        fd = fairdag_runs[fd_label]

        # Panel 1: Throughput comparison over time (overlay)
        ax1 = fig.add_subplot(gs[0, 0:2])
        # Average across replicas for each protocol
        for proto_label, pdata, color in [(f'Autobahn ({ab_label})', ab, COLORS['autobahn']),
                                           (f'FairDAG ({fd_label})', fd, COLORS['fairdag'])]:
            rd = pdata['replica_data']
            # Merge all replicas by time
            all_dfs = []
            for rid, df in rd.items():
                if 'time' in df.columns and 'commit' in df.columns:
                    all_dfs.append(df[['time', 'commit']].set_index('time'))
            if all_dfs:
                merged = pd.concat(all_dfs, axis=1)
                avg = merged.mean(axis=1)
                ax1.plot(avg.index, avg.values, label=proto_label, color=color,
                        linewidth=2.0, alpha=0.9)

        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('Avg Committed Txns / s')
        ax1.set_title('Throughput Over Time (averaged across replicas)')
        ax1.legend(fontsize=11)
        ax1.set_ylim(bottom=0)

        # Panel 2: Bar chart comparison
        ax2 = fig.add_subplot(gs[0, 2])
        proto_labels = []
        avg_vals = []
        peak_vals = []
        colors_list = []
        for lbl, pdata in [(ab_label, ab), (fd_label, fd)]:
            proto_labels.append(lbl)
            avg_vals.append(pdata['stats']['avg_tps'])
            peak_vals.append(pdata['stats']['peak_tps'])
            colors_list.append(COLORS.get('autobahn' if 'autobahn' in lbl else 'fairdag'))

        x = np.arange(len(proto_labels))
        width = 0.35
        ax2.bar(x - width/2, avg_vals, width, label='Avg TPS', color=colors_list, alpha=0.7, edgecolor='white')
        ax2.bar(x + width/2, peak_vals, width, label='Peak TPS', color=colors_list, alpha=1.0, edgecolor='white')
        ax2.set_xticks(x)
        ax2.set_xticklabels(proto_labels, fontsize=9)
        ax2.set_ylabel('Transactions / s')
        ax2.set_title('Throughput Comparison')
        ax2.legend()

        # Panel 3: Cumulative commits
        ax3 = fig.add_subplot(gs[1, 0])
        for proto_label, pdata, color in [(ab_label, ab, COLORS['autobahn']),
                                           (fd_label, fd, COLORS['fairdag'])]:
            rd = pdata['replica_data']
            first_rid = min(rd.keys())
            df = rd[first_rid]
            if 'time' in df.columns and 'commit' in df.columns:
                ax3.plot(df['time'], df['commit'].cumsum(),
                        label=proto_label, color=color, linewidth=2.0)

        ax3.set_xlabel('Time (s)')
        ax3.set_ylabel('Cumulative Committed Txns')
        ax3.set_title('Cumulative Commits (Replica 1)')
        ax3.legend(fontsize=9)
        ax3.set_ylim(bottom=0)

        # Panel 4: Execute latency comparison
        ax4 = fig.add_subplot(gs[1, 1])
        for proto_label, pdata, color in [(ab_label, ab, COLORS['autobahn']),
                                           (fd_label, fd, COLORS['fairdag'])]:
            rd = pdata['replica_data']
            first_rid = min(rd.keys())
            df = rd[first_rid]
            if 'time' in df.columns and 'execute_latency' in df.columns:
                valid = df[df['execute_latency'].notna() & (df['execute_latency'] > 0)]
                if not valid.empty:
                    ax4.plot(valid['time'], valid['execute_latency'] * 1000,
                            label=proto_label, color=color, linewidth=1.5, alpha=0.8)

        ax4.set_xlabel('Time (s)')
        ax4.set_ylabel('Execute Latency (ms)')
        ax4.set_title('Execute Latency')
        ax4.legend(fontsize=9)
        ax4.set_ylim(bottom=0)

        # Panel 5: CPU usage comparison
        ax5 = fig.add_subplot(gs[1, 2])
        for proto_label, pdata, color in [(ab_label, ab, COLORS['autobahn']),
                                           (fd_label, fd, COLORS['fairdag'])]:
            rd = pdata['replica_data']
            all_cpu = []
            all_time = []
            for rid, df in sorted(rd.items()):
                if 'time' in df.columns and 'cpu_usage' in df.columns:
                    valid = df[df['cpu_usage'].notna() & (df['cpu_usage'] > 0) & (df['cpu_usage'] < 100)]
                    if not valid.empty:
                        ax5.plot(valid['time'], valid['cpu_usage'],
                                color=color, linewidth=0.8, alpha=0.4)
            # Add a label for the legend
            ax5.plot([], [], label=proto_label, color=color, linewidth=2)

        ax5.set_xlabel('Time (s)')
        ax5.set_ylabel('CPU Usage (%)')
        ax5.set_title('CPU Usage (all replicas)')
        ax5.legend(fontsize=9)
        ax5.set_ylim(0, 50)

        # Panel 6: Broadcasts comparison
        ax6 = fig.add_subplot(gs[2, 0])
        for proto_label, pdata, color in [(ab_label, ab, COLORS['autobahn']),
                                           (fd_label, fd, COLORS['fairdag'])]:
            rd = pdata['replica_data']
            first_rid = min(rd.keys())
            df = rd[first_rid]
            if 'time' in df.columns and 'broad_cast' in df.columns:
                ax6.plot(df['time'], df['broad_cast'],
                        label=proto_label, color=color, linewidth=1.5, alpha=0.8)

        ax6.set_xlabel('Time (s)')
        ax6.set_ylabel('Broadcasts / interval')
        ax6.set_title('Network Messages (Replica 1)')
        ax6.legend(fontsize=9)
        ax6.set_ylim(bottom=0)

        # Panel 7: Total committed bar
        ax7 = fig.add_subplot(gs[2, 1])
        total_vals = [ab['stats']['total_committed_per_replica'],
                      fd['stats']['total_committed_per_replica']]
        bars = ax7.bar(proto_labels, total_vals,
                       color=[COLORS['autobahn'], COLORS['fairdag']], alpha=0.85, edgecolor='white')
        for bar, v in zip(bars, total_vals):
            ax7.text(bar.get_x() + bar.get_width()/2, v + max(total_vals)*0.02,
                    f'{v:,.0f}', ha='center', fontsize=10, fontweight='bold')
        ax7.set_ylabel('Total Committed Txns (per replica)')
        ax7.set_title('Total Committed')

        # Panel 8: Summary table
        ax8 = fig.add_subplot(gs[2, 2])
        ax8.axis('off')
        summary = (
            f"{'Metric':<25} {'Autobahn':>12} {'FairDAG':>12}\n"
            f"{'─'*49}\n"
            f"{'Replicas':<25} {ab['stats']['n_replicas']:>12} {fd['stats']['n_replicas']:>12}\n"
            f"{'Fault Model':<25} {'2f+1 (sync)':>12} {'3f+1 (async)':>12}\n"
            f"{'Avg TPS':<25} {ab['stats']['avg_tps']:>12.0f} {fd['stats']['avg_tps']:>12.0f}\n"
            f"{'Peak TPS':<25} {ab['stats']['peak_tps']:>12.0f} {fd['stats']['peak_tps']:>12.0f}\n"
            f"{'Median TPS':<25} {ab['stats']['median_tps']:>12.0f} {fd['stats']['median_tps']:>12.0f}\n"
            f"{'Total Committed/replica':<25} {ab['stats']['total_committed_per_replica']:>12,.0f} {fd['stats']['total_committed_per_replica']:>12,.0f}\n"
            f"{'Avg Execute Lat (ms)':<25} {ab['stats']['avg_exec_lat_ms']:>12.3f} {fd['stats']['avg_exec_lat_ms']:>12.3f}\n"
            f"{'P99 Execute Lat (ms)':<25} {ab['stats']['p99_exec_lat_ms']:>12.3f} {fd['stats']['p99_exec_lat_ms']:>12.3f}\n"
            f"{'Avg CPU (%)':<25} {ab['stats']['avg_cpu']:>12.1f} {fd['stats']['avg_cpu']:>12.1f}\n"
        )
        ax8.text(0.05, 0.95, summary, transform=ax8.transAxes, fontsize=10,
                verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='#e8f5e9', alpha=0.9))
        ax8.set_title('Performance Summary')

        plt.savefig(os.path.join(plots_dir, 'comparison_dashboard.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Saved comparison_dashboard.png")

    # Also generate per-run comparison if multiple block sizes exist
    all_labels = sorted(protocols.keys())
    if len(all_labels) >= 2:
        fig, ax = plt.subplots(figsize=(10, 6))
        x = np.arange(len(all_labels))
        avg = [protocols[l]['stats']['avg_tps'] for l in all_labels]
        peak = [protocols[l]['stats']['peak_tps'] for l in all_labels]
        colors = [COLORS.get('autobahn' if 'autobahn' in l else 'fairdag', '#888')
                  for l in all_labels]

        width = 0.35
        ax.bar(x - width/2, avg, width, label='Avg TPS', color=colors, alpha=0.7, edgecolor='white')
        ax.bar(x + width/2, peak, width, label='Peak TPS', color=colors, alpha=1.0, edgecolor='white')
        ax.set_xticks(x)
        ax.set_xticklabels(all_labels, rotation=30, ha='right')
        ax.set_ylabel('Transactions / second')
        ax.set_title('Throughput Comparison — All Configurations')
        ax.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'all_comparison.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Saved all_comparison.png")

    # Print text summary
    print("\n" + "=" * 60)
    print("  BENCHMARK RESULTS SUMMARY")
    print("=" * 60)
    for label, pdata in sorted(protocols.items()):
        s = pdata['stats']
        print(f"\n--- {label} ({s['n_replicas']} replicas) ---")
        print(f"  Avg TPS:    {s['avg_tps']:.0f}")
        print(f"  Peak TPS:   {s['peak_tps']:.0f}")
        print(f"  Median TPS: {s['median_tps']:.0f}")
        print(f"  Total committed (per replica): {s['total_committed_per_replica']:,.0f}")
        print(f"  Avg execute latency: {s['avg_exec_lat_ms']:.3f} ms")
        print(f"  Avg CPU: {s['avg_cpu']:.1f}%")

    print(f"\nAll plots saved to: {plots_dir}/")


if __name__ == '__main__':
    main()
