#!/usr/bin/env python3
"""Re-parse all das_runtime logs with the current parse_log and rewrite n_scaling.csv."""
import re, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from das import parse_log, RUNTIME_BASE, RESULTS_DIR

WARMUP  = 15
RUNTIME = 60
TARGET_TPS = 350

dirs = sorted(d for d in RUNTIME_BASE.iterdir() if d.is_dir())

# Group dirs by (n, mode) in discovery order to assign rep numbers
from collections import defaultdict
groups = defaultdict(list)
for d in dirs:
    m = re.match(r'\d{8}_\d{6}_n(\d+)_(\w+)_bs\d+', d.name)
    if m:
        groups[(int(m.group(1)), m.group(2))].append(d)

csv_path = RESULTS_DIR / "n_scaling.csv"
header = "n,mode,rep,tps,consensus_tps,consensus_latency_s,slots_committed,txns_committed,target_tps,runtime_s"
lines = [header]

for (n, mode), exp_dirs in sorted(groups.items()):
    for rep, d in enumerate(exp_dirs, 1):
        log_dir = d / "logs"
        best = (0.0, 0.0, 0.0, 0, 0)
        for i in range(1, n + 1):
            log = log_dir / f"server_{i}.log"
            r = parse_log(log, WARMUP, RUNTIME)
            if r[0] > best[0]:
                best = r
        tps, con_tps, con_lat, slots, txns = best
        lines.append(f"{n},{mode},{rep},{tps},{con_tps},{con_lat},{slots},{txns},{TARGET_TPS},{RUNTIME}")
        print(f"  n={n} {mode} rep={rep}: tps={tps}  con_tps={con_tps}  lat={con_lat}s  slots={slots}  txns={txns}")

csv_path.write_text("\n".join(lines) + "\n")
print(f"\nWrote {len(lines)-1} rows to {csv_path}")
