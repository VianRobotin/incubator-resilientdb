#!/bin/bash
# Re-run the failed FairDAG-OL/Tusk rate rows once every sweep that uses the
# baselines/fairdag/scripts tree has finished (OL/Tusk, then BOF).
while pgrep -f run_fairdag_ol_tusk.sh > /dev/null || pgrep -f run_fairdag_bof.sh > /dev/null; do sleep 60; done
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
echo "=== reruns started $(date) ===" >> $L/driver.log
# tusk n=22 rate=1000 reps 1 and 3 (344.7 and 1.7 tx/s) are failures but have
# tps>0, so the orchestrator would keep them: move them out first.  Rows with
# tps<=0 are retried automatically.
mkdir -p $L/rerun_rows
python3 - <<'EOF'
import csv
L = "/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29"
src = L + "/rate/tusk.csv"
with open(src) as f:
    rows = list(csv.reader(f))
head, body = rows[0], rows[1:]
bad = lambda r: r[1] == "22" and r[4] == "1000" and r[5] in ("1", "3") and float(r[8]) < 500
with open(L + "/rerun_rows/tusk_rate.csv", "w", newline="") as f:
    csv.writer(f).writerows([head] + [r for r in body if bad(r)])
with open(src + ".new", "w", newline="") as f:
    csv.writer(f).writerows([head] + [r for r in body if not bad(r)])
EOF
mv $L/rate/tusk.csv.new $L/rate/tusk.csv
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
python3 -u performance/das_baselines_reps.py --systems fairdag-ol tusk >> $L/rate_reruns.log 2>&1
echo "reruns done $(date)" >> $L/driver.log
