#!/usr/bin/env bash
# Rest of the full rerun on binary 5d2b68da: wait for the rate sweep
# (PID passed as $1), then run the faulty, batch and 3f+1 committee sweeps
# one after another.  Each sweep resumes, so rerunning this skips done rows.
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
R=das_results/pearl/2026-10-26  # was full_2026-10-26 while it ran
L=../../$R

if [ -n "$1" ]; then
  while kill -0 "$1" 2>/dev/null; do sleep 60; done
fi
echo "=== started $(date) ===" >> $L/driver.log
python3 -u performance/das_faulty.py --systems pearl --pearl-mode ol  --reps 3 --results-dir $R/faulty >> $L/faulty.log 2>&1
python3 -u performance/das_faulty.py --systems pearl --pearl-mode bof --reps 3 --results-dir $R/faulty >> $L/faulty.log 2>&1
echo "faulty done $(date)" >> $L/driver.log
python3 -u performance/das_batch.py  --systems pearl --reps 3 --results-dir $R/batch >> $L/batch.log 2>&1
echo "batch done $(date)" >> $L/driver.log
python3 -u performance/das_pearl_3f1_bof.py --nodes 22 --results-dir $R/committee >> $L/committee.log 2>&1
echo "committee done $(date)" >> $L/driver.log
