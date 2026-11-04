#!/bin/bash
# Themis, then Pompe (Pompe passed the 2026-10-31 smoke test after the
# commit_set race fix). Uses the baselines/narwhal tree: run nothing else
# from that tree meanwhile.
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
for s in themis pompe; do
  echo "=== $s started $(date) ===" >> $L/driver.log
  python3 -u performance/das_baselines_reps.py --systems $s >> $L/rate_$s.log 2>&1
  echo "$s rate done $(date)" >> $L/driver.log
  python3 -u performance/das_faulty.py --systems $s --reps 3 >> $L/faulty_$s.log 2>&1
  echo "$s faulty done $(date)" >> $L/driver.log
  python3 -u performance/das_batch.py --systems $s --reps 3 >> $L/batch_$s.log 2>&1
  echo "$s batch done $(date)" >> $L/driver.log
done
