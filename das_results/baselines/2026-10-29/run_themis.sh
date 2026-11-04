#!/bin/bash
# Themis with the tx_uid race and O(L^2) missing-edges fixes (2026-10-31).
# Capacity ~2000 tx/s at n=13; beyond it the prototype collapses (option (a):
# run as-is). Uses baselines/narwhal/benchmark: run nothing else from it.
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
echo "=== themis started $(date) ===" >> $L/driver.log
python3 -u performance/das_baselines_reps.py --systems themis >> $L/rate_themis.log 2>&1
echo "themis rate done $(date)" >> $L/driver.log
python3 -u performance/das_faulty.py --systems themis --reps 3 >> $L/faulty_themis.log 2>&1
echo "themis faulty done $(date)" >> $L/driver.log
python3 -u performance/das_batch.py --systems themis --reps 3 >> $L/batch_themis.log 2>&1
echo "themis batch done $(date)" >> $L/driver.log
