#!/bin/bash
# FairDAG-OL + Tusk rerun (passed the 2026-10-29 smoke test). Uses the
# baselines/fairdag/scripts tree: run nothing else from that tree meanwhile.
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
echo "=== fairdag-ol+tusk started $(date) ===" >> $L/driver.log
python3 -u performance/das_baselines_reps.py --systems fairdag-ol tusk >> $L/rate_fairdag_ol_tusk.log 2>&1
echo "rate done $(date)" >> $L/driver.log
python3 -u performance/das_faulty.py --systems fairdag-ol tusk --reps 3 >> $L/faulty_fairdag_ol_tusk.log 2>&1
echo "faulty done $(date)" >> $L/driver.log
python3 -u performance/das_batch.py --systems fairdag-ol tusk --reps 3 >> $L/batch_fairdag_ol_tusk.log 2>&1
echo "batch done $(date)" >> $L/driver.log
