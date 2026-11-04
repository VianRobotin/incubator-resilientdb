#!/bin/bash
# FairDAG-BOF (client requests batched, blocks sized in requests; passed the
# 2026-10-31 smoke test). Same baselines/fairdag/scripts tree as OL/Tusk, so
# wait for that driver to finish first.
while pgrep -f run_fairdag_ol_tusk.sh > /dev/null; do sleep 60; done
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
echo "=== fairdag-bof started $(date) ===" >> $L/driver.log
python3 -u performance/das_baselines_reps.py --systems fairdag-bof >> $L/rate_fairdag_bof.log 2>&1
echo "fairdag-bof rate done $(date)" >> $L/driver.log
python3 -u performance/das_faulty.py --systems fairdag-bof --reps 3 >> $L/faulty_fairdag_bof.log 2>&1
echo "fairdag-bof faulty done $(date)" >> $L/driver.log
python3 -u performance/das_batch.py --systems fairdag-bof --reps 3 >> $L/batch_fairdag_bof.log 2>&1
echo "fairdag-bof batch done $(date)" >> $L/driver.log
