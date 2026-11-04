#!/bin/bash
# Tusk silent-replica sweep with the missing-leader fix in tusk/algorithm/tusk.cpp
# (the old binary waited forever for a silent leader's vertex, so every run
# recorded 0).  Uses the baselines/fairdag/scripts tree: run nothing else from it.
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
echo "=== tusk faulty rerun started $(date) ===" >> $L/driver.log
python3 -u performance/das_faulty.py --systems tusk --reps 3 >> $L/faulty_tusk_rerun.log 2>&1
echo "tusk faulty rerun done $(date)" >> $L/driver.log
