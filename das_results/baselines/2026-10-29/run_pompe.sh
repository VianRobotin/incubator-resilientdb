#!/bin/bash
# Pompe (commit_set race + check_stable_point_index sort fix, 2026-10-31;
# per-call signature serialization buffers, 2026-11-01; one transaction per
# command, 2026-11-03).
# Runs from the benchmark_smoke copy of baselines/narwhal/benchmark (own logs/
# and results/) so it can run alongside the Themis sweep, which uses the
# original folder; Pompe and Themis build in separate of_protocols/ folders.
export NARWHAL_BENCH_DIR=/var/scratch/vrobotin/incubator-resilientdb/baselines/narwhal/benchmark_smoke
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
echo "=== pompe started $(date) (from benchmark_smoke) ===" >> $L/driver.log
python3 -u performance/das_baselines_reps.py --systems pompe >> $L/rate_pompe.log 2>&1
echo "pompe rate done $(date)" >> $L/driver.log
python3 -u performance/das_faulty.py --systems pompe --reps 3 >> $L/faulty_pompe.log 2>&1
echo "pompe faulty done $(date)" >> $L/driver.log
python3 -u performance/das_batch.py --systems pompe --reps 3 >> $L/batch_pompe.log 2>&1
echo "pompe batch done $(date)" >> $L/driver.log
