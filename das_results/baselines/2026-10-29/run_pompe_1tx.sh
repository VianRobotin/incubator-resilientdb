#!/bin/bash
# Pompe rerun with one transaction per command (client-batch 1, 2026-11-03).
# The batched sweep (one timestamp per <=100 tx) moves to pompe_batch100_rows/.
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
mkdir -p $L/pompe_batch100_rows
for d in rate faulty batch; do
  mv $L/$d/pompe.csv $L/pompe_batch100_rows/${d}_pompe.csv
done
for f in rate_pompe.log faulty_pompe.log batch_pompe.log; do
  [ -f $L/$f ] && mv $L/$f $L/pompe_batch100_rows/$f
done
echo "--- $(date +%H:%M) pompe batched rows -> pompe_batch100_rows/; rerun with 1 tx per command ---" >> $L/driver.log
exec $L/run_pompe.sh
