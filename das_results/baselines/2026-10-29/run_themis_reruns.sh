#!/bin/bash
# Re-run the Themis rate points that never ran (every attempt failed to get
# nodes within the old 60 s reservation wait, so they were recorded as 0).
# Collapse zeros, where Themis did run, are kept.  Waits for run_themis.sh,
# which uses the same baselines/narwhal/benchmark folder.
while pgrep -f run_themis.sh > /dev/null; do sleep 60; done
L=/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29
echo "=== themis reruns started $(date) ===" >> $L/driver.log
POINTS=$(python3 - <<'EOF'
import re
L = "/var/scratch/vrobotin/incubator-resilientdb/das_results/baselines/2026-10-29"
last = {}  # (n, rate, rep) -> (tps, attempts that ran); the last result wins
seg = []
for line in open(L + "/rate_themis.log", errors="replace"):
    seg.append(line)
    m = re.match(r"\s+themis n=(\d+) mode=\S+ rate=(\d+) rep=(\d+) -> exec_tps=([\d.]+)", line)
    if m:
        txt = "".join(seg)
        seg = []
        last[m.groups()[:3]] = (float(m.group(4)), len(re.findall(r"^TPS:", txt, re.M)))
for (n, rate, rep), (tps, ran) in sorted(last.items(), key=lambda kv: tuple(map(int, kv[0]))):
    if tps <= 0 and ran == 0:
        print("%s:%s:%s" % (n, rate, rep))
EOF
)
echo "themis never-ran points: $POINTS" >> $L/driver.log
cd /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy
for p in $POINTS; do
  IFS=: read n rate rep <<< "$p"
  python3 -u performance/das_baselines_reps.py --systems themis --n $n --rates $rate --reps $rep >> $L/rate_themis_reruns.log 2>&1
done
echo "themis reruns done $(date)" >> $L/driver.log
