#!/usr/bin/env bash
# Pearl BOF rate sweep at 3f+1 committee sizes (n ∈ {10, 16, 22}).
#
# Builds binaries (skip with --skip-build), then sweeps the same 14-point
# rate ladder as das_results/tput_latency.csv with 5 reps per point and
# writes to das_results/tput_latency_pearl_3f1.csv.
#
# Total points: 3 N × 14 rates × 5 reps = 210 runs.
# Each run ≈ runtime (60s) + reservation/setup/teardown (~2–3 min) ≈ ~4 min,
# so plan for ~14h end-to-end on DAS5. Resume-safe: rerun the script if it's
# interrupted and it will skip any (n, rate, rep) row already in the CSV with
# consensus_tps > 0.
#
# Usage:
#   ./run_pearl_3f1_bof.sh                  # full sweep, rebuild binaries
#   ./run_pearl_3f1_bof.sh --skip-build     # reuse existing bazel-bin/
#   ./run_pearl_3f1_bof.sh --dry-run        # print plan, don't reserve
#   ./run_pearl_3f1_bof.sh --nodes 10       # subset (forwarded to wrapper)
#   ./run_pearl_3f1_bof.sh --rates 500 2000 # subset (forwarded to wrapper)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

SKIP_BUILD=0
PASSTHROUGH=()
for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=1 ;;
    *)            PASSTHROUGH+=("$arg") ;;
  esac
done

if [[ $SKIP_BUILD -eq 0 ]]; then
  echo "=== Building Pearl binaries ==="
  cd "${PROJ_ROOT}"
  bazel build \
    //benchmark/protocols/autobahn:kv_server_performance \
    //benchmark/protocols/autobahn:kv_service_tools \
    //tools:key_generator_tools \
    //tools:certificate_tools
fi

echo "=== Launching Pearl BOF 3f+1 rate sweep ==="
echo "    CSV: ${PROJ_ROOT}/das_results/tput_latency_pearl_3f1.csv"

cd "${PROJ_ROOT}/scripts/deploy"
exec python3 performance/das_pearl_3f1_bof.py "${PASSTHROUGH[@]}"
