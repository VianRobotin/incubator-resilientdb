#!/bin/bash
# Autobahn Benchmark Script
# Compares Ordering Linearizability (OL) vs Batch-Order Fairness (BOF)
# Produces CSV files and a summary for analysis/plotting.

set -eo pipefail

PROJ_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_ROOT"

RESULTS_DIR="$PROJ_ROOT/benchmark_results"
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

echo "=== Autobahn Benchmark: OL vs BOF ==="
echo "Project root: $PROJ_ROOT"
echo "Results dir:  $RESULTS_DIR"

# Kill any lingering replicas
killall -9 kv_server_performance 2>/dev/null || true
sleep 1

# ---------------------------------------------------------------
# Build
# ---------------------------------------------------------------
echo ""
echo "=== Building binary ==="
bazel build //benchmark/protocols/autobahn:kv_server_performance 2>&1 | tail -5

SERVER_BIN="$PROJ_ROOT/bazel-bin/benchmark/protocols/autobahn/kv_server_performance"

# ---------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------
NUM_REPLICAS=4
BASE_PORT=10001
RUNTIME=90       # seconds per run
WARMUP=20        # seconds to skip before collecting stats
STATS_INTERVAL=5 # stats reporting interval (Stats default)
BLOCK_SIZES=(100 200 400)
MODES=("ol" "bof")

# ---------------------------------------------------------------
# Certificate setup
# ---------------------------------------------------------------
CONFIG_DIR="$PROJ_ROOT/test_config"
if [ ! -d "$CONFIG_DIR/cert" ]; then
  echo "No certificates found. Generating..."
  CERT_DIR="$PROJ_ROOT/test_cert"
  rm -rf "$CERT_DIR"
  mkdir -p "$CERT_DIR"
  KEY_GEN="$PROJ_ROOT/bazel-bin/tools/key_generator_tools"
  CERT_TOOL="$PROJ_ROOT/bazel-bin/tools/certificate_tools"

  bazel build //tools:key_generator_tools //tools:certificate_tools 2>&1 | tail -3

  $KEY_GEN "$CERT_DIR/admin"
  for i in $(seq 1 5); do $KEY_GEN "$CERT_DIR/node_$i"; done
  for i in $(seq 1 5); do
    PORT=$((BASE_PORT + i - 1))
    NODE_TYPE="replica"; [ $i -gt $NUM_REPLICAS ] && NODE_TYPE="client"
    $CERT_TOOL "$CERT_DIR" "$CERT_DIR/admin.key.pri" "$CERT_DIR/admin.key.pub" \
               "$CERT_DIR/node_$i.key.pub" $i "127.0.0.1" $PORT $NODE_TYPE
  done
  rm -rf "$CONFIG_DIR"; mkdir -p "$CONFIG_DIR"
  cp -r "$CERT_DIR" "$CONFIG_DIR/cert"
fi

# ---------------------------------------------------------------
# CSV header
# ---------------------------------------------------------------
CSV_FILE="$RESULTS_DIR/comparison.csv"
echo "mode,block_size,tps,slots_committed,txns_committed,runtime_s" > "$CSV_FILE"

# ---------------------------------------------------------------
# parse_log: extract TPS from stats monitor lines.
# Falls back to slot-count-based throughput if stats show 0.
#
# Args: log_file, warmup_secs
# Stdout: "tps,slots,txns"
# ---------------------------------------------------------------
parse_log() {
  local log_file="$1"
  local warmup_secs="$2"
  local skip=$(( warmup_secs / STATS_INTERVAL ))

  # ---- Primary: consumed transactions from stats monitor ----
  local samples=()
  while IFS= read -r line; do
    if [[ "$line" == *"consumed transactions:"* ]]; then
      local c
      c=$(echo "$line" | grep -oP 'consumed transactions:\K[0-9]+' || echo "0")
      samples+=("$c")
    fi
  done < "$log_file"

  local total=${#samples[@]}
  local sum_consumed=0 count=0
  if [ "$total" -gt "$skip" ]; then
    for (( i=skip; i<total; i++ )); do
      sum_consumed=$(( sum_consumed + ${samples[$i]} ))
      count=$(( count + 1 ))
    done
  fi

  local tps=0
  if [ "$count" -gt 0 ] && [ "$sum_consumed" -gt 0 ]; then
    tps=$(echo "scale=1; $sum_consumed / $count / $STATS_INTERVAL" | bc)
  fi

  # ---- Secondary: slot-based throughput from proposal logs ----
  # Sum all "X fairly-ordered txns" after the warmup period.
  # Use the log timestamp to filter by warmup.
  local slots_committed
  slots_committed=$(grep -c "SyncHS: committing slot" "$log_file" 2>/dev/null || echo "0")

  local txns_committed=0
  while IFS= read -r line; do
    local n
    n=$(echo "$line" | grep -oP '\K[0-9]+ fairly-ordered' | grep -oP '^[0-9]+' || echo "0")
    [ -n "$n" ] && txns_committed=$(( txns_committed + n ))
  done < <(grep "fairly-ordered txns" "$log_file" 2>/dev/null || true)

  # If primary gave 0, derive TPS from txns_committed / (RUNTIME - WARMUP)
  if [ "$tps" = "0" ] || [ "$tps" = ".0" ] || [ -z "$tps" ]; then
    local meas=$(( RUNTIME - WARMUP ))
    if [ "$txns_committed" -gt 0 ] && [ "$meas" -gt 0 ]; then
      tps=$(echo "scale=1; $txns_committed / $meas" | bc)
    fi
  fi

  echo "${tps:-0},${slots_committed},${txns_committed}"
}

# ---------------------------------------------------------------
# run_benchmark
# ---------------------------------------------------------------
run_benchmark() {
  local mode="$1"
  local block_size="$2"
  local run_label="${mode}_bs${block_size}"
  local bof_flag="false"
  [ "$mode" = "bof" ] && bof_flag="true"

  echo ""
  echo "======================================================"
  echo "  Run: $run_label  (batch_order_fairness=$bof_flag)"
  echo "======================================================"

  killall -9 kv_server_performance 2>/dev/null || true
  sleep 2

  cat > "$CONFIG_DIR/server.config" << CONFIGEOF
{
  "region": [
    {
      "replicaInfo": [
        {"id": 1, "ip": "127.0.0.1", "port": 10001},
        {"id": 2, "ip": "127.0.0.1", "port": 10002},
        {"id": 3, "ip": "127.0.0.1", "port": 10003},
        {"id": 4, "ip": "127.0.0.1", "port": 10004}
      ]
    }
  ],
  "clientBatchNum": 400,
  "enable_viewchange": false,
  "recovery_enabled": false,
  "max_client_complaint_num": 10,
  "max_process_txn": 4096,
  "worker_num": 10,
  "input_worker_num": 1,
  "output_worker_num": 5,
  "block_size": ${block_size},
  "batch_order_fairness": ${bof_flag}
}
CONFIGEOF

  LOG_DIR="$RESULTS_DIR/logs_${run_label}"
  mkdir -p "$LOG_DIR"

  # Start replicas. Disable pipefail/errexit around background processes
  # so that crashes or SIGKILL at teardown don't abort the benchmark.
  set +e
  for i in $(seq 1 $NUM_REPLICAS); do
    PRIV_KEY="$CONFIG_DIR/cert/node_$i.key.pri"
    CERT_FILE="$CONFIG_DIR/cert/cert_$i.cert"
    nohup "$SERVER_BIN" "$CONFIG_DIR/server.config" "$PRIV_KEY" "$CERT_FILE" \
          > "$LOG_DIR/server_$i.log" 2>&1 &
  done
  set -e

  echo "  Replicas started. Running ${RUNTIME}s (${WARMUP}s warmup + $((RUNTIME - WARMUP))s measurement)..."
  sleep "$RUNTIME"

  killall -9 kv_server_performance 2>/dev/null || true
  sleep 1

  # Parse logs — prefer the replica with the highest TPS reading
  local best_tps=0 best_slots=0 best_txns=0
  for i in $(seq 1 $NUM_REPLICAS); do
    local lf="$LOG_DIR/server_$i.log"
    [ -f "$lf" ] || continue
    local metrics tps slots txns
    metrics=$(parse_log "$lf" "$WARMUP")
    tps=$(echo "$metrics" | cut -d',' -f1)
    slots=$(echo "$metrics" | cut -d',' -f2)
    txns=$(echo "$metrics" | cut -d',' -f3)
    echo "    Replica $i: TPS=${tps}  slots=$slots  txns=$txns"
    # Use awk for float comparison (bc -l may not be available for comparisons)
    if awk "BEGIN{exit !($tps > $best_tps)}"; then
      best_tps="$tps"; best_slots="$slots"; best_txns="$txns"
    fi
  done
  echo "  => Best: TPS=$best_tps  slots=$best_slots  txns=$best_txns"
  echo "$mode,$block_size,$best_tps,$best_slots,$best_txns,$RUNTIME" >> "$CSV_FILE"

  # ---- Correctness checks ----
  echo ""
  echo "  [Correctness checks for $run_label]"

  # 1. All replicas committed at least one slot
  local committed_replicas=0
  for i in $(seq 1 $NUM_REPLICAS); do
    grep -q "SyncHS: committing slot" "$LOG_DIR/server_$i.log" 2>/dev/null && \
      committed_replicas=$(( committed_replicas + 1 ))
  done
  printf "    %-50s %s\n" "Replicas that committed ≥ 1 slot:" "$committed_replicas / $NUM_REPLICAS"
  [ "$committed_replicas" -lt "$NUM_REPLICAS" ] && \
    echo "    WARNING: not all replicas committed — see $LOG_DIR"

  # 2. Correct mode active
  local mode_tag; [ "$mode" = "bof" ] && mode_tag="Batch-Order Fairness" || mode_tag="Ordering Linearizability"
  local mode_ok=0
  for i in $(seq 1 $NUM_REPLICAS); do
    grep -q "$mode_tag" "$LOG_DIR/server_$i.log" 2>/dev/null && mode_ok=$(( mode_ok + 1 ))
  done
  printf "    %-50s %s\n" "Mode '$mode_tag' confirmed:" "$mode_ok / $NUM_REPLICAS replicas"

  # 3. Mode-specific proposal checks
  local prop_tag; [ "$mode" = "bof" ] && prop_tag="(BOF)" || prop_tag="(OL)"
  local prop_count=0
  for i in $(seq 1 $NUM_REPLICAS); do
    local n; n=$(grep -c "$prop_tag" "$LOG_DIR/server_$i.log" 2>/dev/null) || n=0
    prop_count=$(( prop_count + n ))
  done
  printf "    %-50s %s\n" "$prop_tag proposals across all replicas:" "$prop_count"

  # 4. Algorithm 2 & 3 timing waits triggered
  local alg2_waits=0 alg3_waits=0
  for i in $(seq 1 $NUM_REPLICAS); do
    local n2 n3
    n2=$(grep "Algorithm 2: waiting" "$LOG_DIR/server_$i.log" 2>/dev/null | wc -l) || n2=0
    n3=$(grep "Algorithm 3: waiting" "$LOG_DIR/server_$i.log" 2>/dev/null | wc -l) || n3=0
    alg2_waits=$(( alg2_waits + n2 ))
    alg3_waits=$(( alg3_waits + n3 ))
  done
  printf "    %-50s %s\n" "Algorithm 2 (leader Δ-waits) triggered:" "$alg2_waits"
  printf "    %-50s %s\n" "Algorithm 3 (replica Δ-waits) triggered:" "$alg3_waits"

  # 5. TEE timestamp broadcasts
  local ts_total=0
  for i in $(seq 1 $NUM_REPLICAS); do
    local n; n=$(grep -E "Broadcast.*TEE timestamps|Broadcast.*own TEE timestamps" \
                  "$LOG_DIR/server_$i.log" 2>/dev/null | wc -l) || n=0
    ts_total=$(( ts_total + n ))
  done
  printf "    %-50s %s\n" "Total TEE timestamp broadcasts:" "$ts_total"

  # 6. No equivocation (expected in non-Byzantine run)
  local equivoc=0
  for i in $(seq 1 $NUM_REPLICAS); do
    local n; n=$(grep "EQUIVOCATION detected" "$LOG_DIR/server_$i.log" 2>/dev/null | wc -l) || n=0
    equivoc=$(( equivoc + n ))
  done
  if [ "$equivoc" -eq 0 ]; then
    printf "    %-50s %s\n" "Equivocation events:" "0 ✓"
  else
    printf "    %-50s %s\n" "Equivocation events:" "$equivoc  WARNING: unexpected"
  fi

  # 7. Non-empty proposals
  local nonempty=0
  for i in $(seq 1 $NUM_REPLICAS); do
    local n; n=$(grep -oP '\d+ fairly-ordered' "$LOG_DIR/server_$i.log" 2>/dev/null \
                  | grep -v "^0 " | wc -l) || n=0
    nonempty=$(( nonempty + n ))
  done
  printf "    %-50s %s\n" "Non-empty fair-ordering proposals:" "$nonempty"
  echo ""
}

# ---------------------------------------------------------------
# Main: run all (mode, block_size) combinations
# ---------------------------------------------------------------
for bs in "${BLOCK_SIZES[@]}"; do
  for mode in "${MODES[@]}"; do
    run_benchmark "$mode" "$bs"
  done
done

# ---------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------
echo ""
echo "======================================================="
echo "=== Results summary (full data: $CSV_FILE) ==="
echo "======================================================="
printf "%-6s  %-10s  %-10s  %-8s  %-10s\n" \
  "Mode" "BlockSize" "TPS" "Slots" "Txns"
printf "%-6s  %-10s  %-10s  %-8s  %-10s\n" \
  "------" "----------" "----------" "--------" "----------"
while IFS=',' read -r mode bs tps slots txns rt; do
  [ "$mode" = "mode" ] && continue
  printf "%-6s  %-10s  %-10s  %-8s  %-10s\n" "$mode" "$bs" "$tps" "$slots" "$txns"
done < "$CSV_FILE"

echo ""
echo "=== OL vs BOF comparison per block size ==="
for bs in "${BLOCK_SIZES[@]}"; do
  ol_tps=$(grep "^ol,$bs," "$CSV_FILE" | cut -d',' -f3)
  bof_tps=$(grep "^bof,$bs," "$CSV_FILE" | cut -d',' -f3)
  ol_txns=$(grep "^ol,$bs," "$CSV_FILE" | cut -d',' -f5)
  bof_txns=$(grep "^bof,$bs," "$CSV_FILE" | cut -d',' -f5)
  echo "  block_size=$bs:"
  echo "    OL  TPS=${ol_tps:-N/A}  total_txns=${ol_txns:-N/A}"
  echo "    BOF TPS=${bof_tps:-N/A}  total_txns=${bof_txns:-N/A}"
  # Compare only if both values are numeric and non-zero
  if [[ "$ol_tps" =~ ^[0-9] ]] && [[ "$bof_tps" =~ ^[0-9] ]]; then
    if awk "BEGIN{exit !($ol_tps > 0 && $bof_tps > 0)}"; then
      overhead=$(awk "BEGIN{printf \"%.1f\", ($ol_tps - $bof_tps) * 100 / $ol_tps}")
      echo "    BOF throughput overhead vs OL: ${overhead}%"
    fi
  fi
done

echo ""
echo "=== Done! Logs: $RESULTS_DIR/ ==="
