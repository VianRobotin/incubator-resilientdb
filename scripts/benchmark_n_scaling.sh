#!/bin/bash
# N-Scaling Benchmark: OL vs BOF across n=3,5,7 replicas
# Fixed block_size=100, Δ=1s (set in autobahn.cpp)
# Measures how performance changes as the number of replicas grows.

set -eo pipefail

PROJ_ROOT="/mnt/c/Users/viene/incubator-resilientdb"
cd "$PROJ_ROOT"

RESULTS_DIR="$PROJ_ROOT/benchmark_results"
mkdir -p "$RESULTS_DIR"

export TEE_ENCLAVE_PATH="$PROJ_ROOT/platform/consensus/ordering/autobahn/tee/tee_enclave.signed.so"
export LD_LIBRARY_PATH="/opt/intel/sgxsdk/lib64:${LD_LIBRARY_PATH:-}"

echo "=== N-Scaling Benchmark: OL vs BOF ==="
echo "Project root: $PROJ_ROOT"

killall -9 kv_server_performance 2>/dev/null || true
sleep 1

# ---------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------
BASE_PORT=10001
RUNTIME=90
WARMUP=20
STATS_INTERVAL=5
BLOCK_SIZE=100
REPLICA_COUNTS=(3 5 7)
MODES=("ol" "bof")
MAX_REPLICAS=7

SERVER_BIN="$PROJ_ROOT/bazel-bin/benchmark/protocols/autobahn/kv_server_performance"

# ---------------------------------------------------------------
# Generate certs for up to MAX_REPLICAS nodes (done once)
# ---------------------------------------------------------------
CONFIG_DIR="$PROJ_ROOT/test_config_n"
echo ""
echo "=== Generating certificates for $MAX_REPLICAS nodes ==="
rm -rf "$CONFIG_DIR"
mkdir -p "$CONFIG_DIR"

KEY_GEN="$PROJ_ROOT/bazel-bin/tools/key_generator_tools"
CERT_TOOL="$PROJ_ROOT/bazel-bin/tools/certificate_tools"
bazel build //tools:key_generator_tools //tools:certificate_tools 2>&1 | tail -3

CERT_DIR="$CONFIG_DIR/cert"
mkdir -p "$CERT_DIR"
$KEY_GEN "$CERT_DIR/admin"
for i in $(seq 1 $MAX_REPLICAS); do
  $KEY_GEN "$CERT_DIR/node_$i"
done
for i in $(seq 1 $MAX_REPLICAS); do
  PORT=$((BASE_PORT + i - 1))
  $CERT_TOOL "$CERT_DIR" "$CERT_DIR/admin.key.pri" "$CERT_DIR/admin.key.pub" \
             "$CERT_DIR/node_$i.key.pub" $i "127.0.0.1" $PORT "replica"
done
echo "  Certs ready for nodes 1..$MAX_REPLICAS"

# ---------------------------------------------------------------
# CSV header
# ---------------------------------------------------------------
CSV_FILE="$RESULTS_DIR/n_scaling.csv"
echo "n,mode,tps,slots_committed,txns_committed,runtime_s" > "$CSV_FILE"

# ---------------------------------------------------------------
# parse_log (same as benchmark_autobahn.sh)
# ---------------------------------------------------------------
parse_log() {
  local log_file="$1"
  local warmup_secs="$2"
  local skip=$(( warmup_secs / STATS_INTERVAL ))

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

  local slots_committed
  slots_committed=$(grep -c "SyncHS: committing slot" "$log_file" 2>/dev/null || echo "0")

  local txns_committed=0
  while IFS= read -r line; do
    local n
    n=$(echo "$line" | grep -oP '\K[0-9]+ fairly-ordered' | grep -oP '^[0-9]+' || echo "0")
    [ -n "$n" ] && txns_committed=$(( txns_committed + n ))
  done < <(grep "fairly-ordered txns" "$log_file" 2>/dev/null || true)

  if [ "$tps" = "0" ] || [ "$tps" = ".0" ] || [ -z "$tps" ]; then
    local meas=$(( RUNTIME - WARMUP ))
    if [ "$txns_committed" -gt 0 ] && [ "$meas" -gt 0 ]; then
      tps=$(echo "scale=1; $txns_committed / $meas" | bc)
    fi
  fi

  echo "${tps:-0},${slots_committed},${txns_committed}"
}

# ---------------------------------------------------------------
# run_benchmark n mode
# ---------------------------------------------------------------
run_benchmark() {
  local num_replicas="$1"
  local mode="$2"
  local run_label="n${num_replicas}_${mode}"
  local bof_flag="false"
  [ "$mode" = "bof" ] && bof_flag="true"

  echo ""
  echo "======================================================"
  echo "  Run: $run_label  (n=$num_replicas, batch_order_fairness=$bof_flag)"
  echo "======================================================"

  killall -9 kv_server_performance 2>/dev/null || true
  sleep 2

  # Build replicaInfo for n replicas
  local replica_entries=""
  for i in $(seq 1 $num_replicas); do
    local port=$(( BASE_PORT + i - 1 ))
    [ $i -gt 1 ] && replica_entries+=","$'\n'
    replica_entries+="        {\"id\": $i, \"ip\": \"127.0.0.1\", \"port\": $port}"
  done

  cat > "$CONFIG_DIR/server_n${num_replicas}.config" << CONFIGEOF
{
  "region": [
    {
      "replicaInfo": [
${replica_entries}
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
  "block_size": ${BLOCK_SIZE},
  "batch_order_fairness": ${bof_flag}
}
CONFIGEOF

  LOG_DIR="$RESULTS_DIR/logs_${run_label}"
  mkdir -p "$LOG_DIR"

  set +e
  for i in $(seq 1 $num_replicas); do
    PRIV_KEY="$CONFIG_DIR/cert/node_$i.key.pri"
    CERT_FILE="$CONFIG_DIR/cert/cert_$i.cert"
    nohup "$SERVER_BIN" "$CONFIG_DIR/server_n${num_replicas}.config" "$PRIV_KEY" "$CERT_FILE" \
          > "$LOG_DIR/server_$i.log" 2>&1 &
  done
  set -e

  echo "  $num_replicas replicas started. Running ${RUNTIME}s..."
  sleep "$RUNTIME"

  killall -9 kv_server_performance 2>/dev/null || true
  sleep 1

  local best_tps=0 best_slots=0 best_txns=0
  for i in $(seq 1 $num_replicas); do
    local lf="$LOG_DIR/server_$i.log"
    [ -f "$lf" ] || continue
    local metrics tps slots txns
    metrics=$(parse_log "$lf" "$WARMUP")
    tps=$(echo "$metrics" | cut -d',' -f1)
    slots=$(echo "$metrics" | cut -d',' -f2)
    txns=$(echo "$metrics" | cut -d',' -f3)
    echo "    Replica $i: TPS=${tps}  slots=$slots  txns=$txns"
    if awk "BEGIN{exit !($tps > $best_tps)}"; then
      best_tps="$tps"; best_slots="$slots"; best_txns="$txns"
    fi
  done
  echo "  => Best: TPS=$best_tps  slots=$best_slots  txns=$best_txns"
  echo "$num_replicas,$mode,$best_tps,$best_slots,$best_txns,$RUNTIME" >> "$CSV_FILE"
}

# ---------------------------------------------------------------
# Main: run all (n, mode) combinations
# ---------------------------------------------------------------
for n in "${REPLICA_COUNTS[@]}"; do
  for mode in "${MODES[@]}"; do
    run_benchmark "$n" "$mode"
  done
done

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
echo ""
echo "======================================================="
echo "=== N-Scaling Results (full data: $CSV_FILE) ==="
echo "======================================================="
printf "%-4s  %-6s  %-10s  %-8s  %-10s\n" "n" "Mode" "TPS" "Slots" "Txns"
printf "%-4s  %-6s  %-10s  %-8s  %-10s\n" "----" "------" "----------" "--------" "----------"
while IFS=',' read -r n mode tps slots txns rt; do
  [ "$n" = "n" ] && continue
  printf "%-4s  %-6s  %-10s  %-8s  %-10s\n" "$n" "$mode" "$tps" "$slots" "$txns"
done < "$CSV_FILE"

echo ""
echo "=== Done! Logs: $RESULTS_DIR/ ==="
