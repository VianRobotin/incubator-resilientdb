#!/bin/bash
# Autobahn Benchmark Script - Runs multiple configurations and collects data
# Produces CSV files for the Python plotting script

set -e

PROJ_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_ROOT"

RESULTS_DIR="$PROJ_ROOT/benchmark_results"
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

echo "=== Autobahn Benchmark Suite ==="
echo "Project root: $PROJ_ROOT"
echo "Results dir: $RESULTS_DIR"

# Kill any previous instances
killall -9 kv_server_performance 2>/dev/null || true
sleep 1

# Build
echo ""
echo "=== Building binaries ==="
bazel build //benchmark/protocols/autobahn:kv_server_performance 2>&1 | tail -5

SERVER_BIN="$PROJ_ROOT/bazel-bin/benchmark/protocols/autobahn/kv_server_performance"

# We'll run benchmarks with different block sizes to see throughput/latency tradeoffs
BLOCK_SIZES=(100 200 400)
NUM_REPLICAS=4
BASE_PORT=10001
RUNTIME=90  # seconds to run each benchmark
WARMUP=15   # seconds warmup before collecting

# Use existing certs from test_config if available, else generate
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
  for i in $(seq 1 5); do
    $KEY_GEN "$CERT_DIR/node_$i"
  done
  for i in $(seq 1 5); do
    PORT=$((BASE_PORT + i - 1))
    if [ $i -le $NUM_REPLICAS ]; then
      NODE_TYPE="replica"
    else
      NODE_TYPE="client"
    fi
    $CERT_TOOL "$CERT_DIR" "$CERT_DIR/admin.key.pri" "$CERT_DIR/admin.key.pub" "$CERT_DIR/node_$i.key.pub" $i "127.0.0.1" $PORT $NODE_TYPE
  done

  rm -rf "$CONFIG_DIR"
  mkdir -p "$CONFIG_DIR"
  cp -r "$CERT_DIR" "$CONFIG_DIR/cert"
fi

run_benchmark() {
  local block_size=$1
  local run_label=$2

  echo ""
  echo "======================================================"
  echo "  Benchmark: $run_label (block_size=$block_size)"
  echo "======================================================"

  # Kill previous
  killall -9 kv_server_performance 2>/dev/null || true
  sleep 2

  # Generate config with this block size
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
  "block_size": ${block_size}
}
CONFIGEOF

  # Start replicas
  LOG_DIR="$RESULTS_DIR/logs_${run_label}"
  mkdir -p "$LOG_DIR"

  for i in $(seq 1 $NUM_REPLICAS); do
    PRIV_KEY="$CONFIG_DIR/cert/node_$i.key.pri"
    CERT_FILE="$CONFIG_DIR/cert/cert_$i.cert"
    LOG_FILE="$LOG_DIR/server_$i.log"
    nohup $SERVER_BIN "$CONFIG_DIR/server.config" "$PRIV_KEY" "$CERT_FILE" > "$LOG_FILE" 2>&1 &
  done

  echo "  Started $NUM_REPLICAS replicas. Waiting ${WARMUP}s warmup + ${RUNTIME}s run..."
  sleep $RUNTIME

  echo "  Collecting results..."

  # Kill servers
  killall -9 kv_server_performance 2>/dev/null || true
  sleep 1

  # Copy logs for analysis
  echo "  Logs saved to $LOG_DIR/"
}

# Run benchmarks with different block sizes
for bs in "${BLOCK_SIZES[@]}"; do
  run_benchmark $bs "bs${bs}"
done

echo ""
echo "=== All benchmarks complete ==="
echo "Running analysis and generating plots..."

# Run the plotting script
python3 "$PROJ_ROOT/scripts/plot_benchmark.py" "$RESULTS_DIR"

echo ""
echo "=== Done! ==="
echo "Results in: $RESULTS_DIR/"
echo "Plots in: $RESULTS_DIR/plots/"
