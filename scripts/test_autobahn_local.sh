#!/bin/bash
# Local test script for Autobahn with Sync HotStuff + Fair Ordering
# Runs 4 replicas + 1 client on localhost

set -e

PROJ_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_ROOT"

echo "=== Autobahn Local Test ==="
echo "Project root: $PROJ_ROOT"

# Kill any previous instances
echo "Cleaning up old processes..."
killall -9 kv_server_performance 2>/dev/null || true
sleep 1

# Step 1: Build everything
echo ""
echo "=== Step 1: Building binaries ==="
bazel build //tools:key_generator_tools //tools:certificate_tools //benchmark/protocols/autobahn:kv_server_performance //benchmark/protocols/autobahn:kv_service_tools 2>&1 | tail -5

# Step 2: Generate keys and certificates
echo ""
echo "=== Step 2: Generating keys and certificates ==="
CERT_DIR="$PROJ_ROOT/test_cert"
rm -rf "$CERT_DIR"
mkdir -p "$CERT_DIR"

KEY_GEN="$PROJ_ROOT/bazel-bin/tools/key_generator_tools"
CERT_TOOL="$PROJ_ROOT/bazel-bin/tools/certificate_tools"

# Generate admin key first
echo "Generating admin key..."
$KEY_GEN "$CERT_DIR/admin"

# Generate keys for 5 nodes (4 replicas + 1 client)
NUM_REPLICAS=4
NUM_CLIENTS=1
TOTAL=$((NUM_REPLICAS + NUM_CLIENTS))
BASE_PORT=10001

for i in $(seq 1 $TOTAL); do
  echo "Generating key for node $i..."
  $KEY_GEN "$CERT_DIR/node_$i"
done

# Generate certificates
for i in $(seq 1 $TOTAL); do
  PORT=$((BASE_PORT + i - 1))
  if [ $i -le $NUM_REPLICAS ]; then
    NODE_TYPE="replica"
  else
    NODE_TYPE="client"
  fi
  echo "Generating certificate for node $i ($NODE_TYPE) at 127.0.0.1:$PORT..."
  $CERT_TOOL "$CERT_DIR" "$CERT_DIR/admin.key.pri" "$CERT_DIR/admin.key.pub" "$CERT_DIR/node_$i.key.pub" $i "127.0.0.1" $PORT $NODE_TYPE
done

# Step 3: Generate config files
echo ""
echo "=== Step 3: Generating config files ==="
CONFIG_DIR="$PROJ_ROOT/test_config"
rm -rf "$CONFIG_DIR"
mkdir -p "$CONFIG_DIR"

# Copy certs to config dir
cp -r "$CERT_DIR" "$CONFIG_DIR/cert"

# Create server.config (plain text: id ip port)
SERVER_CONFIG_TXT="$CONFIG_DIR/server_list.txt"
> "$SERVER_CONFIG_TXT"
for i in $(seq 1 $NUM_REPLICAS); do
  PORT=$((BASE_PORT + i - 1))
  echo "$i 127.0.0.1 $PORT" >> "$SERVER_CONFIG_TXT"
done

# Create client config (plain text)
CLIENT_CONFIG_TXT="$CONFIG_DIR/client_list.txt"
> "$CLIENT_CONFIG_TXT"
for i in $(seq $((NUM_REPLICAS + 1)) $TOTAL); do
  PORT=$((BASE_PORT + i - 1))
  echo "$i 127.0.0.1 $PORT" >> "$CLIENT_CONFIG_TXT"
done

# Generate JSON config using the Python tool
TEMPLATE="$PROJ_ROOT/scripts/deploy/config/autobahn.config"
echo "Generating JSON config with template: $TEMPLATE"

# Combine server and client into one file for config generation
cat "$SERVER_CONFIG_TXT" "$CLIENT_CONFIG_TXT" > "$CONFIG_DIR/all_nodes.txt"

# Use the config generator
PYTHONPATH="$PROJ_ROOT/bazel-bin" python3 "$PROJ_ROOT/bazel-bin/tools/generate_region_config" "$CONFIG_DIR/all_nodes.txt" "$CONFIG_DIR/server.config" "$TEMPLATE" 2>/dev/null || {
  echo "Python config generator failed, creating config manually..."

  # Create the protobuf JSON config manually
  cat > "$CONFIG_DIR/server.config" << 'CONFIGEOF'
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
  "block_size": 300
}
CONFIGEOF
}

# Also create a client-specific config
cat > "$CONFIG_DIR/client.config" << CLIENTEOF
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
  "selfInfo": {"id": 5, "ip": "127.0.0.1", "port": 10005}
}
CLIENTEOF

echo "Server config:"
cat "$CONFIG_DIR/server.config"
echo ""

# Step 4: Start replicas
echo ""
echo "=== Step 4: Starting $NUM_REPLICAS replicas ==="
SERVER_BIN="$PROJ_ROOT/bazel-bin/benchmark/protocols/autobahn/kv_server_performance"
LOG_DIR="$PROJ_ROOT/test_logs"
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

for i in $(seq 1 $NUM_REPLICAS); do
  PRIV_KEY="$CONFIG_DIR/cert/node_$i.key.pri"
  CERT_FILE="$CONFIG_DIR/cert/cert_$i.cert"
  LOG_FILE="$LOG_DIR/server_$i.log"
  echo "Starting replica $i (logging to $LOG_FILE)..."
  nohup $SERVER_BIN "$CONFIG_DIR/server.config" "$PRIV_KEY" "$CERT_FILE" > "$LOG_FILE" 2>&1 &
  echo "  PID: $!"
done

echo ""
echo "Waiting for replicas to discover each other..."
sleep 10

# Check if servers are running
echo ""
echo "=== Checking server status ==="
for i in $(seq 1 $NUM_REPLICAS); do
  if grep -q "receive public size:$NUM_REPLICAS" "$LOG_DIR/server_$i.log" 2>/dev/null; then
    echo "Replica $i: READY (discovered all peers)"
  else
    echo "Replica $i: still starting..."
    tail -3 "$LOG_DIR/server_$i.log" 2>/dev/null
  fi
done

# Wait a bit more for initialization
sleep 5

# Step 5: Monitor for 60 seconds
echo ""
echo "=== Step 5: Monitoring performance (60 seconds) ==="
echo "Waiting for transactions to flow..."

for t in 10 20 30 40 50 60; do
  sleep 10
  echo ""
  echo "--- After ${t}s ---"
  for i in $(seq 1 $NUM_REPLICAS); do
    # Extract key metrics from the last monitor line
    LAST_MONITOR=$(grep "monitor =========" "$LOG_DIR/server_$i.log" 2>/dev/null | tail -1)
    if [ -n "$LAST_MONITOR" ]; then
      TXN=$(echo "$LAST_MONITOR" | grep -oP 'txn:\K[0-9]+')
      COMMIT=$(echo "$LAST_MONITOR" | grep -oP 'commit:\K[0-9]+')
      EXECUTE=$(echo "$LAST_MONITOR" | grep -oP 'execute:\K[0-9]+')
      PROPOSE=$(echo "$LAST_MONITOR" | grep -oP 'propose:\K[0-9]+')
      echo "  Replica $i: txn=$TXN commit=$COMMIT execute=$EXECUTE propose=$PROPOSE"
    else
      echo "  Replica $i: no monitor data yet"
    fi
  done
done

# Step 6: Results
echo ""
echo "=== Final Results ==="
for i in $(seq 1 $NUM_REPLICAS); do
  echo ""
  echo "--- Replica $i (last 5 monitor lines) ---"
  grep "monitor =========" "$LOG_DIR/server_$i.log" 2>/dev/null | tail -5 || echo "  No monitor output"
done

# Check for errors
echo ""
echo "=== Error Check ==="
for i in $(seq 1 $NUM_REPLICAS); do
  ERRORS=$(grep -c "ERROR\|FATAL\|SEGV\|abort" "$LOG_DIR/server_$i.log" 2>/dev/null || echo "0")
  echo "Replica $i: $ERRORS error lines"
  if [ "$ERRORS" -gt 0 ]; then
    echo "  Last errors:"
    grep "ERROR\|FATAL\|SEGV\|abort" "$LOG_DIR/server_$i.log" 2>/dev/null | tail -3
  fi
done

echo ""
echo "=== Cleaning up ==="
killall -9 kv_server_performance 2>/dev/null || true
echo "Done. Logs are in $LOG_DIR/"
