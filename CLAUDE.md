# CLAUDE.md — Fair Ordering with TEEs (MSc Thesis)

## Project Overview

This is the implementation codebase for the MSc thesis **"Fair Ordering with TEEs"** by vrobotin.
The system is **Autobahn**: a DAG-based BFT consensus protocol extended with TEE-enabled fair
transaction ordering. Experiments run on the **DAS5 supercomputer**.

### Key Thesis Claim

Autobahn achieves fair ordering with a **2f+1** committee (vs. the 3f+1 required by competing
systems). It should outperform all benchmarks in throughput and/or latency as N grows, thanks to
the smaller quorum size.

---

## Repository Layout

```
incubator-resilientdb/
├── platform/consensus/ordering/autobahn/
│   ├── algorithm/          # Core protocol implementation
│   │   ├── autobahn.cpp/h      # Main AutoBahn class (Algorithms 1-3)
│   │   ├── proposal_graph.*    # DAG structure
│   │   ├── proposal_manager.*  # Proposal lifecycle
│   │   ├── proposal_state.h    # State machine per proposal
│   │   └── ranking.*           # Fair ordering / ranking logic
│   ├── tee/                # TEE integration (Intel SGX)
│   │   ├── tee_host.cpp/h      # Host-side TEE calls
│   │   ├── enclave/            # SGX enclave source
│   │   └── tee_enclave.signed.so  # Built enclave (SGX hardware or sim)
│   ├── framework/          # ResilientDB framework wiring
│   └── proto/              # Protobuf message definitions
├── benchmark/protocols/autobahn/
│   └── kv_server_performance   # Built server binary (bazel-bin/)
│       kv_service_tools        # Client trigger binary
├── tools/
│   └── key_generator_tools     # Keypair generation
│       certificate_tools       # Certificate generation
├── scripts/deploy/performance/
│   ├── das.py              # Main experiment orchestrator (see below)
│   ├── preserve.py         # DAS5 reservation manager wrapper
│   ├── METRICS.md          # Metric definitions and interpretation
│   └── plot.py             # Result plotting
├── das_results/            # CSV outputs from das.py experiments
└── das_runtime/            # Per-run certs, configs, logs (NFS-shared)
```

---

## Protocol Architecture

Autobahn implements three algorithms from the thesis paper:

- **Algorithm 1 — TEE-enabled data dissemination**: Each replica TEE-timestamps received
  transactions. After Δ, computes local ordering key `K_r(t)` = (f+1)-th smallest timestamp.
- **Algorithm 2 — Pre-Ordered Sync HotStuff (proposal)**: Leader extracts transactions in
  window `(τ_prev, τ_current]`, sorts by `K(t)`, proposes ordered block.
- **Algorithm 3 — Voting and commit**: Replicas validate ordering before voting; commit after
  `f+1` votes (2f+1 total size).

**Execution threshold τ**: (f+1)-th smallest of max last-seen timestamps per replica. Guarantees
no future transaction can arrive with ordering key ≤ τ (Lemma 5 in paper).

**Two modes** controlled by `batch_order_fairness` flag in config:
- `ol` — Ordering Linearizability (full TEE-based fair ordering, the thesis contribution)
- `bof` — Batch Order Fairness (baseline comparison mode, Pompe-style)

---

## Build

```bash
# From project root
bazel build \
  //benchmark/protocols/autobahn:kv_server_performance \
  //benchmark/protocols/autobahn:kv_service_tools \
  //tools:key_generator_tools \
  //tools:certificate_tools
```

SGX enclave is pre-built at `platform/consensus/ordering/autobahn/tee/tee_enclave.signed.so`.
SGX SDK libs: `/var/scratch/vrobotin/sgxsdk/lib64`

---

## Running Experiments (DAS5)

Main script: `scripts/deploy/performance/das.py`

```bash
# From scripts/deploy/ directory:
python3 performance/das.py              # run full experiment suite
python3 performance/das.py --dry-run   # print plan only (no machines reserved)
python3 performance/das.py --skip-build  # skip bazel build
```

### What das.py does

1. Reserves DAS5 machines via `preserve` (NFS-shared `/var/scratch`)
2. Generates certificates and `server.config` with real IPs
3. SSH-starts replica nodes (server binary) on reserved machines
4. Waits for all replicas to connect (`receive public size:N` in logs)
5. SSH-starts client nodes, waits for port to open, triggers via `kv_service_tools`
6. Runs for `RUNTIME` seconds, kills all nodes, parses logs, releases reservation
7. Appends results to CSV in `das_results/`

### Experiment Parameters (matching Giulio's setup)

| Parameter | Value |
|-----------|-------|
| Block size | 100 |
| Transaction size | 128 bytes |
| Runtime | 60 s |
| Warmup | 15 s |
| Repetitions | 5 per config |
| Clients | n (rate-controlled via `target_input_tps`) |

### Rate-sweep experiment (L-curve)

For each N ∈ {7, 11, 15} and each mode ∈ {ol, bof}: vary injection rate from well below
saturation to above it. Each (N, mode, rate) point → 5 repetitions.

| N | Injection rates swept (tx/s) |
|---|------------------------------|
| 7 | 500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000 |
| 11 | 500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000 |
| 15 | 500, 1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000 |

Results saved to: `das_results/tput_latency.csv`

Plot: `scripts/deploy/performance/plot.py` → `das_results/plots/tput_vs_latency.png`
(throughput x-axis, latency y-axis, one L-curve per (N, mode))

---

## Benchmarks (Giulio's work — trusted reference)

Giulio Segalin's thesis implementation is at `/var/scratch/gsegalin/giulio-msc-thesis/`.
It uses **Narwhal/DAG-Rider** with 3f+1 committee size. His results are the baseline
that Autobahn (2f+1) should beat.

### Giulio's result files

```
/var/scratch/gsegalin/giulio-msc-thesis/results/
├── our-work/          # Giulio's main system (OL baseline, Narwhal-based, 3f+1)
│   └── local-0-1-0-1-{N}.txt   # N ∈ {10,13,16,19,22,25}
├── themis/            # Themis benchmark results
├── pompe/             # Pompe benchmark results
├── fairdag/           # FairDAG benchmark results
├── fairdag-comparison/
├── our-work-preccs/   # PRECCS variants
└── ...
```

### Giulio's result format

Each `.txt` file contains 5+ runs with this structure:
```
Committee size: N node(s)
Input rate: X tx/s
Transaction size: 128 B
Consensus TPS: X tx/s
End-to-end TPS: X tx/s
End-to-end latency: X ms
```

### Baseline numbers at N=16 (Giulio's system, 3f+1)

- Input rate: 10,000 tx/s
- Consensus TPS: ~9,400–9,950 tx/s
- End-to-end TPS: ~9,370–9,935 tx/s
- End-to-end latency: ~514–636 ms

---

## Known Performance Issues (diagnosed 2026-04-15, updated 2026-04-19)

### Root causes of low throughput / high latency

**Issue 1 — ACK sent after O(block_size) signing work (FIXED)**
`ReceiveBlock()` was computing 100 Ed25519 timestamp signatures *before* sending the BlockACK.
Fix: moved `Broadcast(BlockACK)` to before `TimestampTransactions()` in `ReceiveBlock`.

**Issue 2 — Δ = 1000ms (FIXED, now 50ms)**
`delta_ms_ = 1000` caused 2Δ = 2000ms commit latency.
Fix: changed to `delta_ms_ = 50` (DAS5 LAN RTT < 1ms; 50ms is very conservative).

**Issue 3 — Excessive LOG(ERROR) in hot paths (FIXED)**
Downgraded chatty log lines in hot paths from `LOG(ERROR)` to `LOG(INFO)`.

**Issue 4 — BlockACK broadcast to all N nodes instead of point-to-point (FIXED)**
`ReceiveBlock()` was calling `Broadcast(CMD_BlockACK)` which sends the ACK to ALL N nodes.
Since every receiver discards ACKs for other nodes' blocks, this produced O(N³) wasted ACK
messages per round (N senders × N ACKs each × N receivers). At N=22 this was ~10,000 msgs/round
vs ~500 with point-to-point, causing severe network congestion and 270–2100ms cert latency.
Fix: changed to `SendMessage(CMD_BlockACK, ..., block->sender_id())` — point-to-point to
the block sender only. ACK traffic is now O(N²) for the whole system.

**Issue 5 — Sequential block dissemination (fundamental, inherent to PoA chain)**
The PoA chain requires block N's f+1 ACKs before block N+1 can be sent. This is protocol-
correct and limits throughput to `(1/cert_latency) × block_size` per replica. Post all fixes,
cert_latency should drop to ~5–10ms → ~10,000–20,000 TPS at N=10.

### Performance after all fixes (expected)
- Block certification latency: ~5–10ms (was 270–2100ms before Issue 4 fix)
- Commit latency: 2Δ = 100ms (vs Giulio's 500-636ms — should be better)
- Throughput: target > 9,935 TPS at N=10

---

## Key Paths

| What | Path |
|------|------|
| Main implementation | `/var/scratch/vrobotin/incubator-resilientdb/platform/consensus/ordering/autobahn/` |
| Experiment script | `/var/scratch/vrobotin/incubator-resilientdb/scripts/deploy/performance/das.py` |
| Experiment results | `/var/scratch/vrobotin/incubator-resilientdb/das_results/` |
| Runtime artifacts | `/var/scratch/vrobotin/incubator-resilientdb/das_runtime/` |
| SGX SDK | `/var/scratch/vrobotin/sgxsdk/lib64` |
| Giulio's implementation | `/var/scratch/gsegalin/giulio-msc-thesis/` |
| Giulio's results | `/var/scratch/gsegalin/giulio-msc-thesis/results/our-work/` |
| Giulio's benchmarks | `/var/scratch/gsegalin/giulio-msc-thesis/results/{themis,pompe,fairdag}/` |
| Thesis PDF | `/var/scratch/vrobotin/bin/Fair_Ordering_with_TEEs (4).pdf` |
| Giulio's thesis PDF | `/var/scratch/vrobotin/bin/Giulio___Fair_ordering_MSc_thesis.pdf` |

---

## DAS5 Assumptions

- `/var/scratch` is NFS-shared across all DAS5 nodes — no SCP needed for binaries or configs
- SSH from headnode to compute nodes is passwordless (shared `~/.ssh` via NFS home)
- `preserve` command is on PATH (DAS5 reservation system)
- Machines are referenced by hostname; IPs resolved via `socket.gethostbyname()`
