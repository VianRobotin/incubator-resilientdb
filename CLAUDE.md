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
├── das_results/            # CSV outputs: pearl/<run>/{rate,faulty,batch,committee}/,
│                           #   baselines/{rate,faulty,batch}/, paper_plots/
└── das_runtime/            # Per-run certs, configs, logs (NFS-shared)
```

---

## Protocol Architecture

The code follows the Pearl paper, Sec. 5-7.

- **Dissemination (Alg. 1):** every replica attests a transaction on first
  reception (OL: TEE timestamp; BOF: TEE Car placement), echoes it with the
  evidence, and after a 2Delta collection window has the enclave compute
  K_r(t) (OL) or record the placement announcements (BOF).  Cars are sealed by
  the enclave when full or every X = Delta and carry the frontier (OL: G_r;
  BOF: (w_r, M_r)) plus the PoA of the lane's latest certified Car.
- **Consensus (Alg. 2):** Sync HotStuff, stable leader per view.  Blocks are a
  tip cut + parent certificate (+ BOF delinquency claims), signed by
  TEE.SignBlock.  Votes wait for (ii) OL collection windows / (iii) BOF
  delinquency timers; commit after 2Delta; blame budget (3p+4)Delta.
- **Execution:** `algorithm/ol_ordering.*` (K(t), tau, K(t) < tau) and
  `algorithm/bof_ordering.*` (settlement, theta = ceil(gamma(n-f)) edge rule,
  SCC batches) derive the order from committed Cars only.
- **Enclave:** `tee/` (rebuild with `make`; `make check` tests the enclave
  rules; `sgx_edger8r` is built from `linux-sgx` sources, needs opam OCaml).
- **Config:** `batch_order_fairness`, `bof_gamma`, `delta_ms` (default 50),
  `heartbeat_ms`, `blame_p`, `fault_num`.
- **Benchmark client** sends every transaction to all n replicas and runs
  open-loop at `target_input_tps` (das.py sets `max_process_txn` huge).  One
  client process tops out around 3k req/s: use several (`num_clients`).
- Each replica logs `executed_batch ... order_digest:<hex>`; equal digests at
  equal slots across replicas = identical execution order.

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

## Reproducing the Final Paper Results

Pearl's CSVs are in `das_results/pearl/2026-10-26/`, the baselines' in
`das_results/baselines/2026-10-29/` (one dated folder per full run; older runs are in git
history). That folder's `driver.log` records every launch, rerun and moved row; superseded
rows are archived in its `*_rows/` subfolders. The paper figures are generated from copies of
these CSVs by the paper's own `scripts/plot_paper_all_results_new.py` (paper folder, not in
this repo; needs matplotlib >= 3.8).

### Rate-sweep CSVs (n × input_rate × rep)

| System              | Mode | Result CSV                                |
| ------------------- | ---- | ----------------------------------------- |
| **Pearl** (ours)    | —    | `das_results/pearl/<run>/rate/tput_latency.csv` |
| **FairDAG**         | OL   | `das_results/baselines/<run>/rate/fairdag-ol.csv`    |
| **FairDAG**         | BOF  | `das_results/baselines/<run>/rate/fairdag-bof.csv`   |
| **Tusk**            | —    | `das_results/baselines/<run>/rate/tusk.csv`          |
| **Pompe**           | —    | `das_results/baselines/<run>/rate/pompe.csv`         |
| **Themis**          | —    | `das_results/baselines/<run>/rate/themis.csv`        |

**Baseline location**: the FairDAG and narwhal baselines now live *inside* this repo at
`baselines/fairdag` and `baselines/narwhal` (so their changes commit alongside Pearl). The
orchestrators below resolve `FAIRDAG_ROOT`/`NARWHAL_ROOT` to these in-repo paths; the old
out-of-repo copies at `/var/scratch/vrobotin/{baselines-fairdag,narwhal}` are retired.

**Tusk** is the plain DAG-BFT substrate that FairDAG is layered on (FairDAG = fair ordering
on Tusk), drawn from the **fairdag** baseline's `platform/consensus/ordering/tusk` +
`benchmark/protocols/tusk:kv_server_performance`. It is its own system (peer to FairDAG, not a
FairDAG mode) and serves as the no-fair-ordering throughput/latency baseline. As upstream,
its client sends each transaction to one replica in round robin (every other system's client
broadcasts). Its commit loop skips a round whose leader vertex is missing, as FairDAG's own
Tusk does; it used to wait forever, so any silent replica stalled it (2026-11-04).

**Pearl** is reproducible from source via `scripts/deploy/performance/das.py`.

**Baselines (FairDAG OL/BOF, Tusk, Pompe, Themis)**: a single orchestrator at
`scripts/deploy/performance/das_baselines_reps.py` sweeps `(n × input_rate × rep)`
across the baseline systems with no induced faults and appends rows to the
existing baseline CSVs. For each `(system, n, input_rate, rep)` it shells into the
per-point single-run mechanism:
- FairDAG-OL/BOF: `python3 baselines/fairdag/scripts/deploy/performance/das_rate.py --n N --rate R --rl {0,1}` (thin CLI around `das.run(faults=0)`; sibling of the existing `das_faulty.py` / `das_batch.py` shims)
- Tusk: same shim with `--system tusk` (ignores `--rl`); `das.run(system="tusk")` routes to `tusk_performance.sh` + `config/tusk.config`, writing `results/tusk-{n}.txt`. Tusk's `tusk.cpp` block size defaults to 100.
- Pompe/Themis: `fab of-rate --flavor=<pompe|themis> -n=N --rate=R --runs=1` (single-point no-fault task added in `narwhal/benchmark/fabfile.py`; mirrors the inner body of `fab of`, writes to the same `narwhal/benchmark/results/local-0-0-0-{wks}-{nodes}.txt` so reps land in the same file as the original rep=1)

Per-system rows are appended in the existing 11-column baseline schema. Rows with
`tps>0` are auto-skipped on re-run, so resuming after an interruption is safe.
Default reps: `{2,3,4,5}` — rep=1 was produced earlier (FairDAG via a now-deleted
local wrapper around `das.py`/`das_rl.py`; Pompe/Themis via `fab of --flavor=...`),
both pre-existing pathways share the same `das.run(faults=0)` / `OFBench(...).run()`
codepath as the reps 2..5 entry points, so rep=1 and reps 2..5 are comparable.

Pompe/Themis runs require the conda `workenv` environment, which the orchestrator
activates in its narwhal subshell.

**Pompe TPS caveat (June CSVs only)**: until 2026-10-29 the Pompe client sent every command
`block-size` (100) times, replicas processed it on the 100th copy, and the client counted it
as 100 transactions, so narwhal's Pompe TPS was divided by 100: the June `pompe.csv` rows count
*commands* (each 100 transactions' worth of messages). Now a command is sent once and
carries `client-batch` transactions (`pompe_client.cpp`, default 1): Pompe fair-orders a
command as one unit, so 1 gives per-transaction ordering, as Pearl does. `block-size` only
sets the replicas' block size. Serialization buffers in `crypto.h` are per call (a shared
static buffer corrupted block hashes across threads).

### Faulty-node sweep (silent_faults × system)

A single orchestrator at `scripts/deploy/performance/das_faulty.py` drives the
faulty-node sweep across all systems. For each `(system, silent_faults)` it shells
into the underlying single-run mechanism:
- Pearl: `from das import run_experiment` (in-process)
- FairDAG-OL/BOF: `python3 baselines/fairdag/scripts/deploy/performance/das_faulty.py --rl {0,1}` (a thin wrapper around `das.run` that injects a `FAULTS` env var consumed by `fair_performance.sh` / `fairrl_performance.sh`)
- Tusk: same shim with `--system tusk` (n=16, same 3f+1 as FairDAG)
- Pompe/Themis: `fab of-faulty --flavor=<pompe|themis>` (note hyphen — fabric converts the underscore in `def of_faulty` to a hyphen on the command line)

Per-system rows are appended to `das_results/baselines/<run>/faulty/{system}.csv` (Pearl: `das_results/pearl/<run>/faulty/pearl.csv`). Rows with `tps>0` are
auto-skipped on re-run, but rows with `tps<=0` are rerun and *appended*: move the old zero row out afterwards.
Pearl can be run in either OL or BOF via `--pearl-mode {ol,bof}`; both modes share the
same `pearl.csv` and the dedup key includes `mode` so they don't shadow each other.

### Batch-size sweep (block_size × system)

Mirrors the faulty sweep. Orchestrator at `scripts/deploy/performance/das_batch.py`.
Holds n fixed per protocol (same as the faulty sweep), no induced faults, rate=500 tx/s,
sweeps batch_size in {25, 50, 100, 200, 400}.

"Batch size" is a different physical knob per system — the closest comparable parameter:
- Pearl: `block_size` JSON config (# txns per block). Directly settable.
- FairDAG-OL/BOF: `FAIRDAG_BLOCK_SIZE` env var, read by patched `tusk.cpp` constructors
  in `baselines/fairdag/platform/consensus/ordering/{fairdag,fairdag_rl}/algorithm/tusk.cpp`.
  Both fall back to **100** when unset (was `total_num` for OL and `15` for BOF before; changed
  so OL, BOF and Tusk all use the same 100-txn blocks in the rate/faulty sweeps). Env var is
  propagated to remote replicas via `deploy.sh` prefixing the `nohup` launch.
- Tusk: reuses the same `FAIRDAG_BLOCK_SIZE` env var (read by
  `baselines/fairdag/platform/consensus/ordering/tusk/algorithm/tusk.cpp`), so no extra
  plumbing — same `deploy.sh` propagation. Also falls back to 100 when unset.
- Pompe/Themis: narwhal's worker accumulates batches by bytes; converted via
  `node_params['batch_size'] = batch * tx_size` (tx_size=128). New fab task `of-batch`.

Per-system rows are appended to `das_results/baselines/<run>/batch/{system}.csv` (Pearl: `das_results/pearl/<run>/batch/pearl.csv`) with a `batch_size`
column in place of `silent_faults`.

The `plot_paper*.py` scripts in `scripts/deploy/performance/` predate the 2026-10 runs and
read the old CSV layout; the paper figures come from the paper folder's script (above).

---

## Throughput/latency metrics: one definition for every system (since 2026-10-29)

Every baseline replica now logs Pearl's `executed_batch` line every 100 ms
(`exec_metrics.h`, copied into `baselines/fairdag/.../common/framework/`,
`Pompe-HS/include/hotstuff/` and `Themis_tx/include/hotstuff/`): transactions executed and
their mean latency from first arrival at that replica (from a client) to execution, where
execution = commit in the final order (FairDAG/Tusk `CommitMsgInternal`, Themis's execute
callback, Pompe's leader consensus callback; Pompe executes only at server#1).
FairDAG-BOF clients send batched requests (up to 100 tx, fair-ordered as one unit, as upstream
FairDAG; Tusk blocks sized in requests, ~100 tx) because its pairwise weights are O(n·pending²);
OL/Tusk send one tx per request.  Pompe's `commit_set` is only touched on the HotStuff thread
(a cross-thread race used to deadlock it).
`scripts/deploy/performance/baseline_metrics.py` computes throughput and latency from those
lines exactly as `das.py` does for Pearl (window [start+15 s, start+60 s], start = first
client transaction; trimmed mean; median across replicas), and both baseline runners use
it. The consensus columns of baseline rows mirror the execution columns. The rest of this
section describes the old pipelines, which produced the June CSVs.

## Throughput-metric consistency across systems (historical, June CSVs)

Each system computes throughput in its own pipeline, and they were NOT all measuring
the same thing. Reference (correct) definition is **sustained goodput = total executed
÷ wall-clock span**:

| System            | Pipeline                                  | Formula                                                        | Status |
| ----------------- | ----------------------------------------- | -------------------------------------------------------------- | ------ |
| **Pearl**         | `das.py::_compute_tps_latency`            | total executed ÷ span over `[warmup, runtime]` (per-slot)      | ✅ tracks offered (500→495, 1000→999) |
| **Pompe/Themis**  | narwhal `benchmark/benchmark/logs.py`     | `committed_count / (max(commit) − min(start))`                 | ✅ sustained goodput, no inflation |
| **FairDAG/Tusk**  | baseline `calculate_result.py`            | per-window monitor stats (`execute:`, `commit_txn:`, `txn:`)   | ⚠️ outlier — see below |

Pearl, Pompe and Themis all use `total ÷ span` and are mutually consistent. The
FairDAG/Tusk baseline is the only one using per-window monitor stats, and reconciling it
with the per-slot metric has proven fragile.

### Bugs fixed this round (FairDAG/Tusk pipeline)

The first overnight rerun came back **all-zero**. Two independent causes, both from the
in-repo baseline move:
1. **Missing `results/` dir** — `das.py` parsed throughput correctly then crashed appending
   to `results/<file>.txt`, so the shim never printed `RESULT_JSON` and the orchestrator
   recorded 0. Fixed: `das.py` now `os.makedirs("results", exist_ok=True)`; dir also created.
2. **Tusk `execute:` always 0** — Tusk commits via `AddExecuteMessage` (→ `IncCommit`/
   `commit_txn`), never `IncExecute`, so the counter `calculate_result.py` sums was 0 →
   `ZeroDivisionError` → empty `results.log` → `IndexError`. Fixed: added
   `global_stats_->IncExecute()` to Tusk's commit loop in
   `platform/consensus/ordering/tusk/algorithm/tusk.cpp` (mirrors `fairdag.cpp:271`); **rebuilt**.
`calculate_result.py` was also hardened against empty inputs (return 0, don't crash).

### Metric rewrite in `calculate_result.py` (FairDAG/Tusk)

Original metric = **mean of non-zero `execute:` windows** → reported *burst rate*, not
goodput: it dropped idle windows from the denominator, so throughput could exceed the
offered input (e.g. OL n=10 rate-500 → 783 ≫ 500), because fair ordering releases txns in
bursts. Rewritten to **sustained goodput** = `sum(execute) ÷ (arrival-span × 5)`, where the
arrival span = first→last window with request arrivals (`txn:` > 0), and zero-execution
files (client logs, idle/silent replicas) are skipped so they don't dilute the mean.
This is the baseline analog of narwhal's `min(start)→max(commit)` and Pearl's
warmup-trimmed window.

### KNOWN REMAINING ISSUES (revisit after the in-flight sweep finishes)

- **OL low-rate still over-reads** (~1.57× offered: 500→782, 1000→1573), because for OL the
  arrival span equals the execution span (every arrival window also executes), so dividing
  by it still yields the drain rate. The rewrite only helps when arrivals span *wider* than
  execution. **The plateau / max-throughput is correct and stable** (~1900–2000 across n),
  so saturation comparisons are sound; the low end of the L-curve is not yet trustworthy.
- **FairDAG consensus vs execution disagree** (e.g. 317 vs 782 at offered 500) due to
  different divisors (consensus keeps a `/(N)` redundancy factor, execution uses red=1),
  whereas Pearl's are equal (~495/495). The per-window vs per-slot mismatch is structural.
- **BOF looks genuinely degraded** at these settings (n=16 rate-500 → ~12 tps, ~13 s latency;
  n=10 flat ~300 tps, ~11 s latency) — likely a real BOF progress problem, not the metric.
- **Next diagnostic step**: capture ONE OL rate-500 run with `result_*_log` preserved (while
  no sweep is running — `das.py` kills the `last` reservation) to see the true window
  structure and actual client send-rate, then fix the low-rate metric for real.

All FairDAG/Tusk config knobs were also aligned to Pearl this round: `block_size = 100`
(OL/BOF/Tusk), `max_process_txn = 4096`, `clientBatchNum = 1` (1 tx = 1 timestamp;
`clientBatchNum` controls how many txns share a timestamp — verified in
`response_manager.cpp:164`). The fire-and-forget rerun is
`scripts/deploy/performance/rerun_fairdag_and_tusk.sh`.

