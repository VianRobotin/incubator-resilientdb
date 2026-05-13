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

## Reproducing the Final Paper Results

Each system has a CSV in `das_results/` (or `das_results/baselines/`). The paper plots
are generated from these CSVs by `scripts/deploy/performance/plot_paper.py`.

### Rate-sweep CSVs (n × input_rate)

| System              | Mode | Result CSV                                |
| ------------------- | ---- | ----------------------------------------- |
| **Pearl** (ours)    | —    | `das_results/tput_latency.csv`            |
| **FairDAG**         | OL   | `das_results/baselines/fairdag-ol.csv`    |
| **FairDAG**         | BOF  | `das_results/baselines/fairdag-bof.csv`   |
| **Pompe**           | —    | `das_results/baselines/pompe.csv`         |
| **Themis**          | —    | `das_results/baselines/themis.csv`        |

**Pearl** is reproducible from source via `scripts/deploy/performance/das.py`.

**FairDAG (OL/BOF)** CSVs were produced on 2026-04-25 by a now-deleted orchestrator at
`scripts/deploy/baselines/run_baselines.py` that wrapped
`/var/scratch/vrobotin/baselines-fairdag/scripts/deploy/performance/das.py` (OL, `rl=False`)
and `das_rl.py` (BOF, `rl=True`), reading `results.log` after each run and appending rows
directly to the CSV. The wrapper was never committed; the underlying single-run scripts
write to `baselines-fairdag/scripts/deploy/results/fairdag-{N}.txt` and `fairdagrl-{N}.txt`,
not to the CSVs. To reproduce the rate-sweep CSVs from scratch you would have to recreate
that wrapper.

**Pompe** and **Themis** were produced via `fab of --flavor=<pompe|themis>` in the conda
`workenv` environment, from `/var/scratch/vrobotin/narwhal/benchmark/`. The fabric task
writes to `narwhal/benchmark/results/local-{att}-{arb}-{faults}-{wks}-{nodes}.txt`.

Pompe and Themis runs require the conda `workenv` environment first (`conda activate workenv`).

**Pompe TPS caveat**: throughput reported by `narwhal/benchmark` for the `pompe` flavor is
inflated 100× and must be divided by 100. The CSV at `das_results/baselines/pompe.csv` should
contain the corrected (divided) values; `pompe-rate/run_pompe.py` output does not need this
correction.

### Faulty-node sweep (silent_faults × system)

A single orchestrator at `scripts/deploy/performance/das_faulty.py` drives the
faulty-node sweep across all five systems. For each `(system, silent_faults)` it shells
into the underlying single-run mechanism:
- Pearl: `from das import run_experiment` (in-process)
- FairDAG-OL/BOF: `python3 baselines-fairdag/scripts/deploy/performance/das_faulty.py --rl {0,1}` (a thin wrapper around `das.run` that injects a `FAULTS` env var consumed by `fair_performance.sh` / `fairrl_performance.sh`)
- Pompe/Themis: `fab of-faulty --flavor=<pompe|themis>` (note hyphen — fabric converts the underscore in `def of_faulty` to a hyphen on the command line)

Per-system rows are appended to `das_results/faulty/{system}.csv`. Rows with `tps>0` are
auto-skipped on re-run. Plots: `scripts/deploy/performance/plot_paper_faulty_curated.py`.
Pearl can be run in either OL or BOF via `--pearl-mode {ol,bof}`; both modes share the
same `pearl.csv` and the dedup key includes `mode` so they don't shadow each other.

### Batch-size sweep (block_size × system)

Mirrors the faulty sweep. Orchestrator at `scripts/deploy/performance/das_batch.py`.
Holds n fixed per protocol (same as the faulty sweep), no induced faults, rate=500 tx/s,
sweeps batch_size in {25, 50, 100, 200, 400}.

"Batch size" is a different physical knob per system — the closest comparable parameter:
- Pearl: `block_size` JSON config (# txns per block). Directly settable.
- FairDAG-OL/BOF: `FAIRDAG_BLOCK_SIZE` env var, read by patched `tusk.cpp` constructors
  in `baselines-fairdag/platform/consensus/ordering/{fairdag,fairdag_rl}/algorithm/tusk.cpp`.
  Falls back to the original defaults (`total_num` for OL, `15` for BOF) when unset, so
  pre-existing scripts that don't set the var keep their old behavior. Env var is
  propagated to remote replicas via `deploy.sh` prefixing the `nohup` launch.
- Pompe/Themis: narwhal's worker accumulates batches by bytes; converted via
  `node_params['batch_size'] = batch * tx_size` (tx_size=128). New fab task `of-batch`.

Per-system rows are appended to `das_results/batch/{system}.csv` with a `batch_size`
column in place of `silent_faults`. Plots: `plot_paper_batch_curated.py`.

Final paper plots are produced by:
```bash
python /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy/performance/plot_paper.py
python /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy/performance/plot_paper_faulty_curated.py
python /var/scratch/vrobotin/incubator-resilientdb/scripts/deploy/performance/plot_paper_batch_curated.py
```

