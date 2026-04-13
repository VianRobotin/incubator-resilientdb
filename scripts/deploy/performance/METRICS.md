# Experiment Metrics

This document describes every metric collected by `das.py` and how to interpret
them in the context of an Autobahn OL vs BOF comparison.

---

## Application-layer metrics (client perspective)

### Throughput (`tps`, tx/s)

Measured on **replica nodes** from the `consumed transactions:N` lines emitted
by the Stats monitor every 5 seconds.  Each sample counts how many transactions
were **committed and executed** in the preceding 5-second window.  A trimmed
mean is computed over the measurement window (after warmup): the lowest and
highest samples are dropped before averaging, to suppress cold-start and
teardown noise.

*What it tells you:* how many client transactions the system commits per second
under the given input load.  For a fair comparison, both OL and BOF receive the
same input rate (`target_tps`); a lower committed TPS means the protocol is
failing to keep up.

### End-to-end latency (`latency_s`, seconds)

Measured on **client nodes** from the `req client latency:X` lines in the Stats
output.  Each sample is the average time from when a batch of transactions was
sent by the client to when the client received `f+1` acknowledgements from
replicas (i.e., the transaction was durably committed).  Trimmed mean across
samples after warmup.

*What it tells you:* how long a transaction takes from submission to confirmed
commit, as seen by an external client.  This is the primary user-facing latency
metric.  BOF imposes additional waiting (the Δ-alignment wait in Algorithms 2/3)
that should show up here as higher latency relative to OL.

---

## Consensus-layer metrics (protocol perspective)

### Consensus throughput (`consensus_tps`, slots/s)

Derived from the number of `consensus commit slot:N` log lines in the replica
log, counting only those after the warmup period, divided by the measurement
window length.  Each slot represents one round of Sync HotStuff in which the
leader assembled a DAG snapshot, proposed it, and collected `f+1` votes before
committing.

*What it tells you:* how fast the consensus protocol itself is making progress,
independent of how many transactions each slot carries.  If consensus TPS is
stable while application TPS drops, it means slots are being committed but with
fewer transactions per slot (the DAG lanes are running dry).  For BOF the leader
must additionally wait for relative-ordering messages before proposing, so
consensus TPS may be lower than OL under the same input load.

### Consensus latency (`consensus_latency_s`, seconds)

Measured on **replica nodes** from the `consensus_latency_us:X` field in the
structured `consensus commit slot` log lines.  For each committed slot this is
`commit_time − propose_time`, i.e., the wall-clock time from when the leader
broadcast the proposal to when it was committed (after the mandatory 2Δ timer).
Trimmed mean across all slots after warmup, converted to seconds.

*What it tells you:* the round-trip time of one consensus instance.  Under OL
this is dominated by 2Δ (the Sync HotStuff safety timer, ~2 s with Δ=1 s).
Under BOF the leader additionally waits for TEE timestamps and relative-ordering
vectors before proposing (Algorithm 2), so consensus latency should be higher,
and the gap grows with `n` because the leader must wait for more replicas'
ordering messages.  This metric directly quantifies the fairness overhead.

---

## Experimental parameters

| Parameter | Value | Rationale |
|---|---|---|
| `n` | 3, 5, 7, 9, 11 | Valid Sync HotStuff committee sizes (2f+1, f=1..5) |
| `target_tps` | 350 tx/s total | Matches gsegalin; below saturation so latency signal is visible |
| `num_clients` | n (one per replica) | Matches gsegalin; each client feeds one replica's DAG lane |
| `block_size` | 100 | Transactions per DAG block per replica |
| `runtime` | 60 s | Matches gsegalin |
| `warmup` | 15 s | Excluded from all metric averages |
| `repetitions` | 5 | Matches gsegalin; enables standard-deviation error bars |

---

## Notes for the paper

- Report means ± standard deviation across the 5 repetitions.
- Throughput and latency are the primary application metrics; consensus TPS and
  latency are the protocol-level metrics that explain *why* BOF differs from OL.
- Consensus latency was not successfully measured in the fairdag reference
  experiments (gsegalin set it to `−1`); our implementation records it properly
  via `propose_time → commit_time` in Autobahn's `Commit()`.
- The 2Δ floor on consensus latency means you should expect ~2 s as a baseline
  even for OL; BOF overhead appears as the *difference* above this floor.
