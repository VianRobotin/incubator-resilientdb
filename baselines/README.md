# Baselines

Source and run scripts for the external baseline systems used in the Pearl
evaluation (the systems that produced `das_results/baselines/*.csv` and the
pompe numbers). These are vendored snapshots — **source and run/deploy scripts
only**. Build artifacts (`CMakeFiles/`, `*.o/.a/.so`, compiled binaries),
benchmark logs (`logs/`, `final_ordering_*`, `result_*_log`, `*.exec.log`,
`*.order.log`), and downloaded installers were stripped before committing.

## `narwhal/`
DAG-BFT baselines (Tusk plus the order-fairness protocols in `of_protocols/`:
Pompe-HS, Themis, Rashnu, dikaios). Vendored from the upstream fork
`https://github.com/randomUserGithub123/narwhal` at commit `56c852c`
("update_graph fix"), with local modifications applied during the evaluation.

- `benchmark/fabfile.py` and `benchmark/benchmark/` — the Fabric/Python harness
  used to launch and aggregate runs (`das.py`, `of_protocols.py`, `plot.py`, ...).
- `of_protocols/<proto>/` — the individual protocol implementations.

> **Pompe throughput caveat:** the pompe TPS reported by the narwhal/benchmark
> harness is inflated 100x and must be divided by 100. (The numbers emitted by
> `pompe-rate/run_pompe.py` are already correct and should *not* be divided.)

## `fairdag/`
FairDAG baselines (the `fairdag-bof` and `fairdag-ol` results). A ResilientDB-derived
C++ codebase; run/deploy scripts are under `scripts/deploy/`. The original copy
under `/var/scratch/vrobotin/baselines-fairdag` was not under version control.
