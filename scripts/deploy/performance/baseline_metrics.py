"""Pearl-consistent execution throughput and latency for the baselines.

Every baseline replica logs Pearl's "executed_batch" line every 100 ms
(ExecMetrics, exec_metrics.h in each baseline tree): transactions executed in
the interval and their mean latency from first arrival at that replica (from a
client) to execution.  This module computes the metrics from those lines the
way das.py does for Pearl:

  - window [start + warmup, start + runtime], start = the first client
    transaction any replica saw (Pearl: when its clients were triggered);
  - per replica, throughput = executed / logged span, latency = trimmed mean
    of the per-interval averages;
  - median across the replicas that executed anything.

Self-contained (no `import das`: the FairDAG tree has its own das.py) and
Python 3.6 compatible.  CLI: python3 baseline_metrics.py LOG [LOG ...]
"""

import datetime
import gzip
import json
import re
import sys

WARMUP = 15   # das.WARMUP
RUNTIME = 60  # das.RUNTIME

_TS = r'^[EW](\d{8}) (\d{2}:\d{2}:\d{2}\.\d+)'
# Same pattern as das._EXEC_RE.
_EXEC_RE = re.compile(
    _TS + r'.*?executed_batch slot:\d+ count:(\d+) avg_latency_us:(\d+) '
    r'max_latency_us:\d+ pending:\d+', re.MULTILINE)
_START_RE = re.compile(_TS + r'.*?first_client_txn', re.MULTILINE)


def _dt(day, clock):
    return datetime.datetime.strptime(day + " " + clock, "%Y%m%d %H:%M:%S.%f")


def _read(path):
    path = str(path)
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", errors="replace") as f:
        return f.read()


def parse(text):
    """(first client arrival or None, [(dt, count, avg_latency_us)])."""
    m = _START_RE.search(text)
    start = _dt(m.group(1), m.group(2)) if m else None
    entries = [(_dt(m.group(1), m.group(2)), int(m.group(3)), int(m.group(4)))
               for m in _EXEC_RE.finditer(text)]
    return start, entries


def trimmed_mean(values, drop=1):  # das.trimmed_mean
    v = sorted(x for x in values if x > 0)
    if len(v) > 2 * drop:
        v = v[drop:-drop]
    return sum(v) / len(v) if v else 0.0


def _window(entries, start, warmup, runtime):  # das._apply_warmup
    lo = start + datetime.timedelta(seconds=warmup)
    hi = start + datetime.timedelta(seconds=runtime)
    strict = [e for e in entries if lo <= e[0] <= hi]
    if strict:
        return strict
    return [e for e in entries if start <= e[0] <= hi]


def _tps_latency(entries, fallback_duration):  # das._compute_tps_latency
    if not entries:
        return 0.0, 0.0
    if len(entries) >= 2:
        span_s = (entries[-1][0] - entries[0][0]).total_seconds()
        duration = span_s + span_s / (len(entries) - 1)
    else:
        duration = max(1.0, fallback_duration)
    tps = sum(e[1] for e in entries) / max(0.001, duration)
    return tps, trimmed_mean([e[2] for e in entries]) / 1000.0


def _median(xs):
    s = sorted(xs)
    n = len(s)
    if not s:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def compute(log_paths, warmup=WARMUP, runtime=RUNTIME):
    """{'execution_tps', 'execution_latency_ms', 'replicas'} over the
    replica logs in `log_paths` (missing or unreadable ones are skipped)."""
    parsed = []
    for p in log_paths:
        try:
            parsed.append(parse(_read(p)))
        except (IOError, OSError):
            continue
    starts = [s for s, _ in parsed if s is not None]
    if not starts:
        return {"execution_tps": 0.0, "execution_latency_ms": 0.0, "replicas": 0}
    start = min(starts)
    tps_l, lat_l = [], []
    for _, entries in parsed:
        tps, lat = _tps_latency(_window(entries, start, warmup, runtime),
                                runtime - warmup)
        if tps > 0:
            tps_l.append(tps)
            lat_l.append(lat)
    return {"execution_tps": round(_median(tps_l), 1),
            "execution_latency_ms": round(_median(lat_l), 2),
            "replicas": len(tps_l)}


if __name__ == "__main__":
    print(json.dumps(compute(sys.argv[1:])))
