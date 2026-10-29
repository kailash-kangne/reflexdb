# Benchmarks

These are development measurements, not published performance claims. One measurement pass was
run on one Apple M5 machine (10 cores, 24GB RAM, macOS) using a release build. Re-measure before
quoting numbers externally; the runs are not yet a reproducible suite.

## Measurements to collect

For each representative workload, record the exact build, machine, dataset, key/value sizes,
concurrency, pipelining, warmup, and persistence settings. Repeat runs and retain raw output.

| Workload | Measure |
|---|---|
| Small-key mixed reads and writes | Throughput, p50/p99 latency, CPU, RSS, bytes per key |
| Large values and high cardinality | Throughput, RSS, disk bytes, allocator growth |
| Hot-key contention | p99 latency, throughput scaling by client count, CPU |
| Persistence enabled | Write throughput, fsync cost, recovery time, disk write amplification |
| Tiered working set | Hot/cold read latency, RAM saved, disk consumed, promotion/eviction churn |
| Workflow recovery prototype | Time to identify bad state and resume, data-loss boundary, history overhead |

## Baseline (no persistence), pipelined (`-P 16`, 50 connections)

| Command | req/s |
|---|---|
| SET | 227,894 |
| GET | 244,141 |
| INCR | 248,386 |
| LPUSH | 275,179 |
| SADD | 219,394 |
| HSET | 218,245 |
| ZADD | 244,379 |
| XADD | 253,165 |

Non-pipelined (`-P 1`), commands land around 13–16k req/s / ~1.6–1.7ms p50 in this run; loopback
round-trip dominates the measurement.

## Persistence overhead

| Config | Pipelined | Non-pipelined |
|---|---|---|
| No persistence | 227,894 req/s | 15,820 req/s |
| `appendfsync everysec` | 225,225 req/s (-1%) | 15,871 req/s (+0%) |
| `appendfsync always` | 229,885 req/s (noise) | 18,975 req/s (noise) |

These results are noisy, single-run observations. Repeat them on a controlled setup before drawing
conclusions about persistence cost.

## `GETAT`/`HISTORY` overhead (`-P 16`, SET)

| Config | Random keys | Same key hammered |
|---|---|---|
| `history-enabled no` | 269,542 req/s | 223,380 req/s |
| `history-enabled yes` | 213,402 req/s (-21%) | 217,391 req/s (-3%) |

Re-measure before treating these percentages as fixed; they have drifted between sessions.

## `ZPOPMIN`/`ZPOPMAX` — before/after the `BTreeSet` index

2,000 sequential `ZPOPMIN` calls against a 200,000-member sorted set:

| | Time | Per-call |
|---|---|---|
| Before (full `sorted()` per pop) | 67.1s | ~33.5ms |
| Partial fix (`select_nth_unstable_by`) | 7.9s | ~4.0ms |
| Current (`BTreeSet` index, O(log n)/pop) | **0.52s** | **~0.26ms** |

This was 129x faster than the original measurement. `ZSet` now keeps a
`by_score: BTreeSet<(score, member)>` alongside the `HashMap`, kept in sync on each insert and
remove. `sorted()` (backing `ZRANGE` and related commands) is O(n) instead of O(n log n). `SPOP`
uses `indexmap::IndexSet` so selecting and removing a random member is O(1), rather than cloning
all members into a temporary vector.

## Reproducibility gap

The current notes lack repeated-run distributions, RSS and bytes-per-key results, disk usage,
write amplification, and a checked-in workload harness. Add these before publishing external
performance or cost comparisons. Keep each result tied to a named workload and its settings; do
not generalize a win beyond what was measured.
