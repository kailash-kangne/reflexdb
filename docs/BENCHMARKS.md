# Benchmarks

These are development measurements, not published performance claims. The historical baselines
below came from one Apple M5 machine; the Engine shootout was rerun on 2026-10-30 on an arm64 Mac.
Both are single-machine observations, so re-measure before quoting numbers externally.
`benchmarks/run.py` provides a checked-in, standard-library-only RESP2 harness for repeatable runs
against a running server.

## Reproducible harness

Start a release server in one terminal, then run the harness from the repository root:

```sh
cargo run --release -p reflexdb-server -- --port 7379
python3 benchmarks/run.py --runs 5 --seconds 10 --clients 16 --pipeline 1,16 --output /tmp/reflexdb-bench.jsonl
```

The default workloads are `small-mixed` (70% GET / 30% SET over a bounded keyspace), `hot-key`
(GET/INCR on one shared key), and `live-fanout` (one `LIVE GET` key watched by 1,000 clients while
a writer issues `INCR`). Each JSONL row records the workload settings, operations, throughput,
errors, and client-observed p50/p99/p999 latency for that run. Repeating `--runs` emits a separate
row per run so run-to-run variance remains visible. Change `--clients`, `--pipeline`, `--keys`, or
`--live-subscribers` to test another profile. Keep the exact command, server build, OS, CPU, RAM,
and persistence configuration with any result; the script intentionally does not start or tune
the server for you.

The same controls are available in the visual benchmark app: start it with
`cargo run --bin reflexdb-bench-ui`, open `http://127.0.0.1:8080`, then choose **Latency lab**.
The UI streams each completed run into KPI cards, a logarithmic p50/p99/p999 chart, and a
comparison table. Python 3 must be available on the UI host (`PYTHON` can select its executable),
and the target database server must already be running.

The current LIVE path re-runs each matching fingerprint's command synchronously on the write
path, caches its latest result, and broadcasts the fingerprint on a bounded server-wide event
bus. Each interested connection sends the latest result; lagging connections can skip intermediate
states. The event bus has 1,024 slots. Pub/sub and tracking use a separate 1,024-item per-client
outbound queue, whose overflow triggers disconnect after 25 ms. The measurements below are
historical and do not establish current performance; re-run the harness against the current build
before using them as a baseline.

### LIVE fan-out measurement and change

Release-build runs on the Apple M5 machine described above (no persistence, one writer) measured
`INCR live:hot` with 0 / 100 / 1,000 `LIVE GET live:hot` subscribers:

| Watchers | Writes/s | p50 | p99 | p999 |
|---:|---:|---:|---:|---:|
| 0 | 20,377 | 48 µs | 73 µs | 150 µs |
| 100 | 907 | 1.10 ms | 1.22 ms | 1.59 ms |
| 1,000 | 80 | 12.4 ms | 17.5 ms | 24.5 ms |

The 0- and 100-watcher runs used two-second samples; the 1,000-watcher result is the range from two
10-second runs. The updated harness uses one selector loop to drain all subscriber sockets; earlier
runs used one Python thread per watcher and are not directly comparable as an A/B measurement. On
the 1,000-watcher case, two longer 10-second runs on the measured source measured 79.6–80.6 writes/s,
p50 12.35–12.59 ms, p99
16.78–17.54 ms, and p999 23.33–24.54 ms, with zero errors. The 100-watcher two-second run measured 907 writes/s, p50 1.10
ms, p99 1.20 ms, and p999 1.59 ms. The 1,000-watcher result still shows a large server-wide cost:
this remains unsuitable for low-latency hot keys at that fan-out. The
10-second p999 is based on only about 850 writes per run, so treat it as a diagnostic rather than
a stable tail guarantee. Slow subscribers may skip intermediate states by design.

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

## Engine shootout: ReflexDB vs. Redis vs. Dragonfly

Runs on 2026-10-30 used the current working tree, `benchmarks/run.py`, and a freshly built
ReflexDB image. Host: arm64 macOS 26.6, 10 logical CPUs, Docker Desktop 29.8. Persistence was off;
each engine ran **alone** to avoid CPU oversubscription. Both CPU settings used 2 GiB memory,
16 clients, 1,000 keys, 5 seconds per case, and identical workloads. Dragonfly used 1 or 4
proactor threads to match its CPU limit. Images: `redis:7-alpine`, Dragonfly `latest`, and
ReflexDB built from this checkout. Values are operations/second from one run; all reported zero
client errors.

| Workload | CPUs | Pipeline 1: Redis / Dragonfly / ReflexDB | Pipeline 16: Redis / Dragonfly / ReflexDB |
|---|---:|---:|---:|
| small-mixed | 1 | 19,464 / 19,547 / **20,630** | **177,530** / 175,716 / 168,818 |
| small-mixed | 4 | **20,046** / 18,341 / 19,369 | **178,679** / 170,944 / 174,401 |
| hot-key | 1 | 20,800 / 19,657 / **21,184** | **193,896** / 192,020 / 192,512 |
| hot-key | 4 | **20,697** / 20,142 / 19,798 | **195,523** / 183,525 / 187,850 |

Pipeline depth 16 delivered about 8–10× the throughput of pipeline depth 1 in these tests.
Changing the CPU cap from 1 to 4 produced small, mixed changes rather than a consistent gain.
ReflexDB led two of four cases at 1 CPU; Redis led all four at 4 CPUs. These are single runs,
so repeat them before drawing firm conclusions.

Caveats:

- **One run per case, one host.** Results can vary; rerun with `--runs 5` or more before drawing
  firm conclusions.
- Engines ran one at a time to keep the 4-CPU tests from oversubscribing the host.
- Redis and Dragonfly used default configuration apart from persistence being disabled and
  Dragonfly's thread count being set to 1 / 4 to match the CPU limit. ReflexDB
  used its default settings with `maxmemory`, tiering, history, and replication off.
- There was no persistence, large-value workload, RSS/bytes-per-key, or disk comparison. See
  "Remaining measurement gaps" below.

Rebuild ReflexDB with `docker build -f Dockerfile.bench -t reflexdb-bench:latest .`, then run
`benchmarks/run.py` against each engine separately using the settings above. The UI's **Engine
shootout** tab runs all selected engines concurrently, so its results can differ under host load.

## Remaining measurement gaps

The harness does not yet measure RSS/bytes per key, disk usage, write amplification, or recovery
time. Add those as separate measurements before publishing external cost or durability comparisons.
Keep each result tied to a named workload and its settings; do not generalize a win beyond what was
measured.
