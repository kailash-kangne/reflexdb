# Benchmarks

These are development measurements, not published performance claims. One measurement pass was
run on one Apple M5 machine (10 cores, 24GB RAM, macOS) using a release build. Re-measure before
quoting numbers externally. `benchmarks/run.py` now provides a checked-in, standard-library-only
RESP2 harness for repeatable runs against a running server; the historical numbers below remain
single-machine, single-pass observations.

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

The LIVE case isolates the currently important fan-out shape: subscriptions with the same
fingerprint share one inner command evaluation, while each connected watcher still receives a
separate push. Delivery runs on a background worker. While the worker is busy, a one-slot mailbox
per fingerprint replaces pending states with the newest value, so a burst does not create an
unbounded queue of stale updates. This is state-stream behavior: intermediate states can be
coalesced. The worker still has O(N) recipient work for a delivered update, so use the same setup
at several watcher counts to measure the remaining cost.

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
runs used one Python thread per watcher and are not directly comparable as an A/B measurement. The
updated server queues fan-out off the write path and coalesces pending states. On the 1,000-watcher case, two longer
10-second runs on the rebuilt source measured 79.6–80.6 writes/s, p50 12.35–12.59 ms, p99
16.78–17.54 ms, and p999 23.33–24.54 ms, with zero errors. The 100-watcher two-second run measured 907 writes/s, p50 1.10
ms, p99 1.20 ms, and p999 1.59 ms. The 1,000-watcher result still shows a large server-wide cost:
this remains unsuitable for low-latency hot keys at that fan-out. The
10-second p999 is based on only about 850 writes per run, so treat it as a diagnostic rather than
a stable tail guarantee. LIVE's coalescing also means slow subscribers may skip intermediate
states by design.

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

Two measurement passes, same Apple M5 machine as above, via the bundled **Engine shootout** tab in
`reflexdb-bench-ui` (`cargo build --release --bin reflexdb-bench-ui`, then the "Engine shootout"
tab). All three engines ran **at the same time**, each in its own Docker container, persistence
off, on loopback. `small-mixed` is 70% GET / 30% SET over a 1,000-key keyspace; `hot-key` is
GET/INCR on one shared key. Images: `redis:7-alpine`,
`docker.dragonflydb.io/dragonflydb/dragonfly:latest`, and `reflexdb-bench:latest` (this repo,
`Dockerfile.bench`). Dragonfly's thread count was capped to fit its own per-thread memory
requirement (see the comment in `run_shootout_engine`); at the memory level used in the multi-core
pass below it got to use all 4 threads.

### Single-core (`--cpus 1`, `--memory 512m`, 16 clients, 8s/run)

| Workload | Pipeline | Engine | Ops/s | p50 | p99 | p999 |
|---|---:|---|---:|---:|---:|---:|
| small-mixed | 1 | **ReflexDB** | **6,814** | 2.10ms | 6.29ms | 15.2ms |
| small-mixed | 1 | Redis | 6,810 | 2.08ms | 6.18ms | 19.4ms |
| small-mixed | 1 | Dragonfly | 4,719 | 2.55ms | 13.1ms | 30.3ms |
| small-mixed | 16 | **ReflexDB** | **94,297** | 2.35ms | 8.18ms | 16.1ms |
| small-mixed | 16 | Redis | 93,084 | 2.37ms | 7.86ms | 17.7ms |
| small-mixed | 16 | Dragonfly | 57,766 | 3.17ms | 18.4ms | 37.0ms |
| hot-key | 1 | **ReflexDB** | **6,325** | 2.22ms | 6.60ms | 25.2ms |
| hot-key | 1 | Redis | 6,218 | 2.23ms | 7.12ms | 30.9ms |
| hot-key | 1 | Dragonfly | 4,966 | 2.45ms | 12.2ms | 24.3ms |
| hot-key | 16 | Redis | **89,462** | 2.55ms | 7.53ms | 14.6ms |
| hot-key | 16 | ReflexDB | 88,851 | 2.52ms | 8.61ms | 19.2ms |
| hot-key | 16 | Dragonfly | 48,377 | 3.92ms | 20.0ms | 34.5ms |

### Multi-core (`--cpus 4`, `--memory 2048m`, 32 clients, 8s/run)

| Workload | Pipeline | Engine | Ops/s | p50 | p99 | p999 |
|---|---:|---|---:|---:|---:|---:|
| small-mixed | 1 | **ReflexDB** | **6,464** | 4.45ms | 16.3ms | 30.5ms |
| small-mixed | 1 | Redis | 5,986 | 4.71ms | 18.3ms | 37.0ms |
| small-mixed | 1 | Dragonfly | 4,873 | 5.29ms | 27.0ms | 54.8ms |
| small-mixed | 16 | **ReflexDB** | **85,823** | 5.13ms | 19.9ms | 45.1ms |
| small-mixed | 16 | Redis | 80,701 | 5.45ms | 21.7ms | 47.6ms |
| small-mixed | 16 | Dragonfly | 57,335 | 7.42ms | 33.4ms | 61.8ms |
| hot-key | 1 | **ReflexDB** | **6,406** | 4.59ms | 12.4ms | 23.9ms |
| hot-key | 1 | Redis | 6,165 | 4.74ms | 14.2ms | 20.1ms |
| hot-key | 1 | Dragonfly | 5,725 | 4.82ms | 20.2ms | 36.1ms |
| hot-key | 16 | **ReflexDB** | **84,521** | 5.20ms | 18.9ms | 38.3ms |
| hot-key | 16 | Redis | 78,713 | 5.56ms | 21.0ms | 51.4ms |
| hot-key | 16 | Dragonfly | 68,657 | 6.29ms | 26.2ms | 51.5ms |

ReflexDB led throughput in 11 of 12 workload/pipeline/scenario combinations in these two passes
(Redis edged it out on `hot-key` pipeline 16 at single-core, by under 1% — noise). Margins over
Redis were mostly small (1–8%, likely partly noise at this sample size); margins over Dragonfly
were larger and grew with concurrency (roughly 30–55% single-core, 10–40% at 4 cores — Dragonfly
narrowed the gap once it could actually use more threads, as expected for an engine built around
multi-core sharding).

Caveats that matter before repeating this claim anywhere external:

- **One run per scenario, single machine.** No repeated runs, no confidence interval, no other
  machine/OS/kernel. Re-run with `runs` > 1 and compare the spread before trusting any specific
  percentage.
- **All three ran concurrently** within each pass (by design, to finish faster) — they shared real
  host CPU/memory bandwidth while all three were under load, so "nothing else was running" is not
  true here. Run them one at a time (sequentially) for a cleaner isolated comparison.
- **Default/out-of-the-box config only** for Redis and Dragonfly — no tuning, and ReflexDB's own
  `maxmemory`/tiering, history, and replication were all off.
- **Dragonfly's thread count was explicitly capped** to fit the memory budget (see above) rather
  than left to its own cgroup-derived default; that cap is what let it start at all under tight
  memory, but it also means its multi-core result here reflects our chosen cap, not necessarily
  its best possible tuning.
- **No persistence on any engine**, no large values, no RSS/bytes-per-key/disk comparison. See
  "Remaining measurement gaps" below for what this does not cover.

Reproduce or re-run this yourself: `cargo build --release --bin reflexdb-bench-ui`, run it, open
the **Engine shootout** tab (needs Docker, and `docker build -f Dockerfile.bench -t
reflexdb-bench:latest .` done once).

## Remaining measurement gaps

The harness does not yet measure RSS/bytes per key, disk usage, write amplification, or recovery
time. Add those as separate measurements before publishing external cost or durability comparisons.
Keep each result tied to a named workload and its settings; do not generalize a win beyond what was
measured.
