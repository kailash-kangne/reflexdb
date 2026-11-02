# Benchmarks

These are development measurements, not published performance claims. The historical baselines
below came from one Apple M5 machine; the Engine shootout was rerun on an arm64 Mac.
Both are single-machine observations, so re-measure before quoting numbers externally.
`benchmarks/run.py` provides a checked-in, standard-library-only RESP2 harness for repeatable runs
against a running server; `benchmarks/run_rfx.py` is the same harness speaking RFX
(`docs/RFX.md`). `benchmarks/compare.py` diffs a `--output` JSONL history across `--label`ed builds
and exits non-zero on regressions past a threshold — see [OPERATIONS.md](OPERATIONS.md).
`benchmarks/ab.sh` runs the comparison for you: it alternates the two binaries A,B,A,B… across
rounds so machine drift hits both sides equally (sequential "all of A then all of B" runs show
fake ±10–25% regressions on a loaded dev box). Always compare builds through `ab.sh`, never by
eyeballing two `run.py` batches. `benchmarks/smoke.sh` is a redis-cli feature check that runs against any
RESP-compatible server — the shared command suite passes on ReflexDB, Redis, and Dragonfly, with
ReflexDB-only commands (`LIVE`, `SCHEDULE`, single-flight, `GETAT`) auto-detected and skipped on
other engines.

Latest verified state: `ab.sh` (5 interleaved rounds, `--workloads small-mixed,hot-key`,
16 clients, pipelines 1 and 16) comparing the current tree — compact collection
representations, channel-pattern ACLs, salted password hashing, `metrics-token`,
auto-AOF-rewrite, history flush acknowledgements — against `ea5fcb9` (the commit before
the phase work began) showed every workload within ±1%:

| Workload | Pipeline | Base ops/s | Head ops/s | Delta |
|---|---:|---:|---:|---:|
| hot-key | 1 | 24,557 | 24,449 | −0.4% |
| hot-key | 16 | 179,154 | 180,530 | +0.8% |
| small-mixed | 1 | 24,879 | 24,751 | −0.5% |
| small-mixed | 16 | 151,412 | 151,979 | +0.4% |

All ACL hot-path work (channel check, key-pattern operand decode) stays behind the
`allkeys`/`allchannels` fast path — the default user pays zero extra lookups per command.

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
row per run so run-to-run variance remains visible. Change `--clients`, `--pipeline`, `--keys`,
`--value-size`, or `--live-subscribers` to test another profile. Keep the exact command, server build, OS, CPU, RAM,
and persistence configuration with any result; the script intentionally does not start or tune
the server for you.

The same controls are available in the visual benchmark app: start it with
`cargo run --bin reflexdb-bench-ui`, open `http://127.0.0.1:8080`, then choose **Latency lab**.
The UI streams each completed run into KPI cards, a logarithmic p50/p99/p999 chart, and a
comparison table. Python 3 must be available on the UI host (`PYTHON` can select its executable),
and the target database server must already be running.

The current LIVE path deduplicates changed keys into a wake queue drained by a dedicated
`live-fanout` thread: it re-runs each matching fingerprint's command, caches the latest result,
pre-encodes the push once per wire variant (RESP2/RESP3/RFX), and `try_lock`s each subscriber's
shared socket writer for a direct `poll_write` — a contended writer falls back to that client's
bounded 1,024-item queue, whose overflow triggers disconnect after 25 ms. Connections only get a
shared writer once they hold a LIVE subscription, so the plain reply path stays a direct
`write_all`. The measurements below are historical and do not establish current performance;
re-run the harness against the current build before using them as a baseline.

### LIVE fan-out measurement and change

The first fan-out design (one thread feeding subscriber sockets, before sweep coalescing and
pacing existed) collapsed at high watcher counts — `INCR live:hot` with N
`LIVE GET live:hot` subscribers, release build, no persistence, one writer:

| Watchers | Writes/s | p50 | p99 | p999 |
|---:|---:|---:|---:|---:|
| 0 | 20,377 | 48 µs | 73 µs | 150 µs |
| 100 | ~900–1,120 | ~1.1 ms | ~1.2 ms | ~1.6 ms |
| 1,000 | ~80–93 | ~12.4 ms | ~17.5 ms | ~24.5 ms |

With the current design (sweep coalescing `live-sweep-ms` 10 ms, per-sweep pacing
`live-pace-chunk`/`live-pace-ms`, shared pre-encoded pushes): **~2,700–2,840 writes/s at 100
watchers** (p50 ~280 µs), **~1,138 writes/s at 1,000 watchers** (p50 ~484 µs), and
**~3,055–3,640 writes/s at 5,000 watchers** (p50 ~150 µs, p999 ~3 ms, zero errors) — past the old
1,000-watcher collapse point. The trade: subscribers see slightly staler intermediate values
under load (that's what the dedup window buys).

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
selects and removes a random element in O(1) — `swap_remove` on the packed `Vec` under 128
members, `indexmap::IndexSet::swap_remove_index` above it — rather than cloning all members
into a temporary vector.

## Engine shootout: ReflexDB vs. Redis vs. Dragonfly

Same setup as below, but the host had an unrelated container stack (Kafka, ZooKeeper,
application services) running alongside, so absolute numbers are ~15–25% lower and run-to-run
variance is higher; only relative placement is meaningful. ReflexDB's image was the current
working tree — lazy shared-writer fix, `live-sweep-ms`/pacing changes, and the security/
durability hardening (per-command ACL enforcement, `MISCONF` AOF-failure gate,
`min-replicas-to-write`, `tiered-overflow-policy` capacity check, `SHUTDOWN`/signal handling);
later A/B passes against `ea5fcb9` confirmed none of those gates cost measurable latency
(see the "Latest verified state" table above), and the `native` rows — ReflexDB only,
uncapped release build of that tree — show the same. Medians of 3 runs:

| Workload | CPUs | Pipeline 1: Redis / Dragonfly / ReflexDB | Pipeline 16: Redis / Dragonfly / ReflexDB |
|---|---:|---:|---:|
| small-mixed | 1 | 12,734 / 11,264 / **17,249** | 146,259 / 128,202 / **149,301** |
| small-mixed | 4 | 14,699 / 14,388 / **16,200** | 153,239 / 144,954 / **154,605** |
| hot-key | 1 | 15,494 / 12,010 / **16,708** | 147,983 / 142,760 / **152,644** |
| hot-key | 4 | 14,925 / 13,990 / **16,866** | 161,254 / 153,218 / **163,143** |
| small-mixed | native | — / — / **24,025** | — / — / **208,158** |
| hot-key | native | — / — / **24,124** | — / — / **219,935** |

ReflexDB led all eight medians this pass, Dragonfly none — though all three sit inside a noisy
~14–17k ops/s P1 band, and the idle-host pass below shows placement can flip run to run; the
gap stays within ~±15%.

The idle-host pass used an earlier working tree (pre-hardening), `benchmarks/run.py`, and a
freshly built
ReflexDB image. Host: arm64 macOS 26.6, 10 logical CPUs, Docker Desktop 29.8. Persistence was off;
each engine ran **alone** to avoid CPU oversubscription. Both CPU settings used 2 GiB memory,
16 clients, 1,000 keys, 5 seconds per case, and identical workloads. Dragonfly used 1 or 4
proactor threads to match its CPU limit. Images: `redis:7-alpine`, Dragonfly `latest`, and
ReflexDB built from this checkout. Values are the **median operations/second of 3 runs** per case;
all reported zero client errors.

| Workload | CPUs | Pipeline 1: Redis / Dragonfly / ReflexDB | Pipeline 16: Redis / Dragonfly / ReflexDB |
|---|---:|---:|---:|
| small-mixed | 1 | 18,814 / 17,007 / **21,498** | 172,702 / 161,951 / **187,945** |
| small-mixed | 4 | 18,842 / 14,815 / **20,574** | 174,222 / 134,244 / **181,563** |
| hot-key | 1 | 19,003 / 15,895 / **22,366** | 180,397 / 158,948 / **201,045** |
| hot-key | 4 | 18,652 / 14,838 / **21,773** | 175,988 / 172,598 / **199,243** |

Pipeline depth 16 again delivered roughly 9–10× the throughput of pipeline depth 1. ReflexDB led
all eight medians this run — an earlier measurement of the same working tree trailed Redis in six
of eight cases; the difference was a per-reply `Mutex<dyn>` socket write introduced by the LIVE
fan-out rework, removed again after this shootout exposed it (plain connections now write replies
directly; only LIVE subscribers share their socket with the fan-out thread). Dragonfly trailed in
every case this run — its 4-CPU results in particular had high run-to-run variance (one `hot-key`
pipeline-1 run dropped to ~7.6k ops/s while the others stayed near 15–18k). Engines were measured
at different points in the session, so treat any individual placing as noise, not a ranking.

Caveats:

- **Median of only 3 runs per case, one host.** Results can vary; rerun with `--runs 5` or more
  before drawing firm conclusions.
- Engines ran one at a time to keep the 4-CPU tests from oversubscribing the host.
- Redis and Dragonfly used default configuration apart from persistence being disabled and
  Dragonfly's thread count being set to 1 / 4 to match the CPU limit. ReflexDB
  used its default settings with `maxmemory`, tiering, history, and replication off.
- There was no persistence, RSS/bytes-per-key, or disk comparison. Large values and keyspace
  scale are covered separately in "Aggressive profile" below; see also "Remaining measurement
  gaps".

Rebuild ReflexDB with `docker build -f Dockerfile.bench -t reflexdb-bench:latest .`, then run
`benchmarks/run.py` against each engine separately using the settings above. The UI's **Engine
shootout** tab runs all selected engines concurrently, so its results can differ under host load.

## Aggressive profile

Same loaded-host Docker setup as the shootout above (`--cpus 4`, 2 GiB,
engines run one at a time, `small-mixed`, median of 3 × 5 s runs). Stresses dimensions the main
table does not — client count, pipeline depth, keyspace size, value size:

| Case | Redis | Dragonfly | ReflexDB |
|---|---:|---:|---:|
| 64 clients, pipeline 1 | 15,244 | 16,485 | **18,314** |
| 64 clients, pipeline 1, hot-key | 18,146 | 18,206 | **19,205** |
| 16 clients, pipeline 64 | 282,380 | 270,308 | **286,638** |
| 100k-key keyspace, pipeline 16 | 140,996 | 147,472 | **149,601** |
| 8 KiB values, pipeline 16 | 48,356 | 46,633 | **50,688** |

ReflexDB led all five medians. Deep pipelines scale best for every engine (P64 roughly triples the
P16 baseline); the 100k-key run shows no keyspace-size penalty versus the 1k-key baseline; the
8 KiB run is payload/alloc-bound — ~51k ops/s is ~400 MiB/s of stored payload on ReflexDB. The
last row uses the harness's new `--value-size` flag.

## RESP vs. RFX wire format

RFX (`docs/RFX.md`) is the optional binary framing auto-detected per connection by a leading `!`
byte; `benchmarks/run_rfx.py` is the same harness as `run.py` with RFX encode/decode swapped in.
One release server (Apple M5, no persistence) ran both harnesses **simultaneously** — 16 RESP
clients + 16 RFX clients at once, `small-mixed`/`hot-key`, 3 runs × 5 s, pipelines 1 and 16 —
so the figures share CPU contention equally and measure the relative codec cost, not solo peaks:

| Workload | RESP ops/s | RFX ops/s |
|---|---:|---:|
| small-mixed, P1 | ~18.8K | ~18.7K |
| small-mixed, P16 | ~194K | ~201K |
| hot-key, P1 | ~18.0K | ~17.8K |
| hot-key, P16 | ~196K | ~204K |

RFX edged out RESP in all six pipelined runs (~4%); non-pipelined runs tie because latency, not
parsing, dominates. The auto-detect branch adds no measurable cost to RESP connections. Both
formats ran concurrently with zero errors — the numbers do not establish an RFX advantage beyond
pipelined decode/encode savings on this one machine.

Re-ran concurrently in Docker (`--cpus 4`, loaded host): both formats completed all
runs with zero errors and stayed within run-to-run noise of each other (medians ~7.6–10.4k ops/s
P1, ~106–142k ops/s P16 on both codecs — the host was too loaded to separate the two this
session). No regression observed versus the table above.

## Remaining measurement gaps

The harness does not yet measure RSS/bytes per key (`INFO`/`metrics-port` report
`used_memory`/`used_memory_peak`, but `run.py` doesn't capture them per run), disk usage, write
amplification, or recovery time. Add those as separate measurements before publishing external
cost or durability comparisons. Keep each result tied to a named workload and its settings; do
not generalize a win beyond what was measured.
