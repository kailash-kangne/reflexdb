# Benchmarks

One measurement pass, one dev machine (Apple M5, 10 cores, 24GB RAM, macOS), `cargo build
--release`, `redis-benchmark`. Not a published/audited/reproducible suite — re-measure before
quoting these numbers externally.

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

Non-pipelined (`-P 1`), every command lands around 13-16k req/s / ~1.6-1.7ms p50 — dominated by
loopback round-trip, not server-side work.

## AOF overhead

| Config | Pipelined | Non-pipelined |
|---|---|---|
| No persistence | 227,894 req/s | 15,820 req/s |
| `appendfsync everysec` | 225,225 req/s (-1%) | 15,871 req/s (+0%) |
| `appendfsync always` | 229,885 req/s (noise) | 18,975 req/s (noise) |

Under pipelined load, `appendonly yes` — even with `appendfsync always` — costs no measurable
throughput. The group-commit AOF writer thread is doing its job.

## `GETAT`/`HISTORY` overhead (`-P 16`, SET)

| Config | Random keys | Same key hammered |
|---|---|---|
| `history-enabled no` | 269,542 req/s | 223,380 req/s |
| `history-enabled yes` | 213,402 req/s (-21%) | 217,391 req/s (-3%) |

Re-measure before treating these percentages as fixed — they've drifted between sessions.

## `ZPOPMIN`/`ZPOPMAX` — before/after the `BTreeSet` index

2,000 sequential `ZPOPMIN` calls against a 200,000-member sorted set:

| | Time | Per-call |
|---|---|---|
| Before (full `sorted()` per pop) | 67.1s | ~33.5ms |
| Partial fix (`select_nth_unstable_by`) | 7.9s | ~4.0ms |
| Current (`BTreeSet` index, O(log n)/pop) | **0.52s** | **~0.26ms** |

129x faster than the original. `ZSet` now keeps `by_score: BTreeSet<(score, member)>` alongside
the `HashMap`, kept in sync on every insert/remove; `sorted()` (backing `ZRANGE`/etc.) is `O(n)`
instead of `O(n log n)` as a side effect. `SPOP` got its own separate fix: `Set` is backed by
`indexmap::IndexSet` (not `HashSet`), so picking a random index and `swap_remove_index`ing it is
O(1) per pop, instead of cloning every member into a `Vec` just to throw most of it away.

## vs. another RESP-compatible engine (single-threaded, Valkey-based) — same machine

| | Other engine | reflexdb |
|---|---|---|
| Correctness (47 commands) | — | 0 mismatches (after adding 10 missing commands) |
| Single-request p50 | 0.039-0.055ms | 0.047-0.055ms (tie) |
| Pipelined SET (16/round-trip) | ~194-270k req/s | ~262-422k req/s |
| Pipelined GET | ~210-304k req/s | ~296-579k req/s |
| CPU per request (200 conns, no pipelining) | ~20µs (1 core) | ~30µs (1.25 cores) |

reflexdb's extra CPU is Tokio's cross-thread task handoff, not command logic (`dispatch()`
itself: 570ns for SET, 195ns for GET) — the same cost that lets it spread load across cores and
win the pipelined numbers above.

## vs. another RESP-compatible engine (multi-threaded, shared-nothing) — same environment (both in Docker)

| | Other engine | reflexdb |
|---|---|---|
| Correctness (37 commands) | — | 0 real mismatches |
| Single-request p50 (SET/GET) | 0.19-0.23ms | 0.17-0.19ms |
| Pipelined SET | 277-295k req/s | 275-427k req/s (statistical tie) |
| Pipelined GET | 238-267k req/s | 274-370k req/s (modest edge) |
| CPU at matched throughput | ~79-91% of one core | ~52-58% of one core |

A same-OS, same-Docker-host comparison (an earlier native-vs-Docker run showed a much bigger
gap that was mostly a networking artifact, not a code difference). This workload is
mostly-unique-keys, low/moderate concurrency, no persistence — it doesn't stress the other
engine's actual strength (lock-free scaling under heavy hot-key contention across many cores),
which reflexdb hasn't built equivalent sharding for yet.

## AOF throughput cost (pipelined, native macOS)

| Config | SET req/s | GET req/s |
|---|---|---|
| No persistence | ~220k | ~206k |
| `appendonly yes`, `everysec` | ~258k | ~244k |
| `appendonly yes`, `always` | ~251k | ~221k |
