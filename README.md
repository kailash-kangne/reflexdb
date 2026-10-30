# reflexdb

A database that speaks RESP and explores recoverable live state for long-running
workflows, written in **Rust**. The current prototype combines live query updates
(`LIVE` / `UNLIVE`), optional per-key string history (`GETAT` / `HISTORY`), and tiered storage.

The product hypothesis is that developers need to inspect and recover workflow state after a
bad step, while still using a fast shared store. This is a hypothesis, not a validated market
position. ReflexDB has not established a speed, memory, or cost advantage across representative
production workloads. See
[the product direction](docs/PRODUCT_DIRECTION.md) and [benchmark notes](docs/BENCHMARKS.md).

RESP2/RESP3 compatibility is intended to make experiments easier; command and behavior coverage
is limited and should be checked before use.

Everything is original Rust code.

## Demo

[▶ Watch the demo recording](docs/assets/demo.mov)

## Quick start

Build and run the server (requires the Rust toolchain — [rustup.rs](https://rustup.rs)):

```sh
git clone <this-repo>
cd reflexdb
cargo build --release
./target/release/reflexdb-server reflexdb.conf
```

By default it listens on `127.0.0.1:7379`. Try supported commands with `redis-cli` or another RESP client:

```sh
redis-cli -p 7379 set foo bar
redis-cli -p 7379 get foo
```

Or use the bundled CLI instead of `redis-cli`:

```sh
cargo run --release --bin reflexdb-cli -- -p 7379
```

Try the live query prototype:

```sh
# LIVE: subscribe to a command's result, get pushed the new result on every change
> LIVE GET foo
1) "live"  2) "fingerprint"  3) "<hex crc64>"  4) "result"  5) "bar"
# (from another connection) SET foo baz  →  pushes an updated result automatically

# Tiered storage: evicted keys spill to disk instead of being dropped, and
# come back transparently on the next read. Turn it on in reflexdb.conf:
#   tiered-storage yes
#   tiered-memory-limit 250mb
```

See `reflexdb.conf` in the repo root for all config directives (passwords, persistence,
replication, TLS, etc.), and [docs/COMMANDS.md](docs/COMMANDS.md) for the full command list.

Run the test suite with `cargo test`, or build the production Docker image with
`docker build -t reflexdb .`.

To try the server together with the visual benchmark UI in one go:

```sh
cargo build --release --bin reflexdb-server --bin reflexdb-bench-ui && \
./target/release/reflexdb-server reflexdb.conf & BENCH_UI_BIND=127.0.0.1:8080 ./target/release/reflexdb-bench-ui
```

Then open `http://127.0.0.1:8080` and pick a workload tab.

## What's new

- **Engine shootout benchmark** — a tab in `reflexdb-bench-ui` that runs Redis, Dragonfly, and
  ReflexDB side by side in Docker, each under the same `--memory`/`--cpus` cap, and charts
  throughput/tail latency per workload so engines are compared directly, bar-for-bar, instead of
  across separate reports. See [the real numbers below](#one-real-comparison-so-far).
- **Resource-limit (OOM/disk-full) test** — runs `reflexdb-server` in Docker with a real
  OS-enforced memory cap and a size-capped disk mount at the same time, and reports whether/why it
  stops responding, rather than relying on `maxmemory`'s own graceful degradation.
- **Durability / crash-recovery test** — force-kills a running server (`SIGKILL`) under write load
  per `appendfsync` policy and compares acknowledged writes to what AOF replay actually recovered.
- **Tiered storage demo** and **LIVE demo** tabs in the same UI, for watching eviction-to-disk and
  push updates happen in real time against a running server.
- `appendfsync` now defaults explicitly to `no` (fastest, OS decides when to flush) instead of
  silently behaving like `everysec` — set it explicitly in `reflexdb.conf` if you need a bounded
  worst case (see the comments in `reflexdb.conf`).

### One real comparison so far

We ran Redis, Dragonfly, and ReflexDB side by side in Docker, same machine, same memory/CPU cap
per engine, once with 1 CPU and once with 4 CPUs. Short summary:

| Scenario | Who was fastest | By how much |
|---|---|---|
| Single core | ReflexDB (tied with Redis) | ReflexDB vs. Redis: within noise. ReflexDB vs. Dragonfly: ~30–55% faster |
| 4 cores | ReflexDB | ReflexDB vs. Redis: ~1–8% faster. ReflexDB vs. Dragonfly: ~10–40% faster |

ReflexDB came out ahead in almost every workload we tried (11 of 12), with Redis close behind and
Dragonfly further back, though Dragonfly closed some of that gap once it had more cores to use.
This is from one run of each scenario on one machine, not a guarantee you'll see the same numbers —
see [the full results, setup, and caveats in docs/BENCHMARKS.md](docs/BENCHMARKS.md#engine-shootout-reflexdb-vs-redis-vs-dragonfly)
before citing it anywhere else.

## Vision

ReflexDB's bet: most teams running long-lived or multi-step workflows (agents, pipelines, job
queues) eventually hit the same wall — a step fails or produces bad state, and nothing in their
fast shared store tells them what changed, which step changed it, or how to get back to a
known-good point. Today that means bolting on separate logging, snapshotting, or event-sourcing
infrastructure next to whatever cache/store they already run. The idea is one RESP-compatible store
that is fast enough to use as the primary hot path *and* keeps enough history/rewind machinery
(`LIVE`, `GETAT`/`HISTORY`, tiered storage) to answer "what changed and how do I recover" without a
second system. This is a hypothesis, not a finished product: see
[docs/PRODUCT_DIRECTION.md](docs/PRODUCT_DIRECTION.md) for what's validated, what's still a guess,
and what we're explicitly not building yet.

## Product direction (hypothesis)

The first customer problem to test is recovering and inspecting state in long-running workflows:
what changed, which step changed it, and how to resume from a known-good point. The current
prototype does not provide a complete workflow checkpoint or rollback guarantee. `LIVE` and
`GETAT`/`HISTORY` are experiments that may help prove the idea, not finished promises. See
[docs/PRODUCT_DIRECTION.md](docs/PRODUCT_DIRECTION.md) for the validation plan and scope.

## Binaries

| Binary | Purpose |
|--------|---------|
| `reflexdb-server` | The database itself. |
| `reflexdb-proxy` | A pass-through relay (`--listen`/`--upstream`) that sits in front of an *existing* RESP-compatible server — pure byte splice, no RESP parsing, zero data migration required to try. Doesn't add any feature on its own yet — it's the foundation a future `LIVE`/single-flight-at-the-proxy layer would sit on top of. |
| `reflexdb-sentinel` | Automatic failover watcher for a monitored primary/replica group. |
| `reflexdb-cli` | Interactive CLI client. |
| `reflexdb-bench-ui` | Local web UI for running and comparing workloads against a running server, with editable pipeline/concurrency/fan-out settings and p50/p99/p999 latency charts. |

## Documentation Index

| File | Contents |
|------|----------|
| [docs/FEATURES.md](docs/FEATURES.md) | Everything reflexdb has today: data types, persistence, replication, security, scripting, `LIVE`, time travel, single-flight |
| [docs/COMMANDS.md](docs/COMMANDS.md) | Full command reference, grouped, with what's cut/planned vs. implemented |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Crate layout, concurrency model, internals, dependency choices |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Focused sequence of product and engineering decisions |
| [docs/PRODUCT_DIRECTION.md](docs/PRODUCT_DIRECTION.md) | Customer hypothesis, validation plan, and proposed scope |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | Development measurements and reproducibility notes |

## Compatibility

ReflexDB implements a subset of the Redis command protocol over RESP. Compatibility varies by
command and behavior; check [docs/COMMANDS.md](docs/COMMANDS.md) before connecting an application.
We write our own implementation from scratch.
