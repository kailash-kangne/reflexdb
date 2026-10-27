# reflexdb

A **Redis-protocol-compatible** in-memory database written from scratch in **Rust**, with two
first-class features of our own:

1. **Live query subscriptions (`LIVE` / `UNLIVE`)** — subscribe to the *result* of any read-only
   command and get push updates whenever the underlying keys change.
2. **Tiered storage** — evicted keys flow transparently to an on-disk LSM tier and are
   transparently restored on read, so the working set can be larger than RAM.

On top of that it speaks RESP2/RESP3, so any existing Redis client, SDK, or CLI works out of
the box.

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

By default it listens on `127.0.0.1:7379`. Talk to it with `redis-cli` or any Redis client:

```sh
redis-cli -p 7379 set foo bar
redis-cli -p 7379 get foo
```

Or use the bundled CLI instead of `redis-cli`:

```sh
cargo run --release --bin reflexdb-cli -- -p 7379
```

Try the two features that aren't in Redis:

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

## Who this might be for (unproven — read this as a hypothesis, not a claim)

AI agent memory is hot, short-lived, and high-churn — the shape reflexdb is already built for.
Three things that shape needs and most teams currently bolt on with an app-level
checkpointer instead:

- **Undo a bad step** — roll state back to what it was a moment ago, not just delete the key.
- **Explain itself** — a version history of memory, not external audit tooling bolted on after.
- **Be watched live** — push updates on state changes, not polling.

`LIVE`/`UNLIVE` (above) covers "watched live." `GETAT`/`HISTORY` (point-in-time reads and
per-key version logs, off by default behind `history-enabled yes` — see
[docs/FEATURES.md](docs/FEATURES.md)) are a proof-of-concept of "undo"/"explain itself." This
positioning is unvalidated — see [docs/ROADMAP.md](docs/ROADMAP.md).

## Binaries

| Binary | Purpose |
|--------|---------|
| `reflexdb-server` | The database itself. |
| `reflexdb-proxy` | A pass-through relay (`--listen`/`--upstream`) that sits in front of an *existing* RESP-compatible server — pure byte splice, no RESP parsing, zero data migration required to try. Doesn't add any feature on its own yet — it's the foundation a future `LIVE`/single-flight-at-the-proxy layer would sit on top of. |
| `reflexdb-sentinel` | Automatic failover watcher for a monitored primary/replica group. |
| `reflexdb-cli` | Interactive CLI client. |
| `reflexdb-bench-ui` | Local web UI for running `redis-benchmark` against a running server with live RPS/latency/RAM/disk charts. |

## Documentation Index

| File | Contents |
|------|----------|
| [docs/FEATURES.md](docs/FEATURES.md) | Everything reflexdb has today: data types, persistence, replication, security, scripting, `LIVE`, time travel, single-flight |
| [docs/COMMANDS.md](docs/COMMANDS.md) | Full command reference, grouped, with what's cut/planned vs. implemented |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Crate layout, concurrency model, internals, dependency choices |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Designed-but-not-built features, and the open architectural decisions |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | Measured throughput, the `ZPOPMIN` perf fix, and comparisons against other engines |

## Compatibility

reflexdb targets wire compatibility with the modern Redis command set, so any standard
Redis client, SDK, or CLI works against it out of the box. We write our own implementation
from scratch.
