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
| `reflexdb-bench-ui` | Local web UI for running `benchmark tool` against a running server with live RPS/latency/RAM/disk charts. |

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
