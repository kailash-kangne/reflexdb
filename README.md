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
