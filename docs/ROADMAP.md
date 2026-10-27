# Roadmap — designed, not built

Nothing in this doc has a command handler in `crates/commands/src/dispatch.rs` today. If you
start implementing one of these, move its design into [FEATURES.md](FEATURES.md) and check it
off here.

## Time travel extensions

| Command | Meaning |
|---------|---------|
| `OBJECTAT <cmd> <key> [args] AT <ms-epoch>` | any read-only command evaluated as of a timestamp |
| `RESTOREAT <key> <ms-epoch>` | roll a key back to its state at a timestamp |
| `LIVE <cmd> <key> SINCE <ms-epoch>` | replay missed versions, then switch to normal `LIVE` push |
| `EXPIREACTION.SET/GET/UNSET <key>` | run a Lua script when a key expires (no webhook variant — SSRF risk) |

## MVCC snapshot reads

```
SNAPSHOT BEGIN   # pin a logical timestamp for this connection
GET user:5 / HGETALL cart:5 / ...
SNAPSHOT END
```

Would be a read-mode over the `GETAT`/`HISTORY` version index — reads between `BEGIN`/`END`
see a consistent instant regardless of concurrent writes, no locks taken.

## Dependency-tracked invalidation

```
SET page:home <html> DEPENDS ON user:5 post:12 post:13 [ONINVALIDATE LUA <sha>]
```

Writing `post:12` would auto-invalidate `page:home`. Would reuse the same change bus that
powers `LIVE`, routed to a server-side dependency graph instead of a client connection.

## AI-native caching

```
SIMSET bucket:qa <key> <value> VECTOR <f32,...> [EX <seconds>]
SIMGET bucket:qa VECTOR <f32,...> THRESHOLD 0.92 [LIMIT 1] [SIMFLIGHT <ms>]
```

Bounded (not general-vector-index) similarity cache for LLM/agent request caching — brute-force
cosine similarity over a capped, LRU-evicted bucket (`simcache-max-candidates-per-bucket`,
default 2000), not a sub-linear ANN index. `SIMFLIGHT` extends single-flight to "nobody has
asked this exact question, but someone asked something semantically close" — one model call
instead of N for near-duplicate concurrent requests.

## CRDT types + active-active replication

```
PNCOUNTER.INCRBY/GET <key>            # G-counter pair, handles negative deltas
ORSET.ADD/REM/MEMBERS <key>           # add-wins set
LWWREG.SET/GET <key>                  # last-write-wins register, (timestamp, node-id) tagged
```

Phase A (single-primary CRDT types) would be useful standalone. Phase B (active-active: more
than one primary accepting writes to the same CRDT keys, merged instead of last-writer-wins)
is a bigger architectural bet — needs a second replication mode alongside the existing
primary→replica one, and a cross-region clock/ordering scheme (proposed: Lamport counters, not
wall-clock NTP).

## Functions API

`FUNCTION LOAD`/`FCALL` — named, versioned, RDB-persisted functions (vs. `EVAL`'s anonymous
cache-only scripts). Deliberately deferred; see `crates/commands/src/script.rs`'s module docs.

## Other designed-but-cut ergonomics

- **Per-tenant quotas**: `ACL SETUSER tenant42 maxmemory 100mb maxkeys 10000` — would hook into
  the existing ACL user table and eviction path.
- **Key analytics**: `TOPKEYS BY reads/writes`, `KEYSTATS <key>`, `COLDKEYS OLDERTHAN <dur>` —
  first-class hot/cold visibility without configuring an eviction policy just to get it.
- **`EVICT [key...]`** — manual eviction via the same path as memory-pressure eviction.
- **`PSYNC` partial resync with backlog** — today `PSYNC` always does a full resync; a backlog
  (`repl-backlog-size`) + replica offset tracking would allow replaying just the delta.
- **Diskless replication, chain replication, `min-replicas-to-write`/`-max-lag`** write gating.
- **HTTP/WebSocket gateway** alongside the RESP TCP port.

## Open architectural decisions

**Is the version index (`GETAT`/`HISTORY`) the keyspace's optional side-car, or should "current
value" become a query (`at t=now`) over an append-only per-key log?** The log model is strictly
more powerful (KV/TTL/changefeed/event-stream/audit-trail/MVCC all collapse into "read the log
at different points"), but is slower for pure overwrite-heavy workloads. This needs deciding
before a replication protocol is built on top of either model, since replicating a mutable
keyspace and replicating a log are different protocols. Current plan: keep it opt-in
(`history-enabled`) until this is explicitly revisited.

**Positioning**: the likely best-fit customer is agent state (hot, short-lived, high-churn,
needs undo/audit/live-watch — exactly `RESTOREAT`/`HISTORY`/`LIVE`), not general web-app
caching. Unvalidated — no conversations with agent-framework builders have happened yet.

**Distribution wedge**: `reflexdb-proxy` (pass-through today) could grow into a thin RESP proxy
in front of an existing server, adding `LIVE`/single-flight/dependency-invalidation without a
data migration. Lowest priority of the three — a go-to-market move for after the storage-owning
features above exist, not a replacement for them.

## Permanently out of scope

See [ARCHITECTURE.md](ARCHITECTURE.md#whats-intentionally-not-built) — cluster mode, full Geo/
HyperLogLog/JSON/Bloom command parity, `.so` module hosting. These aren't "future roadmap," they
won't be built.
