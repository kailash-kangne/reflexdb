# Features — what reflexdb has today

Implements a subset of the Redis command protocol over RESP2/RESP3. Client compatibility depends
on the commands used. Two features of its own: **`LIVE` query subscriptions** and **tiered storage**.
See [ROADMAP.md](ROADMAP.md) for what's designed but not built yet, and
[COMMANDS.md](COMMANDS.md) for the full command list.

## Data types

| Type | Notes |
|------|-------|
| String (incl. Bitmap) | `int`/`raw` representation; `INCR` is O(1) |
| List | `VecDeque<Bytes>` |
| Hash | `HashMap<Bytes, Bytes>` |
| Set | `IndexSet<Bytes>` — insertion-ordered, enables O(1) `SPOP` via `swap_remove_index` |
| Sorted Set (ZSET) | `HashMap<Bytes, f64>` + a `BTreeSet<(score, member)>` index — O(log n) `ZPOPMIN`/`ZPOPMAX`, O(n) range scans |
| Stream | `XADD`/`XLEN`/`XRANGE`/`XREVRANGE`/`XDEL`/`XTRIM`/`XGROUP`/`XREADGROUP`/`XACK`/`XPENDING`/`XREAD`. Not implemented: `XCLAIM`/`XAUTOCLAIM`, `XINFO`, `BLOCK`ing reads, `MINID` trimming, exclusive range bounds |

No small-collection compact encodings (listpack/intset/ziplist) — every collection is always
its full representation. Geo and HyperLogLog are not implemented (see
[ROADMAP.md](ROADMAP.md) for scope decisions).

## TTL & eviction

- Passive (checked on read) + active (background sweep every 100ms) expiry.
- Sliding TTL: `EXPIRE key seconds SLIDING` resets the countdown on every read, not just
  `TOUCH`/`GETEX`. Config: `sliding-ttl-min-ms` (floor to avoid rewriting TTL on every hot-key
  read).
- Eviction policies under `maxmemory`: `noeviction` (default), `allkeys-lru/lfu/random`,
  `volatile-lru/lfu/random/ttl`. LFU is a plain access counter, not a decayed approximation.
- `SCHEDULE AT/IN <command>`, `SCHEDULE LIST`, `SCHEDULE CANCEL` — delayed command execution,
  in-memory only (`scheduled-commands-max`, default 100000). Doesn't survive a restart yet.

## Reactivity — `LIVE` / `UNLIVE`

Subscribe to the result of any read-only command; get a fresh result pushed every time the
underlying key changes.

```
> LIVE GET user:name
1) "live"  2) "fingerprint"  3) "<hex crc64>"  4) "result"  5) (nil)
# ... later, after `SET user:name Alice` from another connection ...
1) "live"  2) "fingerprint"  3) "<same hex>"   4) "result"  5) "Alice"
```

- Fingerprint = CRC64 of `<command> <args>`, used to dedupe subscriptions and for `UNLIVE`.
- RESP2 connections in `LIVE` mode are restricted to `LIVE`/`UNLIVE`/`PING`/`QUIT`/`RESET`;
  RESP3 allows mixing live subscriptions with normal commands.
- Live clients never time out. `live-debounce-period-ms` (default `0`) batches rapid changes
  to one push per period.
- Implementation: one broadcast bus per shard; every write posts a `KeyChange` event, `LIVE`
  subscribers re-run their command and push the new result.

## Persistence

| Mode | Config | Use case |
|------|--------|----------|
| Cache only | `save ""`, `appendonly no` | pure in-memory |
| Snapshot only | `save 900 1 ...`, `appendonly no` | restartable, tolerates small loss |
| AOF only | `save ""`, `appendonly yes` | max durability |
| Hybrid (default) | both | fast restart + low loss |

- **RDB**: `SAVE` (sync) / `BGSAVE` (keyspace walk on the caller's thread, disk write+rename on
  a background thread — no `fork()`). `DUMP`/`RESTORE` for single-key dump/load.
- **AOF**: per-command log, replayed at startup. `appendfsync always/everysec/no`. Writes are
  fire-and-forget to a dedicated writer thread that batches everything queued into one
  `write`/`fsync` ("group commit") — a crash can lose the last few acked commands under
  `everysec`; `WAITAOF` is the call that explicitly
  blocks for a real fsync. `BGREWRITEAOF` compacts to one command per live key (manual trigger
  only — no automatic rewrite-on-growth yet).

## Tiered storage

Evicted keys are written to an on-disk LSM store (`fjall`, pure Rust) instead of being dropped,
and promoted back to RAM on the next read — so the working set can exceed the memory budget.

| Config | Default | Meaning |
|--------|---------|---------|
| `tiered-storage` | `no` | enable the disk tier |
| `tiered-memory-limit` | 250MB | RAM budget before tiering kicks in |
| `tiered-dir` | `./tiered` | where LSM files land |
| `tiered-promotion` | `on-access` | when to pull a key back to RAM |
| `tiered-disk-limit` | unset (unbounded) | once the disk tier itself exceeds this, the oldest-evicted entries are dropped outright, oldest-first, down to 50% of the limit (real data loss — there's nowhere left to move them) |
| `eviction-tick-ms` | 100 | how often the background task re-checks `maxmemory`/`tiered-memory-limit`/`tiered-disk-limit` and runs TTL active-expire — a background task, not the request path, so lowering it reacts to write bursts faster for free (small extra idle-tick CPU, not request latency) |

A tiered key and its RAM copy never coexist (promotion is a move, not a copy). `EXISTS` answers
truthfully for tiered keys without a disk read. Tier I/O is synchronous (not off-loaded to a
blocking pool) — a cold read can tie up a worker thread briefly. TTLs on sliding-mode keys
don't survive a tier round-trip.

## Replication & HA

- `REPLICAOF <host> <port>` / `REPLICAOF NO ONE`; `replica-read-only yes` by default.
- `PSYNC` always does a **full resync** (serializes an RDB snapshot, streams it, then streams
  live writes) — no partial resync/backlog yet.
- `WAIT`/`WAITAOF` — write-ack sync points, offset-compare based.
- `reflexdb-sentinel` binary: monitors primaries, votes with peer Sentinels on failure, promotes
  a replica and re-points the others. Replicas/peers are listed explicitly in
  `sentinel.conf` (no auto-discovery yet); failover always picks "the first configured replica".
- No cluster mode (hash-slot sharding) — not planned, see [ROADMAP.md](ROADMAP.md).
- Not implemented: partial resync, diskless replication, chain replication,
  `min-replicas-to-write`/`-max-lag` write gating, sharded pub/sub (`SPUBLISH`/`SSUBSCRIBE` —
  tied to cluster mode).

## Security

- `AUTH` (`requirepass`) and ACL: users, on/off, passwords (plaintext/hashed), command
  allow/deny (`+cmd`/`-cmd`/`allcommands`/`nocommands`), key-glob patterns (`~pattern`).
- Accepted but not enforced yet: ACL command categories (`+@read`/`+@write`), read/write-scoped
  key patterns (`%R~`/`%W~`).
- Not implemented: `ACL LOG`/`LOAD`/`SAVE`/`DRYRUN`, `&pattern` pub/sub ACLs, `aclfile`.
- TLS (`rustls`) on client and replication links, mutual TLS optional.
- `protected-mode`, `rename-command`, `sanitize-dump-payload`.

## Scripting

- Lua 5.1 (via `mlua`), sandboxed — `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`,
  `SCRIPT LOAD/EXISTS/FLUSH/KILL`. `os`/`io` removed from the sandbox; `math.random` seeded
  from the script's own source for determinism.
- Extension points are compiled-in Rust traits, not `.so` modules.
- Functions API (`FUNCTION LOAD`/`FCALL`) is not implemented — see [ROADMAP.md](ROADMAP.md).

## Time travel — `GETAT` / `HISTORY`

Proof-of-concept, gated behind `history-enabled yes` (default `no`), strings only.

```
GETAT <key> <ms-epoch>   # value of a string key as of a timestamp
HISTORY <key> [LIMIT n]  # versions: (timestamp, command, summary), newest-first
```

Versions are written to a disk-backed side-car store (`fjall`, same engine as the tier), not
reconstructed from the AOF, and not replicated. Recording a version costs one channel push on
the write path (fire-and-forget to a batching writer thread). Retention: `history-retention-ms`
(default 24h), reclaimed by a periodic sweep. `OBJECTAT`, `RESTOREAT`, `LIVE ... SINCE`, and
`EXPIREACTION` are designed but not built — see [ROADMAP.md](ROADMAP.md).

## Cache correctness — single-flight

`GET key SINGLEFLIGHT <ms>` — stampede protection. First caller on a miss gets `nil` plus
becomes the leaseholder; every other caller in that window blocks server-side until the
leaseholder calls `SET` (wakes everyone with the fresh value) or the lease expires (next waiter
becomes leaseholder). Config: `singleflight-max-wait-ms` (default `10000`).

MVCC snapshot reads (`SNAPSHOT BEGIN`/`END`) and dependency-tracked invalidation
(`DEPENDS ON`) are designed but not built — see [ROADMAP.md](ROADMAP.md).

## Operational

- `INFO` sectioned metrics, `SLOWLOG`, `LATENCY` histograms, `MEMORY USAGE` (hand-counted, not
  allocator-reported).
- `CONFIG GET/SET/REWRITE`.
- Multi-threaded: one Tokio task per connection, sharded keyspace locking (see
  [ARCHITECTURE.md](ARCHITECTURE.md)).

## Binaries

| Binary | Purpose |
|--------|---------|
| `reflexdb-server` | The database. |
| `reflexdb-cli` | Interactive client. |
| `reflexdb-sentinel` | Failover watcher for a monitored primary/replica group. |
| `reflexdb-proxy` | Dumb `--listen`/`--upstream` byte-splice pass-through in front of an existing RESP-compatible server — no RESP parsing, no feature on its own, foundation for a future proxy-side `LIVE`/single-flight layer. |
| `reflexdb-bench-ui` | Local web UI (`BENCH_UI_BIND`, default `127.0.0.1:8080`) that drives command benchmarks against a running server and charts RPS/latency/RAM/disk live, one card per command. Its Latency Lab exposes editable scenarios for mixed small keys, hot-key contention, pipeline depth, repeated runs, and `LIVE` fan-out with p50/p99/p999 charts. Separate tabs watch `LIVE`/`UNLIVE` updates and demo tiered storage eviction/promotion. |
