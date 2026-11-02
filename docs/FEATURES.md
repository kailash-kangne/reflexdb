# Features — what reflexdb has today

Implements a subset of the Redis command protocol over RESP2/RESP3, plus an optional binary
wire format, **RFX** ([docs/RFX.md](RFX.md)), auto-detected per connection. Client compatibility
depends
on the commands used. Two features of its own: **`LIVE` query subscriptions** and **tiered storage**.
See [ROADMAP.md](ROADMAP.md) for what's designed but not built yet, and
[COMMANDS.md](COMMANDS.md) for the full command list.

## Data types

| Type | Notes |
|------|-------|
| String (incl. Bitmap) | `int`/`raw` representation; `INCR` is O(1) |
| List | packed `Vec<Bytes>` under 128 elements, `VecDeque<Bytes>` beyond (one-way promotion) |
| Hash | packed `Vec<(Bytes, Bytes)>` under 128 fields, `IndexMap` beyond |
| Set | packed `Vec<Bytes>` under 128 members, `IndexSet` beyond — insertion-ordered, enables O(1) `SPOP` via `swap_remove_index` |
| Sorted Set (ZSET) | `HashMap<Bytes, f64>` + a `BTreeSet<(score, member)>` index — O(log n) `ZPOPMIN`/`ZPOPMAX`, O(n) range scans |
| Stream | `XADD`/`XLEN`/`XRANGE`/`XREVRANGE`/`XDEL`/`XTRIM`/`XGROUP`/`XREADGROUP`/`XACK`/`XPENDING`/`XREAD`/`XCLAIM`/`XAUTOCLAIM`. Not implemented: `XINFO`, `BLOCK`ing reads, `MINID` trimming, exclusive range bounds |

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
- `SCHEDULE AT/IN <command>`, `SCHEDULE LIST`, `SCHEDULE CANCEL` — delayed command execution.
  Registration writes a `~sched:<id>` marker key, so tasks persist via AOF/RDB, replicate, and
  re-arm on restart and on replica promotion (cap: `scheduled-commands-max`, default 100000).
  Markers are user-visible bookkeeping keys — a manual `DEL ~sched:<id>` cancels the task.
  Firing deletes the marker *after* the command runs: a crash in between re-fires it once —
  at-least-once, never silently lost.

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
- Writes enqueue deduplicated wake-ups; a dedicated `live-fanout` thread re-runs the matching
  read-only commands, caches the latest result, pre-encodes the push per wire format, and writes
  it straight to each subscriber's socket via `try_lock` (falling back to the client's bounded
  push queue when contended). A connection that falls behind can skip intermediate states.
  Pub/sub and tracking use a separate 1,024-item outbound queue; overflow marks that client for
  disconnect after 25 ms. Fan-out pacing is tunable: `live-sweep-ms` (default 10 — an idle gap
  where writes dedup into one re-evaluation; `0` pushes every batch immediately) and
  `live-pace-chunk`/`live-pace-ms` (defaults 64/1 — socket-write burst size and yield, `0`
  disables pacing) trade subscriber freshness against fan-out CPU under write storms.

## Persistence

Three operating profiles via `profile <name>` — a preset bundle so a deployment's durability
posture is one word, not a handful of knobs. Directives listed *after* `profile` in the config
file override its individual values:

| Profile | Sets | Guarantee |
|---------|------|-----------|
| `cache` | `appendonly no` | pure in-memory — **total data loss on restart** (an existing `dbfilename` snapshot still loads) |
| `durable` | `appendonly yes` + `appendfsync always` | every acknowledged write is fsync'd before the client sees it — survives `kill -9` with zero loss; OS/hardware crash can still lose it |
| `replicated` | `appendonly yes` + `appendfsync everysec` + `min-replicas-to-write 1` | writes are refused (`NOREPLICAS`) unless at least one healthy replica is attached; `WAIT`/`WAITAOF` give per-write acks; failover keeps data the replica already applied |

Raw knobs for non-profile setups:

| Mode | Config | Use case |
|------|--------|----------|
| Snapshot only | `appendonly no` + explicit `SAVE`/`BGSAVE` (no periodic `save` directive — verified 2026-11-01) | restartable, tolerates total loss since last snapshot |
| AOF only | `appendonly yes` | max durability |
| Hybrid | `appendonly yes` + periodic `SAVE`/`BGSAVE` | fast restart + low loss |

- `min-replicas-to-write <n>` works standalone too: write commands fail while fewer than `n`
  connected replicas are healthy (not flagged slow). The replication-apply path and AOF replay
  are exempt — a replica must keep applying its primary's stream regardless.

- **RDB**: `SAVE` is synchronous to the client but runs on Tokio's blocking pool; `BGSAVE`
  serializes on that pool and writes/renames on a background OS thread — no `fork()`. New
  snapshots are zstd-compressed; uncompressed format-v1 snapshots remain readable.
  `DUMP`/`RESTORE` handle single-key dump/load.
- **AOF**: per-command log, replayed at startup. `appendfsync always/everysec/no`. Writes are
  fire-and-forget to a dedicated writer thread that batches everything queued into one
  write/fsync ("group commit"). New AOF files use compressed batches; legacy RESP-only files
  remain readable and are upgraded by `BGREWRITEAOF`. A crash can lose the last few acked
  commands under `everysec`; `WAITAOF` is the call that explicitly blocks for a real fsync.
  `BGREWRITEAOF` compacts to one command per live key; it also fires automatically once the
  AOF exceeds `auto-aof-rewrite-min-size` and has grown by `auto-aof-rewrite-percentage`
  since the last rewrite (defaults 64mb / 100%).
- **AOF failure handling**: because `append` is fire-and-forget, a batch the writer thread
  fails to write (disk full, I/O error) has already been acked to its client. Once that
  happens all subsequent writes are refused with `MISCONF Errors writing to the AOF file`
  (same contract Redis uses) until a batch lands cleanly — acknowledged-but-unpersisted
  writes never silently pile up.
- **Graceful shutdown**: `SIGTERM`/`SIGINT` and `SHUTDOWN` run the same ritual — block
  until every queued AOF write is fsync'd, then take a final RDB snapshot only when AOF
  is off and a dump path is configured (`SHUTDOWN SAVE`/`NOSAVE` overrides either way).
  `kill -9` gets no ritual; that's what `appendfsync`/`profile` is for.

## Tiered storage

Evicted keys are written to an on-disk LSM store (`fjall`, pure Rust) instead of being dropped,
and promoted back to RAM on the next read — so the working set can exceed the memory budget.

| Config | Default | Meaning |
|--------|---------|---------|
| `tiered-storage` | `no` | enable the disk tier |
| `tiered-memory-limit` | 250MB | RAM budget before tiering kicks in |
| `tiered-dir` | `./tiered` | where LSM files land |
| `tiered-disk-limit` | unset (unbounded) | disk-tier capacity; what "full" does is `tiered-overflow-policy`'s choice |
| `tiered-overflow-policy` | `reject-writes` | `reject-writes` — once RAM is over `tiered-memory-limit` *and* the disk tier is at `tiered-disk-limit`, new-key writes fail with `ERR TIERED_STORAGE_FULL` (deletes/updates of existing keys still work — every acknowledged byte is preserved). `evict-oldest` — drops oldest tiered entries down to ~50% of the limit (real data loss, cache semantics). `no-limit` — no enforcement even with a limit set. Only consulted when `tiered-disk-limit` is set |
| `eviction-tick-ms` | 100 | how often the background task re-checks `maxmemory`/`tiered-memory-limit`/`tiered-disk-limit` and runs TTL active-expire — a background task, not the request path, so lowering it reacts to write bursts faster for free (small extra idle-tick CPU, not request latency) |

A tiered key and its RAM copy never coexist (promotion is a move, not a copy). `EXISTS` answers
truthfully for tiered keys without a disk read. RAM hits avoid a blocking-pool handoff; actual
Fjall I/O uses Tokio's `block_in_place` on its multi-thread runtime. Tier spill ordering follows
the configured maxmemory policy, including LRU/LFU ranking. TTLs on sliding-mode keys don't
survive a tier round-trip. If the disk tier is full when a key would spill, the key stays in RAM
(spill failure is counted, not silently dropped).

A ~60-second background task does tier housekeeping off the latency path: GCs entries that
expired while untouched, then `major_compact`s the LSM once ≥10K tombstones accumulated or an
hour passed — deletes don't reclaim file space until compaction. `INFO` reports
`tiered_evictions`, `tiered_disk_drops`, `tiered_rejected_writes`, `tiered_spill_failures`,
`tiered_expired_dropped`, `tiered_entries_removed`, `tiered_compactions`, `tiered_compacting`,
`tiered_disk_used_bytes`, and the active `tiered_overflow_policy`.

## Replication & HA

- `REPLICAOF <host> <port>` / `REPLICAOF NO ONE`; `replica-read-only yes` by default.
  `masterauth` sets the AUTH a replica sends to a `requirepass` primary on connect.
- `PSYNC <replid> <offset>` does a **partial resync** when the offset is still covered by the
  primary's resync backlog (`partial-resync-backlog`, default `1mb`, `0` disables): the replica
  gets `+CONTINUE` plus just the commands it missed. Anything else — unknown replid, evicted or
  future offset — falls back to `FULLRESYNC` (RDB snapshot, then live writes). A replica
  promoted with `REPLICAOF NO ONE` always full-resyncs on re-attach. The offset is a logical
  command count, not Redis's byte-stream offset. `INFO replication` reports `sync_full` /
  `sync_partial_ok`.
- Replication writes use a dedicated bounded per-replica queue (32,768 messages / 64 MiB); the
  pub/sub slow-client timeout does not apply. Queue overflow means the replica has missed writes
  and it is disconnected after a five-second grace period.
- `WAIT`/`WAITAOF` — write-ack sync points, offset-compare based.
  `min-replicas-to-write` refuses writes (NOREPLICAS) while fewer than N healthy replicas
  are connected.
- `reflexdb-sentinel` binary: monitors primaries, votes with peer Sentinels on failure, promotes
  a replica and re-points the others. Replicas/peers are listed explicitly in
  `sentinel.conf` (no auto-discovery yet); failover always picks "the first configured replica".
- No cluster mode (hash-slot sharding) — not planned, see [ROADMAP.md](ROADMAP.md).
- Not implemented: diskless replication, chain replication, `-max-lag` write gating,
  sharded pub/sub (`SPUBLISH`/`SSUBSCRIBE` — tied to cluster mode).

## Security

- `AUTH` (`requirepass`) and ACL: users, on/off, passwords (plaintext/hashed), command
  allow/deny (`+cmd`/`-cmd`/`allcommands`/`nocommands`), command categories
  (`+@read`/`-@write`/...), key-glob patterns (`~pattern`) and read/write-scoped key
  patterns (`%R~`/`%W~`). Rule precedence: explicit `+cmd`/`-cmd` over category rules over
  `allcommands`/`nocommands` (Redis applies tokens left-to-right instead).
- Pub/sub channel globs are enforced too: `&pattern`/`allchannels`/`resetchannels` gate every
  channel operand of `(P)SUBSCRIBE`/`PUBLISH`. Rejected rather than silently accepted:
  `(...)` selectors and unknown `+@category` names — rules that can't be enforced fail
  `SETUSER` loudly. Denied commands and failed AUTHs are logged at `warn` level.
- `aclfile <path>` persists users in Redis's aclfile format (`user <name> <rules>` per line):
  loaded at startup (a missing file warns and continues with in-memory users; a malformed file
  refuses to start), `ACL SAVE` writes the live table back atomically (0600), `ACL LOAD` swaps
  the table from disk. A file without `user default` loads a disabled `default` — never fails
  open. Without `aclfile`, users are in-memory only. Not implemented: `ACL LOG`/`DRYRUN`.
  One format caveat: `ACL SAVE` writes salted password entries as `!s$…` tokens that stock
  Redis won't parse (`#sha256` lines still load fine here).
- TLS (`rustls`) on the client listener. Set `port 0` for TLS-only operation — plaintext is
  then never bound. `tls-auth-clients yes` + `tls-ca-cert-file` enable mTLS (clients must
  present a CA-signed certificate). `tls-replication yes` wraps the replica→primary link in
  TLS — the replica verifies the primary against `tls-ca-cert-file` and presents its own
  `tls-cert-file`/`tls-key-file` as a client cert. Both directives fail startup loudly if
  `tls-ca-cert-file` is missing.

## Scripting

- Lua 5.1 (via `mlua`), sandboxed — `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`,
  `SCRIPT LOAD/EXISTS/FLUSH/KILL`. `os`/`io` removed from the sandbox; `math.random` seeded
  from the script's own source for determinism.
- Extension points are compiled-in Rust traits, not `.so` modules.
- Functions API (`FUNCTION LOAD`/`FCALL`) is not implemented — see [ROADMAP.md](ROADMAP.md).

## Time travel — `GETAT` / `HISTORY`

Proof-of-concept, gated behind `history-enabled yes` (default `yes`), strings only.

```
GETAT <key> <ms-epoch>   # value of a string key as of a timestamp
HISTORY <key> [LIMIT n]  # versions: (timestamp, command, summary), newest-first
```

Versions are written to a disk-backed side-car store (`fjall`, same engine as the tier), not
reconstructed from the AOF. Replicas with `history-enabled yes` record their own versions for
every write they apply (timestamps are replica-local; history predating the attach doesn't
transfer, and an RDB/full-resync load records nothing). Recording a version costs one channel push on
the write path (fire-and-forget to a batching writer thread). `history-retention-ms` defaults to
24h; `0` disables age-based expiry. `history-max-versions-per-key` defaults to 200 (range
1–1,000,000); the writer trims older versions and `GETAT`/`HISTORY` observe only the capped
window. A per-key cap does not limit the number of distinct keys, so total disk use can still grow
when age retention is disabled.

**Crash guarantee** — weaker than the AOF, deliberately: `record()` is fire-and-forget, so a
crash can lose up to ~one in-flight batch of the *newest* versions (already-committed versions
are never lost). `GETAT`/`HISTORY` may therefore miss the last ~milliseconds of history after a
crash; older versions are complete. A failed batch commit sets `history_io_failed` in
`INFO persistence` and the `metrics-port` exposition — history loss never gates keyspace writes.
`OBJECTAT`, `RESTOREAT`, `LIVE ... SINCE`, and `EXPIREACTION` are
designed but not built — see [ROADMAP.md](ROADMAP.md).

## Workflow recovery — `RUN.*` / `IDEMPOTENT`

Recovery primitives for workflow runners — not a workflow engine. All bookkeeping is ordinary
keyspace data (`run:<id>` hash, `run:<id>~audit` list, `run:<id>~cp~<label>` checkpoint blobs,
`run:<id>:<...>` for state covered by checkpoints), so it persists, replicates, and is
inspectable with regular commands.

```
RUN BEGIN <id>                    # register a run (id must be free)
RUN STEP <id> <name>              # record the resume point
RUN CHECKPOINT <id> <label>       # snapshot all `run:<id>:*` keys → count
RUN RESTORE <id> <label>          # wipe `run:<id>:*`, restore snapshot; resumes a closed run
RUN END <id> [DONE|FAILED]        # close (default DONE)
RUN STATUS <id>                   # status, timestamps, last_step, checkpoint labels
RUN LIST                          # [[id, status], ...]
RUN AUDIT <id> [LIMIT n]          # append-only "<ms> <event>" trail
IDEMPOTENT <key> <ttl_ms> <cmd> <args...>
```

`IDEMPOTENT` claims `key` atomically, runs `cmd`, stores its reply under `key`, and answers
retries with the stored reply — no re-execution. Errors release the claim instead of caching.
Connection-state commands, blockers, and `RUN`/`IDEMPOTENT` itself can't be wrapped.
The durable record is a single internal `IDKAPPLY` command packing the inner command's resolved
effects plus the stored reply — effects and marker land on disk and on replicas together or not
at all, so a crash (or failover) mid-execution can't leave an applied-but-unmarked run that a
retry would double-execute: exactly-once, not just at-least-once. `IDKAPPLY` itself only exists
on the replication link and in AOF replay; clients can't issue it.
`CHECKPOINT`/`RESTORE` serialize against each other per run id (a checkpoint can't capture a
half-restored state); like `BGSAVE`, they aren't atomic against unrelated concurrent writes to
`run:<id>:*` keys. `GETAT`/`HISTORY` on the `run:` keys give a second audit view
when `history-enabled` is on.

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
- `metrics-port` — Prometheus text exposition over plain HTTP (`GET /metrics`);
  `metrics-token` requires `Authorization: Bearer <token>` when set; see
  [OPERATIONS.md](OPERATIONS.md).
- `auto-aof-rewrite-percentage`/`auto-aof-rewrite-min-size` — automatic AOF compaction on
  growth (defaults 100 / 64mb), plus manual `BGREWRITEAOF`.
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
