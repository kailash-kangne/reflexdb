# Architecture

## Crate layout

```
reflexdb/
├── crates/
│   ├── resp/          # RESP2/RESP3 codec (hand-rolled on bytes::Bytes)
│   ├── keyspace/       # Storage engine: String/List/Hash/Set/ZSet/Stream, TTL, eviction, tiering trait
│   ├── commands/       # Command dispatch (~3,200 lines) — LIVE/UNLIVE, pub/sub, MULTI/EXEC+WATCH,
│   │                   # scripting, single-flight, history
│   ├── persistence/    # RDB writer/reader, AOF, history writer
│   ├── tiered/         # fjall-backed disk tier
│   └── server/         # main binary, connection loop, config loader, sentinel + proxy + cli bins
└── fuzz/               # protocol fuzz targets (resp_decode, restore_deserialize)
```

## Concurrency model

Multithreaded: `tokio` multi-thread runtime, one spawned task per TCP connection
(`crates/server/src/main.rs`).

**Keyspace** (`keyspace::Db`, `crates/keyspace/src/lib.rs`): `papaya::HashMap<Bytes,
RwLock<Entry>>`. `papaya` instead of `DashMap` for the hot-path keyspace — it's lock-free with no
shard-wide locks, so two unrelated keys never contend just because they land in the same
internal shard (measured 3-5x higher throughput than `DashMap` on a mostly-unique-key
workload). `DashMap` is still used for lower-contention registries: pub/sub channel maps,
LIVE/WATCH subscriptions, ACL users. Each keyspace entry is wrapped in its own
`parking_lot::RwLock<Entry>` (not a `Mutex`) so concurrent reads (`GET`/`LRANGE`/`HGETALL`/type
checks/...) of the *same* key run in parallel instead of serializing; writes still take the
exclusive lock, and LRU/LFU/sliding-TTL bookkeeping is kept off the read path (best-effort,
skipped if contended, except sliding TTL which always takes the write lock to avoid expiring an
actively-read key early).

```rust
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
    sliding: Option<Duration>,   // EXPIRE key seconds SLIDING
    last_access: Instant,        // LRU
    access_count: u64,           // LFU (plain counter, no decay)
}
```

`Value` is a tagged enum of owned collections (`List`, `Hash`, `Set`, `ZSet`). `List`/`Hash`/`Set`
are packed small representations (`crates/keyspace/src/collections.rs`): a linear-scanned `Vec`
below 128 elements — no per-element index/hash overhead — promoting one-way to `VecDeque`/
`IndexMap`/`IndexSet` on growth, mirroring Redis's listpack→table conversion. `Stream` (by far the
largest variant — consumer groups, PELs, ...) is boxed
(`Box<Stream>`) so its size doesn't get paid by every `String`/`List`/`Hash`/`Set`/`ZSet` stored;
serde treats `Box<T>` like `T` on the wire, so this doesn't touch the RDB/tiered-storage format.
`Db::get` deep-clones the value (O(1) for `String` via `Bytes`' refcount, O(n) for
collections); anything that only needs existence/type checking uses
`Db::with_value`/`with_value_mut` instead to avoid an unnecessary clone.

## TTL / expiry

Passive (checked on every lookup) + active (background Tokio task every 100ms by default,
sampling up to 20 entries per tick). `Instant::now() + duration` uses a `saturating_deadline()`
helper to avoid a panic on an absurdly large client-supplied TTL.

The same configurable maintenance tick runs active expiry, scheduled commands, and memory
maintenance on Tokio's blocking pool. With all memory limits disabled, memory maintenance returns
without scanning the keyspace. When eviction is needed, it selects/ranks one bounded candidate
batch per database (up to 10,000 keys per pass) rather than rebuilding the key list for every
victim; LRU/LFU/TTL policies preserve their ordering.

## Persistence

- `SAVE` and the `BGSAVE` keyspace serialization run on Tokio's blocking pool, keeping large
  snapshots off async worker threads. `BGSAVE`'s disk write and atomic rename then run on a
  spawned OS thread after serialization produces an owned `Vec<u8>`. There is no `fork()` anywhere
  in this codebase (Rust + `fork()` is UB-risk territory with allocators/threads). New RDB files
  are zstd-compressed and carry format version 2; uncompressed version-1 files remain readable.
- AOF: one dedicated background OS thread owns the file and is the only thing that calls
  `write`/`fsync`. `append()`/`fsync()` RESP-encode the command and push onto an `mpsc` channel,
  returning immediately (fire-and-forget). The writer thread drains everything already queued
  and coalesces it into one `write_all` (+ at most one `fsync`) — group commit, turning N
  concurrent callers into O(1) syscalls. `WAITAOF` is the one blocking exception
  (`AofWriter::flush_and_fsync`, waits for a real ack).
  New AOFs store independently compressed writer batches; the loader accepts legacy RESP-only
  logs, which are upgraded when `BGREWRITEAOF` replaces them.
- `GETAT`/`HISTORY` versions go through the same fire-and-forget-to-a-writer-thread shape, on
  `fjall` (`persistence::history::HistoryWriter`). Age retention defaults to 24 hours (`0`
  disables it); the writer maintains a configurable per-key cap (default 200). Changing the cap
  compacts existing history on the writer thread in bounded batches.

### Commit order and crash windows

There is no atomic commit across the places a write lands — the defined order per write is:

`mutate value` → `enqueue LIVE wake` → `enqueue AOF` → `enqueue replication` → `enqueue history` → `reply`

Consequences of a crash between any two steps (all enqueues are in-memory queues — "enqueued" is
not "durable"; only `appendfsync`'s writer-thread cadence and `WAIT`/`WAITAOF` bound the loss):

- After the value mutation but before the LIVE/AOF enqueues: the write is lost entirely on
  restart (only the in-memory dataset ever had it). LIVE subscribers can only ever observe
  values that reached their own enqueue step.
- Between AOF and replication enqueues: the primary has the write queued for durability but no
  replica was offered it — consistent with an ordinary replica lag window.
- Between replication and history enqueues: the AOF may contain a version history never
  recorded — a gap. History is deliberately last so the reverse (a history version the AOF
  lacks — a phantom) can't happen: replaying the AOF then can't resurrect state that history
  claims it saw.
- Before the reply: a client may time out on a write that did enqueue; the client can't know.
  Same ambiguity every network store has — mitigate with `WAIT`/`WAITAOF` when it matters.

History's per-key snapshot is captured while the writer still holds the entry lock on the
sites that pass one (`SET`-family); sites passing `None` re-read at drain time and can
interleave with a concurrent write to the same key — a remaining edge in the prototype, not
a corruption vector, since a re-read can only yield a newer committed value, never an
uncommitted one.

## Tiered storage

`crates/tiered` implements a `keyspace::TieringHandler` trait on `fjall`:

```rust
trait TieringHandler: Send + Sync {
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String>;
    fn promote(&self, key: &[u8]) -> Option<(Value, Option<Duration>)>;
    fn contains(&self, key: &[u8]) -> bool;
    fn remove(&self, key: &[u8]);
    // capacity/maintenance hooks, all with safe defaults:
    fn disk_usage_bytes(&self) -> Option<u64>;
    fn oldest_keys(&self, max: usize) -> Vec<Bytes>;
    fn sweep_expired(&self) -> usize;   // GC of stale tiered entries
    fn compact(&self) -> Result<(), String>; // reclaim tombstone space
    fn stats(&self) -> TieredStats;     // engine counters for INFO
}
```

`promote` is a one-way move, not a cache copy — a tiered key and its RAM copy never coexist.
RAM hits avoid any blocking-pool handoff. On Tokio's multi-thread runtime, actual synchronous
Fjall operations use `block_in_place` so Tokio can replace the occupied worker; outside that
runtime they execute synchronously. Tier spill selection follows the configured maxmemory
eviction policy, preserving LRU/LFU ordering where selected.

When the disk tier has its own `tiered-disk-limit`, `tiered-overflow-policy` decides what a
full tier means: `reject-writes` (default) refuses new-key writes with
`ERR TIERED_STORAGE_FULL` — a spill failure keeps the key in RAM and no acknowledged data is
lost — while `evict-oldest` drops the oldest tiered entries (cache semantics, real loss) and
`no-limit` doesn't enforce at all. A ~60s background task GCs expired tiered entries and
major-compacts the LSM once ≥10K tombstones accumulate or an hour passes, since fjall deletes
don't reclaim file space until compacted.

Replication uses a separate bounded queue per connected replica (32,768 messages and 64 MiB).
The normal pub/sub slow-client timeout does not apply to replicas. If the replica queue overflows,
the connection is closed after five seconds; the replica then partial-resyncs from the backlog
(`partial-resync-backlog`, 1 MiB default) when its offset is still covered, else full-resyncs.

## `LIVE` (reactivity)

1. `LIVE GET somekey` → CRC-64 fingerprint of the inner request computed, so identical
   subscriptions from different clients share one registration.
2. Inner command runs immediately (recursive `dispatch()` call, safe because it's read-only),
   reply includes the fingerprint for later `UNLIVE`.
3. Subscription registered in `live_subs: fingerprint -> {db, cmd, watched_key, clients}` and
   `key_watchers: (db, key) -> {fingerprints}`.
4. A write enqueues a `LiveWake` (deduplicated per (db, key) — a write burst collapses to one
   re-eval per drain) and a dedicated `live-fanout` thread does the re-evaluation: it re-runs
   each matching fingerprint's inner command, caches the latest result, pre-encodes the push
   once per wire variant (RESP2/RESP3/RFX), then `try_lock`s each subscribed connection's shared
   socket writer and `poll_write`s the bytes directly — no per-subscriber task wakeup. A
   contended or missing writer falls back to the client's bounded push queue. After each drain
   the thread idles `live-sweep-ms` (default 10 ms), so writes landing in the gap coalesce into
   the next batch, and every `live-pace-chunk` socket writes inside a sweep it yields
   `live-pace-ms` — that's what keeps a 1,000-subscriber key writable (~1.1k writes/s in Docker
   benchmarks) instead of collapsing the writer behind a syscall herd. The shared writer
   only exists once a connection has a LIVE subscription; plain connections keep a direct
   `write_all` reply path. Slow connections can skip intermediate states; a stalled socket marks
   the client slow and it is disconnected after 25 ms. Pub/sub and tracking use a separate
   1,024-item outbound push queue, whose overflow marks a client for disconnect after 25 ms.

Re-execution, not incremental diffing — a fast bail-out (`key_watchers_len: AtomicUsize`)
means zero extra cost on the write path when nobody's subscribed. Work scales with distinct
matching fingerprints per write; delivery also fans out to each client subscribed to a matching
fingerprint.

## Single-flight

`dispatch()` is fully synchronous on purpose (called recursively from `EVAL`/`LIVE`/`MULTI`,
none of which can `.await`), so it can only *detect* "this needs to block" and return
`Outcome::SingleFlightWait`. Only the outermost connection-loop task actually parks on the
lease's `tokio::sync::Notify`. `GET ... SINGLEFLIGHT` inside `EVAL`/`MULTI` degrades to a plain
miss instead of blocking (no `.await` point available in that recursive context).

## RESP / wire protocol

RESP2 (default) and RESP3 (`HELLO 3`). Pipelining: the connection loop drains everything already
buffered from one `read(2)` before a single `write_all`, so a pipelined burst of N commands
becomes one syscall instead of N (with `TCP_NODELAY`). Protocol limits in
`crates/resp/src/codec.rs`: `MAX_MULTIBULK_COUNT` (1M) and `MAX_BULK_LEN` (64 MiB) cap declared
lengths before any allocation happens, so a bare `*99999999999\r\n` can't make the decoder try
to allocate gigabytes.

`Value::encode` (`crates/resp/src/value.rs`) writes every integer/length with a hand-rolled,
allocation-free digit writer (`put_i64`/`put_u64`) instead of `n.to_string()` — this runs on
every single reply. RESP2 downgrades of `Map`/`Set`/`Push` write items directly instead of
cloning into a throwaway `Value::Array` first.

`RESTORE`/RDB deserialization uses
`bincode::DefaultOptions::new().with_fixint_encoding().with_limit(payload.len())` — size-bounded
against untrusted payloads; `with_fixint_encoding()` is mandatory because every other
`keyspace::Value` (de)serialization uses fixint by default, and mixing encodings silently
misreads length-prefixed fields instead of erroring.

## Dependency choices

Release builds use optimization level 3, thin LTO, one codegen unit, and symbol stripping; debug
builds retain Cargo's normal development profile.

| Crate | Why |
|---|---|
| `tokio` | async runtime, one task per connection |
| `papaya` | lock-free keyspace map — no shard-wide lock |
| `dashmap` | lower-contention registries (pub/sub, LIVE, WATCH, ACL) |
| `parking_lot::RwLock` | per-entry keyspace lock (shared for reads, exclusive for writes) |
| `indexmap` | `Set` backing store — insertion-ordered, `swap_remove_index` gives O(1) `SPOP` |
| `bytes::Bytes` | zero-copy-clone keys/values (refcounted) |
| `fjall` | pure-Rust LSM store (tiered storage + `GETAT`/`HISTORY`) — no C/C++ toolchain needed |
| `bincode` + `serde` | RDB snapshot and tiered-entry wire format |
| `crc` | `LIVE` fingerprinting (CRC-64/XZ) |
| `mlua` | Lua 5.1 sandbox |
| `rustls` | TLS |

## What's intentionally not built

- Cluster mode (hash-slot sharding, gossip, resharding) and anything tied to it (sharded
  pub/sub, cluster-routing command flags).
- Chasing full command parity (Geo, HyperLogLog, Functions API, JSON/Bloom families) beyond
  what reflexdb actually uses.
- Hosting third-party `.so` module binaries — extensions are compiled-in Rust crates.
- Active defrag (Rust's allocator doesn't need the same trick), 32-bit targets.

See [ROADMAP.md](ROADMAP.md) for designed-but-not-built features (as opposed to the above,
which are permanently out of scope).
