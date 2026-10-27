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

`Value` is a tagged enum of owned collections (`VecDeque<Bytes>`, `HashMap<Bytes,Bytes>`,
`IndexSet<Bytes>`, `ZSet`) — always the "full" representation, no small-collection compact
encodings. `Stream` (by far the largest variant — consumer groups, PELs, ...) is boxed
(`Box<Stream>`) so its size doesn't get paid by every `String`/`List`/`Hash`/`Set`/`ZSet` stored;
serde treats `Box<T>` like `T` on the wire, so this doesn't touch the RDB/tiered-storage format.
`Db::get` deep-clones the value (O(1) for `String` via `Bytes`' refcount, O(n) for
collections); anything that only needs existence/type checking uses
`Db::with_value`/`with_value_mut` instead to avoid an unnecessary clone.

## TTL / expiry

Passive (checked on every lookup) + active (background tokio task every 100ms, samples up to 20
entries). `Instant::now() + duration` uses a `saturating_deadline()` helper to avoid a panic on
an absurdly large client-supplied TTL.

## Persistence

- `SAVE`: fully synchronous. `BGSAVE`: the keyspace walk (`persistence::rdb::serialize`) runs on
  the calling thread (needs the borrowed `&Store`), but once serialized into an owned `Vec<u8>`,
  the disk write + atomic rename happens on a spawned OS thread — no `fork()` anywhere in this
  codebase (Rust + `fork()` is UB-risk territory with allocators/threads).
- AOF: one dedicated background OS thread owns the file and is the only thing that calls
  `write`/`fsync`. `append()`/`fsync()` RESP-encode the command and push onto an `mpsc` channel,
  returning immediately (fire-and-forget). The writer thread drains everything already queued
  and coalesces it into one `write_all` (+ at most one `fsync`) — group commit, turning N
  concurrent callers into O(1) syscalls. `WAITAOF` is the one blocking exception
  (`AofWriter::flush_and_fsync`, waits for a real ack).
- `GETAT`/`HISTORY` versions go through the same fire-and-forget-to-a-writer-thread shape, on
  `fjall` (`persistence::history::HistoryWriter`).

## Tiered storage

`crates/tiered` implements a `keyspace::TieringHandler` trait on `fjall`:

```rust
trait TieringHandler: Send + Sync {
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>);
    fn promote(&self, key: &[u8]) -> Option<(Value, Option<Duration>)>;
    fn contains(&self, key: &[u8]) -> bool;
    fn remove(&self, key: &[u8]);
}
```

`promote` is a one-way move, not a cache copy — a tiered key and its RAM copy never coexist.
`evict`/`promote` are synchronous disk I/O on whatever path triggers them (not off-loaded to a
blocking thread pool).

## `LIVE` (reactivity)

1. `LIVE GET somekey` → CRC-64 fingerprint of the inner request computed, so identical
   subscriptions from different clients share one registration.
2. Inner command runs immediately (recursive `dispatch()` call, safe because it's read-only),
   reply includes the fingerprint for later `UNLIVE`.
3. Subscription registered in `live_subs: fingerprint -> {db, cmd, watched_key, clients}` and
   `key_watchers: (db, key) -> {fingerprints}`.
4. Every write calls `notify_key_change`, which looks up `key_watchers` and re-runs the inner
   command for each matching fingerprint, pushing the new result per-client.

Re-execution, not incremental diffing — a fast bail-out (`key_watchers_len: AtomicUsize`)
means zero extra cost on the write path when nobody's subscribed. N subscribers on one hot key
means N re-executions per write.

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
`crates/resp/src/codec.rs`: `MAX_MULTIBULK_COUNT` (1M) and `MAX_BULK_LEN` (512MB) cap declared
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
