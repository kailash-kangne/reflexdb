//! In-memory keyspace: sharded-by-DB storage with TTL semantics.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use papaya::HashMap as PapayaMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

mod collections;
mod stream;
mod zset;
/// Packed-under-128-elements representations — see `collections`.
pub use collections::{Hash, List, Set};
pub use stream::{parse_for_add, ConsumerGroup, IdSpec, PendingEntry, Stream, StreamId, XaddError};
pub use zset::ZSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    String(Bytes),
    List(List),
    Hash(Hash),
    Set(Set),
    ZSet(ZSet),
    /// Boxed, unlike every other variant: `Stream` is by far the largest
    /// (consumer groups, PELs, ...) and by far the least common value type
    /// in practice, so leaving it unboxed would make `Value` itself as big
    /// as a `Stream` — paid on every `String`/`List`/`Hash`/`Set`/`ZSet`
    /// stored, not just on actual streams. Serde treats `Box<T>` exactly
    /// like `T` on the wire, so this doesn't change the RDB/tiered-storage
    /// format.
    Stream(Box<Stream>),
    /// Integer-encoded string — semantically identical to `String` (same
    /// Redis type; `GET` returns the decimal text). Written by `INCR`-
    /// family commands only; every other string mutation demotes it back
    /// to `String` via `string_bytes_mut`. Skips the parse + `Bytes`
    /// alloc per increment on hot counters. LAST variant on purpose:
    /// appending keeps the serde discriminant indices of the earlier
    /// variants stable, so pre-`Int` snapshots/tiered data still decode.
    Int(i64),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) | Value::Int(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
            Value::Stream(_) => "stream",
        }
    }

    /// String-typed values as bytes: `String` cloned, `Int` formatted to
    /// its decimal text — exactly what a client reads either way. `None`
    /// for collection types, which are `WRONGTYPE` to string commands.
    pub fn string_bytes(&self) -> Option<Bytes> {
        match self {
            Value::String(b) => Some(b.clone()),
            Value::Int(i) => Some(Bytes::from(i.to_string())),
            _ => None,
        }
    }

    /// `&mut` string form for in-place edits: an `Int` is first demoted
    /// to its decimal `String` (any non-numeric mutation loses the int
    /// encoding, matching Redis), then yields the buffer. `None` for
    /// collection types.
    pub fn string_bytes_mut(&mut self) -> Option<&mut Bytes> {
        if let Value::Int(i) = self {
            *self = Value::String(Bytes::from(i.to_string()));
        }
        match self {
            Value::String(b) => Some(b),
            _ => None,
        }
    }

    /// List/Hash/Set/ZSet keys disappear once their last element is removed
    /// (matches Redis: `LPOP` on a single-element list deletes the key).
    /// Strings never auto-delete this way, even when empty — and neither
    /// does a `Stream`: unlike every other collection here, a stream with
    /// zero entries is still a valid, addressable stream (it may still
    /// have consumer groups attached), matching real Redis.
    pub fn is_empty_container(&self) -> bool {
        match self {
            Value::String(_) | Value::Int(_) => false,
            Value::List(l) => l.is_empty(),
            Value::Hash(h) => h.is_empty(),
            Value::Set(s) => s.is_empty(),
            Value::ZSet(z) => z.is_empty(),
            Value::Stream(_) => false,
        }
    }

    /// Rough memory estimate for `maxmemory` accounting — not meant to be
    /// byte-exact, just proportionate enough to make eviction decisions.
    pub fn approx_bytes(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::Int(_) => 8,
            Value::List(l) => l.approx_bytes(),
            Value::Hash(h) => h.approx_bytes(),
            Value::Set(s) => s.approx_bytes(),
            Value::ZSet(z) => z.approx_bytes(),
            Value::Stream(s) => s.approx_bytes(),
        }
    }
}

/// `Instant::now() + dur` panics if the sum overflows the clock's internal
/// representation — reachable from an `EXPIRE ... SLIDING` duration or a
/// tiering/snapshot-loaded TTL that's absurdly large. Saturates to the
/// farthest representable instant instead of panicking: indistinguishable
/// from "a very long time from now" to every caller here.
fn saturating_deadline(dur: Duration) -> Instant {
    saturating_add(Instant::now(), dur)
}

fn saturating_add(base: Instant, dur: Duration) -> Instant {
    if let Some(t) = base.checked_add(dur) {
        return t;
    }
    let mut probe = dur;
    loop {
        probe /= 2;
        if probe.is_zero() {
            return base;
        }
        if let Some(t) = base.checked_add(probe) {
            return t;
        }
    }
}

/// Anchor for `Instant` → compact-ms conversions in `Entry`. Monotonic,
/// process-local — persisted formats never see these values (TTLs are
/// exported as relative `Duration`s).
static EPOCH: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

fn now_ms() -> u64 {
    Instant::now().saturating_duration_since(*EPOCH).as_millis() as u64
}

fn now_secs() -> u32 {
    (now_ms() / 1000).min(u32::MAX as u64) as u32
}

/// `Instant` → `expires_ms`. A past/sub-ms instant maps to `0`, which
/// `is_expired` treats as already elapsed ("no expiry" is `u64::MAX`,
/// not `0`, so `0` is free to mean "expired").
fn instant_to_ms(at: Instant) -> u64 {
    at.saturating_duration_since(*EPOCH).as_millis() as u64
}

/// `Duration` → `sliding_ms`. Same `.max(1)` sentinel rule: a sub-ms
/// sliding window can't be allowed to read as "sliding off".
fn duration_to_ms(d: Duration) -> u64 {
    (d.as_millis().min(u64::MAX as u128) as u64).max(1)
}

fn ms_to_instant(ms: u64) -> Instant {
    saturating_add(*EPOCH, Duration::from_millis(ms))
}

/// If a tiered operation contends on a synchronous lock while called from a
/// multi-thread Tokio runtime, let the runtime replace this worker during the
/// wait. Outside Tokio or on a current-thread runtime, preserve the normal
/// synchronous library behavior.
fn wait_without_blocking_tokio_worker<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    /// Absolute expiry in ms since `EPOCH`; `u64::MAX` = no expiry.
    expires_ms: u64,
    /// `EXPIRE ... SLIDING` reset window in ms; `0` = sliding off.
    /// `PERSIST`/plain `EXPIRE` clears it. (u64, not u32: >49-day
    /// sliding windows are legal inputs even if absurd.)
    sliding_ms: u64,
    /// LRU bookkeeping: last access, whole seconds since `EPOCH` —
    /// eviction ranking only needs ordering, not precision.
    last_access_secs: u32,
    /// LFU bookkeeping: saturating access counter (approximation, see
    /// `access_stats`'s callers).
    access_count: u32,
}

impl Entry {
    fn new(value: Value, expires_at: Option<Instant>, sliding: Option<Duration>) -> Self {
        Self {
            value,
            expires_ms: expires_at.map(instant_to_ms).unwrap_or(u64::MAX),
            sliding_ms: sliding.map(duration_to_ms).unwrap_or(0),
            last_access_secs: now_secs(),
            access_count: 0,
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_ms != u64::MAX && self.expires_ms <= now_ms()
    }

    fn expires_instant(&self) -> Option<Instant> {
        (self.expires_ms != u64::MAX).then(|| ms_to_instant(self.expires_ms))
    }

    fn remaining_ttl(&self) -> Option<Duration> {
        (self.expires_ms != u64::MAX).then(|| Duration::from_millis(self.expires_ms.saturating_sub(now_ms())))
    }

    /// Reset the sliding-TTL countdown on read access, if enabled, and bump
    /// the LRU/LFU access bookkeeping — called from every read/write path
    /// that touches an existing entry.
    fn touch(&mut self) {
        if self.sliding_ms != 0 {
            self.expires_ms = now_ms().saturating_add(self.sliding_ms);
        }
        self.last_access_secs = now_secs();
        self.access_count = self.access_count.saturating_add(1);
    }
}

/// The result of a `TTL`-style lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// Key does not exist (or has already expired).
    NoKey,
    /// Key exists but has no associated expiry.
    Persistent,
    /// Key exists and expires in the given duration.
    Expires(Duration),
}

/// Pluggable disk tier for keys evicted out of RAM (see
/// `docs/05-persistence-tiering.md` §5). `keyspace` only defines the
/// contract; the concrete LSM-backed implementation lives in the `tiered`
/// crate so this crate doesn't need to know about any particular storage
/// engine.
/// Error returned by `TieringHandler::evict` when the tier's configured
/// capacity is exhausted — the dispatch layer maps it to the client-facing
/// `ERR TIERED_STORAGE_FULL` reply and treats it differently from an
/// ordinary I/O failure (which just keeps the key in RAM and retries).
pub const TIERED_FULL: &str = "TIERED_STORAGE_FULL";

pub trait TieringHandler: Send + Sync {
    /// Persist a key being dropped from RAM. May fail with
    /// [`TIERED_FULL`] when a configured capacity is exhausted — the key
    /// must stay in RAM in that case; no data is lost.
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String>;
    /// Read a previously evicted key WITHOUT removing it: the tier entry
    /// is the durable record and stays put until a delete, overwrite or
    /// flush drops it — RAM is just a read cache. Removing it here would
    /// leave the key volatile-only until the next eviction re-spilled it,
    /// a crash window that loses an acknowledged key entirely.
    fn fetch(&self, key: &[u8]) -> Option<(Value, Option<Duration>)>;
    /// Whether the tier holds this key, without promoting it — used by
    /// `EXISTS` so it doesn't have to pull the whole value off disk just to
    /// answer a boolean question.
    fn contains(&self, key: &[u8]) -> bool;
    /// Drop a key from the tier outright (`DEL` on a tiered-only key).
    fn remove(&self, key: &[u8]);
    /// Snapshot the keys held in the disk tier. Implementations that cannot
    /// enumerate their keys may retain the empty default, but then database
    /// enumeration cannot expose tiered keys.
    fn keys_snapshot(&self) -> Vec<Bytes> {
        Vec::new()
    }
    /// Bytes currently used on disk by this tier, if the implementation can
    /// report it — used to enforce `tiered-disk-limit`. `None` means "don't
    /// know", which disables that enforcement rather than treating it as
    /// zero (an implementation that can't measure its own usage shouldn't
    /// silently look empty and never get capacity-evicted from).
    fn disk_usage_bytes(&self) -> Option<u64> {
        None
    }
    /// Whether the tier would accept `additional` charged bytes — lets the
    /// `reject-writes` gate refuse a new-key write *before* it lands in RAM
    /// when the tier couldn't absorb its eventual spill. Default `true`
    /// (unbounded/unknowable tiers never trigger the gate).
    fn can_accept(&self, additional: u64) -> bool {
        let _ = additional;
        true
    }
    /// Up to `max` of the tier's keys, oldest-evicted-first — used by
    /// `tiered-disk-limit` so capacity eviction drops genuinely stale
    /// entries first instead of a random pick. Default falls back to an
    /// arbitrary (not actually age-ordered) `max` keys from
    /// `keys_snapshot`, for implementations that don't track eviction
    /// age — correct but not age-aware.
    fn oldest_keys(&self, max: usize) -> Vec<Bytes> {
        let mut keys = self.keys_snapshot();
        keys.truncate(max);
        keys
    }
    /// Remove every key held in the tier.
    fn clear(&self) {
        for key in self.keys_snapshot() {
            self.remove(&key);
        }
    }
    /// Periodic GC: drop expired or undecodable entries still sitting in
    /// the tier — the batch partner to the lazy checks in `contains`/
    /// `fetch`. Returns how many entries were dropped.
    fn sweep_expired(&self) -> usize {
        0
    }
    /// Reclaim disk space held by tombstoned entries (LSM compaction or
    /// equivalent). Deletions don't free file space until this or the
    /// engine's own merging runs. Default no-op for implementations that
    /// don't need offline reclamation.
    fn compact(&self) -> Result<(), String> {
        Ok(())
    }
    /// Operational counters surfaced via `INFO`.
    fn stats(&self) -> TieredStats {
        TieredStats::default()
    }
}

/// `TieringHandler::stats` — engine-side counters the dispatch layer can't
/// see (tombstones written, compaction progress). All fields optional-ish:
/// implementations that can't measure leave them zeroed.
#[derive(Debug, Default, Clone, Copy)]
pub struct TieredStats {
    /// Entries removed since open (deletes, GC sweeps, evict-oldest
    /// drops) — every removal writes a tombstone, so this is the signal
    /// for "compaction is worth running".
    pub entries_removed: u64,
    pub compactions_completed: u64,
    pub compacting: bool,
    /// Total wall-clock time the engine has spent compacting.
    pub compact_time: Duration,
}

/// Namespaces one shared `TieringHandler` per logical db index: the
/// engine keys entries by the raw key bytes alone, so without a db
/// prefix `db0.foo` and `db1.foo` would clobber each other's spilled
/// copy. Capacity, GC and compaction calls pass through — they describe
/// the whole engine, not one db's slice of it.
struct DbScopedTier {
    db_index: u32,
    inner: Arc<dyn TieringHandler>,
}

impl DbScopedTier {
    fn scoped(&self, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + key.len());
        out.extend_from_slice(&self.db_index.to_be_bytes());
        out.extend_from_slice(key);
        out
    }

    fn unscope(&self, key: Bytes) -> Option<Bytes> {
        key.strip_prefix(&self.db_index.to_be_bytes()[..]).map(Bytes::copy_from_slice)
    }
}

impl TieringHandler for DbScopedTier {
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String> {
        self.inner.evict(&Bytes::from(self.scoped(key)), value, ttl)
    }

    fn fetch(&self, key: &[u8]) -> Option<(Value, Option<Duration>)> {
        self.inner.fetch(&self.scoped(key))
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.inner.contains(&self.scoped(key))
    }

    fn remove(&self, key: &[u8]) {
        self.inner.remove(&self.scoped(key))
    }

    fn keys_snapshot(&self) -> Vec<Bytes> {
        self.inner.keys_snapshot().into_iter().filter_map(|k| self.unscope(k)).collect()
    }

    fn oldest_keys(&self, max: usize) -> Vec<Bytes> {
        self.inner.oldest_keys(usize::MAX).into_iter().filter_map(|k| self.unscope(k)).take(max).collect()
    }

    /// Only this db's slice — `FLUSHDB` must not wipe other dbs' tiered
    /// data, which a shared-engine `clear` would have done.
    fn clear(&self) {
        for key in self.keys_snapshot() {
            self.remove(&key);
        }
    }

    fn disk_usage_bytes(&self) -> Option<u64> {
        self.inner.disk_usage_bytes()
    }

    fn can_accept(&self, additional: u64) -> bool {
        self.inner.can_accept(additional)
    }

    fn sweep_expired(&self) -> usize {
        self.inner.sweep_expired()
    }

    fn compact(&self) -> Result<(), String> {
        self.inner.compact()
    }

    fn stats(&self) -> TieredStats {
        self.inner.stats()
    }
}

/// A single logical database (what Redis calls `SELECT 0..N`).
///
/// Storage is a lock-free `papaya::HashMap` instead of `DashMap`: papaya has
/// no shard-wide locks at all (two unrelated keys never contend with each
/// other just for landing in the same shard, the way they can with
/// DashMap's per-shard `RwLock`), which measured 3-5x higher throughput
/// than `DashMap` on a realistic mostly-unique-key workload in
/// benchmarking before this change. Mutating a value in place still needs
/// *some* synchronization (papaya only hands out shared references), so
/// each entry is individually wrapped in a `parking_lot::RwLock` —
/// contention only happens when the exact same key is hit concurrently,
/// same as `DashMap`'s per-key behavior once you're past its shard lock.
/// `RwLock`, not `Mutex`: reads (`GET`/`LRANGE`/`HGETALL`/.../type checks)
/// take a shared read lock, so N concurrent readers of the *same* key run
/// in parallel instead of fully serializing the way they would behind a
/// plain `Mutex` — see `with_value`'s and `get`'s docs for how the
/// LRU/LFU/sliding-TTL bookkeeping (which does need exclusive access)
/// avoids forcing every reader back onto the slow path.
#[derive(Default)]
pub struct Db {
    map: PapayaMap<Bytes, RwLock<Entry>>,
    tiering: Option<Arc<dyn TieringHandler>>,
    // Serialize tier moves with key mutations so a delayed promotion or
    // eviction cannot overwrite/remove a newer value for the same key.
    tier_ops: Mutex<()>,
    expire_cursor: std::sync::atomic::AtomicUsize,
}

impl Db {
    pub fn new() -> Self {
        Self {
            map: PapayaMap::new(),
            tiering: None,
            tier_ops: Mutex::new(()),
            expire_cursor: Default::default(),
        }
    }

    /// `tier_ops` only exists to serialize key mutations against tier
    /// moves (see its doc comment) — with no tiering handler configured
    /// (the common case), there's no tier to race with, so every write
    /// path skips the lock entirely instead of needlessly serializing all
    /// writes to this `Db` through one mutex regardless of key.
    #[inline]
    fn tier_guard(&self) -> Option<parking_lot::MutexGuard<'_, ()>> {
        self.tiering.as_ref()?;
        Some(match self.tier_ops.try_lock() {
            Some(guard) => guard,
            None => wait_without_blocking_tokio_worker(|| self.tier_ops.lock()),
        })
    }

    /// Like `new`, but keys evicted via `evict_to_tier` land in `handler`
    /// instead of being dropped, and reads transparently promote them back.
    pub fn with_tiering(handler: Arc<dyn TieringHandler>) -> Self {
        Self {
            map: PapayaMap::new(),
            tiering: Some(handler),
            tier_ops: Mutex::new(()),
            expire_cursor: Default::default(),
        }
    }

    /// Set a key to a value, optionally with an absolute expiry instant.
    /// Passing `None` clears any existing TTL (matches `SET` semantics).
    /// Always clears sliding-TTL mode — a plain `SET` is a fresh value.
    ///
    /// A pre-existing tier copy is deliberately left alone: RAM and the
    /// tier may hold copies of the same key (the tier's is the last
    /// fsync'd state, RAM's is authoritative while the server is alive),
    /// and every permanent-removal path clears the tier copy via
    /// `drop_tier_copy` — so there's no ghost to resurrect, and a crash
    /// before the next re-eviction still recovers the last durable value.
    pub fn set(&self, key: Bytes, value: Value, expires_at: Option<Instant>) {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        self.map.insert(key, RwLock::new(Entry::new(value, expires_at, None)), &guard);
    }

    /// On a RAM miss, ask the tiering handler (if any) for the key and, if
    /// found, rehydrate it into RAM. Returns whether a promotion happened.
    /// The tier keeps its copy (`fetch`, not a move) — RAM is a read cache
    /// over the tier's durable record, so a key never becomes volatile-only.
    /// Sliding TTLs don't currently survive a trip through the disk tier.
    fn try_promote(&self, key: &[u8]) -> bool {
        let _tier_guard = self.tier_guard();
        self.try_promote_locked(key)
    }

    fn try_promote_locked(&self, key: &[u8]) -> bool {
        let Some(handler) = &self.tiering else { return false };
        let Some((value, ttl)) = handler.fetch(key) else { return false };
        let expires_at = ttl.map(saturating_deadline);
        let guard = self.map.guard();
        self.map.insert(
            Bytes::copy_from_slice(key),
            RwLock::new(Entry::new(value, expires_at, None)),
            &guard,
        );
        true
    }

    /// Drop `key`'s tier copy if the tier still holds one. RAM and the
    /// tier may coexist (RAM is a read cache), so every path that
    /// *permanently* removes a key — `DEL`, `take`/`RENAME`, expiry — must
    /// clear the tier copy too or the older value would resurrect on the
    /// next RAM-miss `fetch`. `contains` first so keys never tiered don't
    /// pay a tombstone write. Callers already hold `tier_guard`.
    fn drop_tier_copy(&self, key: &[u8]) {
        if let Some(handler) = &self.tiering {
            if handler.contains(key) {
                handler.remove(key);
            }
        }
    }

    /// Remove an expired entry only if it is still the same map entry that
    /// was inspected. A concurrent SET may have replaced it after the
    /// reader dropped its lock.
    fn remove_if_same(&self, key: &[u8], inspected: &RwLock<Entry>, guard: &impl papaya::Guard) {
        if self.map.get(key, guard).is_some_and(|current| std::ptr::eq(current, inspected)) {
            self.map.remove(key, guard);
            // Expiry is a delete: drop the coexisting tier copy too — its
            // own deadline can lag after post-promote TTL changes, and
            // leaving it would resurrect the key on the next RAM miss.
            self.drop_tier_copy(key);
        }
    }

    /// Bumps LRU/LFU bookkeeping and, for a sliding-TTL key, resets the
    /// expiry countdown — called after a read already dropped its shared
    /// lock. A sliding-TTL key always takes the exclusive lock: missing a
    /// reset could let an actively-read key expire early, which the LRU/LFU
    /// counters (already a documented approximation, not exact
    /// Redis-compatible tracking) don't need to guarantee. For the common
    /// case (no sliding TTL), this is best-effort: skipped outright if the
    /// lock is contended, instead of making every concurrent reader of the
    /// same key fall back to serializing on an exclusive lock just to bump
    /// a counter.
    fn touch_entry(lock: &RwLock<Entry>, sliding_ms: u64) {
        if sliding_ms != 0 {
            lock.write().touch();
        } else if let Some(mut entry) = lock.try_write() {
            entry.touch();
        }
    }

    /// Passive expiry: look up a key, evicting it first if its TTL has
    /// elapsed. Falls back to the disk tier (if configured) on a RAM miss.
    /// Resets a sliding TTL (if any) on this access.
    ///
    /// Takes only a shared read lock for the actual value clone, so N
    /// concurrent `GET`s of the same key run in parallel instead of
    /// serializing behind each other — see `touch_entry`'s docs for how the
    /// LRU/LFU/sliding-TTL bookkeeping is kept out of that hot path.
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let guard = self.map.guard();
        if let Some(lock) = self.map.get(key, &guard) {
            let entry = lock.read();
            if entry.is_expired() {
                drop(entry);
                self.remove_if_same(key, lock, &guard);
                return None;
            }
            let value = entry.value.clone();
            let sliding_ms = entry.sliding_ms;
            drop(entry);
            Self::touch_entry(lock, sliding_ms);
            return Some(value);
        }
        drop(guard);
        if self.try_promote(key) {
            let guard = self.map.guard();
            if let Some(lock) = self.map.get(key, &guard) {
                let entry = lock.read();
                if entry.is_expired() {
                    drop(entry);
                    self.remove_if_same(key, lock, &guard);
                    return None;
                }
                return Some(entry.value.clone());
            }
        }
        None
    }

    pub fn del(&self, keys: &[Bytes]) -> usize {
        self.del_sized(keys).0
    }

    /// Delete keys and report both the number removed and their approximate
    /// RAM footprint. The byte count lets bounded eviction loops maintain a
    /// running estimate instead of rescanning the full keyspace per key.
    pub fn del_sized(&self, keys: &[Bytes]) -> (usize, usize) {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        let mut removed = 0;
        let mut freed = 0usize;
        for key in keys {
            let in_ram = self.map.remove(key.as_ref(), &guard);
            if let Some(lock) = in_ram {
                let entry = lock.read();
                freed = freed.saturating_add(key.len() + 48 + entry.value.approx_bytes());
                removed += 1;
                drop(entry);
                // RAM and tier copies coexist by design — a delete must
                // drop both or the older tiered value would resurrect.
                self.drop_tier_copy(key);
                continue;
            }
            let in_tier = self
                .tiering
                .as_ref()
                .map(|h| {
                    let was_present = h.contains(key);
                    h.remove(key);
                    was_present
                })
                .unwrap_or(false);
            if in_tier {
                removed += 1;
            }
        }
        (removed, freed)
    }

    /// Remove a key from RAM and return its owned value and absolute expiry
    /// without cloning the value. A key in the disk tier is promoted first,
    /// matching `get`'s lookup behavior. Used by rename-style operations
    /// that immediately install the value under a different key.
    pub fn take(&self, key: &[u8]) -> Option<(Value, Option<Instant>)> {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        let Some(lock) = self.map.remove(key, &guard) else {
            drop(guard);
            drop(_tier_guard);
            if self.try_promote(key) {
                return self.take(key);
            }
            return None;
        };
        self.drop_tier_copy(key);

        let mut entry = lock.write();
        if entry.is_expired() {
            return None;
        }
        // Preserve the existing access/sliding-TTL semantics of `get`.
        entry.touch();
        let expires_at = entry.expires_instant();
        let value = std::mem::replace(&mut entry.value, Value::String(Bytes::new()));
        Some((value, expires_at))
    }

    /// Unlike `get`, doesn't promote a tiered key back into RAM just to
    /// answer "does this exist" — matches `docs/05-persistence-tiering.md`
    /// (`EXISTS` stays truthful about tiered keys without paying for a
    /// disk read).
    pub fn exists(&self, keys: &[Bytes]) -> usize {
        let guard = self.map.guard();
        keys.iter()
            .filter(|k| {
                let in_ram =
                    self.map.get(k.as_ref(), &guard).map(|lock| !lock.read().is_expired()).unwrap_or(false);
                in_ram || self.tiering.as_ref().is_some_and(|h| h.contains(k))
            })
            .count()
    }

    /// Update access metadata for a live key without cloning its value or
    /// promoting a disk-tiered key into RAM. Used by the `TOUCH` command.
    pub fn touch_key(&self, key: &[u8]) -> bool {
        let guard = self.map.guard();
        let Some(lock) = self.map.get(key, &guard) else {
            return self.tiering.as_ref().is_some_and(|handler| handler.contains(key));
        };
        let mut entry = lock.write();
        if entry.is_expired() {
            drop(entry);
            self.remove_if_same(key, lock, &guard);
            return false;
        }
        entry.touch();
        true
    }

    pub fn ttl(&self, key: &[u8]) -> Ttl {
        let guard = self.map.guard();
        match self.map.get(key, &guard) {
            None => {
                drop(guard);
                if self.try_promote(key) {
                    self.ttl(key)
                } else {
                    Ttl::NoKey
                }
            }
            Some(lock) => {
                let entry = lock.read();
                if entry.is_expired() {
                    drop(entry);
                    self.remove_if_same(key, lock, &guard);
                    Ttl::NoKey
                } else {
                    match entry.expires_ms {
                        u64::MAX => Ttl::Persistent,
                        ms => Ttl::Expires(Duration::from_millis(ms.saturating_sub(now_ms()))),
                    }
                }
            }
        }
    }

    /// Set (or clear, with `None`) the expiry of an existing key. Also turns
    /// off sliding-TTL mode — use `expire_sliding` for `EXPIRE ... SLIDING`.
    /// Returns `false` if the key does not exist.
    pub fn expire(&self, key: &[u8], expires_at: Option<Instant>) -> bool {
        let guard = self.map.guard();
        match self.map.get(key, &guard) {
            Some(lock) => {
                let mut entry = lock.write();
                if entry.is_expired() {
                    false
                } else {
                    entry.expires_ms = expires_at.map(instant_to_ms).unwrap_or(u64::MAX);
                    entry.sliding_ms = 0;
                    true
                }
            }
            None => {
                drop(guard);
                self.try_promote(key) && self.expire(key, expires_at)
            }
        }
    }

    /// `EXPIRE key seconds SLIDING` — resets to `duration` now, and again on
    /// every subsequent read/write access to the key, until `PERSIST` or a
    /// plain `expire`/`EXPIRE` turns it off. Returns `false` if the key does
    /// not exist.
    pub fn expire_sliding(&self, key: &[u8], duration: Duration) -> bool {
        let guard = self.map.guard();
        match self.map.get(key, &guard) {
            Some(lock) => {
                let mut entry = lock.write();
                if entry.is_expired() {
                    false
                } else {
                    entry.sliding_ms = duration_to_ms(duration);
                    entry.expires_ms = now_ms().saturating_add(entry.sliding_ms);
                    true
                }
            }
            None => {
                drop(guard);
                self.try_promote(key) && self.expire_sliding(key, duration)
            }
        }
    }

    /// Current expiry of a key, for implementing `SET ... KEEPTTL`.
    /// Returns `None` if the key doesn't exist (or just expired), `Some(None)`
    /// if it exists with no TTL, `Some(Some(instant))` if it has one.
    pub fn current_expiry(&self, key: &[u8]) -> Option<Option<Instant>> {
        let guard = self.map.guard();
        match self.map.get(key, &guard) {
            Some(lock) => {
                let entry = lock.read();
                if entry.is_expired() {
                    None
                } else {
                    Some(entry.expires_instant())
                }
            }
            None => {
                drop(guard);
                if self.try_promote(key) {
                    self.current_expiry(key)
                } else {
                    None
                }
            }
        }
    }

    /// Read-only access to a key's value, with passive expiry applied first
    /// and a tiered-storage promotion attempt on a RAM miss. Resets a
    /// sliding TTL (if any) on this access.
    ///
    /// `f` runs under only a shared read lock, so concurrent callers reading
    /// the *same* key (e.g. many clients running `LRANGE`/`HGETALL`/
    /// `SMEMBERS`/`TYPE` against one hot key) run in parallel instead of
    /// fully serializing on an exclusive lock — see `touch_entry`'s docs for
    /// how the LRU/LFU/sliding-TTL bookkeeping stays out of that path.
    pub fn with_value<R>(&self, key: &[u8], f: impl FnOnce(&Value) -> R) -> Option<R> {
        let guard = self.map.guard();
        if let Some(lock) = self.map.get(key, &guard) {
            let entry = lock.read();
            if entry.is_expired() {
                drop(entry);
                self.remove_if_same(key, lock, &guard);
                return None;
            }
            let result = f(&entry.value);
            let sliding_ms = entry.sliding_ms;
            drop(entry);
            Self::touch_entry(lock, sliding_ms);
            return Some(result);
        }
        drop(guard);
        if self.try_promote(key) {
            let guard = self.map.guard();
            if let Some(lock) = self.map.get(key, &guard) {
                let entry = lock.read();
                if entry.is_expired() {
                    drop(entry);
                    self.remove_if_same(key, lock, &guard);
                    return None;
                }
                return Some(f(&entry.value));
            }
        }
        None
    }

    /// Mutable access to an existing key's value, with passive expiry and
    /// promotion applied the same way as `with_value`. Does *not* create the
    /// key if it's missing from both RAM and the tier — see `with_entry_mut`.
    pub fn with_value_mut<R>(&self, key: &[u8], f: impl FnOnce(&mut Value) -> R) -> Option<R> {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        if let Some(lock) = self.map.get(key, &guard) {
            let mut entry = lock.write();
            if entry.is_expired() {
                drop(entry);
                self.remove_if_same(key, lock, &guard);
                return None;
            }
            entry.touch();
            return Some(f(&mut entry.value));
        }
        drop(guard);
        if self.try_promote_locked(key) {
            let guard = self.map.guard();
            if let Some(lock) = self.map.get(key, &guard) {
                let mut entry = lock.write();
                if entry.is_expired() {
                    drop(entry);
                    self.remove_if_same(key, lock, &guard);
                    return None;
                }
                entry.touch();
                return Some(f(&mut entry.value));
            }
        }
        None
    }

    /// Mutable access to an existing value and its expiry under one lock.
    /// Returns `None` for a missing or expired key; unlike `with_entry_mut_expiry`,
    /// this never creates a value.
    pub fn with_existing_entry_mut_expiry<R>(
        &self,
        key: &[u8],
        f: impl FnOnce(&mut Value, &mut Option<Instant>) -> R,
    ) -> Option<R> {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        if let Some(lock) = self.map.get(key, &guard) {
            let mut entry = lock.write();
            if entry.is_expired() {
                drop(entry);
                self.remove_if_same(key, lock, &guard);
                return None;
            }
            entry.touch();
            // `expires_ms` lives in the compact repr; hand the callback a
            // real `Option<Instant>` and write it back afterward.
            let mut exp = entry.expires_instant();
            let result = f(&mut entry.value, &mut exp);
            entry.expires_ms = exp.map(instant_to_ms).unwrap_or(u64::MAX);
            return Some(result);
        }
        drop(guard);
        if self.try_promote_locked(key) {
            let guard = self.map.guard();
            if let Some(lock) = self.map.get(key, &guard) {
                let mut entry = lock.write();
                if entry.is_expired() {
                    drop(entry);
                    self.remove_if_same(key, lock, &guard);
                    return None;
                }
                entry.touch();
                let mut exp = entry.expires_instant();
                let result = f(&mut entry.value, &mut exp);
                entry.expires_ms = exp.map(instant_to_ms).unwrap_or(u64::MAX);
                return Some(result);
            }
        }
        None
    }

    /// Mutable access to a key's value, inserting `default()` (with no TTL)
    /// if the key is missing (after trying to promote it from the tier
    /// first) or has expired. Used by collection commands like
    /// `LPUSH`/`HSET`/`SADD`/`ZADD` that create their key on first write.
    pub fn with_entry_mut<R>(
        &self,
        key: Bytes,
        default: impl FnOnce() -> Value,
        f: impl FnOnce(&mut Value) -> R,
    ) -> R {
        self.with_entry_mut_expiry(key, default, |value, _, _| f(value))
    }

    /// Mutable access to a value and its absolute expiry under the same
    /// entry lock. Use this for commands that must change both atomically.
    pub fn with_entry_mut_expiry<R>(
        &self,
        key: Bytes,
        default: impl FnOnce() -> Value,
        f: impl FnOnce(&mut Value, &mut Option<Instant>, bool) -> R,
    ) -> R {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        // Hot path (key already present and live, which is the common case
        // once a collection key has been created) only needs this one
        // lookup: fold the expiry check and the "does it still exist"
        // question into the same `get`, instead of a separate
        // `contains_key` call right after.
        let mut present = false;
        if let Some(lock) = self.map.get(&key, &guard) {
            let entry = lock.read();
            if entry.is_expired() {
                drop(entry);
                self.map.remove(&key, &guard);
                self.drop_tier_copy(&key);
            } else {
                present = true;
            }
        }
        if !present {
            present = self.try_promote_locked(&key);
        }
        let lock =
            self.map.get_or_insert_with(key, || RwLock::new(Entry::new(default(), None, None)), &guard);
        let mut entry = lock.write();
        entry.touch();
        let mut exp = entry.expires_instant();
        let result = f(&mut entry.value, &mut exp, present);
        entry.expires_ms = exp.map(instant_to_ms).unwrap_or(u64::MAX);
        result
    }

    /// Move a key out of RAM and into the disk tier (if configured).
    /// Returns `false` if there's no tiering handler, the key doesn't
    /// exist, or it had already expired.
    pub fn evict_to_tier(&self, key: &[u8]) -> bool {
        self.evict_to_tier_sized(key).is_some()
    }

    /// Same as `evict_to_tier`, but also reports the freed entry's
    /// approximate RAM footprint on success — lets a caller evicting many
    /// keys in a row track a running total locally instead of recomputing
    /// `Store::approx_bytes()` (an O(total keys) scan) after every single
    /// eviction.
    pub fn evict_to_tier_sized(&self, key: &[u8]) -> Option<usize> {
        self.evict_to_tier_result(key).ok().flatten()
    }

    /// `evict_to_tier_sized` with the tier's error surfaced — callers that
    /// enforce `tiered-disk-limit` need to tell `TIERED_FULL` (stop
    /// asking, the capacity gate handles it) apart from an I/O failure.
    /// `Ok(None)` = key gone already (expired/removed concurrently);
    /// `Err` = key stays in RAM, nothing was written to the tier.
    pub fn evict_to_tier_result(&self, key: &[u8]) -> Result<Option<usize>, String> {
        let _tier_guard = self.tier_guard();
        let Some(handler) = self.tiering.as_ref() else { return Ok(None) };
        let guard = self.map.guard();
        let Some(lock) = self.map.get(key, &guard) else { return Ok(None) };
        let entry = lock.write();
        if entry.is_expired() {
            drop(entry);
            self.map.remove(key, &guard);
            self.drop_tier_copy(key);
            return Ok(None);
        }
        let ttl = entry.remaining_ttl();
        if let Err(error) = handler.evict(&Bytes::copy_from_slice(key), &entry.value, ttl) {
            if error != TIERED_FULL {
                tracing::warn!(?key, %error, "failed to persist evicted key; keeping it in RAM");
            }
            return Err(error);
        }
        let freed = key.len() + 48 + entry.value.approx_bytes();
        drop(entry);
        self.map.remove(key, &guard);
        Ok(Some(freed))
    }

    /// Drop a key if its container value has become empty (see
    /// `Value::is_empty_container`). Call after pop/remove-style mutations.
    pub fn remove_if_empty(&self, key: &[u8]) {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        let empty =
            self.map.get(key, &guard).map(|lock| lock.read().value.is_empty_container()).unwrap_or(false);
        if empty {
            self.map.remove(key, &guard);
            self.drop_tier_copy(key);
        } else if self.map.get(key, &guard).is_none() {
            if let Some(handler) = &self.tiering {
                handler.remove(key);
            }
        }
    }

    /// All keys currently stored (no expiry filtering) — used by `SCAN`-family
    /// commands. Phase 2's scan is a simple single-pass snapshot rather than
    /// a true resumable cursor over a resizing hash table.
    pub fn keys_snapshot(&self) -> Vec<Bytes> {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        let mut keys: std::collections::HashSet<Bytes> = self.map.keys(&guard).cloned().collect();
        if let Some(handler) = &self.tiering {
            keys.extend(handler.keys_snapshot());
        }
        keys.into_iter().collect()
    }

    /// Keys currently resident in RAM, used by eviction selection.
    pub fn ram_keys_snapshot(&self) -> Vec<Bytes> {
        let guard = self.map.guard();
        self.map.keys(&guard).cloned().collect()
    }

    /// `(last_access, access_count)` for `maxmemory-policy
    /// allkeys-lru`/`-lfu`/`volatile-lru`/`-lfu` to rank eviction
    /// candidates by — `None` if the key doesn't exist (it may have
    /// expired/been evicted between `keys_snapshot` and this call under
    /// concurrent access, which is fine, the caller just skips it).
    pub fn access_stats(&self, key: &[u8]) -> Option<(Instant, u64)> {
        let guard = self.map.guard();
        self.map.get(key, &guard).map(|lock| {
            let entry = lock.read();
            (ms_to_instant(entry.last_access_secs as u64 * 1000), entry.access_count as u64)
        })
    }

    pub fn dbsize(&self) -> usize {
        self.keys_snapshot().into_iter().filter(|key| self.exists(std::slice::from_ref(key)) > 0).count()
    }

    pub fn flush(&self) {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        self.map.clear(&guard);
        if let Some(handler) = &self.tiering {
            handler.clear();
        }
    }

    /// Active expire cycle: sample up to `sample_size` entries and evict any
    /// that have expired. Returns the number of keys removed. Intended to be
    /// called periodically from a background tick.
    pub fn active_expire_cycle(&self, sample_size: usize) -> usize {
        let _tier_guard = self.tier_guard();
        let guard = self.map.guard();
        let len = self.map.len();
        if len == 0 || sample_size == 0 {
            return 0;
        }
        let start = self.expire_cursor.fetch_add(sample_size, Ordering::Relaxed) % len;
        let expired: Vec<Bytes> = self
            .map
            .iter(&guard)
            .skip(start)
            .take(sample_size)
            .filter(|(_, lock)| lock.read().is_expired())
            .map(|(k, _)| k.clone())
            .collect();
        let removed = expired.len();
        for key in &expired {
            self.map.remove(key, &guard);
            self.drop_tier_copy(key);
        }
        removed
    }

    /// Rough total memory estimate across every key in this database, for
    /// `maxmemory` accounting. `O(n)` — meant to be called periodically from
    /// a background tick, not on every write.
    pub fn approx_bytes(&self) -> usize {
        let guard = self.map.guard();
        self.map.iter(&guard).map(|(k, lock)| k.len() + 48 + lock.read().value.approx_bytes()).sum()
    }

    /// Every live (non-expired) entry plus its remaining TTL, for `SAVE`/
    /// `BGSAVE`-style snapshotting. The TTL is captured as a relative
    /// duration since persisted snapshots must survive a process restart,
    /// across which the monotonic clock behind `Instant` is meaningless.
    pub fn snapshot_entries(&self) -> Vec<(Bytes, Value, Option<Duration>)> {
        let guard = self.map.guard();
        self.map
            .iter(&guard)
            .filter_map(|(k, lock)| {
                let entry = lock.read();
                if entry.is_expired() {
                    return None;
                }
                let ttl = entry.remaining_ttl();
                Some((k.clone(), entry.value.clone(), ttl))
            })
            .collect()
    }

    /// Insert a key loaded from a snapshot, re-anchoring its relative TTL
    /// (if any) to `Instant::now()`. Sliding-TTL mode doesn't currently
    /// survive a save/load round trip.
    pub fn load_entry(&self, key: Bytes, value: Value, ttl: Option<Duration>) {
        let _tier_guard = self.tier_guard();
        if let Some(handler) = &self.tiering {
            handler.remove(&key);
        }
        let expires_at = ttl.map(saturating_deadline);
        let guard = self.map.guard();
        self.map.insert(key, RwLock::new(Entry::new(value, expires_at, None)), &guard);
    }
}

/// The full set of logical databases, indexed by `SELECT`.
pub struct Store {
    dbs: Vec<Db>,
    /// The unscoped engine handle for cross-db operations (capacity
    /// accounting, GC, compaction, `evict-oldest` victim selection).
    /// Per-db access goes through each `Db`'s `DbScopedTier` wrapper.
    tier: Option<Arc<dyn TieringHandler>>,
}

impl Store {
    pub fn new(num_dbs: usize) -> Self {
        let dbs = (0..num_dbs.max(1)).map(|_| Db::new()).collect();
        Self { dbs, tier: None }
    }

    /// Every database shares the same underlying tiering engine, wrapped
    /// per-db in `DbScopedTier` so keys can't collide across `SELECT`ed
    /// databases. `tier` keeps the raw handle for engine-global ops.
    pub fn new_with_tiering(num_dbs: usize, handler: Arc<dyn TieringHandler>) -> Self {
        let dbs = (0..num_dbs.max(1))
            .map(|i| Db::with_tiering(Arc::new(DbScopedTier { db_index: i as u32, inner: handler.clone() })))
            .collect();
        Self { dbs, tier: Some(handler) }
    }

    pub fn len(&self) -> usize {
        self.dbs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dbs.is_empty()
    }

    pub fn db(&self, index: usize) -> Option<&Db> {
        self.dbs.get(index)
    }

    /// Engine-global views — `tiered_oldest_keys`/`remove_tiered_key_outright`
    /// work on the unscoped (db-prefixed) key namespace, which is fine
    /// since their only caller (`drain_tiered_disk`) treats keys opaquely.
    pub fn tiered_disk_usage_bytes(&self) -> Option<u64> {
        self.tier.as_ref()?.disk_usage_bytes()
    }

    /// `TieringHandler::can_accept` — `true` with no tier attached.
    pub fn tiered_can_accept(&self, additional: u64) -> bool {
        self.tier.as_ref().map(|t| t.can_accept(additional)).unwrap_or(true)
    }

    pub fn tiered_keys_snapshot(&self) -> Vec<Bytes> {
        self.tier.as_ref().map(|t| t.keys_snapshot()).unwrap_or_default()
    }

    pub fn tiered_oldest_keys(&self, max: usize) -> Vec<Bytes> {
        self.tier.as_ref().map(|t| t.oldest_keys(max)).unwrap_or_default()
    }

    pub fn remove_tiered_key_outright(&self, key: &[u8]) -> bool {
        let Some(tier) = &self.tier else { return false };
        if !tier.contains(key) {
            return false;
        }
        tier.remove(key);
        true
    }

    pub fn tiered_sweep_expired(&self) -> usize {
        self.tier.as_ref().map(|t| t.sweep_expired()).unwrap_or(0)
    }

    pub fn tiered_compact(&self) -> Result<(), String> {
        self.tier.as_ref().map(|t| t.compact()).unwrap_or(Ok(()))
    }

    pub fn tiered_stats(&self) -> TieredStats {
        self.tier.as_ref().map(|t| t.stats()).unwrap_or_default()
    }

    /// Run an active expire cycle across every database.
    pub fn active_expire_cycle(&self, sample_size: usize) -> usize {
        self.dbs.iter().map(|db| db.active_expire_cycle(sample_size)).sum()
    }

    /// Rough total memory estimate across every database, for `maxmemory`
    /// accounting (see `Db::approx_bytes`).
    pub fn approx_bytes(&self) -> usize {
        self.dbs.iter().map(|db| db.approx_bytes()).sum()
    }
}

#[cfg(test)]
mod tests {
    use dashmap::DashMap;

    use super::*;

    #[test]
    fn set_get_roundtrip() {
        let db = Db::new();
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), None);
        assert_eq!(db.get(b"k"), Some(Value::String(Bytes::from("v"))));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let db = Db::new();
        assert_eq!(db.get(b"missing"), None);
    }

    /// Regression test for the `Mutex` -> `RwLock` change: many threads
    /// doing `with_value`/`get` reads against the *same* key concurrently
    /// must all see the correct value and must not deadlock or panic (e.g.
    /// from a reentrant `read()` call while a `write()` guard from
    /// `touch_entry` is still held somewhere on the same thread).
    #[test]
    fn concurrent_reads_of_the_same_key_see_the_correct_value_and_dont_deadlock() {
        let db = Arc::new(Db::new());
        db.set(Bytes::from("hot"), Value::List(List::from(vec![Bytes::from("a"), Bytes::from("b")])), None);

        let handles: Vec<_> = (0..16)
            .map(|_| {
                let db = db.clone();
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let len = db.with_value(b"hot", |v| match v {
                            Value::List(l) => l.len(),
                            _ => unreachable!(),
                        });
                        assert_eq!(len, Some(2));
                        assert_eq!(
                            db.get(b"hot"),
                            Some(Value::List(List::from(vec![Bytes::from("a"), Bytes::from("b"),])))
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Uncontended afterward, so the best-effort LRU bookkeeping in
        // `touch_entry` should have landed at least once.
        let (_, access_count) = db.access_stats(b"hot").unwrap();
        assert!(access_count > 0);
    }

    #[test]
    fn expired_key_is_evicted_on_read() {
        let db = Db::new();
        let past = Instant::now() - Duration::from_secs(1);
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), Some(past));
        assert_eq!(db.get(b"k"), None);
        assert_eq!(db.dbsize(), 0);
    }

    #[test]
    fn take_moves_owned_value_and_expiry_without_leaving_source_key() {
        let db = Db::new();
        let expiry = Instant::now() + Duration::from_secs(30);
        let value = Value::List(List::from(vec![Bytes::from("a"), Bytes::from("b")]));
        db.set(Bytes::from("source"), value.clone(), Some(expiry));

        let (taken, taken_expiry) = db.take(b"source").unwrap();
        assert_eq!(taken, value);
        // `Entry` stores ms, so the round-tripped instant isn't bit-exact —
        // within a couple ms of the original is the semantic contract.
        let drift = taken_expiry.unwrap().max(expiry) - taken_expiry.unwrap().min(expiry);
        assert!(drift <= Duration::from_millis(2), "expiry drifted {drift:?}");
        assert_eq!(db.exists(&[Bytes::from("source")]), 0);
    }

    #[test]
    fn ttl_reports_persistent_and_expiring() {
        let db = Db::new();
        db.set(Bytes::from("a"), Value::String(Bytes::from("1")), None);
        assert_eq!(db.ttl(b"a"), Ttl::Persistent);

        let future = Instant::now() + Duration::from_secs(10);
        db.expire(b"a", Some(future));
        assert!(matches!(db.ttl(b"a"), Ttl::Expires(_)));

        assert_eq!(db.ttl(b"nope"), Ttl::NoKey);
    }

    #[test]
    fn sliding_ttl_resets_on_read_access() {
        let db = Db::new();
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), None);
        assert!(db.expire_sliding(b"k", Duration::from_secs(100)));

        // Simulate time having passed by forcing a short-lived expiry
        // directly, then confirm a read slides the TTL back up near 100s.
        {
            let guard = db.map.guard();
            if let Some(lock) = db.map.get(b"k".as_slice(), &guard) {
                lock.write().expires_ms = instant_to_ms(Instant::now() + Duration::from_millis(10));
            }
        }
        assert!(matches!(db.ttl(b"k"), Ttl::Expires(d) if d.as_millis() <= 10));
        db.get(b"k"); // a read should slide the TTL back up
        assert!(matches!(db.ttl(b"k"), Ttl::Expires(d) if d.as_secs() > 90));
    }

    #[test]
    fn persist_clears_sliding_ttl() {
        let db = Db::new();
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), None);
        db.expire_sliding(b"k", Duration::from_secs(100));
        db.expire(b"k", None); // PERSIST goes through the plain `expire` path
        assert_eq!(db.ttl(b"k"), Ttl::Persistent);
        {
            let guard = db.map.guard();
            if let Some(lock) = db.map.get(b"k".as_slice(), &guard) {
                lock.write().expires_ms = instant_to_ms(Instant::now() + Duration::from_millis(10));
            }
        }
        db.get(b"k");
        // No sliding mode anymore, so the short TTL set above should stick.
        assert!(matches!(db.ttl(b"k"), Ttl::Expires(d) if d.as_millis() <= 10));
    }

    #[test]
    fn del_and_exists() {
        let db = Db::new();
        db.set(Bytes::from("a"), Value::String(Bytes::from("1")), None);
        db.set(Bytes::from("b"), Value::String(Bytes::from("2")), None);
        assert_eq!(db.exists(&[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]), 2);
        assert_eq!(db.del(&[Bytes::from("a"), Bytes::from("c")]), 1);
        assert_eq!(db.dbsize(), 1);
    }

    #[test]
    fn del_sized_reports_freed_ram_footprint() {
        let db = Db::new();
        db.set(Bytes::from("large"), Value::String(Bytes::from(vec![b'x'; 4096])), None);
        let (removed, freed) = db.del_sized(&[Bytes::from("large")]);
        assert_eq!(removed, 1);
        assert!(freed >= 4096);
        assert_eq!(db.dbsize(), 0);
    }

    #[test]
    fn active_expire_cycle_removes_expired_keys() {
        let db = Db::new();
        let past = Instant::now() - Duration::from_secs(1);
        db.set(Bytes::from("a"), Value::String(Bytes::from("1")), Some(past));
        db.set(Bytes::from("b"), Value::String(Bytes::from("2")), None);
        let removed = db.active_expire_cycle(20);
        assert_eq!(removed, 1);
        assert_eq!(db.dbsize(), 1);
    }

    #[test]
    fn with_entry_mut_creates_then_reuses_container() {
        let db = Db::new();
        db.with_entry_mut(
            Bytes::from("list"),
            || Value::List(List::new()),
            |v| {
                if let Value::List(l) = v {
                    l.push_back(Bytes::from("a"));
                }
            },
        );
        db.with_entry_mut(
            Bytes::from("list"),
            || Value::List(List::new()),
            |v| {
                if let Value::List(l) = v {
                    l.push_back(Bytes::from("b"));
                }
            },
        );
        let items: Vec<Bytes> = db
            .with_value(b"list", |v| match v {
                Value::List(l) => l.iter().cloned().collect(),
                _ => unreachable!(),
            })
            .unwrap();
        assert_eq!(items, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn remove_if_empty_deletes_emptied_containers() {
        let db = Db::new();
        db.set(Bytes::from("s"), Value::Set(Set::new()), None);
        db.remove_if_empty(b"s");
        assert_eq!(db.get(b"s"), None);
    }

    /// A trivial in-memory stand-in for a real disk-backed `TieringHandler`,
    /// just so this crate can test the promote/evict contract without
    /// depending on the `tiered` crate (which depends on `keyspace`, not
    /// the other way around).
    #[derive(Default)]
    struct MockTier {
        store: DashMap<Bytes, (Value, Option<Duration>)>,
    }

    impl TieringHandler for MockTier {
        fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String> {
            self.store.insert(key.clone(), (value.clone(), ttl));
            Ok(())
        }
        fn fetch(&self, key: &[u8]) -> Option<(Value, Option<Duration>)> {
            self.store.get(key).map(|v| v.clone())
        }
        fn contains(&self, key: &[u8]) -> bool {
            self.store.contains_key(key)
        }
        fn remove(&self, key: &[u8]) {
            self.store.remove(key);
        }
        fn keys_snapshot(&self) -> Vec<Bytes> {
            self.store.iter().map(|entry| entry.key().clone()).collect()
        }
    }

    #[test]
    fn evict_to_tier_then_get_transparently_promotes() {
        let tier = Arc::new(MockTier::default());
        let db = Db::with_tiering(tier.clone());
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), None);

        assert!(db.evict_to_tier(b"k"));
        assert_eq!(db.dbsize(), 1);
        assert!(tier.store.contains_key(b"k".as_slice()));

        // GET transparently rehydrates it into RAM — and the tier keeps
        // its copy: that's the durable record, RAM is just a read cache.
        assert_eq!(db.get(b"k"), Some(Value::String(Bytes::from("v"))));
        assert_eq!(db.dbsize(), 1);
        assert!(tier.store.contains_key(b"k".as_slice()));
    }

    #[test]
    fn exists_is_true_for_tiered_keys_without_promoting() {
        let tier = Arc::new(MockTier::default());
        let db = Db::with_tiering(tier.clone());
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), None);
        db.evict_to_tier(b"k");

        assert_eq!(db.exists(&[Bytes::from("k")]), 1);
        // Still on disk, not pulled back into RAM just to answer EXISTS.
        assert_eq!(db.dbsize(), 1);
        assert!(tier.store.contains_key(b"k".as_slice()));
    }

    #[test]
    fn del_removes_a_tiered_only_key() {
        let tier = Arc::new(MockTier::default());
        let db = Db::with_tiering(tier.clone());
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), None);
        db.evict_to_tier(b"k");

        assert_eq!(db.del(&[Bytes::from("k")]), 1);
        assert!(!tier.store.contains_key(b"k".as_slice()));
        assert_eq!(db.exists(&[Bytes::from("k")]), 0);
    }

    /// A promoted (RAM+tier coexisting) key deleted via `DEL` must drop
    /// the tier copy too — otherwise the older tiered value would
    /// resurrect on the next RAM-miss `fetch`.
    #[test]
    fn del_on_a_coexisting_key_clears_the_tier_copy() {
        let tier = Arc::new(MockTier::default());
        let db = Db::with_tiering(tier.clone());
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), None);
        db.evict_to_tier(b"k");
        assert_eq!(db.get(b"k"), Some(Value::String(Bytes::from("v")))); // promotes into RAM
        assert!(tier.store.contains_key(b"k".as_slice())); // tier copy stays (durable record)

        assert_eq!(db.del(&[Bytes::from("k")]), 1);
        assert!(!tier.store.contains_key(b"k".as_slice()));
        assert_eq!(db.get(b"k"), None);
        assert_eq!(db.exists(&[Bytes::from("k")]), 0);
    }

    #[test]
    fn with_entry_mut_promotes_a_tiered_collection_before_mutating() {
        let tier = Arc::new(MockTier::default());
        let db = Db::with_tiering(tier.clone());
        db.with_entry_mut(
            Bytes::from("l"),
            || Value::List(List::new()),
            |v| {
                if let Value::List(l) = v {
                    l.push_back(Bytes::from("a"));
                }
            },
        );
        db.evict_to_tier(b"l");

        db.with_entry_mut(
            Bytes::from("l"),
            || Value::List(List::new()),
            |v| {
                if let Value::List(l) = v {
                    l.push_back(Bytes::from("b"));
                }
            },
        );

        let items: Vec<Bytes> = db
            .with_value(b"l", |v| match v {
                Value::List(l) => l.iter().cloned().collect(),
                _ => unreachable!(),
            })
            .unwrap();
        assert_eq!(items, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    /// All DBs share one physical tier — `DbScopedTier` namespaces keys
    /// so `db0.same` and `db1.same` don't overwrite each other on disk.
    #[test]
    fn tier_entries_are_scoped_per_db() {
        let tier = Arc::new(MockTier::default());
        let store = Store::new_with_tiering(2, tier.clone());
        let db0 = store.db(0).unwrap();
        let db1 = store.db(1).unwrap();
        db0.set(Bytes::from("same"), Value::String(Bytes::from("zero")), None);
        db1.set(Bytes::from("same"), Value::String(Bytes::from("one")), None);

        assert!(db0.evict_to_tier(b"same"));
        assert!(db1.evict_to_tier(b"same"));
        assert_eq!(tier.store.len(), 2);

        assert_eq!(db0.get(b"same"), Some(Value::String(Bytes::from("zero"))));
        assert_eq!(db1.get(b"same"), Some(Value::String(Bytes::from("one"))));
    }
}
