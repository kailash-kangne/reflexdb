//! In-memory keyspace: sharded-by-DB storage with TTL semantics.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use indexmap::IndexSet;
use papaya::HashMap as PapayaMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

mod stream;
mod zset;
pub use stream::{parse_for_add, ConsumerGroup, IdSpec, PendingEntry, Stream, StreamId, XaddError};
pub use zset::ZSet;

pub type List = VecDeque<Bytes>;
pub type Hash = HashMap<Bytes, Bytes>;
/// Insertion-order-preserving set, not a plain `HashSet`: `SPOP` needs O(1)
/// "pick a random element and remove it" (pick a random index, then
/// `swap_remove_index`), which a `HashSet` can't do without an O(n) scan —
/// see `commands::set::cmd_spop`.
pub type Set = IndexSet<Bytes>;

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
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
            Value::Stream(_) => "stream",
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
            Value::String(_) => false,
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
            Value::List(l) => l.iter().map(|b| b.len() + 16).sum(),
            Value::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 32).sum(),
            Value::Set(s) => s.iter().map(|b| b.len() + 16).sum(),
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
    let now = Instant::now();
    if let Some(t) = now.checked_add(dur) {
        return t;
    }
    let mut probe = dur;
    loop {
        probe /= 2;
        if probe.is_zero() {
            return now;
        }
        if let Some(t) = now.checked_add(probe) {
            return t;
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
    /// Set by `EXPIRE key seconds SLIDING`: the original duration to reset
    /// `expires_at` back to on every read. `PERSIST` (or a plain `EXPIRE`
    /// without `SLIDING`) clears this.
    sliding: Option<Duration>,
    /// For `maxmemory-policy allkeys-lru`/`volatile-lru`: when this key was
    /// last read or written. Updated in `touch()`, same call sites as the
    /// sliding-TTL reset.
    last_access: Instant,
    /// For `allkeys-lfu`/`volatile-lfu`: a plain monotonically-increasing
    /// access counter — a simplified LFU approximation
    /// (that needs a background decay task we don't have), just "fewest
    /// accesses wins" as the eviction tie-break.
    access_count: u64,
}

impl Entry {
    fn new(value: Value, expires_at: Option<Instant>, sliding: Option<Duration>) -> Self {
        Self { value, expires_at, sliding, last_access: Instant::now(), access_count: 0 }
    }

    fn is_expired(&self) -> bool {
        matches!(self.expires_at, Some(at) if at <= Instant::now())
    }

    /// Reset the sliding-TTL countdown on read access, if enabled, and bump
    /// the LRU/LFU access bookkeeping — called from every read/write path
    /// that touches an existing entry.
    fn touch(&mut self) {
        if let Some(duration) = self.sliding {
            self.expires_at = Some(saturating_deadline(duration));
        }
        self.last_access = Instant::now();
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
pub trait TieringHandler: Send + Sync {
    /// Persist a key being dropped from RAM.
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String>;
    /// Fetch (and remove from the tier — promotion is a one-way move back
    /// into RAM, not a cache copy) a previously evicted key.
    fn promote(&self, key: &[u8]) -> Option<(Value, Option<Duration>)>;
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
    /// Remove every key held in the tier.
    fn clear(&self) {
        for key in self.keys_snapshot() {
            self.remove(&key);
        }
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
    /// Also drops any stale copy of `key` from the disk tier first: unlike
    /// `with_entry_mut` (which promotes before mutating), this writes
    /// straight into RAM, so without this it would be possible for a
    /// `SET`/`RENAME`/`RESTORE` on a currently-tiered key to leave RAM and
    /// the tier both holding a copy — breaking the "never coexist"
    /// invariant `DEL`/`EXISTS`/`GET` rely on (a later `DEL` would only
    /// clear the RAM copy, leaving the stale tiered value to resurrect on
    /// the next `GET`).
    pub fn set(&self, key: Bytes, value: Value, expires_at: Option<Instant>) {
        let _tier_guard = self.tier_ops.lock();
        if let Some(handler) = &self.tiering {
            handler.remove(&key);
        }
        let guard = self.map.guard();
        self.map.insert(key, RwLock::new(Entry::new(value, expires_at, None)), &guard);
    }

    /// On a RAM miss, ask the tiering handler (if any) for the key and, if
    /// found, rehydrate it into RAM. Returns whether a promotion happened.
    /// Sliding TTLs don't currently survive a trip through the disk tier.
    fn try_promote(&self, key: &[u8]) -> bool {
        let _tier_guard = self.tier_ops.lock();
        self.try_promote_locked(key)
    }

    fn try_promote_locked(&self, key: &[u8]) -> bool {
        let Some(handler) = &self.tiering else { return false };
        let Some((value, ttl)) = handler.promote(key) else { return false };
        let expires_at = ttl.map(saturating_deadline);
        let guard = self.map.guard();
        self.map.insert(
            Bytes::copy_from_slice(key),
            RwLock::new(Entry::new(value, expires_at, None)),
            &guard,
        );
        true
    }

    /// Remove an expired entry only if it is still the same map entry that
    /// was inspected. A concurrent SET may have replaced it after the
    /// reader dropped its lock.
    fn remove_if_same(&self, key: &[u8], inspected: &RwLock<Entry>, guard: &impl papaya::Guard) {
        if self.map.get(key, guard).is_some_and(|current| std::ptr::eq(current, inspected)) {
            self.map.remove(key, guard);
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
    fn touch_entry(lock: &RwLock<Entry>, sliding: Option<Duration>) {
        if sliding.is_some() {
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
            let sliding = entry.sliding;
            drop(entry);
            Self::touch_entry(lock, sliding);
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
        let _tier_guard = self.tier_ops.lock();
        let guard = self.map.guard();
        let mut removed = 0;
        for key in keys {
            let in_ram = self.map.remove(key.as_ref(), &guard).is_some();
            // Only touch the disk tier when the key wasn't already found in
            // RAM — a tiered key and its RAM copy never coexist (`promote`
            // removes it from the tier), so there's nothing to clean up
            // there once we know it was in RAM.
            let in_tier = !in_ram
                && self
                    .tiering
                    .as_ref()
                    .map(|h| {
                        let was_present = h.contains(key);
                        h.remove(key);
                        was_present
                    })
                    .unwrap_or(false);
            if in_ram || in_tier {
                removed += 1;
            }
        }
        removed
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
                    match entry.expires_at {
                        None => Ttl::Persistent,
                        Some(at) => Ttl::Expires(at.saturating_duration_since(Instant::now())),
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
                    entry.expires_at = expires_at;
                    entry.sliding = None;
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
                    entry.expires_at = Some(saturating_deadline(duration));
                    entry.sliding = Some(duration);
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
                    Some(entry.expires_at)
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
            let sliding = entry.sliding;
            drop(entry);
            Self::touch_entry(lock, sliding);
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
        let _tier_guard = self.tier_ops.lock();
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
        let _tier_guard = self.tier_ops.lock();
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
            } else {
                present = true;
            }
        }
        if !present {
            self.try_promote_locked(&key);
        }
        let lock =
            self.map.get_or_insert_with(key, || RwLock::new(Entry::new(default(), None, None)), &guard);
        let mut entry = lock.write();
        entry.touch();
        f(&mut entry.value)
    }

    /// Move a key out of RAM and into the disk tier (if configured).
    /// Returns `false` if there's no tiering handler, the key doesn't
    /// exist, or it had already expired.
    pub fn evict_to_tier(&self, key: &[u8]) -> bool {
        let _tier_guard = self.tier_ops.lock();
        let Some(handler) = &self.tiering else { return false };
        let guard = self.map.guard();
        let Some(lock) = self.map.get(key, &guard) else { return false };
        let entry = lock.write();
        if entry.is_expired() {
            drop(entry);
            self.map.remove(key, &guard);
            return false;
        }
        let ttl = entry.expires_at.map(|at| at.saturating_duration_since(Instant::now()));
        if let Err(error) = handler.evict(&Bytes::copy_from_slice(key), &entry.value, ttl) {
            tracing::warn!(?key, %error, "failed to persist evicted key; keeping it in RAM");
            return false;
        }
        drop(entry);
        self.map.remove(key, &guard);
        true
    }

    /// Drop a key if its container value has become empty (see
    /// `Value::is_empty_container`). Call after pop/remove-style mutations.
    pub fn remove_if_empty(&self, key: &[u8]) {
        let _tier_guard = self.tier_ops.lock();
        let guard = self.map.guard();
        let empty =
            self.map.get(key, &guard).map(|lock| lock.read().value.is_empty_container()).unwrap_or(false);
        if empty {
            self.map.remove(key, &guard);
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
        let _tier_guard = self.tier_ops.lock();
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
            (entry.last_access, entry.access_count)
        })
    }

    pub fn dbsize(&self) -> usize {
        self.keys_snapshot().into_iter().filter(|key| self.exists(std::slice::from_ref(key)) > 0).count()
    }

    pub fn flush(&self) {
        let _tier_guard = self.tier_ops.lock();
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
        let _tier_guard = self.tier_ops.lock();
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
                let ttl = entry.expires_at.map(|at| at.saturating_duration_since(Instant::now()));
                Some((k.clone(), entry.value.clone(), ttl))
            })
            .collect()
    }

    /// Insert a key loaded from a snapshot, re-anchoring its relative TTL
    /// (if any) to `Instant::now()`. Sliding-TTL mode doesn't currently
    /// survive a save/load round trip.
    pub fn load_entry(&self, key: Bytes, value: Value, ttl: Option<Duration>) {
        let _tier_guard = self.tier_ops.lock();
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
}

impl Store {
    pub fn new(num_dbs: usize) -> Self {
        let dbs = (0..num_dbs.max(1)).map(|_| Db::new()).collect();
        Self { dbs }
    }

    /// Every database shares the same tiering handler — a tiered key still
    /// belongs to its originating db index, same as the doc's "tiered keys
    /// stay owned by their original shard" cluster note.
    pub fn new_with_tiering(num_dbs: usize, handler: Arc<dyn TieringHandler>) -> Self {
        let dbs = (0..num_dbs.max(1)).map(|_| Db::with_tiering(handler.clone())).collect();
        Self { dbs }
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
                lock.write().expires_at = Some(Instant::now() + Duration::from_millis(10));
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
                lock.write().expires_at = Some(Instant::now() + Duration::from_millis(10));
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
        fn promote(&self, key: &[u8]) -> Option<(Value, Option<Duration>)> {
            self.store.remove(key).map(|(_, v)| v)
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

        // GET transparently rehydrates it from the tier.
        assert_eq!(db.get(b"k"), Some(Value::String(Bytes::from("v"))));
        assert_eq!(db.dbsize(), 1);
        assert!(!tier.store.contains_key(b"k".as_slice()));
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

    /// Regression test: `set()` on a key that's currently tiered-only must
    /// drop the stale tier copy instead of leaving it to coexist with the
    /// fresh RAM value — otherwise a later `DEL` (which skips the tier once
    /// it found the key in RAM) leaves the old value on disk to resurrect
    /// on the next `GET`.
    #[test]
    fn set_on_a_tiered_key_drops_the_stale_tier_copy() {
        let tier = Arc::new(MockTier::default());
        let db = Db::with_tiering(tier.clone());
        db.set(Bytes::from("k"), Value::String(Bytes::from("old")), None);
        db.evict_to_tier(b"k");
        assert!(tier.store.contains_key(b"k".as_slice()));

        db.set(Bytes::from("k"), Value::String(Bytes::from("new")), None);
        assert!(!tier.store.contains_key(b"k".as_slice()));

        // DEL only needs to clear RAM now — no ghost left in the tier to
        // resurrect afterward.
        assert_eq!(db.del(&[Bytes::from("k")]), 1);
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
}
