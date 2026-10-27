//! In-memory keyspace: sharded-by-DB storage with TTL semantics.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use papaya::HashMap as PapayaMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

mod stream;
mod zset;
pub use stream::{parse_for_add, ConsumerGroup, IdSpec, PendingEntry, Stream, StreamId, XaddError};
pub use zset::ZSet;

pub type List = VecDeque<Bytes>;
pub type Hash = HashMap<Bytes, Bytes>;
pub type Set = HashSet<Bytes>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    String(Bytes),
    List(List),
    Hash(Hash),
    Set(Set),
    ZSet(ZSet),
    Stream(Stream),
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
    /// access counter — not Redis's actual probabilistic-decay LFU counter
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
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>);
    /// Fetch (and remove from the tier — promotion is a one-way move back
    /// into RAM, not a cache copy) a previously evicted key.
    fn promote(&self, key: &[u8]) -> Option<(Value, Option<Duration>)>;
    /// Whether the tier holds this key, without promoting it — used by
    /// `EXISTS` so it doesn't have to pull the whole value off disk just to
    /// answer a boolean question.
    fn contains(&self, key: &[u8]) -> bool;
    /// Drop a key from the tier outright (`DEL` on a tiered-only key).
    fn remove(&self, key: &[u8]);
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
/// each entry is individually wrapped in a `Mutex` — contention on that
/// per-key lock only happens when the exact same key is hit concurrently,
/// same as `DashMap`'s per-key behavior once you're past its shard lock.
#[derive(Default)]
pub struct Db {
    map: PapayaMap<Bytes, Mutex<Entry>>,
    tiering: Option<Arc<dyn TieringHandler>>,
}

impl Db {
    pub fn new() -> Self {
        Self { map: PapayaMap::new(), tiering: None }
    }

    /// Like `new`, but keys evicted via `evict_to_tier` land in `handler`
    /// instead of being dropped, and reads transparently promote them back.
    pub fn with_tiering(handler: Arc<dyn TieringHandler>) -> Self {
        Self { map: PapayaMap::new(), tiering: Some(handler) }
    }

    /// Set a key to a value, optionally with an absolute expiry instant.
    /// Passing `None` clears any existing TTL (matches `SET` semantics).
    /// Always clears sliding-TTL mode — a plain `SET` is a fresh value.
    pub fn set(&self, key: Bytes, value: Value, expires_at: Option<Instant>) {
        let guard = self.map.guard();
        self.map.insert(key, Mutex::new(Entry::new(value, expires_at, None)), &guard);
    }

    /// On a RAM miss, ask the tiering handler (if any) for the key and, if
    /// found, rehydrate it into RAM. Returns whether a promotion happened.
    /// Sliding TTLs don't currently survive a trip through the disk tier.
    fn try_promote(&self, key: &[u8]) -> bool {
        let Some(handler) = &self.tiering else { return false };
        let Some((value, ttl)) = handler.promote(key) else { return false };
        let expires_at = ttl.map(saturating_deadline);
        let guard = self.map.guard();
        self.map.insert(Bytes::copy_from_slice(key), Mutex::new(Entry::new(value, expires_at, None)), &guard);
        true
    }

    /// Passive expiry: look up a key, evicting it first if its TTL has
    /// elapsed. Falls back to the disk tier (if configured) on a RAM miss.
    /// Resets a sliding TTL (if any) on this access. Returns `None` for
    /// missing-or-expired keys.
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let guard = self.map.guard();
        if let Some(lock) = self.map.get(key, &guard) {
            let mut entry = lock.lock();
            if entry.is_expired() {
                drop(entry);
                self.map.remove(key, &guard);
                return None;
            }
            entry.touch();
            return Some(entry.value.clone());
        }
        drop(guard);
        if self.try_promote(key) {
            let guard = self.map.guard();
            return self.map.get(key, &guard).map(|lock| lock.lock().value.clone());
        }
        None
    }

    pub fn del(&self, keys: &[Bytes]) -> usize {
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
                    self.map.get(k.as_ref(), &guard).map(|lock| !lock.lock().is_expired()).unwrap_or(false);
                in_ram || self.tiering.as_ref().is_some_and(|h| h.contains(k))
            })
            .count()
    }

    pub fn ttl(&self, key: &[u8]) -> Ttl {
        let guard = self.map.guard();
        match self.map.get(key, &guard) {
            None => Ttl::NoKey,
            Some(lock) => {
                let entry = lock.lock();
                if entry.is_expired() {
                    drop(entry);
                    self.map.remove(key, &guard);
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
                let mut entry = lock.lock();
                if entry.is_expired() {
                    false
                } else {
                    entry.expires_at = expires_at;
                    entry.sliding = None;
                    true
                }
            }
            None => false,
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
                let mut entry = lock.lock();
                if entry.is_expired() {
                    false
                } else {
                    entry.expires_at = Some(saturating_deadline(duration));
                    entry.sliding = Some(duration);
                    true
                }
            }
            None => false,
        }
    }

    /// Current expiry of a key, for implementing `SET ... KEEPTTL`.
    /// Returns `None` if the key doesn't exist (or just expired), `Some(None)`
    /// if it exists with no TTL, `Some(Some(instant))` if it has one.
    pub fn current_expiry(&self, key: &[u8]) -> Option<Option<Instant>> {
        let guard = self.map.guard();
        match self.map.get(key, &guard) {
            Some(lock) => {
                let entry = lock.lock();
                if entry.is_expired() {
                    None
                } else {
                    Some(entry.expires_at)
                }
            }
            None => None,
        }
    }

    /// Read-only access to a key's value, with passive expiry applied first
    /// and a tiered-storage promotion attempt on a RAM miss. Resets a
    /// sliding TTL (if any) on this access.
    pub fn with_value<R>(&self, key: &[u8], f: impl FnOnce(&Value) -> R) -> Option<R> {
        let guard = self.map.guard();
        if let Some(lock) = self.map.get(key, &guard) {
            let mut entry = lock.lock();
            if entry.is_expired() {
                drop(entry);
                self.map.remove(key, &guard);
                return None;
            }
            entry.touch();
            return Some(f(&entry.value));
        }
        drop(guard);
        if self.try_promote(key) {
            let guard = self.map.guard();
            return self.map.get(key, &guard).map(|lock| f(&lock.lock().value));
        }
        None
    }

    /// Mutable access to an existing key's value, with passive expiry and
    /// promotion applied the same way as `with_value`. Does *not* create the
    /// key if it's missing from both RAM and the tier — see `with_entry_mut`.
    pub fn with_value_mut<R>(&self, key: &[u8], f: impl FnOnce(&mut Value) -> R) -> Option<R> {
        let guard = self.map.guard();
        if let Some(lock) = self.map.get(key, &guard) {
            let mut entry = lock.lock();
            if entry.is_expired() {
                drop(entry);
                self.map.remove(key, &guard);
                return None;
            }
            entry.touch();
            return Some(f(&mut entry.value));
        }
        drop(guard);
        if self.try_promote(key) {
            let guard = self.map.guard();
            return self.map.get(key, &guard).map(|lock| f(&mut lock.lock().value));
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
        let guard = self.map.guard();
        // Hot path (key already present and live, which is the common case
        // once a collection key has been created) only needs this one
        // lookup: fold the expiry check and the "does it still exist"
        // question into the same `get`, instead of a separate
        // `contains_key` call right after.
        let mut present = false;
        if let Some(lock) = self.map.get(&key, &guard) {
            let entry = lock.lock();
            if entry.is_expired() {
                drop(entry);
                self.map.remove(&key, &guard);
            } else {
                present = true;
            }
        }
        if !present {
            self.try_promote(&key);
        }
        let lock = self.map.get_or_insert_with(key, || Mutex::new(Entry::new(default(), None, None)), &guard);
        let mut entry = lock.lock();
        entry.touch();
        f(&mut entry.value)
    }

    /// Move a key out of RAM and into the disk tier (if configured).
    /// Returns `false` if there's no tiering handler, the key doesn't
    /// exist, or it had already expired.
    pub fn evict_to_tier(&self, key: &[u8]) -> bool {
        let Some(handler) = &self.tiering else { return false };
        let guard = self.map.guard();
        let Some(lock) = self.map.get(key, &guard) else { return false };
        let entry = lock.lock();
        if entry.is_expired() {
            drop(entry);
            self.map.remove(key, &guard);
            return false;
        }
        let ttl = entry.expires_at.map(|at| at.saturating_duration_since(Instant::now()));
        handler.evict(&Bytes::copy_from_slice(key), &entry.value, ttl);
        drop(entry);
        self.map.remove(key, &guard);
        true
    }

    /// Drop a key if its container value has become empty (see
    /// `Value::is_empty_container`). Call after pop/remove-style mutations.
    pub fn remove_if_empty(&self, key: &[u8]) {
        let guard = self.map.guard();
        let empty =
            self.map.get(key, &guard).map(|lock| lock.lock().value.is_empty_container()).unwrap_or(false);
        if empty {
            self.map.remove(key, &guard);
        }
    }

    /// All keys currently stored (no expiry filtering) — used by `SCAN`-family
    /// commands. Phase 2's scan is a simple single-pass snapshot rather than
    /// a true resumable cursor over a resizing hash table.
    pub fn keys_snapshot(&self) -> Vec<Bytes> {
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
            let entry = lock.lock();
            (entry.last_access, entry.access_count)
        })
    }

    pub fn dbsize(&self) -> usize {
        self.map.len()
    }

    pub fn flush(&self) {
        let guard = self.map.guard();
        self.map.clear(&guard);
    }

    /// Active expire cycle: sample up to `sample_size` entries and evict any
    /// that have expired. Returns the number of keys removed. Intended to be
    /// called periodically from a background tick.
    pub fn active_expire_cycle(&self, sample_size: usize) -> usize {
        let guard = self.map.guard();
        let expired: Vec<Bytes> = self
            .map
            .iter(&guard)
            .take(sample_size)
            .filter(|(_, lock)| lock.lock().is_expired())
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
        self.map.iter(&guard).map(|(k, lock)| k.len() + 48 + lock.lock().value.approx_bytes()).sum()
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
                let entry = lock.lock();
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
        let expires_at = ttl.map(saturating_deadline);
        let guard = self.map.guard();
        self.map.insert(key, Mutex::new(Entry::new(value, expires_at, None)), &guard);
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
                lock.lock().expires_at = Some(Instant::now() + Duration::from_millis(10));
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
                lock.lock().expires_at = Some(Instant::now() + Duration::from_millis(10));
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
        fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) {
            self.store.insert(key.clone(), (value.clone(), ttl));
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
    }

    #[test]
    fn evict_to_tier_then_get_transparently_promotes() {
        let tier = Arc::new(MockTier::default());
        let db = Db::with_tiering(tier.clone());
        db.set(Bytes::from("k"), Value::String(Bytes::from("v")), None);

        assert!(db.evict_to_tier(b"k"));
        assert_eq!(db.dbsize(), 0);
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
        assert_eq!(db.dbsize(), 0);
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
