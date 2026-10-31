//! A disk-backed `keyspace::TieringHandler` on top of `fjall` (a pure-Rust,
//! LSM-tree-based embedded store — no C/C++ toolchain needed, unlike
//! RocksDB). See `docs/05-persistence-tiering.md` §5.
//!
//! Values are bincode-encoded entries keyed by the original key bytes, so a
//! promote is a single point lookup. Expiry uses wall-clock deadlines so a
//! tiered key's TTL continues to elapse while the server is stopped.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use keyspace::{TieringHandler, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct TieredEntry {
    value: Value,
    expires_at_ms: Option<u64>,
    /// When this entry was moved into the tier — lets `tiered-disk-limit`
    /// drop the actually-oldest entries first instead of a random pick.
    /// Not updated on `contains()`/`EXISTS` checks (that would turn a
    /// cheap existence check into a disk write, defeating the point of
    /// answering it without promoting), so this is closer to FIFO than a
    /// true access-recency LRU — a tiered key's only other "access",
    /// `promote`, removes it from the tier entirely, so there's no
    /// "touched but stays resident" case to track anyway.
    evicted_at_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct LegacyTieredEntry {
    value: Value,
    ttl_ms: Option<u64>,
}

const TIERED_MAGIC: &[u8] = b"RFXT1";

/// Charged per tiered entry on top of key + payload bytes — approximates
/// each entry's share of LSM record headers and SSTable index space, so a
/// `tiered-disk-limit` reservation accounts for metadata, not just the raw
/// value payload.
const ENTRY_OVERHEAD: u64 = 64;

fn entry_cost(key_len: usize, payload_len: usize) -> u64 {
    key_len as u64 + payload_len as u64 + ENTRY_OVERHEAD
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Keep synchronous LSM work off Tokio worker threads without paying a
/// `spawn_blocking` task handoff for every Redis command. Call this only at
/// the actual tier I/O boundary; RAM hits never enter it.
fn on_blocking_thread<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

fn encode_entry(entry: &TieredEntry) -> Result<Vec<u8>, bincode::Error> {
    let mut payload = TIERED_MAGIC.to_vec();
    payload.extend_from_slice(&bincode::serialize(entry)?);
    Ok(payload)
}

fn decode_entry(payload: &[u8]) -> Result<TieredEntry, bincode::Error> {
    if let Some(encoded) = payload.strip_prefix(TIERED_MAGIC) {
        return bincode::deserialize(encoded);
    }
    // Preserve entries written by the previous format; their stored
    // remaining TTL begins elapsing from their first read after upgrade,
    // and (since the old format didn't track this at all) they look
    // freshly-evicted for age-based eviction purposes until next written.
    let legacy: LegacyTieredEntry = bincode::deserialize(payload)?;
    Ok(TieredEntry {
        value: legacy.value,
        expires_at_ms: legacy.ttl_ms.map(|ttl| now_ms().saturating_add(ttl)),
        evicted_at_ms: now_ms(),
    })
}

pub struct FjallTier {
    keyspace: Keyspace,
    // Keep the handle alive for the process lifetime; fjall persists on
    // drop, but we also fsync explicitly after every write (see `evict`).
    _db: Database,
    // Self-tracked payload-byte total, not `Keyspace::disk_space()` —
    // that only counts data already flushed from fjall's in-memory
    // memtable to an on-disk segment, so under light/bursty write load it
    // can badly undercount (observed: stayed at 0 well past a configured
    // `tiered-disk-limit` in manual testing) and `tiered-disk-limit`
    // enforcement would silently never trigger. Approximate (payload
    // bytes, not actual on-disk bytes after LSM/compaction overhead) but
    // accurate the instant a write/remove happens, which matters more for
    // a capacity guard than being exact.
    used_bytes: AtomicU64,
    /// Byte cap enforced inside `evict` (0 = unbounded). Set once at
    /// startup from `tiered-disk-limit` — under `reject-writes` the check
    /// is the backstop; under `evict-oldest` the dispatch layer drains
    /// before the tier ever reaches it.
    capacity: AtomicU64,
    accounting_lock: Mutex<()>,
    /// Entries removed since open — deletes, promotes, GC sweeps, outright
    /// drops; every one writes an LSM tombstone, so this signals when a
    /// `major_compact` is worth running.
    entries_removed: AtomicU64,
    /// Set while `major_compact` runs (for `INFO tiered_compacting`).
    compacting: AtomicU64,
    /// Manual `compact()` completions — fjall's own counter only counts
    /// its background compactions.
    manual_compactions: AtomicU64,
}

impl FjallTier {
    pub fn open(dir: &Path) -> Result<Self, fjall::Error> {
        std::fs::create_dir_all(dir).map_err(fjall::Error::from)?;
        let db = Database::builder(dir).open()?;
        let keyspace = db.keyspace("reflexdb-tier", KeyspaceCreateOptions::default)?;
        let used_bytes = keyspace
            .iter()
            .filter_map(|guard| guard.into_inner().ok().map(|(k, v)| entry_cost(k.len(), v.len())))
            .sum();
        Ok(Self {
            keyspace,
            _db: db,
            used_bytes: AtomicU64::new(used_bytes),
            capacity: AtomicU64::new(0),
            accounting_lock: Mutex::new(()),
            entries_removed: AtomicU64::new(0),
            compacting: AtomicU64::new(0),
            manual_compactions: AtomicU64::new(0),
        })
    }

    /// `tiered-disk-limit` — `evict` refuses new/changed payloads that
    /// would push `used_bytes` past it (0 disables).
    pub fn set_disk_limit(&self, bytes: u64) {
        self.capacity.store(bytes, Ordering::Relaxed);
    }

    /// Looks up `key`'s stored payload length (if present) and subtracts
    /// it from `used_bytes` before actually removing it — every removal
    /// path (explicit `remove`, promote, corrupt/expired cleanup) goes
    /// through this so the counter can't drift out of sync.
    fn remove_tracked(&self, key: &[u8]) {
        let _guard = self.accounting_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Ok(Some(payload)) = self.keyspace.get(key) {
            if self.keyspace.remove(key).is_ok() {
                self.used_bytes.fetch_sub(entry_cost(key.len(), payload.len()), Ordering::Relaxed);
                self.entries_removed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Drops every expired (and undecodable) entry still sitting in the
    /// tier — the periodic GC partner to the lazy checks in
    /// `contains`/`promote`. Returns how many entries were removed.
    pub fn sweep_expired(&self) -> usize {
        let mut dropped = 0;
        for key in self.keys_snapshot_sync() {
            let expired = match self.keyspace.get(&key) {
                Ok(Some(payload)) => match decode_entry(payload.as_ref()) {
                    Ok(entry) => entry.expires_at_ms.is_some_and(|expiry| expiry <= now_ms()),
                    Err(_) => true,
                },
                _ => false,
            };
            if expired {
                self.remove_tracked(&key);
                dropped += 1;
            }
        }
        dropped
    }

    /// Runs an LSM major compaction, reclaiming the space tombstoned by
    /// `remove`/`sweep_expired`/promotes — deletions don't free disk until
    /// this (or fjall's own background merging) rewrites the segments.
    /// Transiently needs up to ~the current segment size of headroom;
    /// callers should schedule it off the latency path.
    pub fn compact(&self) -> Result<(), fjall::Error> {
        self.compacting.store(1, Ordering::Relaxed);
        let result = self.keyspace.major_compact();
        self.compacting.store(0, Ordering::Relaxed);
        if result.is_ok() {
            self.manual_compactions.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// `(active_compactions, compactions_completed, total_compact_time)` —
    /// fjall-internal stats, surfaced via `INFO`.
    pub fn compaction_stats(&self) -> (usize, usize, Duration) {
        (self._db.active_compactions(), self._db.compactions_completed(), self._db.time_compacting())
    }
}

impl TieringHandler for FjallTier {
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String> {
        on_blocking_thread(|| self.evict_sync(key, value, ttl))
    }

    fn promote(&self, key: &[u8]) -> Option<(Value, Option<Duration>)> {
        on_blocking_thread(|| self.promote_sync(key))
    }

    fn contains(&self, key: &[u8]) -> bool {
        on_blocking_thread(|| self.contains_sync(key))
    }

    fn remove(&self, key: &[u8]) {
        on_blocking_thread(|| self.remove_tracked(key));
    }

    fn keys_snapshot(&self) -> Vec<Bytes> {
        on_blocking_thread(|| self.keys_snapshot_sync())
    }

    fn oldest_keys(&self, max: usize) -> Vec<Bytes> {
        on_blocking_thread(|| self.oldest_keys_sync(max))
    }

    fn disk_usage_bytes(&self) -> Option<u64> {
        Some(self.used_bytes.load(Ordering::Relaxed))
    }

    fn can_accept(&self, additional: u64) -> bool {
        let capacity = self.capacity.load(Ordering::Relaxed);
        capacity == 0 || self.used_bytes.load(Ordering::Relaxed) + additional <= capacity
    }

    fn clear(&self) {
        on_blocking_thread(|| {
            for key in self.keys_snapshot_sync() {
                self.remove_tracked(&key);
            }
        });
    }

    fn sweep_expired(&self) -> usize {
        on_blocking_thread(|| FjallTier::sweep_expired(self))
    }

    fn compact(&self) -> Result<(), String> {
        on_blocking_thread(|| FjallTier::compact(self).map_err(|e| e.to_string()))
    }

    fn stats(&self) -> keyspace::TieredStats {
        let (active, completed, time) = self.compaction_stats();
        keyspace::TieredStats {
            entries_removed: self.entries_removed.load(Ordering::Relaxed),
            compactions_completed: completed as u64 + self.manual_compactions.load(Ordering::Relaxed),
            compacting: active > 0 || self.compacting.load(Ordering::Relaxed) != 0,
            compact_time: time,
        }
    }
}

impl FjallTier {
    fn evict_sync(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String> {
        let entry = TieredEntry {
            value: value.clone(),
            expires_at_ms: ttl.map(|d| now_ms().saturating_add(d.as_millis() as u64)),
            evicted_at_ms: now_ms(),
        };
        let payload = encode_entry(&entry).map_err(|e| format!("serialize tiered value: {e}"))?;
        // Atomic capacity check-and-reserve: the accounting lock serializes
        // every tier write/remove, so concurrent evictions can't race each
        // other past `capacity`. The charged cost covers key + payload +
        // a fixed per-entry LSM record/index share (see `entry_cost`) —
        // overwriting an existing key only pays the payload delta.
        let _guard = self.accounting_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let new_cost = entry_cost(key.len(), payload.len());
        let old_cost = self
            .keyspace
            .get(key.as_ref())
            .ok()
            .flatten()
            .map(|old| entry_cost(key.len(), old.len()))
            .unwrap_or(0);
        let capacity = self.capacity.load(Ordering::Relaxed);
        let used = self.used_bytes.load(Ordering::Relaxed);
        if capacity != 0 && used + new_cost - old_cost > capacity {
            return Err(keyspace::TIERED_FULL.into());
        }
        self.keyspace
            .insert(key.as_ref(), payload)
            .map_err(|e| format!("write evicted key to disk tier: {e}"))?;
        self.used_bytes.fetch_sub(old_cost, Ordering::Relaxed);
        self.used_bytes.fetch_add(new_cost, Ordering::Relaxed);
        // Without this, writes sit in fjall's in-memory journal buffer
        // until its own internal flush threshold — a process crash right
        // after an eviction could lose a key that both RAM and a crash-
        // safe disk tier were supposed to guarantee survives.
        self._db.persist(fjall::PersistMode::SyncAll).map_err(|e| format!("persist disk tier: {e}"))
    }

    fn promote_sync(&self, key: &[u8]) -> Option<(Value, Option<Duration>)> {
        let payload = match self.keyspace.get(key) {
            Ok(Some(p)) => p,
            Ok(None) => return None,
            Err(e) => {
                tracing::warn!(error = %e, "failed to read from disk tier");
                return None;
            }
        };
        let entry = match decode_entry(payload.as_ref()) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = %e, "corrupt tiered entry; dropping it");
                self.remove_tracked(key);
                return None;
            }
        };
        self.remove_tracked(key);
        let remaining = entry.expires_at_ms.map(|expiry| expiry.saturating_sub(now_ms()));
        if remaining == Some(0) {
            return None;
        }
        Some((entry.value, remaining.map(Duration::from_millis)))
    }

    fn contains_sync(&self, key: &[u8]) -> bool {
        let payload = match self.keyspace.get(key) {
            Ok(Some(payload)) => payload,
            _ => return false,
        };
        match decode_entry(payload.as_ref()) {
            Ok(entry) if entry.expires_at_ms.is_none_or(|expiry| expiry > now_ms()) => true,
            _ => {
                self.remove_tracked(key);
                false
            }
        }
    }

    fn keys_snapshot_sync(&self) -> Vec<Bytes> {
        self.keyspace
            .iter()
            .filter_map(|guard| guard.key().ok().map(|key| Bytes::copy_from_slice(&key)))
            .collect()
    }

    fn oldest_keys_sync(&self, max: usize) -> Vec<Bytes> {
        // O(tier size log(tier size)) per call — reads every entry's
        // header to sort by `evicted_at_ms`, same trade-off the
        // `maxmemory` LRU/LFU policies already make for RAM keys (fine at
        // this scale, not meant to hold up at millions of tiered keys).
        let mut by_age: Vec<(Bytes, u64)> = self
            .keyspace
            .iter()
            .filter_map(|guard| {
                let (key, payload) = guard.into_inner().ok()?;
                let ts = decode_entry(payload.as_ref()).map(|e| e.evicted_at_ms).unwrap_or(0);
                Some((Bytes::copy_from_slice(&key), ts))
            })
            .collect();
        by_age.sort_by_key(|(_, ts)| *ts);
        by_age.truncate(max);
        by_age.into_iter().map(|(key, _)| key).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes as B;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("reflexdb-tiered-test-{name}-{}", std::process::id()))
    }

    #[test]
    fn evict_then_promote_round_trips_value_and_ttl() {
        let dir = temp_dir("roundtrip");
        let tier = FjallTier::open(&dir).unwrap();

        let key = B::from("k");
        let value = Value::String(B::from("hello"));
        tier.evict(&key, &value, Some(Duration::from_secs(30))).unwrap();

        assert!(tier.contains(b"k"));
        let (restored, ttl) = tier.promote(b"k").unwrap();
        assert_eq!(restored, value);
        assert!(matches!(ttl, Some(d) if d <= Duration::from_secs(30) && d > Duration::from_secs(29)));

        // Promotion is a one-way move: it's gone from the tier afterward.
        assert!(!tier.contains(b"k"));
        assert!(tier.promote(b"k").is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tier_io_can_run_from_a_tokio_worker() {
        let dir = temp_dir("tokio-worker");
        let tier = FjallTier::open(&dir).unwrap();
        let key = B::from("k");
        let value = Value::String(B::from("v"));

        tier.evict(&key, &value, None).unwrap();
        assert_eq!(tier.promote(b"k").unwrap().0, value);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oldest_keys_returns_eviction_order_not_key_order() {
        let dir = temp_dir("oldest-keys");
        let tier = FjallTier::open(&dir).unwrap();

        // Deliberately not alphabetical/insertion-coincidental key order —
        // if `oldest_keys` secretly fell back to lexicographic (LSM) key
        // order instead of actually sorting by `evicted_at_ms`, this would
        // catch it. A few ms between each guarantees distinct timestamps.
        for key in ["charlie", "alpha", "bravo"] {
            tier.evict(&B::from(key), &Value::String(B::from("v")), None).unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(tier.oldest_keys(10), vec![B::from("charlie"), B::from("alpha"), B::from("bravo")]);
        assert_eq!(tier.oldest_keys(2), vec![B::from("charlie"), B::from("alpha")]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn remove_drops_a_tiered_key_outright() {
        let dir = temp_dir("remove");
        let tier = FjallTier::open(&dir).unwrap();
        tier.evict(&B::from("k"), &Value::String(B::from("v")), None).unwrap();
        assert!(tier.contains(b"k"));
        tier.remove(b"k");
        assert!(!tier.contains(b"k"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_usage_grows_with_evicted_values_and_shrinks_on_remove() {
        let dir = temp_dir("disk-usage");
        let tier = FjallTier::open(&dir).unwrap();

        let empty = tier.disk_usage_bytes().unwrap();
        let big_value = Value::String(B::from(vec![b'x'; 64 * 1024]));
        tier.evict(&B::from("k"), &big_value, None).unwrap();
        let after_write = tier.disk_usage_bytes().unwrap();
        assert!(after_write - empty >= 64 * 1024, "expected usage to grow by at least the value size");

        tier.remove(b"k");
        assert_eq!(tier.disk_usage_bytes().unwrap(), empty, "expected usage to drop back after remove");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = temp_dir("persist");
        {
            let tier = FjallTier::open(&dir).unwrap();
            tier.evict(&B::from("k"), &Value::String(B::from("v")), None).unwrap();
        }
        {
            let tier = FjallTier::open(&dir).unwrap();
            let (value, _) = tier.promote(b"k").unwrap();
            assert_eq!(value, Value::String(B::from("v")));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_limit_rejects_new_entries_but_allows_overwrites() {
        let dir = temp_dir("disk-full");
        let tier = FjallTier::open(&dir).unwrap();
        let value = Value::String(B::from(vec![b'x'; 256]));

        // Size the limit off the actual post-write usage — no guessing at
        // the serialized payload size.
        tier.evict(&B::from("a"), &value, None).unwrap();
        tier.set_disk_limit(tier.disk_usage_bytes().unwrap() + 1);

        let err = tier.evict(&B::from("b"), &value, None).unwrap_err();
        assert!(err.contains(keyspace::TIERED_FULL), "got {err}");
        // Same cost as the entry it replaces → zero net growth → allowed.
        tier.evict(&B::from("a"), &value, None).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_evicts_cannot_exceed_the_configured_limit() {
        let dir = temp_dir("concurrent-limit");
        let tier = std::sync::Arc::new(FjallTier::open(&dir).unwrap());
        let value = Value::String(B::from(vec![b'x'; 256]));
        // Budget for ~10 entries of this size, then race 8 writers × 10
        // writes — the accounting lock must keep usage at/below the limit.
        tier.set_disk_limit(tier.disk_usage_bytes().unwrap() + 10 * entry_cost(4, 512));

        let mut handles = Vec::new();
        for t in 0..8u8 {
            let tier = tier.clone();
            let value = value.clone();
            handles.push(std::thread::spawn(move || {
                let mut accepted = 0;
                for i in 0..10u8 {
                    let key = B::from(format!("t{t}k{i}"));
                    if tier.evict(&key, &value, None).is_ok() {
                        accepted += 1;
                    }
                }
                accepted
            }));
        }
        let accepted: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        assert!(tier.disk_usage_bytes().unwrap() <= tier.capacity.load(Ordering::Relaxed));
        assert!(accepted < 80, "limit should have rejected most of the 80 concurrent writes, got {accepted}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let to = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &to);
            } else {
                std::fs::copy(entry.path(), &to).unwrap();
            }
        }
    }

    #[test]
    fn acknowledged_writes_survive_an_abrupt_exit() {
        let dir = temp_dir("crash");
        let copy = temp_dir("crash-copy");
        {
            let tier = FjallTier::open(&dir).unwrap();
            tier.evict(&B::from("k"), &Value::String(B::from("v")), None).unwrap();
            // Snapshot the files exactly as they sit on disk right now —
            // that's all a crash would preserve (no destructor, no final
            // flush), so the write must already be on the fsync'd journal.
            copy_dir(&dir, &copy);
        }
        {
            let tier = FjallTier::open(&copy).unwrap();
            let (value, _) = tier.promote(b"k").unwrap();
            assert_eq!(value, Value::String(B::from("v")));
        }
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&copy);
    }

    #[test]
    fn sweep_expired_drops_untouched_expired_entries() {
        let dir = temp_dir("sweep");
        let tier = FjallTier::open(&dir).unwrap();
        tier.evict(&B::from("gone"), &Value::String(B::from("v")), Some(Duration::from_millis(20))).unwrap();
        tier.evict(&B::from("stays"), &Value::String(B::from("v")), None).unwrap();
        std::thread::sleep(Duration::from_millis(60));

        assert_eq!(tier.sweep_expired(), 1);
        assert!(!tier.contains(b"gone"));
        assert!(tier.contains(b"stays"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_entries_are_dropped_not_returned() {
        let dir = temp_dir("corrupt");
        let tier = FjallTier::open(&dir).unwrap();
        // Inject a payload that isn't a `TieredEntry` at all.
        tier.keyspace.insert(b"bad", b"not-a-tiered-entry").unwrap();
        assert!(!tier.contains(b"bad"));
        assert!(tier.promote(b"bad").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_reclaims_tombstone_space() {
        let dir = temp_dir("compact");
        let tier = FjallTier::open(&dir).unwrap();
        for i in 0..4 {
            tier.evict(&B::from(format!("k{i}")), &Value::String(B::from("v")), None).unwrap();
        }
        for i in 0..4 {
            tier.remove(format!("k{i}").as_bytes());
        }
        assert_eq!(tier.stats().entries_removed, 4);
        tier.compact().unwrap();
        assert!(tier.stats().compactions_completed >= 1);
        assert!(!tier.stats().compacting);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
