//! A disk-backed `keyspace::TieringHandler` on top of `fjall` (a pure-Rust,
//! LSM-tree-based embedded store — no C/C++ toolchain needed, unlike
//! RocksDB). See `docs/05-persistence-tiering.md` §5.
//!
//! Values are bincode-encoded entries keyed by the original key bytes, so a
//! promote is a single point lookup. Expiry uses wall-clock deadlines so a
//! tiered key's TTL continues to elapse while the server is stopped.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
}

impl FjallTier {
    pub fn open(dir: &Path) -> Result<Self, fjall::Error> {
        std::fs::create_dir_all(dir).map_err(fjall::Error::from)?;
        let db = Database::builder(dir).open()?;
        let keyspace = db.keyspace("reflexdb-tier", KeyspaceCreateOptions::default)?;
        Ok(Self { keyspace, _db: db, used_bytes: AtomicU64::new(0) })
    }

    /// Looks up `key`'s stored payload length (if present) and subtracts
    /// it from `used_bytes` before actually removing it — every removal
    /// path (explicit `remove`, promote, corrupt/expired cleanup) goes
    /// through this so the counter can't drift out of sync.
    fn remove_tracked(&self, key: &[u8]) {
        if let Ok(Some(payload)) = self.keyspace.get(key) {
            self.used_bytes.fetch_sub(payload.len() as u64, Ordering::Relaxed);
        }
        let _ = self.keyspace.remove(key);
    }
}

impl TieringHandler for FjallTier {
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String> {
        let entry = TieredEntry {
            value: value.clone(),
            expires_at_ms: ttl.map(|d| now_ms().saturating_add(d.as_millis() as u64)),
            evicted_at_ms: now_ms(),
        };
        let payload = encode_entry(&entry).map_err(|e| format!("serialize tiered value: {e}"))?;
        let len = payload.len() as u64;
        self.keyspace
            .insert(key.as_ref(), payload)
            .map_err(|e| format!("write evicted key to disk tier: {e}"))?;
        self.used_bytes.fetch_add(len, Ordering::Relaxed);
        // Without this, writes sit in fjall's in-memory journal buffer
        // until its own internal flush threshold — a process crash right
        // after an eviction could lose a key that both RAM and a crash-
        // safe disk tier were supposed to guarantee survives.
        self._db.persist(fjall::PersistMode::SyncAll).map_err(|e| format!("persist disk tier: {e}"))
    }

    fn promote(&self, key: &[u8]) -> Option<(Value, Option<Duration>)> {
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

    fn contains(&self, key: &[u8]) -> bool {
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

    fn remove(&self, key: &[u8]) {
        self.remove_tracked(key);
    }

    fn keys_snapshot(&self) -> Vec<Bytes> {
        self.keyspace
            .iter()
            .filter_map(|guard| guard.key().ok().map(|key| Bytes::copy_from_slice(&key)))
            .collect()
    }

    fn disk_usage_bytes(&self) -> Option<u64> {
        Some(self.used_bytes.load(Ordering::Relaxed))
    }

    fn oldest_keys(&self, max: usize) -> Vec<Bytes> {
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
}
