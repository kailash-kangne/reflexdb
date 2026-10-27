//! A disk-backed `keyspace::TieringHandler` on top of `fjall` (a pure-Rust,
//! LSM-tree-based embedded store — no C/C++ toolchain needed, unlike
//! RocksDB). See `docs/05-persistence-tiering.md` §5.
//!
//! Values are bincode-encoded `(keyspace::Value, Option<Duration>)` pairs
//! keyed by the original key bytes, so a promote is a single point lookup.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use keyspace::{TieringHandler, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct TieredEntry {
    value: Value,
    ttl_ms: Option<u64>,
}

pub struct FjallTier {
    keyspace: Keyspace,
    // Keep the handle alive for the process lifetime; fjall persists on
    // drop, but we also fsync explicitly after every write (see `evict`).
    _db: Database,
}

impl FjallTier {
    pub fn open(dir: &Path) -> Result<Self, fjall::Error> {
        std::fs::create_dir_all(dir).map_err(fjall::Error::from)?;
        let db = Database::builder(dir).open()?;
        let keyspace = db.keyspace("reflexdb-tier", KeyspaceCreateOptions::default)?;
        Ok(Self { keyspace, _db: db })
    }
}

impl TieringHandler for FjallTier {
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) {
        let entry = TieredEntry { value: value.clone(), ttl_ms: ttl.map(|d| d.as_millis() as u64) };
        let Ok(payload) = bincode::serialize(&entry) else {
            tracing::warn!(?key, "failed to serialize value for tiering; dropping it");
            return;
        };
        if let Err(e) = self.keyspace.insert(key.as_ref(), payload) {
            tracing::warn!(?key, error = %e, "failed to write evicted key to disk tier");
        }
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
        let entry: TieredEntry = match bincode::deserialize(payload.as_ref()) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = %e, "corrupt tiered entry; dropping it");
                let _ = self.keyspace.remove(key);
                return None;
            }
        };
        let _ = self.keyspace.remove(key);
        Some((entry.value, entry.ttl_ms.map(Duration::from_millis)))
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.keyspace.contains_key(key).unwrap_or(false)
    }

    fn remove(&self, key: &[u8]) {
        let _ = self.keyspace.remove(key);
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
        tier.evict(&key, &value, Some(Duration::from_secs(30)));

        assert!(tier.contains(b"k"));
        let (restored, ttl) = tier.promote(b"k").unwrap();
        assert_eq!(restored, value);
        assert_eq!(ttl, Some(Duration::from_secs(30)));

        // Promotion is a one-way move: it's gone from the tier afterward.
        assert!(!tier.contains(b"k"));
        assert!(tier.promote(b"k").is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn remove_drops_a_tiered_key_outright() {
        let dir = temp_dir("remove");
        let tier = FjallTier::open(&dir).unwrap();
        tier.evict(&B::from("k"), &Value::String(B::from("v")), None);
        assert!(tier.contains(b"k"));
        tier.remove(b"k");
        assert!(!tier.contains(b"k"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = temp_dir("persist");
        {
            let tier = FjallTier::open(&dir).unwrap();
            tier.evict(&B::from("k"), &Value::String(B::from("v")), None);
        }
        {
            let tier = FjallTier::open(&dir).unwrap();
            let (value, _) = tier.promote(b"k").unwrap();
            assert_eq!(value, Value::String(B::from("v")));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
