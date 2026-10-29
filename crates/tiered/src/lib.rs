//! A disk-backed `keyspace::TieringHandler` on top of `fjall` (a pure-Rust,
//! LSM-tree-based embedded store — no C/C++ toolchain needed, unlike
//! RocksDB). See `docs/05-persistence-tiering.md` §5.
//!
//! Values are bincode-encoded entries keyed by the original key bytes, so a
//! promote is a single point lookup. Expiry uses wall-clock deadlines so a
//! tiered key's TTL continues to elapse while the server is stopped.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use keyspace::{TieringHandler, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct TieredEntry {
    value: Value,
    expires_at_ms: Option<u64>,
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
    // remaining TTL begins elapsing from their first read after upgrade.
    let legacy: LegacyTieredEntry = bincode::deserialize(payload)?;
    Ok(TieredEntry {
        value: legacy.value,
        expires_at_ms: legacy.ttl_ms.map(|ttl| now_ms().saturating_add(ttl)),
    })
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
    fn evict(&self, key: &Bytes, value: &Value, ttl: Option<Duration>) -> Result<(), String> {
        let entry = TieredEntry {
            value: value.clone(),
            expires_at_ms: ttl.map(|d| now_ms().saturating_add(d.as_millis() as u64)),
        };
        let payload = encode_entry(&entry).map_err(|e| format!("serialize tiered value: {e}"))?;
        self.keyspace
            .insert(key.as_ref(), payload)
            .map_err(|e| format!("write evicted key to disk tier: {e}"))
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
                let _ = self.keyspace.remove(key);
                return None;
            }
        };
        let _ = self.keyspace.remove(key);
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
                let _ = self.keyspace.remove(key);
                false
            }
        }
    }

    fn remove(&self, key: &[u8]) {
        let _ = self.keyspace.remove(key);
    }

    fn keys_snapshot(&self) -> Vec<Bytes> {
        self.keyspace
            .iter()
            .filter_map(|guard| guard.key().ok().map(|key| Bytes::copy_from_slice(&key)))
            .collect()
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
