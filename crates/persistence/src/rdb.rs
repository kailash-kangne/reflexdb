//! Point-in-time snapshot of a `keyspace::Store`, written atomically
//! (`temp-<pid>.rdb` then `rename()`, same crash-safety trick the real
//! `dump.rdb` writer uses) and reloaded on startup.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

use bincode::Options;
use bytes::Bytes;
use keyspace::{Store, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct RdbEntry {
    key: Bytes,
    value: Value,
    /// Remaining TTL in milliseconds at save time, `None` if persistent.
    ttl_ms: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct RdbFile {
    version: u32,
    databases: Vec<Vec<RdbEntry>>,
}

const FORMAT_VERSION: u32 = 1;

/// Serialize every database's live entries to bytes — the same payload
/// `save` writes to disk, but usable directly over the wire (e.g. the
/// replication full-resync snapshot in `PSYNC`'s reply).
pub fn serialize(store: &Store) -> io::Result<Vec<u8>> {
    let databases = (0..store.len())
        .map(|i| {
            store
                .db(i)
                .expect("index within store.len()")
                .snapshot_entries()
                .into_iter()
                .map(|(key, value, ttl)| RdbEntry { key, value, ttl_ms: ttl.map(|d| d.as_millis() as u64) })
                .collect()
        })
        .collect();
    bincode::serialize(&RdbFile { version: FORMAT_VERSION, databases }).map_err(io::Error::other)
}

/// Load a snapshot produced by `serialize` into `store`. `payload` isn't
/// necessarily trustworthy — besides a local file (written by this same
/// process), it also arrives over the wire from whatever a replica's
/// configured `REPLICAOF` target happens to be (`PSYNC`'s full-resync
/// reply) — so this uses a size-bounded config rather than
/// `bincode::deserialize`'s unbounded default, same reasoning as
/// `commands::snapshot::cmd_restore`'s docs for `RESTORE`.
/// `with_fixint_encoding()` is required alongside `with_limit()`: the
/// `Options` trait's `DefaultOptions` defaults to *varint* integer
/// encoding, while `serialize` above (the plain `bincode::serialize` free
/// function) defaults to *fixint* — without matching that explicitly,
/// every legitimate RDB load/`PSYNC` resync would fail to parse, not just
/// malicious payloads.
pub fn deserialize(payload: &[u8], store: &Store) -> io::Result<()> {
    let file: RdbFile = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(payload.len() as u64)
        .deserialize(payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if file.version != FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported RDB format version {}", file.version),
        ));
    }
    for (index, entries) in file.databases.into_iter().enumerate() {
        let Some(db) = store.db(index) else { continue };
        for entry in entries {
            db.load_entry(entry.key, entry.value, entry.ttl_ms.map(Duration::from_millis));
        }
    }
    Ok(())
}

pub fn save(path: &Path, store: &Store) -> io::Result<()> {
    let payload = serialize(store)?;
    write_payload(path, &payload)
}

/// `SAVE`'s one true piece of blocking I/O, split out so `BGSAVE` can call
/// it from a background thread with an already-serialized payload instead
/// of inheriting `save`'s synchronous write.
fn write_payload(path: &Path, payload: &[u8]) -> io::Result<()> {
    let tmp_path = match path.parent() {
        Some(dir) => dir.join(format!("temp-{}.rdb", std::process::id())),
        None => PathBuf::from(format!("temp-{}.rdb", std::process::id())),
    };
    fs::write(&tmp_path, payload)?;
    fs::rename(&tmp_path, path)
}

/// `BGSAVE`: `serialize(store)` (an in-memory walk of the keyspace) still
/// has to happen on the caller's thread — it needs `&Store`, which isn't
/// `'static` — but once that produces an owned `payload`, the actual disk
/// write has no remaining dependency on the caller at all. Spawning it here
/// is what makes `BGSAVE` not block whichever connection/thread issued it
/// for however long the write+rename takes, unlike plain `SAVE`. `on_done`
/// runs on the background thread once the write finishes (or fails) — used
/// to update `last_save`/log a warning without the caller waiting on it.
pub fn save_in_background(
    path: PathBuf,
    payload: Vec<u8>,
    on_done: impl FnOnce(io::Result<()>) + Send + 'static,
) -> io::Result<()> {
    std::thread::Builder::new()
        .name("reflexdb-bgsave".into())
        .spawn(move || on_done(write_payload(&path, &payload)))
        .map(|_| ())
        .map_err(|e| io::Error::other(format!("failed to spawn BGSAVE writer thread: {e}")))
}

/// Loads `path` into `store` if it exists. Returns `false` (not an error)
/// when there's simply no snapshot yet — the normal case on first boot.
pub fn load(path: &Path, store: &Store) -> io::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    const MAX_RDB_SIZE: u64 = 512 * 1024 * 1024;
    let file = fs::File::open(path)?;
    let size = file.metadata()?.len();
    if size > MAX_RDB_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "RDB snapshot exceeds 512 MiB limit"));
    }
    let mut payload = Vec::with_capacity(size as usize);
    file.take(MAX_RDB_SIZE + 1).read_to_end(&mut payload)?;
    if payload.len() as u64 > MAX_RDB_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "RDB snapshot exceeds 512 MiB limit"));
    }
    deserialize(&payload, store)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes as B;
    use keyspace::Ttl;

    #[test]
    fn save_then_load_round_trips_values_and_ttl() {
        let dir = std::env::temp_dir().join(format!("reflexdb-rdb-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dump.rdb");

        let store = Store::new(2);
        store.db(0).unwrap().set(B::from("k"), Value::String(B::from("v")), None);
        store.db(0).unwrap().set(
            B::from("ttl-key"),
            Value::String(B::from("soon")),
            Some(std::time::Instant::now() + Duration::from_secs(60)),
        );
        store.db(1).unwrap().set(B::from("other-db"), Value::String(B::from("x")), None);

        save(&path, &store).unwrap();

        let restored = Store::new(2);
        assert!(load(&path, &restored).unwrap());
        assert_eq!(restored.db(0).unwrap().get(b"k"), Some(Value::String(B::from("v"))));
        assert_eq!(restored.db(1).unwrap().get(b"other-db"), Some(Value::String(B::from("x"))));
        assert!(matches!(restored.db(0).unwrap().ttl(b"ttl-key"), Ttl::Expires(_)));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_file_returns_false_not_error() {
        let path = std::env::temp_dir().join("reflexdb-does-not-exist.rdb");
        let store = Store::new(1);
        assert!(!load(&path, &store).unwrap());
    }

    #[test]
    fn malformed_and_unsupported_snapshots_return_errors_without_panicking() {
        let store = Store::new(1);
        for payload in [&[][..], &[0xff; 32][..], &[1, 0, 0][..]] {
            assert!(deserialize(payload, &store).is_err());
        }

        let unsupported =
            bincode::serialize(&RdbFile { version: FORMAT_VERSION + 1, databases: vec![] }).unwrap();
        let error = deserialize(&unsupported, &store).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    /// `Value::Stream` carries consumer-group state (`ConsumerGroup`/
    /// `PendingEntry`), not just entries — round-trip both through a save/
    /// load cycle, not just an empty/entries-only stream.
    #[test]
    fn save_then_load_round_trips_a_stream_with_a_consumer_group() {
        let dir = std::env::temp_dir().join(format!("reflexdb-rdb-stream-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dump.rdb");

        let mut stream = keyspace::Stream::new();
        stream
            .add(
                keyspace::IdSpec::Explicit(keyspace::StreamId::new(1, 0)),
                0,
                vec![(B::from("f"), B::from("v"))],
            )
            .unwrap();
        stream.create_group(B::from("g"), keyspace::StreamId::ZERO);
        stream.read_group(b"g", B::from("c"), None, None, 0, false);

        let store = Store::new(1);
        store.db(0).unwrap().set(B::from("s"), Value::Stream(Box::new(stream)), None);
        save(&path, &store).unwrap();

        let restored = Store::new(1);
        assert!(load(&path, &restored).unwrap());
        match restored.db(0).unwrap().get(b"s") {
            Some(Value::Stream(s)) => {
                assert_eq!(s.len(), 1);
                let (count, ..) = s.pending_summary(b"g").unwrap();
                assert_eq!(count, 1);
            }
            other => panic!("expected a restored Stream, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
