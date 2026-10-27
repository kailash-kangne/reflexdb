//! Disk-backed, time-retention store for `GETAT`/`HISTORY` (the per-key
//! value version log — see `docs/11-time-travel.md` and
//! `docs/18-interview-prep.md` §6 for the full design rationale). Replaces
//! the earlier proof-of-concept, which kept every version in a plain
//! in-memory `DashMap` forever (unbounded by key count, and never cleaned
//! up even after the live key was deleted).
//!
//! # Design
//!
//! - **RAM holds only the current value** (unchanged — that's
//!   `keyspace::Db`, this module never touches it). Only *past* versions
//!   live here, on disk.
//! - Built on `fjall` (same pure-Rust LSM engine the `tiered` crate uses
//!   for the eviction disk tier), in its own on-disk directory — a
//!   completely different access pattern and lifetime from the tiered
//!   crate's keys (append-only + range-scan + time-based sweep, versions
//!   deliberately outlive the live key, unlike a tiered key which is a
//!   1:1 stand-in for it).
//! - **Retention is by age (`sweep_older_than`), not by a per-key version
//!   count.** A background tick (see `server/src/main.rs`) calls it
//!   periodically with a cutoff derived from `history-retention-ms`. This
//!   is also what fixes the old design's leak: a key's history ages out on
//!   its own schedule, whether or not the live key still exists — no
//!   per-delete/per-expire bookkeeping required.
//! - **Writes are batched off the request path**, the same
//!   dedicated-writer-thread-plus-channel shape as `crate::aof::AofWriter`:
//!   `record()` just encodes a small job and pushes it to an
//!   `mpsc::channel`, returning immediately. The writer thread drains
//!   everything already queued and commits it as one `fjall` write batch,
//!   so N concurrent `record()` callers cost O(1) batched commits instead
//!   of O(N) individual ones.
//!
//! # On-disk key layout
//!
//! Two index entries are written per recorded version, both inside the
//! same `fjall` keyspace (distinguished by a one-byte tag prefix so one
//! `range`/`prefix` scan never crosses between them):
//!
//! - **`BY_KEY`**: `[0x01][db:u16 BE][keylen:u32 BE][key bytes][ts_ms:u64 BE]`
//!   — sorted so all of one key's versions sit together, oldest to
//!   newest. `GETAT`/`HISTORY` do a `prefix(db,key)` scan here.
//! - **`BY_TIME`**: `[0x02][ts_ms:u64 BE][db:u16 BE][keylen:u32 BE][key bytes]`
//!   — sorted purely by age, across every key and database. The periodic
//!   sweep does one bounded `range(..cutoff)` scan here to find everything
//!   expired, which is `O(expired since last sweep)`, not `O(total
//!   history size)` — it never has to touch `BY_KEY` entries that are
//!   still fresh.
//!
//! Both entries for a version carry the same encoded value, so the sweep
//! can delete a `BY_TIME` entry and derive its matching `BY_KEY` entry's
//! key directly from the bytes it already scanned, without an extra read.

use std::io;
use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
use std::thread;

use bytes::Bytes;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};

const BY_KEY: u8 = 1;
const BY_TIME: u8 = 2;

/// One queued version append, handed to the writer thread.
enum Job {
    Record {
        db_index: u16,
        key: Bytes,
        ts_ms: u64,
        value: Option<Bytes>,
    },
    /// Ack once every job sent before this one has been committed —
    /// used by tests (and anything else that needs "has this landed yet")
    /// to avoid sleeping/polling. Mirrors `aof::Job::Flush`.
    Flush(mpsc::SyncSender<()>),
}

pub struct HistoryWriter {
    tx: Mutex<Sender<Job>>,
    // Kept alongside `keyspace` so `sweep_older_than` can start a
    // `WriteBatch` (`Database::batch()`) without the writer thread's copy.
    db: Database,
    keyspace: Keyspace,
    // Keeps the writer thread alive for the program's lifetime.
    _worker: thread::JoinHandle<()>,
}

impl HistoryWriter {
    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let db = Database::builder(dir).open().map_err(io::Error::other)?;
        let keyspace =
            db.keyspace("reflexdb-history", KeyspaceCreateOptions::default).map_err(io::Error::other)?;
        let (tx, rx) = mpsc::channel();
        let worker_db = db.clone();
        let worker_keyspace = keyspace.clone();
        let worker = thread::Builder::new()
            .name("reflexdb-history-writer".into())
            .spawn(move || writer_loop(worker_db, worker_keyspace, rx))
            .map_err(|e| io::Error::other(format!("failed to spawn history writer thread: {e}")))?;
        Ok(Self { tx: Mutex::new(tx), db, keyspace, _worker: worker })
    }

    /// Queue one version of `key` for the history log. Fire-and-forget —
    /// returns as soon as the job is handed to the writer thread's queue;
    /// see the module docs for the batching this enables. `value: None`
    /// records a delete (or "wasn't a string"), matching `GETAT`/
    /// `HISTORY`'s existing nil-means-deleted convention.
    pub fn record(&self, db_index: usize, key: Bytes, ts_ms: u64, value: Option<Bytes>) {
        let job = Job::Record { db_index: db_index as u16, key, ts_ms, value };
        // The writer thread only ever exits if the process is shutting
        // down (it loops for the program's lifetime otherwise); a failed
        // send just means "too late to record this one," not a bug worth
        // surfacing on every write's hot path.
        let _ = self.tx.lock().unwrap().send(job);
    }

    /// Blocks until every `record()` call made before this one has been
    /// committed to disk. Not used on any request path — only by tests
    /// (and anything else that needs a deterministic "has this landed
    /// yet" instead of sleeping/polling).
    pub fn flush(&self) {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        if self.tx.lock().unwrap().send(Job::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }

    /// The string value `key` held at or before `ts_ms` — the newest
    /// recorded version whose timestamp doesn't exceed it. `None` if the
    /// key has no version that old (never existed yet, or every
    /// known version is newer than `ts_ms`), or its value at that point
    /// was a delete / not a string.
    pub fn get_at(&self, db_index: usize, key: &[u8], ts_ms: u64) -> Option<Bytes> {
        let prefix = by_key_prefix(db_index as u16, key);
        for guard in self.keyspace.prefix(&prefix).rev() {
            let Ok((full_key, value)) = guard.into_inner() else { continue };
            let Some(entry_ts) = parse_ts_suffix(&full_key, prefix.len()) else { continue };
            if entry_ts > ts_ms {
                continue;
            }
            return decode_value(&value);
        }
        None
    }

    /// Up to `limit` most recent `(ts_ms, value-or-none)` versions
    /// recorded for `key`, newest first.
    pub fn history(&self, db_index: usize, key: &[u8], limit: usize) -> Vec<(u64, Option<Bytes>)> {
        let prefix = by_key_prefix(db_index as u16, key);
        self.keyspace
            .prefix(&prefix)
            .rev()
            .take(limit)
            .filter_map(|guard| {
                let (full_key, value) = guard.into_inner().ok()?;
                let ts = parse_ts_suffix(&full_key, prefix.len())?;
                Some((ts, decode_value(&value)))
            })
            .collect()
    }

    /// Deletes every version older than `cutoff_ms`, across every key and
    /// database, by scanning the `BY_TIME` index — cost proportional to
    /// how much has actually expired since the last sweep, not to the
    /// total size of the history store. Returns the number of versions
    /// removed. Meant to be called periodically from a background tick
    /// (`history-retention-ms`), not synchronously from a request.
    pub fn sweep_older_than(&self, cutoff_ms: u64) -> usize {
        let lower = vec![BY_TIME];
        let mut upper = vec![BY_TIME];
        upper.extend_from_slice(&cutoff_ms.to_be_bytes());

        let stale: Vec<Vec<u8>> = self
            .keyspace
            .range(lower..upper)
            .filter_map(|guard| guard.key().ok().map(|k| k.to_vec()))
            .collect();
        if stale.is_empty() {
            return 0;
        }

        let mut batch = self.db.batch();
        for time_key in &stale {
            batch.remove(&self.keyspace, time_key.clone());
            if let Some(by_key) = by_time_to_by_key(time_key) {
                batch.remove(&self.keyspace, by_key);
            }
        }
        let removed = stale.len();
        if let Err(e) = batch.commit() {
            tracing::warn!(error = %e, "history sweep failed to commit deletions");
        }
        removed
    }
}

fn writer_loop(db: Database, keyspace: Keyspace, rx: Receiver<Job>) {
    while let Ok(first) = rx.recv() {
        let mut jobs = vec![first];
        while let Ok(job) = rx.try_recv() {
            jobs.push(job);
        }

        let mut batch = db.batch();
        let mut acks = Vec::new();
        for job in jobs {
            match job {
                Job::Record { db_index, key, ts_ms, value } => {
                    let payload = encode_value(value.as_ref());
                    batch.insert(&keyspace, by_key_full(db_index, &key, ts_ms), payload.clone());
                    batch.insert(&keyspace, by_time_full(db_index, &key, ts_ms), payload);
                }
                Job::Flush(ack) => acks.push(ack),
            }
        }
        if let Err(e) = batch.commit() {
            tracing::warn!(error = %e, "history writer thread failed to commit a batch");
        }
        for ack in acks {
            let _ = ack.send(());
        }
    }
}

fn by_key_prefix(db_index: u16, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 2 + 4 + key.len());
    out.push(BY_KEY);
    out.extend_from_slice(&db_index.to_be_bytes());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

fn by_key_full(db_index: u16, key: &[u8], ts_ms: u64) -> Vec<u8> {
    let mut out = by_key_prefix(db_index, key);
    out.extend_from_slice(&ts_ms.to_be_bytes());
    out
}

fn by_time_full(db_index: u16, key: &[u8], ts_ms: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 2 + 4 + key.len());
    out.push(BY_TIME);
    out.extend_from_slice(&ts_ms.to_be_bytes());
    out.extend_from_slice(&db_index.to_be_bytes());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

/// Reads the trailing `ts_ms` off a `BY_KEY` entry's full on-disk key,
/// given the length of the fixed `(db, keylen, key)` prefix it was looked
/// up with.
fn parse_ts_suffix(full_key: &[u8], prefix_len: usize) -> Option<u64> {
    full_key.get(prefix_len..prefix_len + 8).map(|b| u64::from_be_bytes(b.try_into().unwrap()))
}

/// Parses a `BY_TIME` entry's on-disk key back into its fields and
/// reconstructs the matching `BY_KEY` entry's key, so the sweep can delete
/// both sides of the index from the `BY_TIME` scan alone.
fn by_time_to_by_key(time_key: &[u8]) -> Option<Vec<u8>> {
    if time_key.first() != Some(&BY_TIME) {
        return None;
    }
    let ts = u64::from_be_bytes(time_key.get(1..9)?.try_into().ok()?);
    let db_index = u16::from_be_bytes(time_key.get(9..11)?.try_into().ok()?);
    let keylen = u32::from_be_bytes(time_key.get(11..15)?.try_into().ok()?) as usize;
    let key = time_key.get(15..15 + keylen)?;
    Some(by_key_full(db_index, key, ts))
}

/// One tag byte (`0` = deleted/not-a-string, `1` = string value follows) —
/// simpler than a serde round trip for a type this small.
fn encode_value(value: Option<&Bytes>) -> Vec<u8> {
    match value {
        None => vec![0],
        Some(b) => {
            let mut out = Vec::with_capacity(1 + b.len());
            out.push(1);
            out.extend_from_slice(b);
            out
        }
    }
}

fn decode_value(raw: &[u8]) -> Option<Bytes> {
    match raw.first() {
        Some(1) => Some(Bytes::copy_from_slice(&raw[1..])),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("reflexdb-history-test-{name}-{}", std::process::id()))
    }

    #[test]
    fn records_and_reads_back_versions_newest_first() {
        let dir = temp_dir("roundtrip");
        let writer = HistoryWriter::open(&dir).unwrap();

        writer.record(0, Bytes::from("k"), 100, Some(Bytes::from("v1")));
        writer.record(0, Bytes::from("k"), 200, Some(Bytes::from("v2")));
        writer.record(0, Bytes::from("k"), 300, None); // deleted
        writer.flush();

        assert_eq!(writer.get_at(0, b"k", 150), Some(Bytes::from("v1")));
        assert_eq!(writer.get_at(0, b"k", 250), Some(Bytes::from("v2")));
        assert_eq!(writer.get_at(0, b"k", 300), None); // deleted as of ts=300
        assert_eq!(writer.get_at(0, b"k", 50), None); // predates the oldest version

        let history = writer.history(0, b"k", 10);
        assert_eq!(
            history,
            vec![(300, None), (200, Some(Bytes::from("v2"))), (100, Some(Bytes::from("v1")))]
        );

        let limited = writer.history(0, b"k", 1);
        assert_eq!(limited, vec![(300, None)]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn different_dbs_and_keys_do_not_cross_contaminate() {
        let dir = temp_dir("isolation");
        let writer = HistoryWriter::open(&dir).unwrap();

        writer.record(0, Bytes::from("k"), 100, Some(Bytes::from("db0")));
        writer.record(1, Bytes::from("k"), 100, Some(Bytes::from("db1")));
        writer.record(0, Bytes::from("other"), 100, Some(Bytes::from("unrelated")));
        writer.flush();

        assert_eq!(writer.get_at(0, b"k", 200), Some(Bytes::from("db0")));
        assert_eq!(writer.get_at(1, b"k", 200), Some(Bytes::from("db1")));
        assert_eq!(writer.history(0, b"k", 10).len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sweep_removes_only_entries_older_than_cutoff() {
        let dir = temp_dir("sweep");
        let writer = HistoryWriter::open(&dir).unwrap();

        writer.record(0, Bytes::from("k"), 100, Some(Bytes::from("old")));
        writer.record(0, Bytes::from("k"), 500, Some(Bytes::from("new")));
        writer.flush();

        let removed = writer.sweep_older_than(300);
        assert_eq!(removed, 1);

        // The old version is gone...
        assert_eq!(writer.get_at(0, b"k", 100), None);
        // ...but the one at/after the cutoff survives.
        assert_eq!(writer.get_at(0, b"k", 500), Some(Bytes::from("new")));
        assert_eq!(writer.history(0, b"k", 10), vec![(500, Some(Bytes::from("new")))]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = temp_dir("persist");
        {
            let writer = HistoryWriter::open(&dir).unwrap();
            writer.record(0, Bytes::from("k"), 100, Some(Bytes::from("v")));
            writer.flush();
        }
        {
            let writer = HistoryWriter::open(&dir).unwrap();
            assert_eq!(writer.get_at(0, b"k", 200), Some(Bytes::from("v")));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
