//! Disk-backed, bounded version store for `GETAT`/`HISTORY` (the per-key
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
//! - History is bounded by both age (`history-retention-ms`) and a hard
//!   per-key version cap (`history-max-versions-per-key`). A zero age
//!   retention disables time-based expiry; the version cap still applies.
//!   Age expiry runs in a periodic maintenance task, while count trimming
//!   runs on the history writer thread. Limit changes compact old entries
//!   in bounded batches.
//! - **Writes are batched off the request path**, the same
//!   dedicated-writer-thread-plus-channel shape as `crate::aof::AofWriter`:
//!   `record()` just encodes a small job and pushes it to an
//!   `mpsc::channel`, returning immediately. The writer thread drains
//!   everything already queued and commits it as one `fjall` write batch,
//!   so N concurrent `record()` callers cost O(1) batched commits instead
//!   of O(N) individual ones.
//!
//! # Crash guarantee — weaker than the AOF, on purpose
//!
//! `record()` is fire-and-forget: a version is durable only once the
//! writer thread's batch commits. A crash (or `kill -9`) can therefore
//! lose up to ~one in-flight batch of the *newest* versions — never
//! already-committed ones. If the writer can't keep up with sustained
//! writes the bounded queue fills and versions are *dropped* (counted in
//! `history_dropped`) rather than throttling the keyspace — history must
//! never gate keyspace writes. `GETAT`/`HISTORY` may thus report the most
//! recent ~milliseconds of history as missing after a crash, or gaps
//! under sustained overload; older committed versions are complete.
//! `flush()` blocks until pending records have been through a commit and
//! returns whether it landed; `io_failed()` reports whether the last
//! commit failed (surfaced via `INFO`/`metrics-port` since `record()`
//! itself can't return the error).
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
//! `BY_TIME` entries carry an empty value — the sweep derives the matching
//! `BY_KEY` entry's key from the `BY_TIME` entry's *key* bytes it already
//! scanned, so duplicating the payload would just double the bytes journaled
//! and compacted per version for nothing.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fjall::config::CompressionPolicy;
use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode};

const BY_KEY: u8 = 1;
const BY_TIME: u8 = 2;
const BY_COUNT: u8 = 3;
const HISTORY_LIMIT_META: &[u8] = b"\0max_versions_per_key";
pub const DEFAULT_MAX_VERSIONS_PER_KEY: usize = 200;
const MAX_VERSIONS_PER_KEY: usize = 1_000_000;
const COMPACT_BATCH: usize = 4096;

/// One queued version-append batch, handed to the writer thread. One
/// command's worth of writes travels as a single channel message so a
/// multi-key write costs one `try_send`, not N.
enum Job {
    Records {
        ts_ms: u64,
        items: Vec<(u16, Bytes, Option<Bytes>)>,
    },
    /// Ack once every job sent before this one has been through the
    /// writer's batch commit — used by tests (and anything else that
    /// needs "has this landed yet") to avoid sleeping/polling. `true` =
    /// committed; `false` = the batch failed. Mirrors `aof::Job::Flush`.
    Flush(mpsc::SyncSender<bool>),
}

pub struct HistoryWriter {
    // A single shared `SyncSender` (it's `Sync`) — no outer mutex. What
    // `Flush` needs is "a `record()` that returned is already queued,"
    // which holds without one: messages land in send-completion order.
    tx: SyncSender<Job>,
    // Kept alongside `keyspace` so `sweep_older_than` can start a
    // `WriteBatch` (`Database::batch()`) without the writer thread's copy.
    db: Database,
    keyspace: Keyspace,
    max_versions_per_key: usize,
    write_lock: Arc<Mutex<()>>,
    /// Set when the writer thread's last `commit` failed, cleared once a
    /// batch lands cleanly. `record()` is fire-and-forget, so a version
    /// dropped by a failed batch has already passed its write — like
    /// `aof`'s `io_failed`, all we can do is report it (INFO/metrics)
    /// rather than pretend it landed.
    io_failed: Arc<AtomicBool>,
    /// Versions dropped on a full queue — the writer couldn't keep up.
    dropped: Arc<AtomicU64>,
    /// Bumped by `sweep_older_than` after it rewrites `BY_COUNT` entries —
    /// the writer thread watches this to invalidate its count cache
    /// (checked under `write_lock`, so it can never observe a half-swept
    /// state).
    sweep_epoch: Arc<AtomicU64>,
    // Keeps the writer thread alive for the program's lifetime.
    _worker: thread::JoinHandle<()>,
}

impl HistoryWriter {
    pub fn open(dir: &Path) -> io::Result<Self> {
        Self::open_with_limit(dir, DEFAULT_MAX_VERSIONS_PER_KEY)
    }

    /// Open the disk history store and enforce a hard cap on the number of
    /// versions retained per key. A limit change triggers a bounded-memory
    /// compaction on the writer thread before it drains new record jobs.
    pub fn open_with_limit(dir: &Path, max_versions_per_key: usize) -> io::Result<Self> {
        if !(1..=MAX_VERSIONS_PER_KEY).contains(&max_versions_per_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("history version limit must be between 1 and {MAX_VERSIONS_PER_KEY}"),
            ));
        }
        std::fs::create_dir_all(dir)?;
        let db = Database::builder(dir).open().map_err(io::Error::other)?;
        // manual_journal_persist: commits land in the journal's OS-buffered
        // pages immediately; the writer thread calls `persist(SyncData)`
        // every PERSIST_INTERVAL instead of paying an fsync per batch —
        // history's crash guarantee already only promises committed-prefix
        // (~a sync interval of newest versions at worst, and only on
        // power/OS loss — `kill -9` leaves OS pages intact). Lz4 on every
        // level: version payloads are the bulk of journal+compaction bytes.
        let keyspace = db
            .keyspace("reflexdb-history", || {
                KeyspaceCreateOptions::default()
                    .manual_journal_persist(true)
                    .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
            })
            .map_err(io::Error::other)?;
        let limit_marker = keyspace.get(HISTORY_LIMIT_META).map_err(io::Error::other)?;
        let expected_marker = (max_versions_per_key as u64).to_be_bytes();
        let compact_on_start = limit_marker.as_deref() != Some(expected_marker.as_slice());
        let (tx, rx) = mpsc::sync_channel(4096);
        let io_failed = Arc::new(AtomicBool::new(false));
        let sweep_epoch = Arc::new(AtomicU64::new(0));
        let worker_db = db.clone();
        let worker_keyspace = keyspace.clone();
        let write_lock = Arc::new(Mutex::new(()));
        let worker_write_lock = write_lock.clone();
        let worker_io_failed = io_failed.clone();
        let worker_sweep_epoch = sweep_epoch.clone();
        let worker = thread::Builder::new()
            .name("reflexdb-history-writer".into())
            .spawn(move || {
                writer_loop(
                    worker_db,
                    worker_keyspace,
                    rx,
                    max_versions_per_key,
                    compact_on_start,
                    worker_write_lock,
                    worker_io_failed,
                    worker_sweep_epoch,
                )
            })
            .map_err(|e| io::Error::other(format!("failed to spawn history writer thread: {e}")))?;
        Ok(Self {
            tx,
            db,
            keyspace,
            max_versions_per_key,
            write_lock,
            io_failed,
            dropped: Arc::new(AtomicU64::new(0)),
            sweep_epoch,
            _worker: worker,
        })
    }

    /// Queue one version of `key` for the history log. Fire-and-forget —
    /// returns as soon as the job is handed to the writer thread's queue;
    /// see the module docs for the batching this enables. `value: None`
    /// records a delete (or "wasn't a string"), matching `GETAT`/
    /// `HISTORY`'s existing nil-means-deleted convention.
    pub fn record(&self, db_index: usize, key: Bytes, ts_ms: u64, value: Option<Bytes>) {
        self.record_batch(ts_ms, vec![(db_index as u16, key, value)]);
    }

    /// Queue several versions sharing one timestamp as a single channel
    /// message — the request path builds one per command, so a multi-key
    /// write costs one `try_send` instead of one per key.
    pub fn record_batch(&self, ts_ms: u64, items: Vec<(u16, Bytes, Option<Bytes>)>) {
        if items.is_empty() {
            return;
        }
        // try_send, not send: a blocking send would throttle the whole
        // write path down to history-writer speed once the queue fills,
        // which is exactly what "never gates keyspace writes" forbids.
        // A full queue drops the versions and counts them — surfaced via
        // `history_dropped` in INFO/metrics. A disconnected writer means
        // shutdown, and is also "too late to record," not a bug worth
        // surfacing on every write's hot path.
        let dropped = items.len() as u64;
        match self.tx.try_send(Job::Records { ts_ms, items }) {
            Ok(()) | Err(mpsc::TrySendError::Disconnected(_)) => {}
            Err(mpsc::TrySendError::Full(_)) => {
                self.dropped.fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Versions dropped because the writer queue was full — surfaced via
    /// `history_dropped` in INFO/`metrics-port`.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Blocks until every `record()` call made before this one has been
    /// through the writer's batch commit; returns whether that batch
    /// landed. Not used on any request path — only by tests (and anything
    /// else that needs a deterministic "has this landed yet" instead of
    /// sleeping/polling).
    pub fn flush(&self) -> bool {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        self.tx.send(Job::Flush(ack_tx)).is_ok() && ack_rx.recv().unwrap_or(false)
    }

    /// `true` once the writer thread has reported a batch commit failure
    /// — surfaced via `INFO persistence`/`metrics` since `record()` is
    /// fire-and-forget and can't return the error itself.
    pub fn io_failed(&self) -> bool {
        self.io_failed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Test hook: simulates the writer thread reporting a commit failure.
    #[doc(hidden)]
    pub fn force_io_failure(&self) {
        self.io_failed.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// The string value `key` held at or before `ts_ms` — the newest
    /// recorded version whose timestamp doesn't exceed it. `None` if the
    /// key has no version that old (never existed yet, or every
    /// known version is newer than `ts_ms`), or its value at that point
    /// was a delete / not a string.
    pub fn get_at(&self, db_index: usize, key: &[u8], ts_ms: u64) -> Option<Bytes> {
        let prefix = by_key_prefix(db_index as u16, key);
        for guard in self.keyspace.prefix(&prefix).rev().take(self.max_versions_per_key) {
            let Ok((full_key, value)) = guard.into_inner() else { continue };
            let Some(entry_ts) = parse_ts_suffix(&full_key, prefix.len()) else { continue };
            if entry_ts > ts_ms {
                continue;
            }
            return decode_value(&value);
        }
        None
    }

    /// Up to `limit`, but never more than the configured per-key cap, most
    /// recent `(ts_ms, value-or-none)` versions for `key`, newest first.
    pub fn history(&self, db_index: usize, key: &[u8], limit: usize) -> Vec<(u64, Option<Bytes>)> {
        let prefix = by_key_prefix(db_index as u16, key);
        self.keyspace
            .prefix(&prefix)
            .rev()
            .take(limit.min(self.max_versions_per_key))
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
        let _write_guard = self.write_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let lower = vec![BY_TIME];
        let mut upper = vec![BY_TIME];
        upper.extend_from_slice(&cutoff_ms.to_be_bytes());

        const SWEEP_BATCH: usize = 4096;
        let stale: Vec<Vec<u8>> = self
            .keyspace
            .range(lower..upper)
            .filter_map(|guard| guard.key().ok().map(|k| k.to_vec()))
            .take(SWEEP_BATCH)
            .collect();
        if stale.is_empty() {
            return 0;
        }

        let mut batch = self.db.batch();
        let mut decremented: HashMap<Vec<u8>, u64> = HashMap::new();
        for time_key in &stale {
            batch.remove(&self.keyspace, time_key.clone());
            if let Some(by_key) = by_time_to_by_key(time_key) {
                if let Some(prefix) = by_key_prefix_from_full(&by_key) {
                    *decremented.entry(by_count_key_from_prefix(prefix)).or_default() += 1;
                }
                batch.remove(&self.keyspace, by_key);
            }
        }
        for (count_key, removed) in decremented {
            let count = self
                .keyspace
                .get(&count_key)
                .ok()
                .flatten()
                .and_then(|bytes| bytes.as_ref().try_into().ok().map(u64::from_be_bytes));
            if let Some(count) = count {
                let remaining = count.saturating_sub(removed);
                if remaining == 0 {
                    batch.remove(&self.keyspace, count_key);
                } else {
                    batch.insert(&self.keyspace, count_key, remaining.to_be_bytes());
                }
            }
        }
        if let Err(e) = batch.commit() {
            tracing::warn!(error = %e, "history sweep failed to commit deletions");
            return 0;
        }
        // Deletions rewrote `BY_COUNT` entries — tell the writer thread to
        // drop its cached counts before its next batch.
        self.sweep_epoch.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        stale.len()
    }
}

/// Writer-thread cache of one key's on-disk state: `count` mirrors the
/// `BY_COUNT` entry and `max_ts` is an upper bound on every `ts_ms` the key
/// has on disk (every version on disk was queued through this writer, or
/// measured by `newest_ts_on_disk` on the cold path). A `ts_ms` above
/// `max_ts` is therefore guaranteed absent — steady-state batches then need
/// no LSM reads at all, which is what keeps throughput flat as the store
/// grows.
struct KeyState {
    count: u64,
    max_ts: u64,
}

/// Bound on `key_states` — cleared rather than grown past this, falling
/// back to disk reads until it repopulates. ~tens of MB worst case for
/// very large keyspaces.
const MAX_KEY_STATES: usize = 250_000;

const MAX_BATCH: usize = 4096;

/// How often the writer thread fdatasyncs the journal while
/// `manual_journal_persist` is set — the durability window the crash
/// guarantee actually exposes on OS/power loss (a `kill -9` leaves the
/// OS-buffered pages intact, so it loses nothing extra).
const PERSIST_INTERVAL: Duration = Duration::from_millis(200);

#[allow(clippy::too_many_arguments)]
fn writer_loop(
    db: Database,
    keyspace: Keyspace,
    rx: Receiver<Job>,
    max_versions_per_key: usize,
    compact_on_start: bool,
    write_lock: Arc<Mutex<()>>,
    io_failed: Arc<AtomicBool>,
    sweep_epoch: Arc<AtomicU64>,
) {
    if compact_on_start {
        if let Err(error) = compact_history(&db, &keyspace, max_versions_per_key, &write_lock) {
            tracing::warn!(%error, "history version-limit compaction failed");
        }
    }

    let mut key_states: HashMap<(u16, Bytes), KeyState> = HashMap::new();
    let mut seen_sweep_epoch = sweep_epoch.load(std::sync::atomic::Ordering::Relaxed);
    let mut jobs = Vec::with_capacity(1024);
    let mut pending_timestamps: HashMap<(u16, Bytes), HashSet<u64>> = HashMap::new();
    let mut acks = Vec::new();
    let mut touched = Vec::new();
    let mut last_persist = Instant::now();
    let mut dirty = false;

    loop {
        let first = match rx.recv_timeout(PERSIST_INTERVAL) {
            Ok(job) => job,
            // Idle tick: a committed-but-unsynced batch must still reach
            // disk inside ~the interval, even with no writes flowing.
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if dirty && last_persist.elapsed() >= PERSIST_INTERVAL {
                    let _write_guard = write_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Err(error) = db.persist(PersistMode::SyncData) {
                        tracing::warn!(%error, "history journal persist failed; will retry next interval");
                    } else {
                        dirty = false;
                    }
                    last_persist = Instant::now();
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        jobs.push(first);
        while jobs.len() < MAX_BATCH {
            let Ok(job) = rx.try_recv() else { break };
            jobs.push(job);
        }

        let _write_guard = write_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        // A sweep rewrites `BY_COUNT` entries under this same lock, so the
        // epoch read here is never mid-sweep; a bump means cached counts are
        // stale-high and would trim more than the cap allows.
        let epoch = sweep_epoch.load(std::sync::atomic::Ordering::Relaxed);
        if epoch != seen_sweep_epoch {
            key_states.clear();
            seen_sweep_epoch = epoch;
        }

        let item_count: usize = jobs
            .iter()
            .map(|j| match j {
                Job::Records { items, .. } => items.len(),
                _ => 0,
            })
            .sum();
        let mut batch = OwnedWriteBatch::with_capacity(db.clone(), item_count * 3 + 8);
        for job in jobs.drain(..) {
            match job {
                Job::Records { ts_ms, items } => {
                    for (db_index, key, value) in items {
                        pending_timestamps.entry((db_index, key.clone())).or_default().insert(ts_ms);
                        batch.insert(
                            &keyspace,
                            by_key_full(db_index, &key, ts_ms),
                            encode_value(value.as_ref()),
                        );
                        batch.insert(&keyspace, by_time_full(db_index, &key, ts_ms), Vec::new());
                    }
                }
                Job::Flush(ack) => acks.push(ack),
            }
        }
        for ((db_index, key), timestamps) in pending_timestamps.drain() {
            let prior = key_states.get(&(db_index, key.clone()));
            let mut count = prior.map_or_else(|| read_count(&keyspace, db_index, &key), |s| s.count);
            let known_max = match prior {
                Some(s) => Some(s.max_ts),
                None => newest_ts_on_disk(&keyspace, db_index, &key),
            };
            let mut added = 0u64;
            for &ts_ms in &timestamps {
                let fresh = match known_max {
                    Some(max) if ts_ms > max => true,
                    _ => keyspace.get(by_key_full(db_index, &key, ts_ms)).ok().flatten().is_none(),
                };
                if fresh {
                    added += 1;
                }
            }
            count = count.saturating_add(added);
            if added > 0 {
                batch.insert(&keyspace, by_count_key(db_index, &key), count.to_be_bytes());
            }
            // Upper bound on this key's on-disk ts after this batch commits:
            // the batch's newest ts or an older ceiling we already know.
            let ceiling = timestamps.iter().copied().max().unwrap_or(0).max(known_max.unwrap_or(0));
            touched.push((db_index, key, count, ceiling));
        }
        let committed = match batch.commit() {
            Ok(()) => {
                io_failed.store(false, std::sync::atomic::Ordering::Relaxed);
                for (db_index, key, count, ceiling) in touched.drain(..) {
                    match trim_key_history(&db, &keyspace, db_index, &key, max_versions_per_key, count) {
                        Ok(retained) => {
                            if key_states.len() >= MAX_KEY_STATES {
                                key_states.clear();
                            }
                            let prev_max = key_states.get(&(db_index, key.clone())).map_or(0, |s| s.max_ts);
                            key_states.insert(
                                (db_index, key),
                                KeyState { count: retained, max_ts: ceiling.max(prev_max) },
                            );
                        }
                        Err(error) => {
                            // Partial trim may have rewritten BY_COUNT —
                            // don't trust what we computed for this key.
                            key_states.remove(&(db_index, key.clone()));
                            tracing::warn!(%error, db_index, key = ?key, "failed to enforce per-key history limit");
                        }
                    }
                }
                true
            }
            Err(error) => {
                io_failed.store(true, std::sync::atomic::Ordering::Relaxed);
                // Batch commits are atomic, so nothing landed — the cache is
                // still valid, but clearing it is cheap insurance against
                // ever reasoning about a partially-applied batch.
                key_states.clear();
                touched.clear();
                tracing::warn!(%error, "history writer thread failed to commit a batch");
                false
            }
        };
        for ack in acks.drain(..) {
            let _ = ack.send(committed);
        }
        if committed {
            dirty = true;
            if last_persist.elapsed() >= PERSIST_INTERVAL {
                if let Err(error) = db.persist(PersistMode::SyncData) {
                    tracing::warn!(%error, "history journal persist failed; will retry next interval");
                } else {
                    dirty = false;
                }
                last_persist = Instant::now();
            }
        }
    }
}

/// Compact legacy or over-limit history once when the configured cap changes.
/// The rolling window bounds memory to `max_versions_per_key` entries while
/// the on-disk indexes are removed in batches.
fn compact_history(
    db: &Database,
    keyspace: &Keyspace,
    max_versions_per_key: usize,
    write_lock: &Mutex<()>,
) -> io::Result<()> {
    let _write_guard = write_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut current_prefix: Option<Vec<u8>> = None;
    let mut retained = VecDeque::with_capacity(max_versions_per_key.min(1024));
    let mut batch = db.batch();
    let mut ops = 0usize;
    let mut stale = 0usize;

    for guard in keyspace.prefix([BY_KEY]) {
        let Ok(raw_key) = guard.key() else { continue };
        let full_key = raw_key.to_vec();
        let Some(prefix) = by_key_prefix_from_full(&full_key) else { continue };
        if current_prefix.as_deref() != Some(prefix) {
            if let Some(previous) = current_prefix.take() {
                batch.insert(
                    keyspace,
                    by_count_key_from_prefix(&previous),
                    (retained.len() as u64).to_be_bytes(),
                );
                ops += 1;
                if ops >= COMPACT_BATCH {
                    batch.commit().map_err(io::Error::other)?;
                    batch = db.batch();
                    ops = 0;
                }
            }
            current_prefix = Some(prefix.to_vec());
            retained.clear();
        }
        retained.push_back(full_key);
        if retained.len() > max_versions_per_key {
            if let Some(oldest) = retained.pop_front() {
                batch.remove(keyspace, oldest.clone());
                if let Some(by_time) = by_key_to_by_time(&oldest) {
                    batch.remove(keyspace, by_time);
                }
                stale += 1;
                ops += 2;
                if ops >= COMPACT_BATCH {
                    batch.commit().map_err(io::Error::other)?;
                    batch = db.batch();
                    ops = 0;
                }
            }
        }
    }
    if let Some(prefix) = current_prefix {
        batch.insert(keyspace, by_count_key_from_prefix(&prefix), (retained.len() as u64).to_be_bytes());
        batch.insert(keyspace, HISTORY_LIMIT_META, (max_versions_per_key as u64).to_be_bytes());
        batch.commit().map_err(io::Error::other)?;
    } else if stale > 0 {
        batch.insert(keyspace, HISTORY_LIMIT_META, (max_versions_per_key as u64).to_be_bytes());
        batch.commit().map_err(io::Error::other)?;
    } else {
        keyspace
            .insert(HISTORY_LIMIT_META, (max_versions_per_key as u64).to_be_bytes())
            .map_err(io::Error::other)?;
    }
    Ok(())
}

/// Trim a key after its write batch commits. The common case removes only a
/// small excess; chunking also bounds temporary memory when upgrading a key
/// that accumulated many versions under an older configuration.
/// Let a key overshoot the cap before paying for a scan+delete: without
/// slack, a key sitting at the cap pays a full trim on *every* write
/// batch that touches it. `history()`/`get_at` still read at most the
/// capped window — the slack just exists on disk briefly.
const TRIM_SLACK: u64 = 64;

/// Returns the version count left on disk, so the caller's `key_states`
/// cache can store exactly what was written.
fn trim_key_history(
    db: &Database,
    keyspace: &Keyspace,
    db_index: u16,
    key: &[u8],
    max_versions_per_key: usize,
    count: u64,
) -> io::Result<u64> {
    if count <= max_versions_per_key as u64 + TRIM_SLACK {
        return Ok(count);
    }
    let prefix = by_key_prefix(db_index, key);
    let count_key = by_count_key(db_index, key);
    let mut remaining = count - max_versions_per_key as u64;
    let mut retained = count;
    while remaining > 0 {
        let stale: Vec<Vec<u8>> = keyspace
            .prefix(&prefix)
            .take(remaining.min(COMPACT_BATCH as u64) as usize)
            .filter_map(|guard| guard.key().ok().map(|key| key.to_vec()))
            .collect();
        if stale.is_empty() {
            break;
        }
        let mut batch = db.batch();
        for by_key in &stale {
            batch.remove(keyspace, by_key.clone());
            if let Some(by_time) = by_key_to_by_time(by_key) {
                batch.remove(keyspace, by_time);
            }
        }
        remaining = remaining.saturating_sub(stale.len() as u64);
        retained = count.saturating_sub((count - max_versions_per_key as u64) - remaining);
        batch.insert(keyspace, count_key.clone(), retained.to_be_bytes());
        batch.commit().map_err(io::Error::other)?;
    }
    Ok(retained)
}

/// The newest `ts_ms` `key` has on disk — one reverse `prefix` step, taken
/// only on the `key_states` cold path so `max_ts` covers versions a
/// previous process wrote (an upper bound below the true on-disk max would
/// let a rewrite of an existing ts skip the existence check and overcount).
fn newest_ts_on_disk(keyspace: &Keyspace, db_index: u16, key: &[u8]) -> Option<u64> {
    let prefix = by_key_prefix(db_index, key);
    keyspace
        .prefix(&prefix)
        .next_back()
        .and_then(|guard| guard.key().ok())
        .and_then(|full| parse_ts_suffix(&full, prefix.len()))
}

fn by_key_prefix_from_full(full_key: &[u8]) -> Option<&[u8]> {
    if full_key.first() != Some(&BY_KEY) || full_key.len() < 15 {
        return None;
    }
    let key_len = u32::from_be_bytes(full_key.get(3..7)?.try_into().ok()?) as usize;
    let prefix_len = 7usize.checked_add(key_len)?;
    (full_key.len() == prefix_len.checked_add(8)?).then_some(&full_key[..prefix_len])
}

fn by_count_key(db_index: u16, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 2 + 4 + key.len());
    out.push(BY_COUNT);
    out.extend_from_slice(&db_index.to_be_bytes());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

fn by_count_key_from_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    if let Some(tag) = out.first_mut() {
        *tag = BY_COUNT;
    }
    out
}

fn read_count(keyspace: &Keyspace, db_index: u16, key: &[u8]) -> u64 {
    keyspace
        .get(by_count_key(db_index, key))
        .ok()
        .flatten()
        .and_then(|bytes| bytes.as_ref().try_into().ok().map(u64::from_be_bytes))
        .unwrap_or_else(|| keyspace.prefix(by_key_prefix(db_index, key)).count() as u64)
}

fn by_key_to_by_time(full_key: &[u8]) -> Option<Vec<u8>> {
    let prefix = by_key_prefix_from_full(full_key)?;
    let db_index = u16::from_be_bytes(full_key.get(1..3)?.try_into().ok()?);
    let key_len = u32::from_be_bytes(full_key.get(3..7)?.try_into().ok()?) as usize;
    let key = full_key.get(7..7 + key_len)?;
    let ts_ms = parse_ts_suffix(full_key, prefix.len())?;
    Some(by_time_full(db_index, key, ts_ms))
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
    fn flush_reports_commit_result_and_io_failed_flag() {
        let dir = temp_dir("flush-ack");
        let writer = HistoryWriter::open(&dir).unwrap();

        writer.record(0, Bytes::from("k"), 100, Some(Bytes::from("v")));
        assert!(writer.flush(), "healthy writer must ack true");
        assert!(!writer.io_failed());
        writer.force_io_failure();
        assert!(writer.io_failed());

        let _ = std::fs::remove_dir_all(&dir);
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

    #[test]
    fn per_key_version_cap_trims_the_oldest_versions() {
        let dir = temp_dir("cap");
        let writer = HistoryWriter::open_with_limit(&dir, 3).unwrap();

        for ts in [100, 200, 300, 400, 500] {
            writer.record(0, Bytes::from("k"), ts, Some(Bytes::from(format!("v{ts}"))));
        }
        writer.flush();

        // Only the 3 most recent versions survive; both the forward-lookup
        // (BY_KEY) and time-ordered (BY_TIME) indexes must agree.
        assert_eq!(
            writer.history(0, b"k", 10),
            vec![
                (500, Some(Bytes::from("v500"))),
                (400, Some(Bytes::from("v400"))),
                (300, Some(Bytes::from("v300"))),
            ]
        );
        assert_eq!(writer.get_at(0, b"k", 200), None, "trimmed version must not be servable via GETAT");
        assert_eq!(writer.get_at(0, b"k", 300), Some(Bytes::from("v300")));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopening_with_a_lower_limit_compacts_existing_history() {
        let dir = temp_dir("recompact");
        {
            let writer = HistoryWriter::open_with_limit(&dir, 10).unwrap();
            for ts in [100, 200, 300, 400, 500] {
                writer.record(0, Bytes::from("k"), ts, Some(Bytes::from(format!("v{ts}"))));
            }
            writer.flush();
            assert_eq!(writer.history(0, b"k", 10).len(), 5);
        }
        {
            // Reopening with a tighter cap must retroactively trim the
            // already-written versions down to the new limit.
            let writer = HistoryWriter::open_with_limit(&dir, 2).unwrap();
            writer.flush();
            assert_eq!(
                writer.history(0, b"k", 10),
                vec![(500, Some(Bytes::from("v500"))), (400, Some(Bytes::from("v400")))]
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recording_after_a_sweep_still_trims_to_the_cap() {
        let dir = temp_dir("post-sweep");
        let writer = HistoryWriter::open_with_limit(&dir, 3).unwrap();
        for ts in [100, 200, 300] {
            writer.record(0, Bytes::from("k"), ts, Some(Bytes::from(format!("v{ts}"))));
        }
        writer.flush();
        // Sweep removes all three; their BY_COUNT entry goes to zero and is
        // deleted. The writer's cached count must be invalidated (sweep
        // epoch) or it would overcount and trim below the cap.
        assert_eq!(writer.sweep_older_than(1000), 3);
        // 70 versions > cap(3) + TRIM_SLACK(64): a stale count would push
        // the trim past every on-disk version and wipe the key's history.
        for ts in 400..470u64 {
            writer.record(0, Bytes::from("k"), ts, Some(Bytes::from(format!("v{ts}"))));
        }
        writer.flush();
        let history = writer.history(0, b"k", 10);
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].0, 469);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn re_recording_an_existing_timestamp_does_not_double_count() {
        let dir = temp_dir("dup-ts");
        let writer = HistoryWriter::open_with_limit(&dir, 3).unwrap();
        writer.record(0, Bytes::from("k"), 100, Some(Bytes::from("a")));
        writer.flush();
        // Same (key, ts) in a later batch: overwrites on disk, must not bump
        // the version count (else the cap trims real versions early).
        writer.record(0, Bytes::from("k"), 100, Some(Bytes::from("b")));
        writer.record(0, Bytes::from("k"), 200, Some(Bytes::from("c")));
        writer.flush();
        assert_eq!(
            writer.history(0, b"k", 10),
            vec![(200, Some(Bytes::from("c"))), (100, Some(Bytes::from("b")))]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_an_out_of_range_version_limit() {
        let dir = temp_dir("bad-limit");
        assert!(HistoryWriter::open_with_limit(&dir, 0).is_err());
        assert!(HistoryWriter::open_with_limit(&dir, 1_000_001).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
