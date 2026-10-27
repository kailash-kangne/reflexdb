//! Append-only command log. Every write command is logged as the exact RESP
//! multibulk it would be received as, prefixed with a `SELECT` whenever the
//! target database changes — so replay is just "feed this file back through
//! the same RESP decoder and dispatcher".
//!
//! # Why a dedicated writer thread
//!
//! `append()` is called synchronously from `commands::dispatch()`, which
//! itself runs inline (no `.await`) on whichever tokio worker thread is
//! handling that connection. The original implementation put the actual
//! `write(2)` (and, for `appendfsync always`, `fsync(2)`) syscall under a
//! plain `Mutex` taken directly on that path. Under concurrent writers that
//! serializes every worker thread touching the AOF on one blocking syscall:
//! whichever thread doesn't hold the lock isn't just waiting, it's parked,
//! so *every other connection scheduled on that worker* stalls too, not
//! just the one doing the write. Measured impact: ~220k req/s SET with no
//! persistence vs. ~64k req/s with `appendonly yes` (default `everysec`)
//! on the same box — a 71% drop from lock/syscall contention, not disk
//! bandwidth.
//!
//! The fix is a single background thread that owns the file and is the
//! only thing that ever calls `write`/`fsync` on it. `append`/`fsync` just
//! hand it pre-encoded bytes over a channel and return immediately —
//! **fire-and-forget, the same trade-off most Redis-compatible servers make**: a
//! command can reply to the client before its AOF bytes have actually hit
//! the kernel, so a process crash (not just a power loss) in that narrow
//! window can lose the last few commands. That's an explicit choice to
//! chase speed over the old "block until written" guarantee, made because
//! no mainstream Redis-compatible server actually gives you more than
//! this in practice either. What we get in exchange: `append()` is now
//! just an encode + channel push, no lock held across a syscall and no
//! waiting on the writer thread at all — the full cost of `write`/`fsync`
//! moves entirely off the request path. The writer thread still drains
//! everything already queued and coalesces it into one `write` (and, for
//! `appendfsync always`, one shared `fsync`) per batch — the same "group
//! commit" trick Postgres uses for `synchronous_commit` — so throughput
//! under concurrent load is bounded by how fast the writer thread loops,
//! not by how many connections are writing at once.
//!
//! `SELECT`-prefix bookkeeping (`last_db`) moves into the writer thread as
//! well, since it's now the only thing touching the file — that drops the
//! second lock the old code needed entirely.
//!
//! One caller still needs a real (not fire-and-forget) answer:
//! `WAITAOF numlocal ...` exists specifically to let a client confirm "yes,
//! my writes are actually durable now" — reporting that honestly requires
//! an actual round trip to the writer thread, not just checking whether
//! AOF is turned on. `AofWriter::flush_and_fsync` is that one blocking
//! escape hatch, used nowhere else.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
use std::thread;

use bytes::{Bytes, BytesMut};
use keyspace::{Store, Value};
use tokio_util::codec::Decoder;

/// How aggressively the AOF is fsync'd to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Fsync {
    /// fsync after every batch the writer thread flushes — slowest,
    /// safest of the three (though, per the module docs, "safest" here
    /// still means "fire-and-forget on the request path").
    Always,
    /// fsync roughly once a second from a background tick (the caller is
    /// responsible for calling `AofWriter::fsync` on that cadence).
    #[default]
    EverySec,
    /// Never fsync explicitly; let the OS flush on its own schedule.
    No,
}

/// One unit of work for the writer thread.
enum Job {
    /// Append `bytes` (already RESP-encoded, not yet `SELECT`-prefixed) for
    /// `db_index`.
    Write { db_index: usize, bytes: Vec<u8> },
    /// Explicit fsync request (the `everysec` background tick).
    Fsync,
    /// Ack, with the write/fsync result, once every job sent before this
    /// one has been written *and* fsync'd — regardless of the configured
    /// `Fsync` policy. Used by `WAITAOF` to report real local durability
    /// instead of just "is AOF turned on" (see its module docs), and by
    /// tests to deterministically wait for the writer thread to drain
    /// without sleeping/polling.
    Flush(mpsc::SyncSender<io::Result<()>>),
    /// `BGREWRITEAOF`, step 1: start mirroring every subsequent `Write`
    /// into a side buffer, on top of (not instead of) writing it to the
    /// current file as normal. See the module docs above
    /// `build_rewrite_base` for why this has to happen *before* the
    /// caller walks the keyspace, not after.
    BeginRewrite,
    /// `BGREWRITEAOF`, step 2: `base` (the reconstructed keyspace state as
    /// of just before `BeginRewrite`) plus everything the side buffer
    /// captured since then together form the complete new AOF — written
    /// to a temp file and renamed over `path` (same atomic-swap trick
    /// `persistence::rdb::save` uses), after which the writer thread
    /// starts appending to the new file and the old one is gone. A
    /// `FinishRewrite` with no matching `BeginRewrite` (or sent twice) is
    /// logged and ignored rather than corrupting anything.
    FinishRewrite { base: Vec<u8>, path: PathBuf },
}

/// Captures every `Write` job that arrives between `Job::BeginRewrite` and
/// `Job::FinishRewrite`, so none of them are lost once the old file is
/// discarded in favor of `base` + this buffer. Tracks its own `SELECT`
/// state independently from the live file's, since the rewritten file
/// starts fresh regardless of what database the live file last wrote to.
struct RewriteState {
    buffer: Vec<u8>,
    last_db: Option<usize>,
}

pub struct AofWriter {
    tx: Mutex<Sender<Job>>,
    // Keeps the writer thread alive for the program's lifetime.
    _worker: thread::JoinHandle<()>,
}

impl AofWriter {
    pub fn open(path: &Path, fsync: Fsync) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let (tx, rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("reflexdb-aof-writer".into())
            .spawn(move || writer_loop(file, rx, fsync))
            .map_err(|e| io::Error::other(format!("failed to spawn AOF writer thread: {e}")))?;
        Ok(Self { tx: Mutex::new(tx), _worker: worker })
    }

    /// Queue one command for the log, to be preceded by a `SELECT` if the
    /// database changed since the last queued append. Returns as soon as
    /// the job is handed to the writer thread's queue — see the module
    /// docs for the fire-and-forget trade-off this implies.
    pub fn append(&self, db_index: usize, req: &[Bytes]) -> io::Result<()> {
        self.send(Job::Write { db_index, bytes: encode(req) })
    }

    /// Queue an explicit fsync, for the `everysec` background tick.
    /// Returns as soon as the request is handed to the writer thread —
    /// doesn't wait for the fsync to actually happen.
    pub fn fsync(&self) -> io::Result<()> {
        self.send(Job::Fsync)
    }

    fn send(&self, job: Job) -> io::Result<()> {
        self.tx.lock().unwrap().send(job).map_err(|_| writer_gone())
    }

    /// Blocks until everything already queued before this call has been
    /// written *and* fsync'd to disk, regardless of the configured `Fsync`
    /// policy, and reports whether that actually succeeded. This is the
    /// one place in this module that still waits on the writer thread —
    /// deliberately so, for the one caller (`WAITAOF`) whose entire job is
    /// to give an honest durability answer rather than a fire-and-forget
    /// one.
    pub fn flush_and_fsync(&self) -> io::Result<()> {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        self.send(Job::Flush(ack_tx))?;
        ack_rx.recv().unwrap_or_else(|_| Err(writer_gone()))
    }

    /// `BGREWRITEAOF`, step 1 — see `Job::BeginRewrite`'s docs. Must be
    /// sent (and this call returning doesn't guarantee the writer thread
    /// has processed it yet, just enqueued it — that's fine, see
    /// `build_rewrite_base`'s docs for why the ordering guarantee doesn't
    /// need it to) before walking the keyspace to build `base`.
    pub fn begin_rewrite(&self) -> io::Result<()> {
        self.send(Job::BeginRewrite)
    }

    /// `BGREWRITEAOF`, step 2 — see `Job::FinishRewrite`'s docs.
    /// Fire-and-forget like `append`/`fsync`: returns once the job is
    /// queued, not once the file swap has actually happened.
    pub fn finish_rewrite(&self, base: Vec<u8>, path: PathBuf) -> io::Result<()> {
        self.send(Job::FinishRewrite { base, path })
    }
}

/// Builds the "minimal command sequence" that reconstructs `store`'s
/// current state — what `BGREWRITEAOF` uses to replace a long, ever-
/// growing command history with just enough `SET`/`RPUSH`/`HSET`/`SADD`/
/// `ZADD`/`PEXPIREAT` calls to recreate what's there right now. Unlike
/// real Redis's rewrite (which batches many keys per command for
/// compactness), this emits one command per key — simpler, and still
/// correct, just a larger file than Redis would produce for the same
/// data.
///
/// Must be called by `BGREWRITEAOF`'s caller only *after*
/// `AofWriter::begin_rewrite` has been sent (not necessarily processed —
/// just sent). That ordering is what guarantees correctness without a
/// `fork()`-based point-in-time snapshot: any write whose keyspace
/// mutation happens after `begin_rewrite` was sent has its corresponding
/// `Job::Write` enqueued after it too (a command's AOF append always
/// follows its keyspace mutation, never precedes it), so the writer
/// thread's side buffer is guaranteed to capture it even if this walk
/// already passed that key with a stale value. A write that completes
/// *before* `begin_rewrite` is sent is simply reflected directly in this
/// walk instead. Either way, nothing is lost — some keys may just be
/// briefly represented twice (once here, once in the side buffer), which
/// is harmless since both reconstruct to the same final state.
pub fn build_rewrite_base(store: &Store) -> Vec<u8> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut out = Vec::new();
    for db_index in 0..store.len() {
        let db = store.db(db_index).expect("index within store.len()");
        let entries = db.snapshot_entries();
        if entries.is_empty() {
            continue;
        }
        out.extend_from_slice(&encode(&select_request(db_index)));
        for (key, value, ttl) in entries {
            for req in reconstruct_commands(&key, &value) {
                out.extend_from_slice(&encode(&req));
            }
            if let Some(ttl) = ttl {
                let at_ms = now_ms.saturating_add(ttl.as_millis() as u64);
                let req = [Bytes::from_static(b"PEXPIREAT"), key.clone(), Bytes::from(at_ms.to_string())];
                out.extend_from_slice(&encode(&req));
            }
        }
    }
    out
}

/// The command(s) that recreate `value` under `key` from scratch. Empty
/// for an empty collection — shouldn't normally happen (`keyspace::Db`
/// deletes a container once its last element is removed, and a `Stream`
/// can be legitimately empty but still needs its groups rebuilt even with
/// zero entries) but a skip here is cheaper than risking e.g. `RPUSH key`
/// with no elements to replay.
fn reconstruct_commands(key: &Bytes, value: &Value) -> Vec<Vec<Bytes>> {
    match value {
        Value::String(s) => vec![vec![Bytes::from_static(b"SET"), key.clone(), s.clone()]],
        Value::List(list) => {
            if list.is_empty() {
                return Vec::new();
            }
            let mut req = vec![Bytes::from_static(b"RPUSH"), key.clone()];
            req.extend(list.iter().cloned());
            vec![req]
        }
        Value::Hash(hash) => {
            if hash.is_empty() {
                return Vec::new();
            }
            let mut req = vec![Bytes::from_static(b"HSET"), key.clone()];
            for (field, val) in hash {
                req.push(field.clone());
                req.push(val.clone());
            }
            vec![req]
        }
        Value::Set(set) => {
            if set.is_empty() {
                return Vec::new();
            }
            let mut req = vec![Bytes::from_static(b"SADD"), key.clone()];
            req.extend(set.iter().cloned());
            vec![req]
        }
        Value::ZSet(zset) => {
            let sorted = zset.sorted();
            if sorted.is_empty() {
                return Vec::new();
            }
            let mut req = vec![Bytes::from_static(b"ZADD"), key.clone()];
            for (member, score) in sorted {
                req.push(Bytes::from(score.to_string()));
                req.push(member);
            }
            vec![req]
        }
        // One explicit-ID `XADD` per entry reproduces the stream's exact
        // entries and IDs. Each group's `last_delivered_id` is restored
        // via `XGROUP CREATE ... SETID`-equivalent (`CREATE` already
        // takes a start ID). **Not** reconstructed: in-flight pending
        // (unacknowledged) entries — a rewrite loses each group's PEL,
        // same category of trade-off as AOF/`BGSAVE` being fire-and-forget
        // elsewhere in this module. A consumer whose entries were still
        // pending across a rewrite+restart would need to re-read them via
        // `XREADGROUP ... >` as if they were new, same recovery path real
        // Redis expects after a crashed consumer anyway (`XCLAIM`, not
        // implemented here, is the real fix — see `keyspace::stream`'s
        // module docs for the full list of what's cut).
        Value::Stream(stream) => {
            let mut reqs = Vec::new();
            for (id, fields) in stream.range(keyspace::StreamId::MIN, keyspace::StreamId::MAX, None) {
                let mut req =
                    vec![Bytes::from_static(b"XADD"), key.clone(), Bytes::from(id.to_display_string())];
                for (field, val) in fields {
                    req.push(field);
                    req.push(val);
                }
                reqs.push(req);
            }
            for (name, start) in stream.group_starts() {
                reqs.push(vec![
                    Bytes::from_static(b"XGROUP"),
                    Bytes::from_static(b"CREATE"),
                    key.clone(),
                    name,
                    Bytes::from(start.to_display_string()),
                    Bytes::from_static(b"MKSTREAM"),
                ]);
            }
            reqs
        }
    }
}

fn writer_gone() -> io::Error {
    io::Error::other("AOF writer thread is no longer running")
}

/// Runs on the dedicated writer thread for the lifetime of the process.
/// Blocks for the first queued job, then drains everything else already
/// queued without blocking, coalescing all of it into one `write` (and at
/// most one `fsync`) — this is what turns N concurrent callers into O(1)
/// syscalls instead of N.
fn writer_loop(mut file: File, rx: Receiver<Job>, fsync_policy: Fsync) {
    let mut last_db: Option<usize> = None;
    let mut rewrite: Option<RewriteState> = None;
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        while let Ok(job) = rx.try_recv() {
            batch.push(job);
        }

        let mut buf = Vec::new();
        let mut explicit_fsync_requested = false;
        let mut flush_acks = Vec::new();
        let mut pending_finish = None;
        for job in batch {
            match job {
                Job::Write { db_index, bytes } => {
                    if last_db != Some(db_index) {
                        buf.extend_from_slice(&encode(&select_request(db_index)));
                        last_db = Some(db_index);
                    }
                    buf.extend_from_slice(&bytes);

                    if let Some(rw) = &mut rewrite {
                        if rw.last_db != Some(db_index) {
                            rw.buffer.extend_from_slice(&encode(&select_request(db_index)));
                            rw.last_db = Some(db_index);
                        }
                        rw.buffer.extend_from_slice(&bytes);
                    }
                }
                Job::Fsync => explicit_fsync_requested = true,
                // A `Flush` job needs the same guarantee an explicit
                // `fsync` does — it just also wants to hear back.
                Job::Flush(ack) => {
                    explicit_fsync_requested = true;
                    flush_acks.push(ack);
                }
                Job::BeginRewrite => {
                    if rewrite.is_some() {
                        tracing::warn!("AOF rewrite already in progress; ignoring duplicate BGREWRITEAOF");
                    } else {
                        rewrite = Some(RewriteState { buffer: Vec::new(), last_db: None });
                    }
                }
                Job::FinishRewrite { base, path } => match rewrite.take() {
                    Some(rw) => pending_finish = Some((base, rw, path)),
                    None => tracing::warn!("no AOF rewrite in progress; ignoring FinishRewrite"),
                },
            }
        }

        let write_result = if buf.is_empty() { Ok(()) } else { file.write_all(&buf) };
        if let Err(e) = &write_result {
            tracing::warn!(error = %e, "AOF writer thread failed to write a batch");
        }

        let fsync_result =
            if write_result.is_ok() && (explicit_fsync_requested || fsync_policy == Fsync::Always) {
                file.sync_data()
            } else {
                Ok(())
            };
        if let Err(e) = &fsync_result {
            tracing::warn!(error = %e, "AOF writer thread failed to fsync");
        }

        // Swap the whole file over to `base` + whatever the side buffer
        // captured, only after this batch's normal write/fsync above has
        // landed — otherwise a batch that contains both ordinary writes
        // and the `FinishRewrite` that follows them could race the rename
        // against `file.write_all` above still targeting the old file.
        if let Some((mut base, rw, path)) = pending_finish {
            base.extend_from_slice(&rw.buffer);
            match swap_aof_file(&path, &base) {
                Ok(new_file) => {
                    tracing::info!(bytes = base.len(), path = %path.display(), "AOF rewrite complete");
                    file = new_file;
                    last_db = rw.last_db;
                }
                Err(e) => tracing::warn!(error = %e, "AOF rewrite failed to swap in the new file"),
            }
        }

        for ack in flush_acks {
            let result = write_result
                .as_ref()
                .map_err(clone_io_error)
                .and_then(|()| fsync_result.as_ref().map_err(clone_io_error).map(|_| ()));
            let _ = ack.send(result);
        }
    }
}

/// Writes `bytes` to a temp file next to `path` and renames it over
/// `path` (atomic on POSIX, same trick `persistence::rdb::save` uses),
/// then reopens `path` in append mode as the writer thread's new live
/// file handle.
fn swap_aof_file(path: &Path, bytes: &[u8]) -> io::Result<File> {
    // Derived from `path`'s own name (not just the pid): two different AOF
    // files written from the same process — e.g. more than one
    // `AofWriter` in one test binary — must not share a temp path, or one
    // rewrite's in-flight temp file can be clobbered/raced by another's.
    let file_name =
        path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| "aof".into());
    let tmp_name = format!("{file_name}.rewrite-tmp-{}", std::process::id());
    let tmp_path = match path.parent() {
        Some(dir) => dir.join(tmp_name),
        None => PathBuf::from(tmp_name),
    };
    fs::write(&tmp_path, bytes)?;
    fs::rename(&tmp_path, path)?;
    OpenOptions::new().create(true).append(true).open(path)
}

fn select_request(db_index: usize) -> [Bytes; 2] {
    [Bytes::from_static(b"SELECT"), Bytes::from(db_index.to_string())]
}

fn clone_io_error(e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), e.to_string())
}

fn encode(req: &[Bytes]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", req.len()).as_bytes());
    for part in req {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Decode every request logged in an AOF file, in order, ready to be
/// replayed through the same command dispatcher that wrote them. Returns an
/// empty list (not an error) if the file doesn't exist yet.
pub fn replay(path: &Path) -> io::Result<Vec<Vec<Bytes>>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = std::fs::read(path)?;
    let mut buf = BytesMut::from(&data[..]);
    let mut decoder = resp::RequestDecoder;
    let mut requests = Vec::new();
    loop {
        match decoder.decode(&mut buf) {
            Ok(Some(req)) => requests.push(req),
            Ok(None) => break,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        }
    }
    Ok(requests)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_then_replay_round_trips_and_injects_select() {
        let path = std::env::temp_dir().join(format!("reflexdb-aof-test-{}.aof", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let writer = AofWriter::open(&path, Fsync::Always).unwrap();
        writer
            .append(0, &[Bytes::from_static(b"SET"), Bytes::from_static(b"a"), Bytes::from_static(b"1")])
            .unwrap();
        writer
            .append(2, &[Bytes::from_static(b"SET"), Bytes::from_static(b"b"), Bytes::from_static(b"2")])
            .unwrap();
        writer.append(2, &[Bytes::from_static(b"DEL"), Bytes::from_static(b"a")]).unwrap();
        writer.flush_and_fsync().unwrap();

        let requests = replay(&path).unwrap();
        let as_strs: Vec<Vec<String>> = requests
            .iter()
            .map(|r| r.iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect())
            .collect();
        assert_eq!(
            as_strs,
            vec![
                vec!["SELECT".to_string(), "0".to_string()],
                vec!["SET".to_string(), "a".to_string(), "1".to_string()],
                vec!["SELECT".to_string(), "2".to_string()],
                vec!["SET".to_string(), "b".to_string(), "2".to_string()],
                vec!["DEL".to_string(), "a".to_string()],
            ]
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn replay_missing_file_returns_empty() {
        let path = std::env::temp_dir().join("reflexdb-aof-does-not-exist.aof");
        assert_eq!(replay(&path).unwrap(), Vec::<Vec<Bytes>>::new());
    }

    /// Many threads hammering `append` concurrently (the group-commit path)
    /// must still (a) eventually land every single append — nothing
    /// silently dropped by the batching — and (b) never interleave/corrupt
    /// a single append's bytes, even though the writer thread coalesces
    /// many of them into one `write`. `flush_and_fsync()` waits for
    /// the writer thread to drain everything sent before it, since
    /// `append()` itself is fire-and-forget.
    #[test]
    fn concurrent_appends_all_land_and_stay_well_formed() {
        let path =
            std::env::temp_dir().join(format!("reflexdb-aof-concurrent-test-{}.aof", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let writer = std::sync::Arc::new(AofWriter::open(&path, Fsync::EverySec).unwrap());
        const THREADS: usize = 16;
        const PER_THREAD: usize = 200;
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let writer = writer.clone();
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let key = Bytes::from(format!("t{t}-k{i}"));
                        writer
                            .append(0, &[Bytes::from_static(b"SET"), key, Bytes::from_static(b"v")])
                            .expect("channel send should succeed while the writer thread is alive");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        writer.flush_and_fsync().unwrap();

        let requests = replay(&path).unwrap();
        // Every SET is well-formed (decoder didn't choke on interleaved
        // bytes) and exactly THREADS * PER_THREAD of them made it in, with
        // no duplicates/drops.
        let sets: std::collections::HashSet<String> = requests
            .iter()
            .filter(|r| r[0].as_ref() == b"SET")
            .map(|r| String::from_utf8_lossy(&r[1]).into_owned())
            .collect();
        assert_eq!(sets.len(), THREADS * PER_THREAD);

        let _ = std::fs::remove_file(&path);
    }

    /// `BGREWRITEAOF`'s basic promise: after a rewrite, the file is
    /// shorter (just the current state, not the whole history that
    /// produced it) and replaying it reconstructs the same final state.
    #[test]
    fn rewrite_shrinks_the_file_and_preserves_final_state() {
        let path = std::env::temp_dir().join(format!("reflexdb-aof-rewrite-test-{}.aof", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let writer = AofWriter::open(&path, Fsync::Always).unwrap();

        // A long history of overwrites to the same key — the rewrite
        // should collapse all of it into one `SET k v100`.
        for i in 0..100 {
            writer
                .append(
                    0,
                    &[Bytes::from_static(b"SET"), Bytes::from_static(b"k"), Bytes::from(i.to_string())],
                )
                .unwrap();
        }
        writer.flush_and_fsync().unwrap();
        let len_before = std::fs::metadata(&path).unwrap().len();

        let store = Store::new(1);
        store.db(0).unwrap().set(Bytes::from_static(b"k"), Value::String(Bytes::from("99")), None);
        let base = build_rewrite_base(&store);
        writer.begin_rewrite().unwrap();
        writer.finish_rewrite(base, path.clone()).unwrap();
        writer.flush_and_fsync().unwrap();

        let len_after = std::fs::metadata(&path).unwrap().len();
        assert!(len_after < len_before, "rewrite should shrink the file: {len_before} -> {len_after}");

        let requests = replay(&path).unwrap();
        let sets: Vec<&Vec<Bytes>> = requests.iter().filter(|r| r[0].as_ref() == b"SET").collect();
        assert_eq!(sets.len(), 1, "expected the 100 overwrites collapsed into one SET, got {sets:?}");
        assert_eq!(sets[0][2].as_ref(), b"99");

        let _ = std::fs::remove_file(&path);
    }

    /// The property the whole `begin_rewrite`-before-the-walk design
    /// exists for: a write that lands *after* `begin_rewrite` was sent
    /// (simulating a concurrent connection's write racing the keyspace
    /// walk `BGREWRITEAOF` does) must still survive the rewrite, even
    /// though it's invisible to `base` (built from a `Store` that predates
    /// the write).
    #[test]
    fn a_write_during_the_rewrite_window_is_not_lost() {
        let path =
            std::env::temp_dir().join(format!("reflexdb-aof-rewrite-race-test-{}.aof", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let writer = AofWriter::open(&path, Fsync::Always).unwrap();

        writer
            .append(0, &[Bytes::from_static(b"SET"), Bytes::from_static(b"old"), Bytes::from_static(b"1")])
            .unwrap();
        writer.flush_and_fsync().unwrap();

        // `base` is built from a store that does NOT have "new" in it yet
        // — standing in for "the walk already read past this key (or
        // never had it) before the concurrent write happened".
        let store = Store::new(1);
        store.db(0).unwrap().set(Bytes::from_static(b"old"), Value::String(Bytes::from("1")), None);
        writer.begin_rewrite().unwrap();
        // This write's `Job::Write` is sent *after* `begin_rewrite`'s, so
        // the writer thread's side buffer must capture it regardless of
        // what `base` (built concurrently, from a stale `store`) contains.
        writer
            .append(0, &[Bytes::from_static(b"SET"), Bytes::from_static(b"new"), Bytes::from_static(b"2")])
            .unwrap();
        let base = build_rewrite_base(&store);
        writer.finish_rewrite(base, path.clone()).unwrap();
        writer.flush_and_fsync().unwrap();

        let requests = replay(&path).unwrap();
        let has_new = requests.iter().any(|r| r[0].as_ref() == b"SET" && r[1].as_ref() == b"new");
        assert!(has_new, "write during the rewrite window must survive, got {requests:?}");

        let _ = std::fs::remove_file(&path);
    }
}
