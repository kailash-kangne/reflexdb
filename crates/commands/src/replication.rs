//! `REPLICAOF`/`SLAVEOF`, `PSYNC`, `REPLCONF`, `WAIT` — primary→replica
//! replication. See `docs/06-ha-replication-cluster.md` §1.
//!
//! Partial resync: every replicated write is appended to a bounded
//! backlog (`partial-resync-backlog`, default 1 MiB) once the first
//! replica attaches. A replica that reconnects with `PSYNC <replid>
//! <offset>` still covered by the backlog gets `+CONTINUE` plus just the
//! missed commands; anything else (different replid, offset evicted or
//! never seen) falls back to `FULLRESYNC`. The "offset" is a **logical
//! count of replicated commands**, not a byte offset into a replication
//! stream like real Redis — a documented simplification, not wire
//! compatibility with real Redis replication.
//!
//! `dispatch()` can't itself hold the connection open or make outbound
//! connections (it only ever gets `&ServerContext`, never the `Arc` that
//! outlives a single call) — so `REPLICAOF` here only flips
//! `ServerContext::replica_of` and bumps `replication_epoch`; the `server`
//! crate's background supervisor (which does have the `Arc`) watches that
//! epoch and owns the actual TCP connection to the primary.

use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::dispatch::{err, reply, wrong_args, ClientState, Outcome, ServerContext};
use resp::{Protocol, Value};

/// Per-replica bookkeeping, keyed by the replica connection's client id.
pub struct ReplicaState {
    pub acked_offset: AtomicU64,
    pub last_db: Mutex<Option<usize>>,
    tx: mpsc::Sender<ReplicaMessage>,
    queued_bytes: Arc<AtomicUsize>,
}

/// One RESP-encoded replication frame queued for a replica. Encoding
/// happens once in `replicate()`; every replica's queue just holds a
/// cheap `Bytes` clone of the same buffer.
pub struct ReplicaMessage {
    pub data: Bytes,
    queued_bytes: Arc<AtomicUsize>,
}

impl Drop for ReplicaMessage {
    fn drop(&mut self) {
        self.queued_bytes.fetch_sub(self.data.len(), Ordering::Relaxed);
    }
}

/// One replicated write in the backlog, tagged with its stream offset
/// and source db. `SELECT`s are not stored — catch-up re-derives db
/// switches from each entry's `db` tag.
struct BacklogEntry {
    seq: u64,
    db: usize,
    data: Bytes,
}

/// The tail of the replication stream kept for partial resync. Activated
/// lazily on the first `PSYNC` — before that no replica has a meaningful
/// offset, so nothing needs buffering. Bounded by
/// `partial-resync-backlog` bytes; oldest entries evict first.
#[derive(Default)]
pub(crate) struct ReplStream {
    entries: VecDeque<BacklogEntry>,
    bytes: usize,
    active: bool,
}

fn encode_request(parts: &[Bytes]) -> Bytes {
    let value = Value::Array(parts.iter().cloned().map(Value::bulk).collect());
    let mut buf = BytesMut::new();
    value.encode(&mut buf, Protocol::Resp2);
    buf.freeze()
}

fn encode_select(db: usize) -> Bytes {
    encode_request(&[Bytes::from_static(b"SELECT"), Bytes::from(db.to_string())])
}

impl ServerContext {
    /// Register a freshly-connected replica for a full resync: activates
    /// the partial-resync backlog (first attach only) and starts it at
    /// the current stream offset.
    pub(crate) fn register_replica(&self, client_id: u64) {
        let mut stream = self.repl_stream.lock().unwrap();
        if self.partial_resync_backlog > 0 {
            stream.active = true;
            self.backlog_active.store(true, Ordering::Relaxed);
        }
        let replica = self.new_replica_state(client_id, self.master_repl_offset.load(Ordering::Relaxed));
        self.replicas.insert(client_id, replica);
    }

    /// `PSYNC <replid> <offset>` with a matching replid: register the
    /// replica and enqueue just the backlog entries past `offset`
    /// (`+CONTINUE` on the wire). Returns false when `offset` isn't
    /// covered — the caller then does a `FULLRESYNC` instead. The whole
    /// check + enqueue runs under the stream lock so a live `replicate`
    /// can't interleave between the catch-up tail and the resume point.
    fn try_partial_resync(&self, client_id: u64, offset: u64) -> bool {
        let stream = self.repl_stream.lock().unwrap();
        if !stream.active || offset > self.master_repl_offset.load(Ordering::Relaxed) {
            return false;
        }
        let covered = match stream.entries.front() {
            Some(front) => front.seq <= offset + 1,
            None => offset == self.master_repl_offset.load(Ordering::Relaxed),
        };
        if !covered {
            return false;
        }
        let replica = self.new_replica_state(client_id, offset);
        let mut cursor_db = None;
        for entry in stream.entries.iter().filter(|e| e.seq > offset) {
            if cursor_db != Some(entry.db) {
                self.push_replica(client_id, &replica, encode_select(entry.db));
                cursor_db = Some(entry.db);
            }
            self.push_replica(client_id, &replica, entry.data.clone());
        }
        *replica.last_db.lock().unwrap() = cursor_db;
        self.replicas.insert(client_id, replica);
        true
    }

    fn new_replica_state(&self, client_id: u64, acked_offset: u64) -> ReplicaState {
        let (tx, rx) = mpsc::channel(self.replica_queue_capacity);
        self.replica_receivers.insert(client_id, Mutex::new(Some(rx)));
        ReplicaState {
            acked_offset: AtomicU64::new(acked_offset),
            last_db: Mutex::new(None),
            tx,
            queued_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn take_replica_receiver(&self, client_id: u64) -> Option<mpsc::Receiver<ReplicaMessage>> {
        self.replica_receivers.get(&client_id).and_then(|rx| rx.lock().unwrap().take())
    }

    pub fn has_replica_client(&self, client_id: u64) -> bool {
        self.replicas.contains_key(&client_id)
    }

    /// Fan a write command out to every connected replica through its
    /// dedicated bounded channel, and append it to the partial-resync
    /// backlog. Prefixes a `SELECT` whenever a given replica's last-sent
    /// db differs, same trick the AOF writer uses. With no replicas and
    /// no active backlog there is nothing to do — the offset stays put,
    /// which is fine: an offset only becomes meaningful once a replica
    /// has synced.
    pub(crate) fn replicate(&self, db_index: usize, req: &[Bytes]) {
        // An `IDKAPPLY` capture in progress bundles these effects into the
        // fused record instead — sending them individually too would
        // double-apply on replicas.
        if crate::dispatch::effect_capture_active() {
            return;
        }
        if self.replicas.is_empty() && !self.backlog_active.load(Ordering::Relaxed) {
            return;
        }
        let data = encode_request(req);
        // The stream lock serializes offset assignment with backlog
        // append and catch-up enqueue, so seqs in the backlog are
        // contiguous and catch-up can never observe a hole.
        let mut stream = self.repl_stream.lock().unwrap();
        let seq = self.master_repl_offset.fetch_add(1, Ordering::Relaxed) + 1;
        if stream.active {
            stream.entries.push_back(BacklogEntry { seq, db: db_index, data: data.clone() });
            stream.bytes += data.len();
            while stream.bytes > self.partial_resync_backlog {
                match stream.entries.pop_front() {
                    Some(evicted) => stream.bytes -= evicted.data.len(),
                    None => {
                        stream.bytes = 0;
                        break;
                    }
                }
            }
        }
        for entry in self.replicas.iter() {
            let replica = entry.value();
            let mut last_db = entry.last_db.lock().unwrap();
            if *last_db != Some(db_index) {
                self.push_replica(*entry.key(), replica, encode_select(db_index));
                *last_db = Some(db_index);
            }
            self.push_replica(*entry.key(), replica, data.clone());
        }
    }

    fn push_replica(&self, client_id: u64, replica: &ReplicaState, data: Bytes) {
        let bytes = data.len();
        let current = replica.queued_bytes.fetch_add(bytes, Ordering::Relaxed);
        if current.saturating_add(bytes) > self.replica_queue_max_bytes {
            replica.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
            self.slow_replicas.entry(client_id).or_insert_with(Instant::now);
            return;
        }
        let message = ReplicaMessage { data, queued_bytes: replica.queued_bytes.clone() };
        if replica.tx.try_send(message).is_err() {
            self.slow_replicas.entry(client_id).or_insert_with(Instant::now);
        }
    }

    pub fn master_repl_offset(&self) -> u64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    pub fn connected_replicas(&self) -> usize {
        self.replicas.len()
    }

    pub(crate) fn ack_replica(&self, client_id: u64, offset: u64) {
        if let Some(r) = self.replicas.get(&client_id) {
            r.acked_offset.store(offset, Ordering::Relaxed);
        }
        self.replica_ack_notify.notify_waiters();
    }

    pub fn count_replicas_at_least(&self, offset: u64) -> usize {
        self.replicas.iter().filter(|r| r.acked_offset.load(Ordering::Relaxed) >= offset).count()
    }

    pub fn is_replica(&self) -> bool {
        self.replica_of.lock().unwrap().is_some()
    }

    pub fn replica_of_addr(&self) -> Option<(String, u16)> {
        self.replica_of.lock().unwrap().clone()
    }

    pub fn replication_epoch(&self) -> u64 {
        self.replication_epoch.load(Ordering::Relaxed)
    }

    pub fn replica_generation(&self) -> u64 {
        self.replica_generation.load(Ordering::Relaxed)
    }

    /// Change (or clear, with `None`) which primary this server replicates
    /// from, and bump the epoch the `server` crate's supervisor task
    /// watches to reconnect elsewhere (or stop replicating, for `None`).
    /// Promotion additionally bumps `replica_generation`, invalidating any
    /// resume cursor a previous link left behind.
    pub(crate) fn set_replica_of(&self, addr: Option<(String, u16)>) {
        let promoted = addr.is_none();
        *self.replica_of.lock().unwrap() = addr;
        self.replication_epoch.fetch_add(1, Ordering::Relaxed);
        if promoted {
            self.replica_generation.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn cmd_replicaof(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let [host, port] = args else { return wrong_args("replicaof") };
    if host.eq_ignore_ascii_case(b"no") && port.eq_ignore_ascii_case(b"one") {
        ctx.set_replica_of(None);
        return reply(Value::ok());
    }
    let host = String::from_utf8_lossy(host).into_owned();
    let port: u16 = match std::str::from_utf8(port).ok().and_then(|s| s.parse().ok()) {
        Some(p) => p,
        None => return err("ERR Invalid master port"),
    };
    ctx.set_replica_of(Some((host, port)));
    reply(Value::ok())
}

/// `PSYNC <replid> <offset>` — partial resync when `replid` is ours and
/// `offset` is still covered by the backlog (`+CONTINUE`, then just the
/// missed commands stream as ordinary replica pushes); `FULLRESYNC`
/// otherwise. Real Redis's SYNC/PSYNC framing is: a simple string status
/// line, then — for the full path only — the RDB payload as a plain bulk
/// string (no further `\r\n` after it — this isn't a reply to a command,
/// it's a raw byte stream the replica reads by length). `Value::Bulk`
/// already encodes exactly that shape, so no special raw-socket handling
/// is needed here.
pub fn cmd_psync(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [replid, offset] = args else { return wrong_args("psync") };
    let offset: i64 = std::str::from_utf8(offset).ok().and_then(|s| s.parse().ok()).unwrap_or(-1);
    if offset >= 0
        && replid.as_ref() == ctx.replid.as_bytes()
        && ctx.try_partial_resync(state.id, offset as u64)
    {
        ctx.partial_resyncs.fetch_add(1, Ordering::Relaxed);
        return reply(Value::Simple(format!("CONTINUE {}", ctx.replid)));
    }
    let snapshot = match persistence::rdb::serialize(&ctx.store) {
        Ok(bytes) => bytes,
        Err(e) => return err(format!("ERR failed to build replication snapshot: {e}")),
    };
    ctx.register_replica(state.id);
    ctx.full_resyncs.fetch_add(1, Ordering::Relaxed);
    let offset = ctx.master_repl_offset();
    Outcome::Multi(vec![Value::Simple(format!("FULLRESYNC {} {offset}", ctx.replid)), Value::bulk(snapshot)])
}

/// `ROLE` — whether this instance is a primary or a replica, in Redis's
/// reply shape: `["master", <offset>, [[ip, port, offset], ...]]` or
/// `["slave", <master host>, <port>, <link state>, <offset>]`. Two scoped-
/// down spots: the link state is `connected` whenever `replica_of` is set
/// (same simplification as `INFO`'s `master_link_status:up`), and the
/// primary-side slave list is empty — `ReplicaState` doesn't track
/// replica peer addresses.
pub fn cmd_role(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    if !args.is_empty() {
        return wrong_args("role");
    }
    match ctx.replica_of_addr() {
        Some((host, port)) => reply(Value::Array(vec![
            Value::bulk(&b"slave"[..]),
            Value::bulk(host),
            Value::bulk(port.to_string()),
            Value::bulk(&b"connected"[..]),
            Value::Integer(ctx.master_repl_offset() as i64),
        ])),
        None => reply(Value::Array(vec![
            Value::bulk(&b"master"[..]),
            Value::Integer(ctx.master_repl_offset() as i64),
            Value::Array(vec![]),
        ])),
    }
}

pub fn cmd_replconf(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    match args {
        [sub, value] if sub.eq_ignore_ascii_case(b"ACK") => {
            if let Some(offset) = std::str::from_utf8(value).ok().and_then(|s| s.parse().ok()) {
                ctx.ack_replica(state.id, offset);
            }
            // Deliberately no reply — see `Outcome::NoReply`'s docs.
            Outcome::NoReply
        }
        // GETACK / listening-port / capa and friends: acknowledge and move on.
        _ => reply(Value::ok()),
    }
}

pub fn cmd_wait(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let [numreplicas, timeout_ms] = args else { return wrong_args("wait") };
    let numreplicas: usize = match std::str::from_utf8(numreplicas).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    let timeout_ms: u64 = match std::str::from_utf8(timeout_ms).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR timeout is not an integer or out of range"),
    };
    let target_offset = ctx.master_repl_offset();
    let satisfied = ctx.count_replicas_at_least(target_offset);
    if satisfied >= numreplicas {
        return reply(Value::Integer(satisfied as i64));
    }
    let timeout = if timeout_ms == 0 { None } else { Some(Duration::from_millis(timeout_ms)) };
    Outcome::WaitForReplicas { numreplicas, target_offset, timeout, numlocal: None }
}

/// `WAITAOF numlocal numreplicas timeout` — real Redis blocks until
/// `numlocal` local AOF fsyncs and `numreplicas` replica AOF fsyncs have
/// happened. We don't track replica-side AOF state at all. For the local
/// side: since `AofWriter::append`/`fsync` are fire-and-forget (see
/// `persistence::aof`'s module docs), we can't just report "1" because
/// `appendonly` is on — a write that reply this command's caller sent
/// moments ago might still be sitting in the writer thread's queue.
/// `ServerContext::flush_aof` does a real (blocking, but brief) round trip
/// to the writer thread to force everything queued so far to actually be
/// written and fsync'd before we answer. The replica count reuses the same
/// offset-ack wait as `WAIT`.
pub fn cmd_waitaof(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let [numlocal, numreplicas, timeout_ms] = args else { return wrong_args("waitaof") };
    let numlocal: i64 = match std::str::from_utf8(numlocal).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    let numreplicas: usize = match std::str::from_utf8(numreplicas).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    let timeout_ms: u64 = match std::str::from_utf8(timeout_ms).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR timeout is not an integer or out of range"),
    };
    if numlocal > 0 && !ctx.aof_enabled() {
        return err("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.");
    }
    let local_acked: i64 = match ctx.flush_aof() {
        None => 0,
        Some(Ok(())) => 1,
        Some(Err(e)) => {
            tracing::warn!(error = %e, "WAITAOF: AOF flush failed, reporting local as not durable");
            0
        }
    };
    let target_offset = ctx.master_repl_offset();
    let satisfied = ctx.count_replicas_at_least(target_offset);
    if satisfied >= numreplicas {
        return reply(Value::Array(vec![Value::Integer(local_acked), Value::Integer(satisfied as i64)]));
    }
    let timeout = if timeout_ms == 0 { None } else { Some(Duration::from_millis(timeout_ms)) };
    Outcome::WaitForReplicas { numreplicas, target_offset, timeout, numlocal: Some(local_acked) }
}
