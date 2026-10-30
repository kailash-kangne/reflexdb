//! `REPLICAOF`/`SLAVEOF`, `PSYNC`, `REPLCONF`, `WAIT` — primary→replica
//! replication. See `docs/06-ha-replication-cluster.md` §1.
//!
//! Scope, deliberately: full resync only (no partial resync / backlog —
//! every `PSYNC` gets a fresh snapshot, regardless of what offset it asks
//! for), and the "offset" is a **logical count of replicated commands**,
//! not a byte offset into a replication stream like real Redis. Both are
//! documented simplifications, not wire compatibility with real Redis
//! replication.
//!
//! `dispatch()` can't itself hold the connection open or make outbound
//! connections (it only ever gets `&ServerContext`, never the `Arc` that
//! outlives a single call) — so `REPLICAOF` here only flips
//! `ServerContext::replica_of` and bumps `replication_epoch`; the `server`
//! crate's background supervisor (which does have the `Arc`) watches that
//! epoch and owns the actual TCP connection to the primary.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::dispatch::{err, reply, wrong_args, ClientState, Outcome, ServerContext};
use resp::Value;

/// Per-replica bookkeeping, keyed by the replica connection's client id.
pub struct ReplicaState {
    pub acked_offset: AtomicU64,
    pub last_db: Mutex<Option<usize>>,
    tx: mpsc::Sender<ReplicaMessage>,
    queued_bytes: Arc<AtomicUsize>,
}

pub struct ReplicaMessage {
    pub value: Arc<Value>,
    bytes: usize,
    queued_bytes: Arc<AtomicUsize>,
}

impl Drop for ReplicaMessage {
    fn drop(&mut self) {
        self.queued_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

pub(crate) const REPLICA_QUEUE_CAPACITY: usize = 32_768;
pub(crate) const REPLICA_QUEUE_MAX_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const REPLICA_SLOW_DISCONNECT_DELAY: Duration = Duration::from_secs(5);

fn decimal_len(mut n: usize) -> usize {
    let mut digits = 1;
    while n >= 10 {
        n /= 10;
        digits += 1;
    }
    digits
}

fn resp_array_size(parts: &[Bytes]) -> usize {
    parts.iter().fold(1 + decimal_len(parts.len()) + 2, |size, part| {
        size.saturating_add(1 + decimal_len(part.len()) + 2 + part.len() + 2)
    })
}

impl ServerContext {
    pub(crate) fn register_replica(&self, client_id: u64) {
        let (tx, rx) = mpsc::channel(REPLICA_QUEUE_CAPACITY);
        self.replica_receivers.insert(client_id, Mutex::new(Some(rx)));
        self.replicas.insert(
            client_id,
            ReplicaState {
                acked_offset: AtomicU64::new(self.master_repl_offset.load(Ordering::Relaxed)),
                last_db: Mutex::new(None),
                tx,
                queued_bytes: Arc::new(AtomicUsize::new(0)),
            },
        );
    }

    pub fn take_replica_receiver(&self, client_id: u64) -> Option<mpsc::Receiver<ReplicaMessage>> {
        self.replica_receivers.get(&client_id).and_then(|rx| rx.lock().unwrap().take())
    }

    pub fn has_replica_client(&self, client_id: u64) -> bool {
        self.replicas.contains_key(&client_id)
    }

    /// Fan a write command out to every connected replica through its larger,
    /// dedicated bounded channel. Prefixes a `SELECT` whenever a given
    /// replica's last-sent db differs,
    /// same trick the AOF writer uses.
    pub(crate) fn replicate(&self, db_index: usize, req: &[Bytes]) {
        if self.replicas.is_empty() {
            return;
        }
        self.master_repl_offset.fetch_add(1, Ordering::Relaxed);
        for entry in self.replicas.iter() {
            let replica = entry.value();
            let mut last_db = entry.last_db.lock().unwrap();
            if *last_db != Some(db_index) {
                let db = Bytes::from(db_index.to_string());
                let select = Value::Array(vec![Value::bulk(&b"SELECT"[..]), Value::bulk(db.clone())]);
                self.push_replica(
                    *entry.key(),
                    replica,
                    select,
                    resp_array_size(&[Bytes::from_static(b"SELECT"), db]),
                );
                *last_db = Some(db_index);
            }
            let command = Value::Array(req.iter().cloned().map(Value::bulk).collect());
            self.push_replica(*entry.key(), replica, command, resp_array_size(req));
        }
    }

    fn push_replica(&self, client_id: u64, replica: &ReplicaState, value: Value, bytes: usize) {
        let current = replica.queued_bytes.fetch_add(bytes, Ordering::Relaxed);
        if current.saturating_add(bytes) > REPLICA_QUEUE_MAX_BYTES {
            replica.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
            self.slow_replicas.entry(client_id).or_insert_with(Instant::now);
            return;
        }
        let message =
            ReplicaMessage { value: Arc::new(value), bytes, queued_bytes: replica.queued_bytes.clone() };
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

    /// Change (or clear, with `None`) which primary this server replicates
    /// from, and bump the epoch the `server` crate's supervisor task
    /// watches to reconnect elsewhere (or stop replicating, for `None`).
    pub(crate) fn set_replica_of(&self, addr: Option<(String, u16)>) {
        *self.replica_of.lock().unwrap() = addr;
        self.replication_epoch.fetch_add(1, Ordering::Relaxed);
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

/// `PSYNC <replid> <offset>` — we ignore the requested replid/offset and
/// always do a full resync. Real Redis's SYNC/PSYNC framing is: a simple
/// string status line, then the RDB payload as a plain bulk string (no
/// further `\r\n` after it — this isn't a reply to a command, it's a raw
/// byte stream the replica reads by length). `Value::Bulk` already encodes
/// exactly that shape, so no special raw-socket handling is needed here.
pub fn cmd_psync(ctx: &ServerContext, state: &ClientState, _args: &[Bytes]) -> Outcome {
    let snapshot = match persistence::rdb::serialize(&ctx.store) {
        Ok(bytes) => bytes,
        Err(e) => return err(format!("ERR failed to build replication snapshot: {e}")),
    };
    ctx.register_replica(state.id);
    let offset = ctx.master_repl_offset();
    Outcome::Multi(vec![Value::Simple(format!("FULLRESYNC {} {offset}", ctx.replid)), Value::bulk(snapshot)])
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
    if numlocal > 0 && ctx.aof.is_none() {
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
