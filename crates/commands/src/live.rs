//! `LIVE <readonly-cmd> <args...>` / `UNLIVE <fingerprint...>` — subscribe to
//! the *result* of a read-only command and get it re-pushed every time the
//! key it reads changes. See `docs/04-reactivity-live.md`.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use crc::{Crc, CRC_64_XZ};
use resp::{Protocol, Request, Value, Wire};

use crate::dispatch::{dispatch, err, reply, wrong_args, ClientState, Outcome, ServerContext};

const FINGERPRINT: Crc<u64> = Crc::<u64>::new(&CRC_64_XZ);

/// Idle gap between fan-out sweeps: writes arriving during it pile into
/// `dirty` and dedup into the next batch for free, so a sustained write
/// stream coalesces to ~1/`SWEEP` re-evals per second instead of running
/// back-to-back sweeps. Only throttles under load — the first wake after
/// an idle period is still delivered immediately (the thread was parked
/// on the condvar, not sleeping). `live-sweep-ms` config default.
pub(crate) const SWEEP: Duration = Duration::from_millis(10);
/// Every `PACE_CHUNK` socket writes inside one sweep the fan-out thread
/// yields `PACE`, spreading a 1000-subscriber burst into ~1ms slices so
/// writers never queue behind a syscall herd. `live-pace-chunk`/
/// `live-pace-ms` config defaults; `PACE_CHUNK == 0` disables pacing.
pub(crate) const PACE_CHUNK: usize = 64;
pub(crate) const PACE: Duration = Duration::from_millis(1);

/// A registered `LIVE` subscription: which read-only command to re-run, the
/// single key it depends on, which DB it runs against, and which connected
/// clients are watching it.
pub struct LiveSub {
    pub db_index: usize,
    pub inner_req: Request,
    pub key: Bytes,
    pub clients: HashSet<u64>,
    /// The last pushed reply as a `Value` (inspection/tests).
    pub latest: Arc<Value>,
    /// The same reply pre-encoded once per wire variant — without this every
    /// subscriber connection re-encodes identical bytes on every push (N
    /// encodes of the same `Value` per write, which dominated fan-out cost
    /// at high subscriber counts).
    pub encoded: Arc<LiveEncoded>,
}

/// One LIVE push reply, encoded once for each reply format a subscriber
/// connection might have negotiated.
pub struct LiveEncoded {
    pub resp2: Bytes,
    pub resp3: Bytes,
    pub rfx: Bytes,
}

impl LiveEncoded {
    fn of(value: &Value) -> Self {
        let mut resp2 = BytesMut::new();
        resp::encode_reply(value, Protocol::Resp2, &mut resp2);
        let mut resp3 = BytesMut::new();
        resp::encode_reply(value, Protocol::Resp3, &mut resp3);
        let mut rfx = BytesMut::new();
        resp::encode_rfx_reply(value, &mut rfx);
        Self { resp2: resp2.freeze(), resp3: resp3.freeze(), rfx: rfx.freeze() }
    }

    pub fn for_wire(&self, wire: Wire, proto: Protocol) -> &Bytes {
        match (wire, proto) {
            (Wire::Rfx, _) => &self.rfx,
            (Wire::Resp, Protocol::Resp3) => &self.resp3,
            _ => &self.resp2,
        }
    }
}

/// A unit of fan-out work: re-evaluate subscriptions watching one changed
/// key, or (after `FLUSHDB`) every subscription in a database.
#[derive(Hash, PartialEq, Eq)]
pub(crate) enum LiveWake {
    Key(usize, Bytes),
    FlushDb(usize),
}

/// Off-loads LIVE re-evaluation + broadcast from the write path. Writers
/// just enqueue a `LiveWake` (deduplicated by the `HashSet` — a burst of
/// writes to one key collapses to one re-eval per drain); a dedicated
/// thread does the query re-runs and `live_events.send`, which wakes every
/// receiver *on that thread* instead of on the writer's. Previously both
/// ran inside the writing client's `dispatch()`, so write latency on a
/// watched key scaled with subscriber count.
pub(crate) struct LiveFanout {
    dirty: std::sync::Mutex<HashSet<LiveWake>>,
    cv: std::sync::Condvar,
    spawned: std::sync::Once,
    shutdown: AtomicBool,
}

impl LiveFanout {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            dirty: std::sync::Mutex::new(HashSet::new()),
            cv: std::sync::Condvar::new(),
            spawned: std::sync::Once::new(),
            shutdown: AtomicBool::new(false),
        })
    }

    fn spawn(self: &Arc<Self>, ctx: Weak<ServerContext>) {
        self.spawned.call_once(|| {
            let fanout = self.clone();
            std::thread::Builder::new()
                .name("live-fanout".into())
                .spawn(move || fanout_loop(ctx, fanout))
                .expect("spawn live-fanout thread");
        });
    }

    fn enqueue(self: &Arc<Self>, ctx: Weak<ServerContext>, wake: LiveWake) {
        self.spawn(ctx);
        self.dirty.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(wake);
        self.cv.notify_one();
    }

    pub(crate) fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.cv.notify_all();
    }
}

fn fanout_loop(ctx: Weak<ServerContext>, fanout: Arc<LiveFanout>) {
    loop {
        let batch: Vec<LiveWake> = {
            let mut dirty = fanout.dirty.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                if !dirty.is_empty() {
                    break dirty.drain().collect();
                }
                if fanout.shutdown.load(Ordering::Relaxed) {
                    return;
                }
                dirty = fanout.cv.wait(dirty).unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        };
        let Some(ctx) = ctx.upgrade() else { return };
        for wake in batch {
            ctx.run_live_wake(wake);
        }
        std::thread::sleep(ctx.live_sweep);
    }
}

/// Read-only commands allowed as a `LIVE` handler. Each one takes the
/// watched key as its first argument — that's what ties a write to a
/// re-evaluation.
const READONLY_HANDLERS: &[&str] = &[
    "GET",
    "GETBIT",
    "BITCOUNT",
    "EXPIRETIME",
    "PEXPIRETIME",
    "EXISTS",
    "TYPE",
    "LLEN",
    "LPOS",
    "LRANGE",
    "HGET",
    "HGETALL",
    "HLEN",
    "HEXISTS",
    "HSTRLEN",
    "SMEMBERS",
    "SISMEMBER",
    "SMISMEMBER",
    "SCARD",
    "ZSCORE",
    "ZMSCORE",
    "ZCARD",
    "ZRANGE",
    "ZRANGEBYSCORE",
];

fn fingerprint(inner_req: &Request) -> u64 {
    let mut digest = FINGERPRINT.digest();
    for (i, part) in inner_req.iter().enumerate() {
        if i > 0 {
            digest.update(b" ");
        }
        digest.update(part);
    }
    digest.finalize()
}

fn push_value(fingerprint_hex: &str, result: Value) -> Value {
    Value::Array(vec![
        Value::bulk(&b"live"[..]),
        Value::bulk(&b"fingerprint"[..]),
        Value::bulk(fingerprint_hex.to_string()),
        Value::bulk(&b"result"[..]),
        result,
    ])
}

pub fn cmd_live(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [cmd_name, key, ..] = args else { return wrong_args("live") };
    let inner_name = String::from_utf8_lossy(cmd_name).to_ascii_uppercase();
    if !READONLY_HANDLERS.contains(&inner_name.as_str()) {
        return err(format!(
            "ERR '{}' is not a supported LIVE handler (read-only key commands only)",
            inner_name.to_ascii_lowercase()
        ));
    }
    // EXISTS accepts multiple keys, but a LIVE registration currently
    // indexes one watched key. Reject the multi-key form rather than
    // silently missing changes to every key after the first.
    if inner_name == "EXISTS" && args.len() != 2 {
        return err("ERR LIVE EXISTS currently requires exactly one key");
    }

    // Keep the initial snapshot and registration atomic with respect to
    // on_key_changed notifications. A writer that mutates during this
    // section will issue its notification after this registration exists.
    let _registration_guard = ctx.live_sync.lock().unwrap_or_else(std::sync::PoisonError::into_inner);

    let inner_req: Request = args.to_vec();
    let fp = fingerprint(&inner_req);
    let fp_hex = format!("{fp:016x}");

    // Execute immediately for the initial snapshot — safe to recurse since
    // the inner command is guaranteed read-only.
    let mut scratch = ClientState::with_id(state.id, false);
    scratch.db_index = state.db_index;
    let initial = match dispatch(ctx, &mut scratch, inner_req.clone()) {
        Outcome::Reply(v) => v,
        _ => Value::NullBulk,
    };

    let initial = Arc::new(push_value(&fp_hex, initial));
    ctx.live_subscribe(
        fp,
        state.db_index,
        inner_req,
        key.clone(),
        state.id,
        initial.clone(),
        Arc::new(LiveEncoded::of(&initial)),
    );
    state.live_fingerprints.insert(fp);
    state.subscriptions += 1;

    reply((*initial).clone())
}

pub fn cmd_unlive(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("unlive");
    }
    let mut removed = 0;
    for hex in args {
        let Ok(text) = std::str::from_utf8(hex) else { continue };
        let Ok(fp) = u64::from_str_radix(text, 16) else { continue };
        if ctx.live_unsubscribe(fp, state.id) {
            removed += 1;
            state.live_fingerprints.remove(&fp);
            state.subscriptions = state.subscriptions.saturating_sub(1);
        }
    }
    reply(Value::Integer(removed))
}

/// Called after a successful write to `key` in `db_index`: re-evaluates and
/// pushes every LIVE subscription watching that key.
pub fn notify_key_change(ctx: &ServerContext, db_index: usize, key: Bytes) {
    let _notification_guard = ctx.live_sync.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    // Fast path: LIVE is opt-in and `key_watchers` is empty unless some
    // client has actually subscribed — skip hashing `key` and locking a
    // `key_watchers` shard on every single write when nobody ever has.
    if ctx.key_watchers_len.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return;
    }
    let Some(fingerprints) = ctx.key_watchers.get(&(db_index, key)) else { return };
    let fingerprints: Vec<u64> = fingerprints.iter().copied().collect();
    for fp in fingerprints {
        push_live_update(ctx, fp);
    }
}

/// Called after `FLUSHDB`: re-evaluates every LIVE subscription scoped to
/// that database (every watched key's result just became empty/nil).
pub fn notify_flush(ctx: &ServerContext, db_index: usize) {
    let _notification_guard = ctx.live_sync.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let fingerprints: Vec<u64> = ctx
        .key_watchers
        .iter()
        .filter(|entry| entry.key().0 == db_index)
        .flat_map(|entry| entry.value().iter().copied().collect::<Vec<_>>())
        .collect();
    for fp in fingerprints {
        push_live_update(ctx, fp);
    }
}

fn push_live_update(ctx: &ServerContext, fp: u64) {
    let Some(sub) = ctx.live_subs.get(&fp) else { return };
    let db_index = sub.db_index;
    let inner_req = sub.inner_req.clone();
    drop(sub);

    let mut scratch = ClientState::with_id(0, false);
    scratch.db_index = db_index;
    let result = match dispatch(ctx, &mut scratch, inner_req) {
        Outcome::Reply(v) => v,
        _ => Value::NullBulk,
    };
    let fp_hex = format!("{fp:016x}");
    let latest = Arc::new(push_value(&fp_hex, result));
    let encoded = Arc::new(LiveEncoded::of(&latest));
    // Copy the client list out before pacing — a 1000-subscriber sweep takes
    // ~16ms+ with pacing, and `get_mut` would hold the `live_subs` shard lock
    // against subscribe/unlive the whole time.
    let clients: Vec<u64> = {
        let Some(mut sub) = ctx.live_subs.get_mut(&fp) else { return };
        sub.latest = latest;
        sub.encoded = encoded.clone();
        let _ = ctx.live_events.send(fp);
        sub.clients.iter().copied().collect()
    };
    for (i, client_id) in clients.into_iter().enumerate() {
        if ctx.live_pace_chunk != 0 && i % ctx.live_pace_chunk == 0 && i > 0 {
            std::thread::sleep(ctx.live_pace);
        }
        ctx.push_live(client_id, encoded.clone());
    }
}

impl ServerContext {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn live_subscribe(
        &self,
        fingerprint: u64,
        db_index: usize,
        inner_req: Request,
        key: Bytes,
        client_id: u64,
        initial: Arc<Value>,
        encoded: Arc<LiveEncoded>,
    ) {
        self.live_subs
            .entry(fingerprint)
            .or_insert_with(|| LiveSub {
                db_index,
                inner_req,
                key: key.clone(),
                clients: HashSet::new(),
                latest: initial,
                encoded,
            })
            .clients
            .insert(client_id);
        match self.key_watchers.entry((db_index, key)) {
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                self.key_watchers_len.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                slot.insert(HashSet::new()).insert(fingerprint);
            }
            dashmap::mapref::entry::Entry::Occupied(mut slot) => {
                slot.get_mut().insert(fingerprint);
            }
        }
    }

    /// Called once by the server after it wraps the context in an `Arc` —
    /// enables the async fan-out path (see `LiveFanout`). Contexts that are
    /// never wrapped (unit tests dispatching directly) stay on the
    /// synchronous fallback in `enqueue_live_wake`, preserving pre-existing
    /// semantics there.
    pub fn bind_live_fanout(self: &Arc<Self>) {
        let _ = self.live_self.set(Arc::downgrade(self));
    }

    /// Writer-side LIVE notification: `O(1)` — inserts the wake into the
    /// deduplicating dirty set and pokes the fan-out thread. Without a bound
    /// `Arc` owner (tests) the wake is processed inline, exactly as before.
    pub(crate) fn enqueue_live_wake(&self, wake: LiveWake) {
        // With zero LIVE subscribers a wake can only be discarded anyway —
        // don't pay a global mutex + hash insert + futex wake per write.
        if self.key_watchers_len.load(Ordering::Relaxed) == 0 {
            return;
        }
        if let Some(weak) = self.live_self.get() {
            self.live_fanout.enqueue(weak.clone(), wake);
        } else {
            self.run_live_wake(wake);
        }
    }

    pub(crate) fn run_live_wake(&self, wake: LiveWake) {
        match wake {
            LiveWake::Key(db_index, key) => notify_key_change(self, db_index, key),
            LiveWake::FlushDb(db_index) => notify_flush(self, db_index),
        }
    }

    pub(crate) fn live_unsubscribe(&self, fingerprint: u64, client_id: u64) -> bool {
        let mut removed = false;
        let mut now_empty = false;
        if let Some(mut entry) = self.live_subs.get_mut(&fingerprint) {
            removed = entry.clients.remove(&client_id);
            now_empty = entry.clients.is_empty();
        }
        if now_empty {
            self.drop_live_sub(fingerprint);
        }
        removed
    }
}
