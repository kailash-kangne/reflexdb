//! `LIVE <readonly-cmd> <args...>` / `UNLIVE <fingerprint...>` — subscribe to
//! the *result* of a read-only command and get it re-pushed every time the
//! key it reads changes. See `docs/04-reactivity-live.md`.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use crc::{Crc, CRC_64_XZ};
use resp::{Request, Value};

use crate::dispatch::{dispatch, err, reply, wrong_args, ClientState, Outcome, ServerContext};

const FINGERPRINT: Crc<u64> = Crc::<u64>::new(&CRC_64_XZ);

/// A registered `LIVE` subscription: which read-only command to re-run, the
/// single key it depends on, which DB it runs against, and which connected
/// clients are watching it.
pub struct LiveSub {
    pub db_index: usize,
    pub inner_req: Request,
    pub key: Bytes,
    pub clients: HashSet<u64>,
    pub latest: Arc<Value>,
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
    ctx.live_subscribe(fp, state.db_index, inner_req, key.clone(), state.id, initial.clone());
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
    if let Some(mut sub) = ctx.live_subs.get_mut(&fp) {
        sub.latest = latest;
        let _ = ctx.live_events.send(fp);
    }
}

impl ServerContext {
    pub(crate) fn live_subscribe(
        &self,
        fingerprint: u64,
        db_index: usize,
        inner_req: Request,
        key: Bytes,
        client_id: u64,
        initial: Arc<Value>,
    ) {
        self.live_subs
            .entry(fingerprint)
            .or_insert_with(|| LiveSub {
                db_index,
                inner_req,
                key: key.clone(),
                clients: HashSet::new(),
                latest: initial,
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
