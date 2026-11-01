//! Workflow-recovery primitives: `RUN.*` run registry + checkpoints, and
//! `IDEMPOTENT` retry-safe execution. Not a workflow engine — just the
//! primitives a runner needs: mark runs, record steps, snapshot and
//! restore a run's keys, and make any command safe to retry.
//!
//! Key layout inside the caller's current db (all ordinary keyspace data —
//! inspectable, persisted, replicated like anything else):
//!
//! - `run:<id>`            Hash   `{status, created, updated, last_step}`
//! - `run:<id>~audit`      List   append-only `"<ms> <event>"` entries
//! - `run:<id>~cp~<label>` String serialized checkpoint blob
//! - `run:<id>:<...>`      user state — `RUN.CHECKPOINT` snapshots every
//!   key under this prefix and `RUN.RESTORE` puts it back verbatim. The
//!   `~` bookkeeping keys deliberately don't match the `:` prefix, so
//!   they're never part of a snapshot.

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bincode::Options;
use bytes::{Bytes, BytesMut};
use keyspace::Value as KVal;
use resp::{Protocol, Value};

use crate::dispatch::{
    dispatch, effect_capture_begin, effect_capture_take, err, on_key_changed, reply, saturating_deadline,
    wrong_args, wrong_type, ClientState, Outcome, ServerContext,
};

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Per-run mutex for the multi-key operations (CHECKPOINT snapshots many
/// keys, RESTORE deletes+rebuilds them). Two of these running at once on
/// the same id could interleave their halves and leave state matching no
/// snapshot; unrelated single-key writes to `run:<id>:*` are *not* covered
/// (same non-atomic point-in-time semantics `BGSAVE` has).
fn run_lock(ctx: &ServerContext, id: &[u8]) -> std::sync::Arc<std::sync::Mutex<()>> {
    ctx.run_locks.entry(Bytes::copy_from_slice(id)).or_default().clone()
}

fn run_key(id: &[u8]) -> Bytes {
    let mut k = BytesMut::with_capacity(4 + id.len());
    k.extend_from_slice(b"run:");
    k.extend_from_slice(id);
    k.freeze()
}

fn audit_key(id: &[u8]) -> Bytes {
    let mut k = BytesMut::with_capacity(10 + id.len());
    k.extend_from_slice(b"run:");
    k.extend_from_slice(id);
    k.extend_from_slice(b"~audit");
    k.freeze()
}

fn cp_key(id: &[u8], label: &[u8]) -> Bytes {
    let mut k = BytesMut::with_capacity(8 + id.len() + label.len());
    k.extend_from_slice(b"run:");
    k.extend_from_slice(id);
    k.extend_from_slice(b"~cp~");
    k.extend_from_slice(label);
    k.freeze()
}

/// `run:<id>:` — the prefix user state under a run must live in to be
/// covered by checkpoints. `~` bookkeeping keys don't match it.
fn user_prefix(id: &[u8]) -> Bytes {
    let mut k = BytesMut::with_capacity(5 + id.len());
    k.extend_from_slice(b"run:");
    k.extend_from_slice(id);
    k.extend_from_slice(b":");
    k.freeze()
}

fn cp_prefix(id: &[u8]) -> Bytes {
    let mut k = BytesMut::with_capacity(8 + id.len());
    k.extend_from_slice(b"run:");
    k.extend_from_slice(id);
    k.extend_from_slice(b"~cp~");
    k.freeze()
}

/// Read `(status, last_step)` from the run record. `Some(None)` for
/// "run exists but isn't a hash" is impossible to express through the
/// outer Option cleanly, so the inner is `Result`.
fn run_status(db: &keyspace::Db, id: &[u8]) -> Option<Result<(Bytes, Option<Bytes>), ()>> {
    db.with_value(&run_key(id), |v| match v {
        KVal::Hash(h) => Ok((
            h.get(b"status".as_ref()).cloned().unwrap_or_default(),
            h.get(b"last_step".as_ref()).cloned(),
        )),
        _ => Err(()),
    })
}

/// Set hash fields on the run record (creates it if absent — callers
/// that require an existing run check `run_status` first). Returns
/// false when the run key holds a non-hash value.
fn set_run_fields(db: &keyspace::Db, id: &[u8], fields: &[(&[u8], Bytes)]) -> bool {
    db.with_entry_mut(
        run_key(id),
        || KVal::Hash(Default::default()),
        |v| match v {
            KVal::Hash(h) => {
                for &(f, ref val) in fields {
                    h.insert(Bytes::copy_from_slice(f), val.clone());
                }
                true
            }
            _ => false,
        },
    )
}

/// Append `"<ms> <event>"` to the run's audit list.
fn audit(db: &keyspace::Db, id: &[u8], event: &str) {
    let entry = Bytes::from(format!("{} {event}", now_ms()));
    db.with_entry_mut(
        audit_key(id),
        || KVal::List(VecDeque::new()),
        |v| {
            if let KVal::List(l) = v {
                l.push_back(entry.clone());
            }
        },
    );
}

pub fn cmd_run(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let Some(sub) = args.first() else { return wrong_args("run") };
    let rest = &args[1..];
    match sub.as_ref() {
        s if s.eq_ignore_ascii_case(b"BEGIN") => run_begin(ctx, state, rest),
        s if s.eq_ignore_ascii_case(b"STEP") => run_step(ctx, state, rest),
        s if s.eq_ignore_ascii_case(b"CHECKPOINT") || s.eq_ignore_ascii_case(b"CP") => {
            run_checkpoint(ctx, state, rest)
        }
        s if s.eq_ignore_ascii_case(b"RESTORE") => run_restore(ctx, state, rest),
        s if s.eq_ignore_ascii_case(b"END") => run_end(ctx, state, rest),
        s if s.eq_ignore_ascii_case(b"STATUS") => run_status_cmd(ctx, state, rest),
        s if s.eq_ignore_ascii_case(b"LIST") => run_list(ctx, state, rest),
        s if s.eq_ignore_ascii_case(b"AUDIT") => run_audit(ctx, state, rest),
        _ => err(format!(
            "ERR Unknown RUN subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(sub)
        )),
    }
}

/// `RUN BEGIN <id>` — register a new run; fails if the id is taken.
fn run_begin(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [id] = args else { return wrong_args("run|begin") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let now = Bytes::from(now_ms().to_string());
    let created = db.with_entry_mut_expiry(
        run_key(id),
        || KVal::Hash(Default::default()),
        |v, _exp, present| {
            if present {
                return false;
            }
            let KVal::Hash(h) = v else { return false };
            h.insert(Bytes::from_static(b"status"), Bytes::from_static(b"running"));
            h.insert(Bytes::from_static(b"created"), now.clone());
            h.insert(Bytes::from_static(b"updated"), now.clone());
            true
        },
    );
    if !created {
        return err("ERR run id already exists");
    }
    audit(db, id, "begin");
    on_key_changed(ctx, state, state.db_index, run_key(id), None);
    on_key_changed(ctx, state, state.db_index, audit_key(id), None);
    reply(Value::ok())
}

/// `RUN STEP <id> <name>` — record a step as the run's resume point.
fn run_step(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [id, name] = args else { return wrong_args("run|step") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match run_status(db, id) {
        None => return err("ERR no such run"),
        Some(Err(())) => return wrong_type(),
        Some(Ok((status, _))) if status.as_ref() != b"running" => {
            return err("ERR run is not running");
        }
        _ => {}
    }
    let now = Bytes::from(now_ms().to_string());
    set_run_fields(db, id, &[(b"last_step", name.clone()), (b"updated", now)]);
    audit(db, id, &format!("step {}", String::from_utf8_lossy(name)));
    on_key_changed(ctx, state, state.db_index, run_key(id), None);
    on_key_changed(ctx, state, state.db_index, audit_key(id), None);
    reply(Value::ok())
}

/// `RUN CHECKPOINT <id> <label>` — snapshot every `run:<id>:*` key into
/// `run:<id>~cp~<label>`. The blob holds each key's `DUMP` payload and
/// remaining TTL, so restores reconstruct values with `RESTORE`
/// semantics.
fn run_checkpoint(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [id, label] = args else { return wrong_args("run|checkpoint") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match run_status(db, id) {
        None => return err("ERR no such run"),
        Some(Err(())) => return wrong_type(),
        Some(Ok((status, _))) if status.as_ref() != b"running" => return err("ERR run is not running"),
        _ => {}
    }
    let lock = run_lock(ctx, id);
    let _guard = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let prefix = user_prefix(id);
    let mut items: Vec<(Bytes, Vec<u8>, Option<u64>)> = Vec::new();
    for key in db.keys_snapshot() {
        if !key.starts_with(&prefix) {
            continue;
        }
        let Some(blob) = db.with_value(&key, |v| bincode::serialize(v).ok()).flatten() else {
            continue;
        };
        let ttl_ms = match db.ttl(&key) {
            keyspace::Ttl::Expires(d) => Some(d.as_millis() as u64),
            _ => None,
        };
        items.push((key, blob, ttl_ms));
    }
    let count = items.len();
    let payload = match bincode::serialize(&items) {
        Ok(p) => p,
        Err(e) => return err(format!("ERR checkpoint serialization failed: {e}")),
    };
    let key = cp_key(id, label);
    db.set(key.clone(), KVal::String(Bytes::from(payload)), None);
    audit(db, id, &format!("checkpoint {} keys={count}", String::from_utf8_lossy(label)));
    on_key_changed(ctx, state, state.db_index, key, None);
    on_key_changed(ctx, state, state.db_index, audit_key(id), None);
    reply(Value::Integer(count as i64))
}

/// `RUN RESTORE <id> <label>` — wipe `run:<id>:*` and put the snapshot
/// back. Every blob is decoded *before* anything is deleted, so a corrupt
/// checkpoint can't half-destroy the run's state. Restoring also flips a
/// `done`/`failed` run back to `running` — that's the resume.
fn run_restore(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [id, label] = args else { return wrong_args("run|restore") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match run_status(db, id) {
        None => return err("ERR no such run"),
        Some(Err(())) => return wrong_type(),
        _ => {}
    }
    let lock = run_lock(ctx, id);
    let _guard = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let key = cp_key(id, label);
    let Some(payload) = db.with_value(&key, |v| v.string_bytes()).flatten() else {
        return err("ERR no such checkpoint");
    };
    let decode = |blob: &[u8]| {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(blob.len() as u64)
            .deserialize::<Vec<(Bytes, Vec<u8>, Option<u64>)>>(blob)
    };
    let Ok(items) = decode(&payload) else {
        return err("ERR corrupt checkpoint data");
    };
    let mut restored: Vec<(Bytes, KVal, Option<Duration>)> = Vec::with_capacity(items.len());
    for (k, blob, ttl_ms) in items {
        let v: KVal = match bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(blob.len() as u64)
            .deserialize(&blob)
        {
            Ok(v) => v,
            Err(_) => return err("ERR corrupt checkpoint data"),
        };
        restored.push((k, v, ttl_ms.map(Duration::from_millis)));
    }

    let prefix = user_prefix(id);
    let doomed: Vec<Bytes> = db.keys_snapshot().into_iter().filter(|k| k.starts_with(&prefix)).collect();
    db.del(&doomed);
    for k in &doomed {
        on_key_changed(ctx, state, state.db_index, k.clone(), None);
    }
    for (k, v, ttl) in restored {
        db.load_entry(k.clone(), v, ttl);
        on_key_changed(ctx, state, state.db_index, k, None);
    }

    let now = Bytes::from(now_ms().to_string());
    set_run_fields(db, id, &[(b"status", Bytes::from_static(b"running")), (b"updated", now)]);
    audit(db, id, &format!("restore {}", String::from_utf8_lossy(label)));
    on_key_changed(ctx, state, state.db_index, run_key(id), None);
    on_key_changed(ctx, state, state.db_index, audit_key(id), None);
    reply(Value::ok())
}

/// `RUN END <id> [DONE|FAILED]` — close the run.
fn run_end(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let (id, status) = match args {
        [id] => (id, Bytes::from_static(b"done")),
        [id, s] if s.eq_ignore_ascii_case(b"done") => (id, Bytes::from_static(b"done")),
        [id, s] if s.eq_ignore_ascii_case(b"failed") => (id, Bytes::from_static(b"failed")),
        _ => return wrong_args("run|end"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match run_status(db, id) {
        None => return err("ERR no such run"),
        Some(Err(())) => return wrong_type(),
        _ => {}
    }
    let now = Bytes::from(now_ms().to_string());
    set_run_fields(db, id, &[(b"status", status.clone()), (b"updated", now)]);
    audit(db, id, &format!("end {}", String::from_utf8_lossy(&status)));
    on_key_changed(ctx, state, state.db_index, run_key(id), None);
    on_key_changed(ctx, state, state.db_index, audit_key(id), None);
    reply(Value::ok())
}

/// `RUN STATUS <id>` — `[["status",s],["created",ms],["updated",ms],
/// ["last_step",name|nil],["checkpoints",[labels]]]`.
fn run_status_cmd(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [id] = args else { return wrong_args("run|status") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let Some(res) = db.with_value(&run_key(id), |v| match v {
        KVal::Hash(h) => Ok(h.clone()),
        _ => Err(()),
    }) else {
        return err("ERR no such run");
    };
    let Ok(hash) = res else { return wrong_type() };
    let prefix = cp_prefix(id);
    let mut label_keys: Vec<Bytes> = db
        .keys_snapshot()
        .into_iter()
        .filter_map(|k| k.strip_prefix(&prefix[..]).map(Bytes::copy_from_slice))
        .collect();
    label_keys.sort();
    let labels = label_keys.into_iter().map(Value::bulk).collect();
    let field = |name: &[u8]| hash.get(name).cloned();
    reply(Value::Array(vec![
        Value::Array(vec![Value::bulk(&b"status"[..]), Value::bulk(field(b"status").unwrap_or_default())]),
        Value::Array(vec![Value::bulk(&b"created"[..]), Value::bulk(field(b"created").unwrap_or_default())]),
        Value::Array(vec![Value::bulk(&b"updated"[..]), Value::bulk(field(b"updated").unwrap_or_default())]),
        Value::Array(vec![
            Value::bulk(&b"last_step"[..]),
            field(b"last_step").map_or(Value::NullBulk, Value::bulk),
        ]),
        Value::Array(vec![Value::bulk(&b"checkpoints"[..]), Value::Array(labels)]),
    ]))
}

/// `RUN LIST` — `[[<id>, <status>], ...]` for every run record.
fn run_list(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    if !args.is_empty() {
        return wrong_args("run|list");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let mut runs: Vec<(Bytes, Bytes)> = db
        .keys_snapshot()
        .into_iter()
        .filter_map(|key| {
            let id = key.strip_prefix(b"run:")?;
            // `run:<id>` exactly — `:`/`~` suffixes mark state/sys keys.
            if id.contains(&b':') || id.contains(&b'~') {
                return None;
            }
            let status = db.with_value(&key, |v| match v {
                KVal::Hash(h) => h.get(b"status".as_ref()).cloned(),
                _ => None,
            })??;
            Some((Bytes::copy_from_slice(id), status))
        })
        .collect();
    runs.sort();
    reply(Value::Array(
        runs.into_iter()
            .map(|(id, status)| Value::Array(vec![Value::bulk(id), Value::bulk(status)]))
            .collect(),
    ))
}

/// `RUN AUDIT <id> [LIMIT n]` — newest-last audit entries.
fn run_audit(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let (id, limit) = match args {
        [id] => (id, usize::MAX),
        [id, kw, n] if kw.eq_ignore_ascii_case(b"LIMIT") => {
            let n: usize = match std::str::from_utf8(n).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return err("ERR LIMIT is not an integer or out of range"),
            };
            (id, n)
        }
        _ => return wrong_args("run|audit"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(&audit_key(id), |v| match v {
        KVal::List(l) => {
            let skip = l.len().saturating_sub(limit);
            l.iter().skip(skip).cloned().map(Value::bulk).collect::<Vec<_>>()
        }
        _ => Vec::new(),
    }) {
        Some(entries) => reply(Value::Array(entries)),
        None => err("ERR no such run"),
    }
}

/// `IDEMPOTENT <key> <ttl_ms> <cmd> <args...>` — executes `cmd` once and
/// stores its reply under `key`; retries with the same key return the
/// stored reply verbatim without re-executing. This is what makes a
/// workflow step retry-safe after a crash mid-flight.
///
/// The claim is a pending marker written atomically under the entry lock
/// (`with_entry_mut_expiry`), so two concurrent attempts can't both
/// execute. `ttl_ms` bounds how long a pending/result marker lives (0 =
/// persist). Error replies are *not* cached — they release the claim so
/// a retry can re-execute, since the failure may be transient. Commands
/// that mutate connection state, block, or nest `IDEMPOTENT` are
/// rejected up front.
///
/// `IDEMPOTENT` is deliberately not in `WRITE_COMMANDS`: logging the raw
/// request would re-execute the inner command on replay/replicas
/// (double-apply for `INCR` & friends). Instead it emits **one** fused
/// `IDKAPPLY` record holding the inner command's already-resolved effect
/// commands *and* the stored reply — captured while the inner dispatch's
/// own AOF/replication enqueues are diverted (see
/// `dispatch::effect_capture_*`). One record is the atomic unit: effects
/// and "done" marker land on disk and on replicas together or not at
/// all, so a crash can never leave an applied-but-unmarked execution
/// that a retry would re-run. This is what makes `IDEMPOTENT`
/// exactly-once across `kill -9`, not just at-least-once.
const IDK_PENDING: &[u8] = b"\x00idk";

const IDK_FORBIDDEN: &[&str] = &[
    "IDEMPOTENT",
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    "SELECT",
    "AUTH",
    "HELLO",
    "QUIT",
    "RESET",
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "SSUBSCRIBE",
    "UNSUBSCRIBE",
    "PUNSUBSCRIBE",
    "SUNSUBSCRIBE",
    "LIVE",
    "UNLIVE",
    "PSYNC",
    "REPLICAOF",
    "SLAVEOF",
    "SHUTDOWN",
    "SAVE",
    "BGSAVE",
    "RUN",
    "IDKAPPLY",
];

enum Claim {
    Fresh,
    InFlight,
    Done(Bytes),
    WrongType,
}

pub fn cmd_idempotent(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [key, ttl_ms, cmd @ ..] = args else { return wrong_args("idempotent") };
    let ttl_ms: u64 = match std::str::from_utf8(ttl_ms).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR ttl_ms is not an integer or out of range"),
    };
    let inner_name = String::from_utf8_lossy(&cmd[0]).to_ascii_uppercase();
    if IDK_FORBIDDEN.contains(&inner_name.as_str()) {
        return err(format!("ERR IDEMPOTENT cannot wrap {inner_name}"));
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");

    // Fast path: a completed marker is a pure read — allowed on replicas,
    // and no write guards are needed to answer it.
    if let Some(done) = db.with_value(key, |v| match v {
        KVal::String(s) if s.as_ref() != IDK_PENDING => Some(s.clone()),
        _ => None,
    }) {
        let Some(stored) = done else { return wrong_type() };
        return match decode_resp2(&stored) {
            Some((v, consumed)) if consumed == stored.len() => reply(v),
            _ => err("ERR corrupt idempotent result"),
        };
    }

    // Anything from here mutates: run the same replica-read-only /
    // maxmemory guards dispatch() applies to WRITE_COMMANDS by hand,
    // since this command is intentionally outside that set.
    if let Some(deny) = crate::dispatch::check_write_allowed(ctx, state) {
        return deny;
    }

    let deadline = (ttl_ms > 0).then(|| saturating_deadline(Duration::from_millis(ttl_ms)));
    let claim = db.with_entry_mut_expiry(
        key.clone(),
        || KVal::String(Bytes::from_static(IDK_PENDING)),
        |v, exp, present| {
            if !present {
                *exp = deadline;
                return Claim::Fresh;
            }
            match v {
                KVal::String(s) if s.as_ref() == IDK_PENDING => Claim::InFlight,
                KVal::String(s) => Claim::Done(s.clone()),
                _ => Claim::WrongType,
            }
        },
    );
    match claim {
        Claim::InFlight => err("ERR idempotent execution for this key is still in flight"),
        Claim::WrongType => wrong_type(),
        Claim::Done(stored) => match decode_resp2(&stored) {
            Some((v, consumed)) if consumed == stored.len() => reply(v),
            _ => err("ERR corrupt idempotent result"),
        },
        Claim::Fresh => {
            // Divert the inner command's effect records (its own
            // AOF/replication enqueues, wherever they're emitted from)
            // into the capture buffer — they get packed into the fused
            // `IDKAPPLY` below instead of committing one by one.
            effect_capture_begin();
            let inner = dispatch(ctx, state, cmd.to_vec());
            let effects = effect_capture_take();
            match inner {
                Outcome::Reply(v) if !matches!(v, Value::Error(_)) => {
                    let mut encoded = BytesMut::new();
                    v.encode(&mut encoded, Protocol::Resp2);
                    let encoded = encoded.freeze();
                    let effects_blob =
                        match bincode::DefaultOptions::new().with_fixint_encoding().serialize(&effects) {
                            Ok(b) => Bytes::from(b),
                            Err(e) => {
                                db.del(std::slice::from_ref(key));
                                return err(format!("ERR idempotent effect serialization failed: {e}"));
                            }
                        };
                    let apply_req = vec![
                        Bytes::from_static(b"IDKAPPLY"),
                        key.clone(),
                        Bytes::from(ttl_ms.to_string()),
                        effects_blob,
                        encoded.clone(),
                    ];
                    // The single durable record — under `appendfsync
                    // always` this returns only after the fsync, so the
                    // reply can never outrun the marker's durability.
                    ctx.append_aof(state.db_index, &apply_req);
                    ctx.replicate(state.db_index, &apply_req);
                    db.set(key.clone(), KVal::String(encoded), deadline);
                    on_key_changed(ctx, state, state.db_index, key.clone(), None);
                    reply(v)
                }
                // Errors and non-reply outcomes (blocking waits, pub/sub
                // pushes, ...) don't count as "executed once" — release
                // the claim so a retry can try again.
                Outcome::Reply(v) => {
                    db.del(std::slice::from_ref(key));
                    reply(v)
                }
                _ => {
                    db.del(std::slice::from_ref(key));
                    err("ERR IDEMPOTENT cannot wrap a command with an asynchronous reply")
                }
            }
        }
    }
}

/// `IDKAPPLY <key> <ttl_ms> <effects_blob> <reply_blob>` — the fused
/// durable record `IDEMPOTENT` emits: replay the inner command's
/// resolved effects, then write the result marker. Internal only — it
/// exists on the replication link and in AOF replay, never accepted from
/// a client connection (rejected like an unknown command).
pub fn cmd_idkapply(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    if !state.is_replication_link {
        return err("ERR unknown command 'IDKAPPLY'");
    }
    let [key, ttl_ms, effects_blob, reply_blob] = args else { return wrong_args("idkapply") };
    let ttl_ms: u64 = match std::str::from_utf8(ttl_ms).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR ttl_ms is not an integer or out of range"),
    };
    let Ok(effects) = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(effects_blob.len() as u64)
        .deserialize::<Vec<Vec<Bytes>>>(effects_blob)
    else {
        return err("ERR corrupt idempotent effects");
    };
    // Suppress each effect's own log/replicate enqueue: this IDKAPPLY is
    // already their durable record (it re-logs/re-replicates itself as a
    // write command), so emitting the effects again would double-apply
    // downstream.
    effect_capture_begin();
    for req in effects {
        dispatch(ctx, state, req);
    }
    effect_capture_take();
    let deadline = (ttl_ms > 0).then(|| saturating_deadline(Duration::from_millis(ttl_ms)));
    let db = ctx.store.db(state.db_index).expect("valid db index");
    db.set(key.clone(), KVal::String(reply_blob.clone()), deadline);
    on_key_changed(ctx, state, state.db_index, key.clone(), None);
    reply(Value::ok())
}

/// Strict RESP2 reply decoder — only used to re-read replies this same
/// file encoded (`Value::encode(_, Resp2)`), so anything that isn't a
/// well-formed single value is just corrupt data, never a wire format to
/// negotiate. Returns `(value, bytes consumed)`.
fn decode_resp2(src: &[u8]) -> Option<(Value, usize)> {
    fn line(src: &[u8]) -> Option<(&[u8], usize)> {
        let end = src.iter().position(|&b| b == b'\n')?;
        if end == 0 || src[end - 1] != b'\r' {
            return None;
        }
        Some((&src[..end - 1], end + 1))
    }
    fn dec(src: &[u8]) -> Option<(Value, usize)> {
        let (&tag, rest) = src.split_first()?;
        match tag {
            b'+' => line(rest).map(|(l, n)| (Value::Simple(String::from_utf8_lossy(l).into_owned()), n + 1)),
            b'-' => line(rest).map(|(l, n)| (Value::Error(String::from_utf8_lossy(l).into_owned()), n + 1)),
            b':' => line(rest).and_then(|(l, n)| {
                std::str::from_utf8(l).ok()?.parse().ok().map(|i| (Value::Integer(i), n + 1))
            }),
            b'$' => {
                let (l, n) = line(rest)?;
                let len: i64 = std::str::from_utf8(l).ok()?.parse().ok()?;
                if len < 0 {
                    return Some((Value::NullBulk, n + 1));
                }
                let len = len as usize;
                let body = rest.get(n..n + len)?;
                if rest.get(n + len..n + len + 2)? != b"\r\n" {
                    return None;
                }
                Some((Value::bulk(Bytes::copy_from_slice(body)), n + 1 + len + 2))
            }
            b'*' => {
                let (l, mut n) = line(rest)?;
                let count: i64 = std::str::from_utf8(l).ok()?.parse().ok()?;
                if count < 0 {
                    return Some((Value::NullArray, n + 1));
                }
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let (v, used) = dec(rest.get(n..)?)?;
                    items.push(v);
                    n += used;
                }
                Some((Value::Array(items), n + 1))
            }
            _ => None,
        }
    }
    dec(src)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: &Value) -> Bytes {
        let mut buf = BytesMut::new();
        v.encode(&mut buf, Protocol::Resp2);
        buf.freeze()
    }

    fn run(ctx: &ServerContext, state: &mut ClientState, words: &[&str]) -> Value {
        let req = words.iter().map(|w| Bytes::copy_from_slice(w.as_bytes())).collect();
        match dispatch(ctx, state, req) {
            Outcome::Reply(v) => v,
            _ => panic!("unexpected non-reply outcome"),
        }
    }

    fn fresh() -> (ServerContext, ClientState) {
        let ctx = ServerContext::new(16, None);
        let (id, _rx) = ctx.register_client();
        (ctx, ClientState::with_id(id, false))
    }

    #[test]
    fn run_lifecycle_and_checkpoint_restore() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["RUN", "BEGIN", "job1"]), Value::ok());
        // Double BEGIN is rejected.
        match run(&ctx, &mut st, &["RUN", "BEGIN", "job1"]) {
            Value::Error(e) => assert!(e.contains("already exists")),
            v => panic!("expected error, got {v:?}"),
        }
        assert_eq!(run(&ctx, &mut st, &["SET", "run:job1:counter", "5"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["SET", "unrelated", "x"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["RUN", "STEP", "job1", "phase-a"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["RUN", "CHECKPOINT", "job1", "v1"]), Value::Integer(1));

        // Diverge, then restore — only the run's own keys are touched.
        run(&ctx, &mut st, &["SET", "run:job1:counter", "99"]);
        run(&ctx, &mut st, &["SET", "run:job1:extra", "new"]);
        assert_eq!(run(&ctx, &mut st, &["RUN", "RESTORE", "job1", "v1"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["GET", "run:job1:counter"]), Value::bulk(&b"5"[..]));
        assert_eq!(run(&ctx, &mut st, &["GET", "run:job1:extra"]), Value::NullBulk);
        assert_eq!(run(&ctx, &mut st, &["GET", "unrelated"]), Value::bulk(&b"x"[..]));

        // END closes it; STEP on a closed run fails; RESTORE resumes it.
        assert_eq!(run(&ctx, &mut st, &["RUN", "END", "job1"]), Value::ok());
        match run(&ctx, &mut st, &["RUN", "STEP", "job1", "nope"]) {
            Value::Error(e) => assert!(e.contains("not running")),
            v => panic!("expected error, got {v:?}"),
        }
        match run(&ctx, &mut st, &["RUN", "STATUS", "job1"]) {
            Value::Array(_) => {}
            v => panic!("expected array, got {v:?}"),
        }
        match run(&ctx, &mut st, &["RUN", "AUDIT", "job1"]) {
            Value::Array(entries) => assert!(entries.len() >= 4),
            v => panic!("expected array, got {v:?}"),
        }
    }

    #[test]
    fn idempotent_executes_once_and_replays() {
        let (ctx, mut st) = fresh();
        let first = run(&ctx, &mut st, &["IDEMPOTENT", "idk1", "60000", "INCR", "idk:n"]);
        assert_eq!(first, Value::Integer(1));
        // Same key → stored reply, no re-execution.
        assert_eq!(run(&ctx, &mut st, &["IDEMPOTENT", "idk1", "60000", "INCR", "idk:n"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["GET", "idk:n"]), Value::bulk(&b"1"[..]));

        // Errors aren't cached — the claim is released for a retry.
        match run(&ctx, &mut st, &["IDEMPOTENT", "idk2", "60000", "LPOP", "idk:n"]) {
            Value::Error(e) => assert!(e.contains("WRONGTYPE")),
            v => panic!("expected error, got {v:?}"),
        }
        assert_eq!(
            run(&ctx, &mut st, &["IDEMPOTENT", "idk2", "60000", "GET", "idk:n"]),
            Value::bulk(&b"1"[..])
        );

        // Forbidden wrappers and missing args.
        match run(&ctx, &mut st, &["IDEMPOTENT", "idk3", "0", "MULTI"]) {
            Value::Error(e) => assert!(e.contains("cannot wrap")),
            v => panic!("expected error, got {v:?}"),
        }
    }

    /// `IDKAPPLY` is the fused record `IDEMPOTENT` logs: replaying it must
    /// land the inner effects *and* the result marker, and a later
    /// `IDEMPOTENT` retry must answer the stored reply without running
    /// anything again. Clients can never issue it themselves.
    #[test]
    fn idkapply_replays_effects_and_marker_atomically() {
        let (ctx, mut st) = fresh();
        let mut repl = ClientState::for_replication_link();

        // Clients can't emit the internal record.
        match run(&ctx, &mut st, &["IDKAPPLY", "k", "0", "x", "y"]) {
            Value::Error(e) => assert!(e.contains("unknown command")),
            v => panic!("expected error, got {v:?}"),
        }

        // Build the same record cmd_idempotent would emit for
        // `IDEMPOTENT idk 0 INCR n`: one INCR effect + the stored `:1`.
        let effects: Vec<Vec<Bytes>> = vec![vec![Bytes::from_static(b"INCR"), Bytes::from_static(b"idk:n")]];
        let effects_blob =
            Bytes::from(bincode::DefaultOptions::new().with_fixint_encoding().serialize(&effects).unwrap());
        let reply_blob = enc(&Value::Integer(1));
        let req = vec![
            Bytes::from_static(b"IDKAPPLY"),
            Bytes::from_static(b"idk"),
            Bytes::from_static(b"0"),
            effects_blob,
            reply_blob,
        ];
        match dispatch(&ctx, &mut repl, req) {
            Outcome::Reply(v) => assert_eq!(v, Value::ok()),
            _ => panic!("unexpected non-reply outcome"),
        }

        assert_eq!(run(&ctx, &mut st, &["GET", "idk:n"]), Value::bulk(&b"1"[..]));
        // Retry on the same key: stored reply, no re-execution (n stays 1).
        assert_eq!(run(&ctx, &mut st, &["IDEMPOTENT", "idk", "0", "INCR", "idk:n"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["GET", "idk:n"]), Value::bulk(&b"1"[..]));
    }

    #[test]
    fn resp2_round_trip_covers_every_stored_shape() {
        let cases = vec![
            Value::ok(),
            Value::Integer(-42),
            Value::bulk(&b"v1"[..]),
            Value::NullBulk,
            Value::NullArray,
            Value::Array(vec![Value::Integer(1), Value::bulk(&b"x"[..]), Value::NullBulk]),
        ];
        for v in cases {
            let bytes = enc(&v);
            let (back, used) = decode_resp2(&bytes).expect("decode");
            assert_eq!(used, bytes.len());
            assert_eq!(back, v);
        }
        assert!(decode_resp2(b"garbage").is_none());
        assert!(decode_resp2(b"$5\r\nabc").is_none());
    }
}
