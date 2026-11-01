//! `SCHEDULE AT|IN|LIST|CANCEL` — delayed command execution, built in
//! instead of hand-rolled as a sorted-set-plus-worker-process pattern. See
//! `docs/13-ergonomics.md` §2.
//!
//! Checked on the same 100ms cron tick that already drives active-expire
//! and `maxmemory` eviction — no new background loop.
//!
//! Durability: registration writes a `~sched:<id>` marker string holding
//! everything needed to re-arm the timer — ordinary keyspace data, so it
//! persists via AOF/RDB and replicates like any write (a manual `DEL` of
//! the marker cancels the task). `load_scheduled` re-arms from markers at
//! startup and on replica promotion. The marker is deleted *after* the
//! command runs, so a crash mid-fire can re-fire the command once more —
//! at-least-once, never silently lost.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bincode::Options;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use resp::{Request, Value};

use crate::dispatch::{
    check_write_allowed, dispatch, err, on_key_changed, reply, saturating_deadline, wrong_args, ClientState,
    Outcome, ServerContext,
};

/// `~sched:` marker keys — the `~` bookkeeping convention `recovery`'s
/// `run:` keys already use, deliberately visible/inspectable like them.
const MARKER_PREFIX: &[u8] = b"~sched:";

/// `scheduled-commands-max` (100,000) — bounds the in-memory heap *and*
/// the marker-key footprint a workload can leave behind.
const MAX_SCHEDULED: usize = 100_000;

fn marker_key(id: u64) -> Bytes {
    let mut k = BytesMut::with_capacity(MARKER_PREFIX.len() + 20);
    k.extend_from_slice(MARKER_PREFIX);
    k.extend_from_slice(id.to_string().as_bytes());
    k.freeze()
}

/// What a `~sched:<id>` marker stores — everything `load_scheduled` needs
/// to rebuild the timer verbatim after a restart or promotion.
#[derive(serde::Serialize, serde::Deserialize)]
struct Marker {
    fire_at_epoch_ms: u64,
    db_index: u64,
    acl_user: String,
    req: Vec<Bytes>,
}

fn encode_marker(m: &Marker) -> Option<Bytes> {
    bincode::DefaultOptions::new().with_fixint_encoding().serialize(m).ok().map(Bytes::from)
}

fn decode_marker(blob: &[u8]) -> Option<Marker> {
    bincode::DefaultOptions::new().with_fixint_encoding().with_limit(blob.len() as u64).deserialize(blob).ok()
}

struct ScheduleEntry {
    fire_at: Instant,
    fire_at_epoch_ms: u64,
    id: u64,
    db_index: usize,
    acl_user: String,
    req: Request,
}

// Reversed so the `BinaryHeap` (a max-heap) pops the *earliest* fire time first.
impl Ord for ScheduleEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.fire_at.cmp(&self.fire_at).then_with(|| other.id.cmp(&self.id))
    }
}
impl PartialOrd for ScheduleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for ScheduleEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for ScheduleEntry {}

/// Scheduler state embedded in `ServerContext`.
#[derive(Default)]
pub struct Scheduler {
    next_id: AtomicU64,
    heap: Mutex<BinaryHeap<ScheduleEntry>>,
    /// Lazily-deleted cancellations: cheaper than scanning/rebuilding the
    /// heap on every `SCHEDULE CANCEL`.
    cancelled: DashMap<u64, ()>,
    /// Ids currently armed — `load_scheduled` skips these so restart and
    /// promotion re-scans stay idempotent.
    active: DashMap<u64, ()>,
}

fn now_epoch_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

impl ServerContext {
    /// Queue `req` to run against `db_index` at `fire_at_epoch_ms` under
    /// the already-allocated `id` (caller persists the marker first).
    fn arm_timer(&self, id: u64, db_index: usize, acl_user: String, fire_at_epoch_ms: u64, req: Request) {
        let now_ms = now_epoch_ms();
        let delay = fire_at_epoch_ms.saturating_sub(now_ms);
        let fire_at = saturating_deadline(Duration::from_millis(delay));
        self.scheduler.active.insert(id, ());
        self.scheduler.heap.lock().unwrap().push(ScheduleEntry {
            fire_at,
            fire_at_epoch_ms,
            id,
            db_index,
            acl_user,
            req,
        });
    }

    /// Register + persist a scheduled command: marker write first (that's
    /// the durable record — logged and replicated as a plain `SET`), then
    /// arm the in-memory timer.
    fn register_scheduled(
        &self,
        state: &ClientState,
        fire_at_epoch_ms: u64,
        req: Request,
    ) -> Result<u64, Outcome> {
        if self.scheduler.active.len() >= MAX_SCHEDULED {
            return Err(err("ERR too many scheduled commands"));
        }
        let id = self.scheduler.next_id.fetch_add(1, AtomicOrdering::Relaxed);
        let marker = Marker {
            fire_at_epoch_ms,
            db_index: state.db_index as u64,
            acl_user: state.acl_user.clone(),
            req: req.clone(),
        };
        let Some(blob) = encode_marker(&marker) else {
            return Err(err("ERR could not serialize scheduled command"));
        };
        let mkey = marker_key(id);
        let db = self.store.db(state.db_index).expect("valid db index");
        db.set(mkey.clone(), keyspace::Value::String(blob.clone()), None);
        on_key_changed(self, state, state.db_index, mkey.clone(), None);
        self.append_aof(state.db_index, &[Bytes::from_static(b"SET"), mkey.clone(), blob]);
        self.replicate(state.db_index, &[Bytes::from_static(b"SET"), mkey]);
        self.arm_timer(id, state.db_index, state.acl_user.clone(), fire_at_epoch_ms, req);
        Ok(id)
    }

    /// Re-arm timers from `~sched:*` markers — runs at startup (after
    /// persistence restore) and again on replica promotion. Ordinary
    /// keyspace scan; already-armed ids are skipped, so repeat calls are
    /// idempotent.
    pub fn load_scheduled(&self) -> usize {
        let mut armed = 0;
        for db_index in 0..self.store.len() {
            let Some(db) = self.store.db(db_index) else { continue };
            for key in db.keys_snapshot() {
                let Some(id_bytes) = key.strip_prefix(MARKER_PREFIX) else { continue };
                let Some(id) = std::str::from_utf8(id_bytes).ok().and_then(|s| s.parse::<u64>().ok()) else {
                    continue;
                };
                self.scheduler.next_id.fetch_max(id + 1, AtomicOrdering::Relaxed);
                if self.scheduler.active.contains_key(&id) {
                    continue;
                }
                let Some(marker) =
                    db.with_value(&key, |v| v.string_bytes()).flatten().and_then(|blob| decode_marker(&blob))
                else {
                    continue;
                };
                self.arm_timer(
                    id,
                    marker.db_index as usize,
                    marker.acl_user,
                    marker.fire_at_epoch_ms,
                    marker.req,
                );
                armed += 1;
            }
        }
        armed
    }

    pub(crate) fn cancel_scheduled(&self, id: u64, acl_user: &str) -> bool {
        let exists = self.scheduler.heap.lock().unwrap().iter().any(|e| e.id == id && e.acl_user == acl_user);
        if exists {
            self.scheduler.cancelled.insert(id, ());
            self.scheduler.active.remove(&id);
        }
        exists
    }

    pub(crate) fn list_scheduled(&self, acl_user: &str) -> Vec<(u64, u64, Request)> {
        let heap = self.scheduler.heap.lock().unwrap();
        heap.iter()
            .filter(|e| e.acl_user == acl_user && !self.scheduler.cancelled.contains_key(&e.id))
            .map(|e| (e.id, e.fire_at_epoch_ms, e.req.clone()))
            .collect()
    }

    /// Run every scheduled command whose fire time has passed. Called from
    /// the same background tick as active-expire/eviction. Replicas arm
    /// timers (so a promotion is instant) but never fire — the primary's
    /// `DEL` of the marker is what unschedules them there.
    pub fn run_due_scheduled(&self) -> usize {
        if self.is_replica() {
            return 0;
        }
        let mut fired = 0;
        loop {
            let due = {
                let mut heap = self.scheduler.heap.lock().unwrap();
                match heap.peek() {
                    Some(entry) if entry.fire_at <= Instant::now() => heap.pop(),
                    _ => None,
                }
            };
            let Some(entry) = due else { break };
            self.scheduler.active.remove(&entry.id);
            if self.scheduler.cancelled.remove(&entry.id).is_some() {
                continue;
            }
            let Some(db) = self.store.db(entry.db_index) else { continue };
            let mkey = marker_key(entry.id);
            // Marker gone = cancelled out from under us (manual DEL,
            // FLUSHDB, or a replicated marker delete).
            if db.exists(std::slice::from_ref(&mkey)) == 0 {
                continue;
            }
            // Do not execute queued work if its principal has since been
            // removed. For existing users, dispatch rechecks current ACLs.
            if !self.acl_users.contains_key(&entry.acl_user) {
                tracing::warn!(user = %entry.acl_user, "skipping scheduled command for deleted ACL user");
                continue;
            }
            let mut scratch = ClientState::with_id(0, false);
            scratch.db_index = entry.db_index;
            scratch.acl_user = entry.acl_user;
            dispatch(self, &mut scratch, entry.req);
            // Marker deleted *after* the run — a crash in between leaves
            // it behind, so the next boot re-fires once: at-least-once.
            db.del(std::slice::from_ref(&mkey));
            self.append_aof(entry.db_index, &[Bytes::from_static(b"DEL"), mkey.clone()]);
            self.replicate(entry.db_index, &[Bytes::from_static(b"DEL"), mkey]);
            fired += 1;
        }
        fired
    }
}

pub fn cmd_schedule(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [sub, rest @ ..] = args else { return wrong_args("schedule") };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "AT" => {
            let [epoch_ms, cmd, cmd_args @ ..] = rest else { return wrong_args("schedule|at") };
            let epoch_ms = match parse_u64(epoch_ms) {
                Some(n) => n,
                None => return err("ERR value is not an integer or out of range"),
            };
            if let Err(message) = authorize_scheduled(ctx, state, cmd, cmd_args) {
                return err(message);
            }
            // Registration writes a `~sched:` marker — a write, so it gets
            // the same replica-read-only / OOM gates as one.
            if let Some(deny) = check_write_allowed(ctx, state) {
                return deny;
            }
            let req = build_request(cmd, cmd_args);
            match ctx.register_scheduled(state, epoch_ms, req) {
                Ok(id) => reply(Value::Integer(id as i64)),
                Err(outcome) => outcome,
            }
        }
        "IN" => {
            let [delay_ms, cmd, cmd_args @ ..] = rest else { return wrong_args("schedule|in") };
            let delay_ms = match parse_u64(delay_ms) {
                Some(n) => n,
                None => return err("ERR value is not an integer or out of range"),
            };
            if let Err(message) = authorize_scheduled(ctx, state, cmd, cmd_args) {
                return err(message);
            }
            if let Some(deny) = check_write_allowed(ctx, state) {
                return deny;
            }
            let req = build_request(cmd, cmd_args);
            let fire_at_epoch_ms = now_epoch_ms().saturating_add(delay_ms);
            match ctx.register_scheduled(state, fire_at_epoch_ms, req) {
                Ok(id) => reply(Value::Integer(id as i64)),
                Err(outcome) => outcome,
            }
        }
        "LIST" => {
            let items = ctx
                .list_scheduled(&state.acl_user)
                .into_iter()
                .map(|(id, epoch_ms, req)| {
                    let mut elems = vec![Value::Integer(id as i64), Value::Integer(epoch_ms as i64)];
                    elems.extend(req.into_iter().map(Value::bulk));
                    Value::Array(elems)
                })
                .collect();
            reply(Value::Array(items))
        }
        "CANCEL" => {
            let [id] = rest else { return wrong_args("schedule|cancel") };
            let id = match parse_u64(id) {
                Some(n) => n,
                None => return err("ERR value is not an integer or out of range"),
            };
            if let Some(deny) = check_write_allowed(ctx, state) {
                return deny;
            }
            if !ctx.cancel_scheduled(id, &state.acl_user) {
                return reply(Value::Integer(0));
            }
            // Drop the durable marker too — a cancelled task must not
            // re-arm on the next restart/promotion.
            let mkey = marker_key(id);
            let db = ctx.store.db(state.db_index).expect("valid db index");
            db.del(std::slice::from_ref(&mkey));
            ctx.append_aof(state.db_index, &[Bytes::from_static(b"DEL"), mkey.clone()]);
            ctx.replicate(state.db_index, &[Bytes::from_static(b"DEL"), mkey]);
            reply(Value::Integer(1))
        }
        other => err(format!("ERR Unknown SCHEDULE subcommand '{}'", other.to_ascii_lowercase())),
    }
}

fn authorize_scheduled(
    ctx: &ServerContext,
    state: &ClientState,
    command: &Bytes,
    args: &[Bytes],
) -> Result<(), String> {
    let name = String::from_utf8_lossy(command).to_ascii_uppercase();
    let Some(user) = ctx.acl_users.get(&state.acl_user) else {
        return Err("NOAUTH Authentication required.".into());
    };
    crate::acl::authorize(user.value(), &name, args)
}

fn build_request(cmd: &Bytes, args: &[Bytes]) -> Request {
    let mut req = Vec::with_capacity(1 + args.len());
    req.push(cmd.clone());
    req.extend_from_slice(args);
    req
}

fn parse_u64(b: &Bytes) -> Option<u64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}
