//! `SCHEDULE AT|IN|LIST|CANCEL` — delayed command execution, built in
//! instead of hand-rolled as a sorted-set-plus-worker-process pattern. See
//! `docs/13-ergonomics.md` §2.
//!
//! Checked on the same 100ms cron tick that already drives active-expire
//! and `maxmemory` eviction — no new background loop.
//!
//! Not yet persisted across a restart (RDB/AOF) — the doc's stated goal,
//! but deferred for now; scheduled tasks are in-memory only in this MVP.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use dashmap::DashMap;
use resp::{Request, Value};

use crate::dispatch::{
    dispatch, err, reply, saturating_deadline, wrong_args, ClientState, Outcome, ServerContext,
};

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
}

fn now_epoch_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

impl ServerContext {
    /// Queue `req` to run against `db_index` at `fire_at`. Returns the
    /// schedule id (for `SCHEDULE CANCEL`).
    pub(crate) fn schedule_at(
        &self,
        db_index: usize,
        acl_user: String,
        fire_at_epoch_ms: u64,
        req: Request,
    ) -> u64 {
        let id = self.scheduler.next_id.fetch_add(1, AtomicOrdering::Relaxed);
        let now_ms = now_epoch_ms();
        let delay = fire_at_epoch_ms.saturating_sub(now_ms);
        let fire_at = saturating_deadline(Duration::from_millis(delay));
        self.scheduler.heap.lock().unwrap().push(ScheduleEntry {
            fire_at,
            fire_at_epoch_ms,
            id,
            db_index,
            acl_user,
            req,
        });
        id
    }

    pub(crate) fn cancel_scheduled(&self, id: u64, acl_user: &str) -> bool {
        let exists = self.scheduler.heap.lock().unwrap().iter().any(|e| e.id == id && e.acl_user == acl_user);
        if exists {
            self.scheduler.cancelled.insert(id, ());
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
    /// the same background tick as active-expire/eviction.
    pub fn run_due_scheduled(&self) -> usize {
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
            if self.scheduler.cancelled.remove(&entry.id).is_some() {
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
            let req = build_request(cmd, cmd_args);
            reply(Value::Integer(
                ctx.schedule_at(state.db_index, state.acl_user.clone(), epoch_ms, req) as i64
            ))
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
            let req = build_request(cmd, cmd_args);
            let fire_at_epoch_ms = now_epoch_ms().saturating_add(delay_ms);
            reply(Value::Integer(ctx.schedule_at(
                state.db_index,
                state.acl_user.clone(),
                fire_at_epoch_ms,
                req,
            ) as i64))
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
            reply(Value::Integer(ctx.cancel_scheduled(id, &state.acl_user) as i64))
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
