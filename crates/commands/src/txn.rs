//! `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` — optimistic-locking
//! transactions, the same model as stock Redis: queue commands after
//! `MULTI`, then `EXEC` runs them all only if none of the `WATCH`ed keys
//! changed in the meantime.

use std::collections::HashSet;

use bytes::Bytes;
use resp::Value;

use crate::dispatch::{dispatch, err, reply, wrong_args, ClientState, Outcome, ServerContext};

pub fn cmd_multi(state: &mut ClientState) -> Outcome {
    if state.in_multi {
        return err("ERR MULTI calls can not be nested");
    }
    state.in_multi = true;
    state.multi_error = false;
    state.queued.clear();
    reply(Value::ok())
}

pub fn cmd_exec(ctx: &ServerContext, state: &mut ClientState) -> Outcome {
    if !state.in_multi {
        return err("ERR EXEC without MULTI");
    }
    let queued = std::mem::take(&mut state.queued);
    let aborted = state.multi_error;
    state.in_multi = false;
    state.multi_error = false;
    let dirty = ctx.dirty_clients.remove(&state.id).is_some();
    clear_watches(ctx, state);

    if aborted {
        return err("EXECABORT Transaction discarded because of previous errors.");
    }
    if dirty {
        return reply(Value::NullArray);
    }

    let results = queued
        .into_iter()
        .map(|req| match dispatch(ctx, state, req) {
            Outcome::Reply(v) => v,
            Outcome::Multi(mut vs) => vs.pop().unwrap_or(Value::NullBulk),
            Outcome::CloseConnection(v) => v,
            // GET ... SINGLEFLIGHT can't block mid-transaction; treat a
            // would-be wait as a miss, same as inside a script.
            Outcome::SingleFlightWait(_) => Value::NullBulk,
            Outcome::NoReply => Value::ok(),
            Outcome::WaitForReplicas { .. } => Value::Integer(0),
            // Same reasoning as `SingleFlightWait` above: a blocking
            // command inside `MULTI`/`EXEC` can't actually block, so it
            // resolves exactly as if its timeout had already elapsed.
            Outcome::BlockWait { on_timeout, .. } => on_timeout,
            // Like Redis's `no-multi-scripting` flag: never let EXEC
            // silently queue a process exit.
            Outcome::Shutdown { .. } => {
                Value::error("ERR SHUTDOWN is not allowed in transactions or scripts")
            }
        })
        .collect();
    reply(Value::Array(results))
}

pub fn cmd_discard(ctx: &ServerContext, state: &mut ClientState) -> Outcome {
    if !state.in_multi {
        return err("ERR DISCARD without MULTI");
    }
    state.in_multi = false;
    state.multi_error = false;
    state.queued.clear();
    ctx.dirty_clients.remove(&state.id);
    clear_watches(ctx, state);
    reply(Value::ok())
}

pub fn cmd_watch(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("watch");
    }
    for key in args {
        match ctx.watchers.entry((state.db_index, key.clone())) {
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                ctx.watchers_len.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                slot.insert(HashSet::new()).insert(state.id);
            }
            dashmap::mapref::entry::Entry::Occupied(mut slot) => {
                slot.get_mut().insert(state.id);
            }
        }
        state.watched.push((state.db_index, key.clone()));
    }
    reply(Value::ok())
}

pub fn cmd_unwatch(ctx: &ServerContext, state: &mut ClientState) -> Outcome {
    clear_watches(ctx, state);
    ctx.dirty_clients.remove(&state.id);
    reply(Value::ok())
}

pub(crate) fn clear_watches(ctx: &ServerContext, state: &mut ClientState) {
    for (db_index, key) in state.watched.drain(..) {
        let mut now_empty = false;
        if let Some(mut subs) = ctx.watchers.get_mut(&(db_index, key.clone())) {
            subs.remove(&state.id);
            now_empty = subs.is_empty();
        }
        if now_empty {
            ctx.watchers.remove(&(db_index, key));
            ctx.watchers_len.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Called after a successful write to `key`: marks every client `WATCH`ing
/// it dirty, so their next `EXEC` aborts (optimistic-lock invalidation).
pub fn touch_watchers(ctx: &ServerContext, db_index: usize, key: Bytes) {
    // Fast path: `WATCH` is rarely used outside of `MULTI`/`EXEC` call
    // sites, so `watchers` is empty for the overwhelming majority of
    // writes — skip hashing `key` and locking a shard for nothing. Same
    // rationale as `live::notify_key_change`'s `key_watchers_len` check.
    if ctx.watchers_len.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return;
    }
    if let Some(subs) = ctx.watchers.get(&(db_index, key)) {
        for &client_id in subs.iter() {
            ctx.dirty_clients.insert(client_id, ());
        }
    }
}

/// Called after `FLUSHDB`: every key in the database effectively changed.
pub fn touch_all_watchers(ctx: &ServerContext, db_index: usize) {
    let client_ids: HashSet<u64> = ctx
        .watchers
        .iter()
        .filter(|e| e.key().0 == db_index)
        .flat_map(|e| e.value().iter().copied().collect::<Vec<_>>())
        .collect();
    for client_id in client_ids {
        ctx.dirty_clients.insert(client_id, ());
    }
}
