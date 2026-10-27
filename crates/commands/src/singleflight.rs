//! `GET <key> SINGLEFLIGHT <ms>` — server-side stampede protection. See
//! `docs/14-cache-correctness.md` §3.
//!
//! `dispatch()` stays fully synchronous (recursive callers from `EVAL`/
//! `LIVE`/`MULTI` all rely on that), so the actual blocking happens one
//! level up, in the connection loop (`server/main.rs`): a cache miss under
//! an active lease returns `Outcome::SingleFlightWait` instead of blocking
//! here, and the connection loop awaits the lease's `Notify` (or a bounded
//! sleep) before retrying the same command.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashEntry;
use keyspace::Value as KVal;
use resp::Value;
use tokio::sync::Notify;

use crate::dispatch::{
    dispatch_get, err, reply, saturating_deadline, wrong_args, wrong_type, ClientState, Outcome,
    ServerContext,
};

pub struct SingleFlightLease {
    pub notify: Notify,
    deadline: Mutex<Instant>,
}

impl SingleFlightLease {
    fn is_expired(&self) -> bool {
        Instant::now() >= *self.deadline.lock().unwrap()
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.lock().unwrap().saturating_duration_since(Instant::now())
    }
}

pub fn cmd_get(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, ms) = match args {
        [key] => return dispatch_get(ctx, state, key),
        [key, flag, ms] if flag.eq_ignore_ascii_case(b"SINGLEFLIGHT") => {
            let ms: u64 = match std::str::from_utf8(ms).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return err("ERR value is not an integer or out of range"),
            };
            (key, ms)
        }
        _ => return wrong_args("get"),
    };

    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: avoid cloning a potentially huge
    // List/Hash/Set/ZSet just to discover it's the wrong type.
    match db.with_value(key, |v| match v {
        KVal::String(s) => Ok(s.clone()),
        _ => Err(()),
    }) {
        Some(Ok(s)) => reply(Value::bulk(s)),
        Some(Err(())) => wrong_type(),
        None => take_or_wait(ctx, state.db_index, key, ms),
    }
}

/// On a cache miss: the first caller (or the first to notice the previous
/// lease expired) becomes the leaseholder and gets `nil` back, responsible
/// for computing the value and `SET`ting it. Everyone else gets told to wait.
fn take_or_wait(ctx: &ServerContext, db_index: usize, key: &Bytes, ms: u64) -> Outcome {
    let map_key = (db_index, key.clone());
    match ctx.singleflight.entry(map_key) {
        DashEntry::Vacant(slot) => {
            slot.insert(Arc::new(SingleFlightLease {
                notify: Notify::new(),
                deadline: Mutex::new(saturating_deadline(Duration::from_millis(ms))),
            }));
            ctx.singleflight_len.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            reply(Value::NullBulk)
        }
        DashEntry::Occupied(slot) => {
            let lease = slot.get().clone();
            if lease.is_expired() {
                *lease.deadline.lock().unwrap() = saturating_deadline(Duration::from_millis(ms));
                reply(Value::NullBulk)
            } else {
                Outcome::SingleFlightWait(lease)
            }
        }
    }
}
