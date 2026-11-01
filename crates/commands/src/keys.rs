//! Keyspace-wide generic commands: `SCAN`. Unlike `HSCAN`/`SSCAN`/`ZSCAN`
//! (which walk a single collection value), this walks every key in the
//! current DB — see `keyspace::Db::keys_snapshot`. Same simplified,
//! single-pass "scan" as the per-type commands: always returns cursor `"0"`.

use bytes::Bytes;
use resp::Value;

use crate::dispatch::{err, reply, wrong_args, ClientState, Outcome, ServerContext};
use crate::pattern;
use crate::scan::{self, ScanReply};

/// `KEYS pattern` — same glob matcher as `SCAN ... MATCH`, but a plain list
/// instead of a cursor-based walk (matches Redis: a blocking, all-at-once
/// scan of the whole keyspace).
pub fn cmd_keys(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [glob] = args else { return wrong_args("keys") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let items = db
        .keys_snapshot()
        .into_iter()
        .filter(|key| pattern::matches(glob, key) && db.with_value(key, |_| ()).is_some())
        .map(Value::bulk)
        .collect();
    reply(Value::Array(items))
}

pub fn cmd_scan(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [cursor, rest @ ..] = args else { return wrong_args("scan") };
    let Some(cursor) = std::str::from_utf8(cursor).ok().and_then(|s| s.parse::<usize>().ok()) else {
        return err("ERR invalid cursor");
    };
    let opts = match scan::parse_opts(rest) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");

    // Sorted so the cursor (an index into this list) is stable across calls —
    // key iteration order isn't.
    let mut matched: Vec<Bytes> = db
        .keys_snapshot()
        .into_iter()
        .filter(|key| {
            if !scan::matches_pattern(&opts, key) {
                return false;
            }
            // No TYPE filter: a cheap non-promoting exists() check is enough —
            // avoids pulling every disk-tier key back into RAM on each call.
            if !opts.has_type_filter() {
                return db.exists(std::slice::from_ref(key)) > 0;
            }
            db.with_value(key, |v| v.type_name()).is_some_and(|t| scan::matches_type(&opts, t))
        })
        .collect();
    matched.sort_unstable();

    let count = opts.count().unwrap_or(10).max(1);
    let page: Vec<Value> =
        matched.iter().skip(cursor).take(count).map(|key| Value::bulk(key.clone())).collect();
    let next = cursor + page.len();
    let next = if next >= matched.len() { 0 } else { next as u64 };
    reply(ScanReply::page(next, page).into_value())
}
