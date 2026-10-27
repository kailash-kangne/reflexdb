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
    if std::str::from_utf8(cursor).ok().and_then(|s| s.parse::<u64>().ok()).is_none() {
        return err("ERR invalid cursor");
    }
    let opts = match scan::parse_opts(rest) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");

    let mut items = Vec::new();
    for key in db.keys_snapshot() {
        if !scan::matches_pattern(&opts, &key) {
            continue;
        }
        let type_name = db.with_value(&key, |v| v.type_name());
        let Some(type_name) = type_name else { continue };
        if !scan::matches_type(&opts, type_name) {
            continue;
        }
        items.push(Value::bulk(key));
    }
    reply(ScanReply::done(items).into_value())
}
