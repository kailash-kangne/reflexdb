//! `GETAT`/`HISTORY` — point-in-time reads and a per-key version log, the
//! command surface sketched in `docs/11-time-travel.md`. Backed by
//! `persistence::history::HistoryWriter`: a disk-backed, time-retention
//! store (see its module docs), not a full implementation of that doc's
//! "current value is just a query over an append-only log" architecture
//! (Bet 1, `docs/15-strategic-bets.md`) — this is a side-car on top of the
//! existing mutable keyspace. Strings only, matching that doc's v1 scope
//! note; see `ServerContext::history`'s docs (in `dispatch.rs`) for the
//! rest of what's simplified.

use bytes::Bytes;
use resp::Value;

use crate::dispatch::{err, reply, wrong_args, ClientState, Outcome, ServerContext};

const HISTORY_DISABLED: &str = "ERR history is disabled — set `history-enabled yes` to use GETAT/HISTORY";

/// `GETAT key ms-epoch` — the string value `key` held at or before the
/// given Unix-epoch-milliseconds timestamp. Replies `NullBulk` if the key
/// didn't exist yet, was deleted, was never a string, or predates the
/// oldest version `history-retention-ms` has kept.
pub fn cmd_getat(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, ts] = args else { return wrong_args("getat") };
    let Some(writer) = &ctx.history else { return err(HISTORY_DISABLED) };
    let ts_ms: u64 = match std::str::from_utf8(ts).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR ms-epoch is not an integer or out of range"),
    };
    match writer.get_at(state.db_index, key, ts_ms) {
        Some(value) => reply(Value::bulk(value)),
        None => reply(Value::NullBulk),
    }
}

/// `HISTORY key [LIMIT n]` — recorded `(ms-epoch, value-or-nil)` versions
/// for `key`, newest first; a `nil` value marks a delete (or "wasn't a
/// string at that point"). Unlike the full design in
/// `docs/11-time-travel.md`, this doesn't report which command produced
/// each version — just the value snapshot.
pub fn cmd_history(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, limit) = match args {
        [key] => (key, usize::MAX),
        [key, kw, n] if kw.eq_ignore_ascii_case(b"LIMIT") => {
            let n: usize = match std::str::from_utf8(n).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return err("ERR LIMIT is not an integer or out of range"),
            };
            (key, n)
        }
        _ => return wrong_args("history"),
    };
    let Some(writer) = &ctx.history else { return err(HISTORY_DISABLED) };
    let items = writer
        .history(state.db_index, key, limit)
        .into_iter()
        .map(|(t, v)| Value::Array(vec![Value::Integer(t as i64), v.map_or(Value::NullBulk, Value::bulk)]))
        .collect();
    reply(Value::Array(items))
}
