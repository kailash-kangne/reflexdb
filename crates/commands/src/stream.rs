//! `XADD`/`XLEN`/`XRANGE`/`XREVRANGE`/`XDEL`/`XTRIM`/`XGROUP`/`XREADGROUP`/
//! `XREAD`/`XACK`/`XPENDING` — Redis Streams, on top of `keyspace::Stream`
//! (see its module docs for the storage design).
//!
//! **Not implemented** (documented scope cut, not an oversight):
//! `XCLAIM`/`XAUTOCLAIM` (reassigning another consumer's pending entries —
//! `XGROUP DELCONSUMER` is the only way to drop a dead consumer's PEL
//! here), `XINFO`, `XSETID`'s standalone form (`XGROUP SETID` covers the
//! consumer-group case), `BLOCK` on `XREAD`/`XREADGROUP` (both are
//! synchronous — see `cmd_xread`'s docs for why that's a real limitation,
//! not just missing syntax), `MINID`-based trimming, and exclusive range
//! bounds (`(id`). `MAXLEN` trimming is always exact — `~` isn't treated
//! differently from `=`, same simplification `docs/02-data-structures.md`
//! already documents for other types' small-collection thresholds.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use keyspace::{parse_for_add, Stream, StreamId, Value as KVal, XaddError};
use resp::Value;

use crate::dispatch::{
    err, notify_keyspace_event, on_key_changed, reply, wrong_args, wrong_type, ClientState, Outcome,
    ServerContext,
};

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn invalid_id() -> Outcome {
    err("ERR Invalid stream ID specified as stream command argument")
}

fn nogroup(key: &[u8], group: &[u8]) -> Outcome {
    err(format!(
        "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
}

/// `-`/`+`/a bare or complete ID, as used for `XRANGE`/`XREVRANGE`/
/// `XPENDING`'s `start`/`end` bounds. `default_seq` is `0` for a lower
/// bound and `u64::MAX` for an upper bound, matching real Redis's
/// bare-number-fills-in-the-other-half behavior.
fn resolve_bound(arg: &[u8], default_seq: u64) -> Option<StreamId> {
    match arg {
        b"-" => Some(StreamId::MIN),
        b"+" => Some(StreamId::MAX),
        _ => StreamId::parse(std::str::from_utf8(arg).ok()?, default_seq),
    }
}

/// `$` (current last ID) or a complete/bare ID, as used by `XGROUP
/// CREATE`/`XGROUP SETID`'s `id` argument.
fn resolve_group_start(arg: &[u8], current_last: StreamId) -> Option<StreamId> {
    if arg == b"$" {
        return Some(current_last);
    }
    StreamId::parse(std::str::from_utf8(arg).ok()?, 0)
}

fn entry_reply(id: StreamId, fields: Vec<(Bytes, Bytes)>) -> Value {
    let mut flat = Vec::with_capacity(fields.len() * 2);
    for (f, v) in fields {
        flat.push(Value::bulk(f));
        flat.push(Value::bulk(v));
    }
    Value::Array(vec![Value::bulk(id.to_display_string()), Value::Array(flat)])
}

/// Consumes a leading `MAXLEN [~|=] count [LIMIT n]` clause (used by both
/// `XADD` and `XTRIM`). `LIMIT`'s count is parsed and discarded — it only
/// bounds approximate-trim cost in real Redis, and trimming here is
/// always exact, so it has nothing to do.
fn parse_maxlen_clause(args: &[Bytes], i: &mut usize) -> Result<Option<usize>, Outcome> {
    if !args.get(*i).is_some_and(|a| a.eq_ignore_ascii_case(b"MAXLEN")) {
        return Ok(None);
    }
    *i += 1;
    if args.get(*i).is_some_and(|a| a.as_ref() == b"~" || a.as_ref() == b"=") {
        *i += 1;
    }
    let Some(n) =
        args.get(*i).and_then(|b| std::str::from_utf8(b).ok()).and_then(|s| s.parse::<usize>().ok())
    else {
        return Err(err("ERR value is not an integer or out of range"));
    };
    *i += 1;
    if args.get(*i).is_some_and(|a| a.eq_ignore_ascii_case(b"LIMIT")) {
        *i += 2;
    }
    Ok(Some(n))
}

/// `XADD key [NOMKSTREAM] [MAXLEN [~|=] count [LIMIT n]] <ID|*|ms-*> field
/// value [field value ...]`
pub fn cmd_xadd(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, rest @ ..] = args else { return wrong_args("xadd") };
    let mut i = 0;
    let mut nomkstream = false;
    loop {
        if rest.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"NOMKSTREAM")) {
            nomkstream = true;
            i += 1;
            continue;
        }
        break;
    }
    let maxlen = match parse_maxlen_clause(rest, &mut i) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let Some(id_arg) = rest.get(i) else { return wrong_args("xadd") };
    i += 1;
    let field_values = &rest[i..];
    if field_values.is_empty() || !field_values.len().is_multiple_of(2) {
        return wrong_args("xadd");
    }
    let Some(spec) = parse_for_add(id_arg) else { return invalid_id() };

    let db = ctx.store.db(state.db_index).expect("valid db index");
    if nomkstream && db.with_value(key, |_| ()).is_none() {
        return reply(Value::NullBulk);
    }

    let fields: Vec<(Bytes, Bytes)> = field_values.chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect();
    let now = now_ms();
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::Stream(Stream::new()),
        |v| match v {
            KVal::Stream(s) => s.add(spec, now, fields).inspect(|_| {
                if let Some(n) = maxlen {
                    s.trim_maxlen(n);
                }
            }),
            _ => Err(XaddError::WrongType),
        },
    );

    match result {
        Ok(id) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 't', "xadd", key);
            reply(Value::bulk(id.to_display_string()))
        }
        Err(XaddError::WrongType) => wrong_type(),
        Err(XaddError::NotMonotonic) => {
            err("ERR The ID specified in XADD is equal or smaller than the target stream top item")
        }
    }
}

pub fn cmd_xlen(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("xlen") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Stream(s) => Ok(s.len()),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(n)) => reply(Value::Integer(n as i64)),
        Some(Err(())) => wrong_type(),
    }
}

fn cmd_xrange_impl(ctx: &ServerContext, state: &ClientState, args: &[Bytes], rev: bool) -> Outcome {
    let (key, a, b, count_arg) = match args {
        [key, a, b] => (key, a, b, None),
        [key, a, b, kw, n] if kw.eq_ignore_ascii_case(b"COUNT") => (key, a, b, Some(n)),
        _ => return wrong_args(if rev { "xrevrange" } else { "xrange" }),
    };
    let count = match count_arg {
        Some(n) => match std::str::from_utf8(n).ok().and_then(|s| s.parse::<usize>().ok()) {
            Some(n) => Some(n),
            None => return err("ERR value is not an integer or out of range"),
        },
        None => None,
    };
    let (lo, hi) = if rev { (b, a) } else { (a, b) };
    let Some(lo) = resolve_bound(lo, 0) else { return invalid_id() };
    let Some(hi) = resolve_bound(hi, u64::MAX) else { return invalid_id() };

    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_value(key, |v| match v {
        KVal::Stream(s) => Ok(if rev { s.range_rev(lo, hi, count) } else { s.range(lo, hi, count) }),
        _ => Err(()),
    });
    match result {
        None => reply(Value::Array(vec![])),
        Some(Ok(entries)) => {
            reply(Value::Array(entries.into_iter().map(|(id, f)| entry_reply(id, f)).collect()))
        }
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_xrange(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    cmd_xrange_impl(ctx, state, args, false)
}

pub fn cmd_xrevrange(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    cmd_xrange_impl(ctx, state, args, true)
}

pub fn cmd_xdel(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, ids @ ..] = args else { return wrong_args("xdel") };
    if ids.is_empty() {
        return wrong_args("xdel");
    }
    let mut parsed = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(id) = resolve_bound(id, 0) else { return invalid_id() };
        parsed.push(id);
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_value_mut(key, |v| match v {
        KVal::Stream(s) => Ok(s.del(&parsed)),
        _ => Err(()),
    });
    match result {
        None => reply(Value::Integer(0)),
        Some(Ok(removed)) => {
            if removed > 0 {
                on_key_changed(ctx, state.db_index, key.clone());
                notify_keyspace_event(ctx, state.db_index, 't', "xdel", key);
            }
            reply(Value::Integer(removed as i64))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `XTRIM key MAXLEN [~|=] count [LIMIT n]`
pub fn cmd_xtrim(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, rest @ ..] = args else { return wrong_args("xtrim") };
    let mut i = 0;
    let maxlen = match parse_maxlen_clause(rest, &mut i) {
        Ok(Some(n)) => n,
        Ok(None) => return err("ERR syntax error"),
        Err(e) => return e,
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_value_mut(key, |v| match v {
        KVal::Stream(s) => Ok(s.trim_maxlen(maxlen)),
        _ => Err(()),
    });
    match result {
        None => reply(Value::Integer(0)),
        Some(Ok(removed)) => {
            if removed > 0 {
                on_key_changed(ctx, state.db_index, key.clone());
                notify_keyspace_event(ctx, state.db_index, 't', "xtrim", key);
            }
            reply(Value::Integer(removed as i64))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `XGROUP CREATE|SETID|DESTROY|CREATECONSUMER|DELCONSUMER ...`
pub fn cmd_xgroup(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [sub, key, group, rest @ ..] = args else { return wrong_args("xgroup") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let group = group.clone();

    if sub.eq_ignore_ascii_case(b"CREATE") {
        let [id_arg, flags @ ..] = rest else { return wrong_args("xgroup") };
        let mkstream = flags.iter().any(|f| f.eq_ignore_ascii_case(b"MKSTREAM"));
        if !mkstream && db.with_value(key, |_| ()).is_none() {
            return err("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you \
                 may want to use the MKSTREAM option to create an empty stream automatically.");
        }
        let id_arg = id_arg.clone();
        let result = db.with_entry_mut(
            key.clone(),
            || KVal::Stream(Stream::new()),
            |v| match v {
                KVal::Stream(s) => match resolve_group_start(&id_arg, s.last_id()) {
                    Some(start) => Ok(s.create_group(group.clone(), start)),
                    None => Err(None),
                },
                _ => Err(Some(())),
            },
        );
        return match result {
            Ok(true) => {
                on_key_changed(ctx, state.db_index, key.clone());
                reply(Value::ok())
            }
            Ok(false) => err("BUSYGROUP Consumer Group name already exists"),
            Err(Some(())) => wrong_type(),
            Err(None) => invalid_id(),
        };
    }

    // Every other subcommand requires the key (and usually the group) to
    // already exist — none of them create a stream.
    if sub.eq_ignore_ascii_case(b"SETID") {
        let [id_arg] = rest else { return wrong_args("xgroup") };
        let id_arg = id_arg.clone();
        let result = db.with_value_mut(key, |v| match v {
            KVal::Stream(s) => match resolve_group_start(&id_arg, s.last_id()) {
                Some(id) => Ok(s.set_group_id(&group, id)),
                None => Err(None),
            },
            _ => Err(Some(())),
        });
        match result {
            Some(Ok(true)) => {
                on_key_changed(ctx, state.db_index, key.clone());
                reply(Value::ok())
            }
            Some(Ok(false)) | None => nogroup(key, &group),
            Some(Err(Some(()))) => wrong_type(),
            Some(Err(None)) => invalid_id(),
        }
    } else if sub.eq_ignore_ascii_case(b"DESTROY") {
        if !rest.is_empty() {
            return wrong_args("xgroup");
        }
        let result = db.with_value_mut(key, |v| match v {
            KVal::Stream(s) => Ok(s.destroy_group(&group)),
            _ => Err(()),
        });
        match result {
            Some(Ok(true)) => {
                on_key_changed(ctx, state.db_index, key.clone());
                reply(Value::Integer(1))
            }
            Some(Ok(false)) | None => reply(Value::Integer(0)),
            Some(Err(())) => wrong_type(),
        }
    } else if sub.eq_ignore_ascii_case(b"CREATECONSUMER") {
        let [consumer] = rest else { return wrong_args("xgroup") };
        let result = db.with_value_mut(key, |v| match v {
            KVal::Stream(s) => Ok(s.create_consumer(&group, consumer.clone())),
            _ => Err(()),
        });
        match result {
            Some(Ok(Some(true))) => {
                on_key_changed(ctx, state.db_index, key.clone());
                reply(Value::Integer(1))
            }
            Some(Ok(Some(false))) => reply(Value::Integer(0)),
            Some(Ok(None)) | None => nogroup(key, &group),
            Some(Err(())) => wrong_type(),
        }
    } else if sub.eq_ignore_ascii_case(b"DELCONSUMER") {
        let [consumer] = rest else { return wrong_args("xgroup") };
        let result = db.with_value_mut(key, |v| match v {
            KVal::Stream(s) => Ok(s.delete_consumer(&group, consumer)),
            _ => Err(()),
        });
        match result {
            Some(Ok(Some(n))) => {
                on_key_changed(ctx, state.db_index, key.clone());
                reply(Value::Integer(n as i64))
            }
            Some(Ok(None)) | None => nogroup(key, &group),
            Some(Err(())) => wrong_type(),
        }
    } else {
        err(format!(
            "ERR Unknown XGROUP subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(sub)
        ))
    }
}

pub fn cmd_xack(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, group, ids @ ..] = args else { return wrong_args("xack") };
    if ids.is_empty() {
        return wrong_args("xack");
    }
    let mut parsed = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(id) = resolve_bound(id, 0) else { return invalid_id() };
        parsed.push(id);
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_value_mut(key, |v| match v {
        KVal::Stream(s) => Ok(s.ack(group, &parsed)),
        _ => Err(()),
    });
    match result {
        None => reply(Value::Integer(0)),
        Some(Ok(n)) => {
            if n > 0 {
                on_key_changed(ctx, state.db_index, key.clone());
            }
            reply(Value::Integer(n as i64))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `XPENDING key group` (summary) or `XPENDING key group start end count
/// [consumer]` (extended). `IDLE <ms>` filtering isn't supported.
pub fn cmd_xpending(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, group, rest @ ..] = args else { return wrong_args("xpending") };
    let db = ctx.store.db(state.db_index).expect("valid db index");

    if rest.is_empty() {
        return match db.with_value(key, |v| match v {
            KVal::Stream(s) => Ok(s.pending_summary(group)),
            _ => Err(()),
        }) {
            Some(Ok(Some((count, min, max, per_consumer)))) => {
                let min_v = min.map_or(Value::NullBulk, |id| Value::bulk(id.to_display_string()));
                let max_v = max.map_or(Value::NullBulk, |id| Value::bulk(id.to_display_string()));
                let consumers = if per_consumer.is_empty() {
                    Value::NullArray
                } else {
                    Value::Array(
                        per_consumer
                            .into_iter()
                            .map(|(c, n)| Value::Array(vec![Value::bulk(c), Value::bulk(n.to_string())]))
                            .collect(),
                    )
                };
                reply(Value::Array(vec![Value::Integer(count as i64), min_v, max_v, consumers]))
            }
            Some(Ok(None)) | None => nogroup(key, group),
            Some(Err(())) => wrong_type(),
        };
    }

    let (start_arg, end_arg, count_arg, consumer) = match rest {
        [s, e, c] => (s, e, c, None),
        [s, e, c, consumer] => (s, e, c, Some(consumer.clone())),
        _ => return wrong_args("xpending"),
    };
    let Some(start) = resolve_bound(start_arg, 0) else { return invalid_id() };
    let Some(end) = resolve_bound(end_arg, u64::MAX) else { return invalid_id() };
    let Some(count) = std::str::from_utf8(count_arg).ok().and_then(|s| s.parse::<usize>().ok()) else {
        return err("ERR value is not an integer or out of range");
    };
    let now = now_ms();
    match db.with_value(key, |v| match v {
        KVal::Stream(s) => Ok(s.pending_detail(group, start, end, count, consumer.as_deref(), now)),
        _ => Err(()),
    }) {
        Some(Ok(Some(items))) => reply(Value::Array(
            items
                .into_iter()
                .map(|(id, consumer, idle, dc)| {
                    Value::Array(vec![
                        Value::bulk(id.to_display_string()),
                        Value::bulk(consumer),
                        Value::Integer(idle as i64),
                        Value::Integer(dc as i64),
                    ])
                })
                .collect(),
        )),
        Some(Ok(None)) | None => nogroup(key, group),
        Some(Err(())) => wrong_type(),
    }
}

/// `XREADGROUP GROUP group consumer [COUNT n] [NOACK] STREAMS key
/// [key...] id [id...]`. Each `id` must be `>` (new entries) or an
/// explicit ID (re-read that consumer's own already-pending entries
/// greater than it, `0` for "my whole PEL"). No `BLOCK` support — see
/// `cmd_xread`'s docs, same reasoning applies here.
///
/// Simplification worth knowing: stream keys are validated/consumed one
/// at a time, left to right, not all validated upfront — an error on the
/// second key (wrong type, missing group) is returned after the first
/// key's delivery/PEL-insertion has already happened. Real Redis
/// validates every key before doing anything. Matters only for a
/// multi-key call that's partially invalid, which is rare in practice.
pub fn cmd_xreadgroup(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [kw_group, group, consumer, rest @ ..] = args else { return wrong_args("xreadgroup") };
    if !kw_group.eq_ignore_ascii_case(b"GROUP") {
        return err("ERR syntax error");
    }
    let mut i = 0;
    let mut count = None;
    let mut noack = false;
    loop {
        if rest.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"COUNT")) {
            let Some(n) = rest
                .get(i + 1)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse::<usize>().ok())
            else {
                return err("ERR value is not an integer or out of range");
            };
            count = Some(n);
            i += 2;
        } else if rest.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"NOACK")) {
            noack = true;
            i += 1;
        } else {
            break;
        }
    }
    if !rest.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"STREAMS")) {
        return err("ERR syntax error");
    }
    i += 1;
    let remaining = &rest[i..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return wrong_args("xreadgroup");
    }
    let n = remaining.len() / 2;
    let keys = &remaining[..n];
    let id_args = &remaining[n..];

    let db = ctx.store.db(state.db_index).expect("valid db index");
    let now = now_ms();
    let mut out = Vec::with_capacity(n);
    for (key, id_arg) in keys.iter().zip(id_args) {
        let after = if id_arg.as_ref() == b">" {
            None
        } else {
            match resolve_bound(id_arg, 0) {
                Some(id) => Some(id),
                None => return invalid_id(),
            }
        };
        let consumer = consumer.clone();
        let result = db.with_value_mut(key, |v| match v {
            KVal::Stream(s) => match s.read_group(group, consumer, after, count, now, noack) {
                Some(entries) => Ok(entries),
                None => Err(true),
            },
            _ => Err(false),
        });
        match result {
            Some(Ok(entries)) => {
                on_key_changed(ctx, state.db_index, key.clone());
                out.push((key.clone(), entries));
            }
            Some(Err(false)) => return wrong_type(),
            Some(Err(true)) | None => return nogroup(key, group),
        }
    }

    if out.iter().all(|(_, entries)| entries.is_empty()) {
        return reply(Value::NullArray);
    }
    reply(Value::Array(
        out.into_iter()
            .map(|(key, entries)| {
                Value::Array(vec![
                    Value::bulk(key),
                    Value::Array(entries.into_iter().map(|(id, f)| entry_reply(id, f)).collect()),
                ])
            })
            .collect(),
    ))
}

/// `XREAD [COUNT n] STREAMS key [key...] id [id...]`.
///
/// **No `BLOCK` support.** This is a real limitation, not just missing
/// syntax: `dispatch()` stays fully synchronous on purpose (it's called
/// recursively from `EVAL`/`LIVE`/`MULTI`, none of which can `.await` —
/// see `singleflight`'s module docs for the established pattern here).
/// Wiring `XREAD BLOCK` in properly would mean teaching the connection
/// loop (`server/main.rs`) a new `Outcome::BlockWait`-style variant keyed
/// on "any of these streams gets a new entry," which is a real feature,
/// not a quick addition — left for later rather than faked with a
/// `sleep`-based poll loop.
pub fn cmd_xread(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let mut i = 0;
    let mut count = None;
    if args.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"COUNT")) {
        let Some(n) =
            args.get(i + 1).and_then(|b| std::str::from_utf8(b).ok()).and_then(|s| s.parse::<usize>().ok())
        else {
            return err("ERR value is not an integer or out of range");
        };
        count = Some(n);
        i += 2;
    }
    if args.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"BLOCK")) {
        return err("ERR XREAD BLOCK is not supported (no blocking XREAD yet, see cmd_xread's docs)");
    }
    if !args.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"STREAMS")) {
        return err("ERR syntax error");
    }
    i += 1;
    let remaining = &args[i..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return wrong_args("xread");
    }
    let n = remaining.len() / 2;
    let keys = &remaining[..n];
    let id_args = &remaining[n..];

    let db = ctx.store.db(state.db_index).expect("valid db index");
    let mut out = Vec::with_capacity(n);
    for (key, id_arg) in keys.iter().zip(id_args) {
        let result = db.with_value(key, |v| match v {
            KVal::Stream(s) => {
                let after =
                    if id_arg.as_ref() == b"$" { Some(s.last_id()) } else { resolve_bound(id_arg, 0) };
                after.map(|after| s.after(after, count)).ok_or(None)
            }
            _ => Err(Some(())),
        });
        match result {
            Some(Ok(entries)) => {
                if !entries.is_empty() {
                    out.push((key.clone(), entries));
                }
            }
            Some(Err(Some(()))) => return wrong_type(),
            Some(Err(None)) => return invalid_id(),
            None => {} // missing key: simply contributes nothing, same as real Redis
        }
    }

    if out.is_empty() {
        return reply(Value::NullArray);
    }
    reply(Value::Array(
        out.into_iter()
            .map(|(key, entries)| {
                Value::Array(vec![
                    Value::bulk(key),
                    Value::Array(entries.into_iter().map(|(id, f)| entry_reply(id, f)).collect()),
                ])
            })
            .collect(),
    ))
}
