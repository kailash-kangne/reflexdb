//! String-specific commands beyond plain `SET`/`GET` (which live in
//! `dispatch.rs`): numeric counters (`INCR`/`INCRBY`/`DECR`/`DECRBY`),
//! `APPEND`, `SETNX`, bulk `MSET`/`MGET`, and range helpers (`STRLEN`,
//! `GETRANGE`).

use bytes::Bytes;
use keyspace::Value as KVal;
use resp::Value;

use crate::dispatch::{err, reply, wrong_args, wrong_type, ClientState, Outcome, ServerContext};
use crate::dispatch::{notify_keyspace_event, on_key_changed};
use crate::range::{normalize, parse_i64};

pub fn cmd_append(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, suffix] = args else { return wrong_args("append") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let key = key.clone();

    let result = db.with_entry_mut(
        key.clone(),
        || KVal::String(Bytes::new()),
        |v| match v {
            KVal::String(s) => {
                let mut buf = Vec::with_capacity(s.len() + suffix.len());
                buf.extend_from_slice(s);
                buf.extend_from_slice(suffix);
                *s = Bytes::from(buf);
                Ok(s.len())
            }
            _ => Err(()),
        },
    );

    match result {
        Ok(len) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, '$', "append", &key);
            reply(Value::Integer(len as i64))
        }
        Err(()) => wrong_type(),
    }
}

enum IncrError {
    NotAnInteger,
    Overflow,
    WrongType,
}

/// Shared `INCR`/`INCRBY`/`DECR`/`DECRBY` implementation: parses the
/// existing string as an `i64` (treating a missing key as `0`, matching
/// Redis), adds `delta`, and stores the result back as a string — without
/// disturbing the key's TTL.
fn incr_by(ctx: &ServerContext, state: &ClientState, key: &Bytes, delta: i64, event: &str) -> Outcome {
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::String(Bytes::from_static(b"0")),
        |v| match v {
            KVal::String(s) => {
                let current: i64 = match std::str::from_utf8(s).ok().and_then(|x| x.parse().ok()) {
                    Some(n) => n,
                    None => return Err(IncrError::NotAnInteger),
                };
                let next = current.checked_add(delta).ok_or(IncrError::Overflow)?;
                *s = Bytes::from(next.to_string());
                Ok(next)
            }
            _ => Err(IncrError::WrongType),
        },
    );
    match result {
        Ok(n) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, '$', event, key);
            reply(Value::Integer(n))
        }
        Err(IncrError::NotAnInteger) => err("ERR value is not an integer or out of range"),
        Err(IncrError::Overflow) => err("ERR increment or decrement would overflow"),
        Err(IncrError::WrongType) => wrong_type(),
    }
}

pub fn cmd_incr(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("incr") };
    incr_by(ctx, state, key, 1, "incrby")
}

pub fn cmd_decr(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("decr") };
    incr_by(ctx, state, key, -1, "decrby")
}

pub fn cmd_incrby(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, amount] = args else { return wrong_args("incrby") };
    let delta: i64 = match std::str::from_utf8(amount).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    incr_by(ctx, state, key, delta, "incrby")
}

pub fn cmd_decrby(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, amount] = args else { return wrong_args("decrby") };
    let delta: i64 = match std::str::from_utf8(amount).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    let Some(delta) = delta.checked_neg() else {
        return err("ERR decrement would overflow");
    };
    incr_by(ctx, state, key, delta, "decrby")
}

/// `GETDEL key` — atomically read a string and delete the key, in one
/// round trip (equivalent to `GET` + `DEL`, but without the race between
/// them).
pub fn cmd_getdel(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("getdel") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: avoid cloning a potentially huge
    // List/Hash/Set/ZSet just to discover it's the wrong type.
    match db.with_value(key, |v| match v {
        KVal::String(s) => Ok(s.clone()),
        _ => Err(()),
    }) {
        Some(Ok(s)) => {
            db.del(std::slice::from_ref(key));
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'g', "del", key);
            reply(Value::bulk(s))
        }
        Some(Err(())) => wrong_type(),
        None => reply(Value::NullBulk),
    }
}

enum IncrFloatError {
    NotAFloat,
    WrongType,
}

/// `INCRBYFLOAT key increment` — like `INCRBY` but for floats (treating a
/// missing key as `0`, same as the integer counters).
pub fn cmd_incrbyfloat(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, increment] = args else { return wrong_args("incrbyfloat") };
    let delta: f64 = match std::str::from_utf8(increment).ok().and_then(|s| s.parse::<f64>().ok()) {
        Some(n) if n.is_finite() => n,
        _ => return err("ERR value is not a valid float"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::String(Bytes::from_static(b"0")),
        |v| match v {
            KVal::String(s) => {
                let current: f64 = match std::str::from_utf8(s).ok().and_then(|x| x.parse().ok()) {
                    Some(n) => n,
                    None => return Err(IncrFloatError::NotAFloat),
                };
                let next = current + delta;
                if !next.is_finite() {
                    return Err(IncrFloatError::NotAFloat);
                }
                let formatted = format_float(next);
                *s = Bytes::from(formatted.clone());
                Ok(formatted)
            }
            _ => Err(IncrFloatError::WrongType),
        },
    );
    match result {
        Ok(formatted) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, '$', "incrbyfloat", key);
            reply(Value::bulk(formatted))
        }
        Err(IncrFloatError::NotAFloat) => err("ERR value is not a valid float"),
        Err(IncrFloatError::WrongType) => wrong_type(),
    }
}

/// Same "print as an integer when it is one, otherwise plain `{}`"
/// formatting as `zset::format_score` — `INCRBYFLOAT`'s reply shouldn't
/// carry a `.0` suffix for whole-number results.
fn format_float(n: f64) -> String {
    if n == n.trunc() && n.is_finite() {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

pub fn cmd_setnx(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, value] = args else { return wrong_args("setnx") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: this only needs to know whether the key
    // exists, not a clone of a potentially huge List/Hash/Set/ZSet value.
    if db.with_value(key, |_| ()).is_some() {
        return reply(Value::Integer(0));
    }
    db.set(key.clone(), KVal::String(value.clone()), None);
    on_key_changed(ctx, state.db_index, key.clone());
    notify_keyspace_event(ctx, state.db_index, '$', "set", key);
    reply(Value::Integer(1))
}

pub fn cmd_mset(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return wrong_args("mset");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    for chunk in args.chunks(2) {
        let key = chunk[0].clone();
        db.set(key.clone(), KVal::String(chunk[1].clone()), None);
        on_key_changed(ctx, state.db_index, key.clone());
        notify_keyspace_event(ctx, state.db_index, '$', "set", &key);
    }
    reply(Value::ok())
}

pub fn cmd_mget(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("mget");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: a non-string key (or a miss) shouldn't pay
    // for cloning a potentially huge List/Hash/Set/ZSet just to be
    // reported as `nil`.
    let values = args
        .iter()
        .map(|key| {
            match db.with_value(key, |v| match v {
                KVal::String(s) => Some(s.clone()),
                _ => None,
            }) {
                Some(Some(s)) => Value::bulk(s),
                _ => Value::NullBulk,
            }
        })
        .collect();
    reply(Value::Array(values))
}

pub fn cmd_strlen(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("strlen") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: don't clone a potentially huge
    // List/Hash/Set/ZSet just to report its length is the wrong concept.
    match db.with_value(key, |v| match v {
        KVal::String(s) => Ok(s.len()),
        _ => Err(()),
    }) {
        Some(Ok(len)) => reply(Value::Integer(len as i64)),
        Some(Err(())) => wrong_type(),
        None => reply(Value::Integer(0)),
    }
}

pub fn cmd_getrange(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, start, end] = args else { return wrong_args("getrange") };
    let (start, end): (i64, i64) = match (parse_i64(start), parse_i64(end)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR value is not an integer or out of range"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: `Bytes::slice` only needs `&Bytes`, so the
    // range can be carved out without ever cloning the whole string — and
    // a wrong-type key doesn't pay for cloning a potentially huge
    // List/Hash/Set/ZSet either.
    match db.with_value(key, |v| match v {
        KVal::String(s) => {
            let len = s.len() as i64;
            if len == 0 {
                return Ok(Bytes::new());
            }
            let (lo, hi) = normalize(start, end, len);
            if lo > hi {
                Ok(Bytes::new())
            } else {
                Ok(s.slice(lo as usize..=hi as usize))
            }
        }
        _ => Err(()),
    }) {
        Some(Ok(slice)) => reply(Value::bulk(slice)),
        Some(Err(())) => wrong_type(),
        None => reply(Value::bulk(Bytes::new())),
    }
}
