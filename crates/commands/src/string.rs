//! String-specific commands beyond plain `SET`/`GET` (which live in
//! `dispatch.rs`): numeric counters (`INCR`/`INCRBY`/`DECR`/`DECRBY`),
//! `APPEND`, `SETNX`, bulk `MSET`/`MGET`, and range helpers (`STRLEN`,
//! `GETRANGE`).

use bytes::Bytes;
use keyspace::Value as KVal;
use resp::Value;
use std::time::{Duration, Instant};

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

pub fn cmd_getset(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, replacement] = args else { return wrong_args("getset") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let key = key.clone();
    let result = db.with_entry_mut_expiry(
        key.clone(),
        || KVal::String(Bytes::new()),
        |value, expires_at, was_present| match value {
            KVal::String(current) => {
                let previous = was_present.then(|| current.clone());
                *current = replacement.clone();
                *expires_at = None;
                Ok(previous)
            }
            _ => Err(()),
        },
    );
    match result {
        Ok(previous) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, '$', "set", &key);
            reply(previous.map_or(Value::NullBulk, Value::bulk))
        }
        Err(()) => wrong_type(),
    }
}

#[derive(Clone, Copy)]
enum GetExExpiry {
    Unchanged,
    Persist,
    At(Instant),
}

pub fn cmd_getex(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() || args.len() > 3 {
        return wrong_args("getex");
    }
    let expiry = match args.get(1).map(|option| option.to_ascii_uppercase()) {
        None => GetExExpiry::Unchanged,
        Some(option) if option == b"PERSIST" && args.len() == 2 => GetExExpiry::Persist,
        Some(option) if args.len() == 3 => {
            let amount: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return err("ERR value is not an integer or out of range"),
            };
            let now = Instant::now();
            match option.as_slice() {
                b"EX" | b"PX" if amount > 0 => {
                    let duration = if option == b"EX" {
                        Duration::from_secs(amount as u64)
                    } else {
                        Duration::from_millis(amount as u64)
                    };
                    let Some(at) = now.checked_add(duration) else {
                        return err("ERR invalid expire time in getex");
                    };
                    GetExExpiry::At(at)
                }
                b"EXAT" | b"PXAT" => {
                    let now_ms = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
                        Err(_) => return err("ERR system clock is before the Unix epoch"),
                    };
                    let target_ms = if option == b"EXAT" {
                        match amount.checked_mul(1000) {
                            Some(value) => value,
                            None => return err("ERR invalid expire time in getex"),
                        }
                    } else {
                        amount
                    };
                    let remaining = target_ms.saturating_sub(now_ms).max(0) as u64;
                    match now.checked_add(Duration::from_millis(remaining)) {
                        Some(at) => GetExExpiry::At(at),
                        None => return err("ERR invalid expire time in getex"),
                    }
                }
                _ => return err("ERR syntax error"),
            }
        }
        Some(_) => return err("ERR syntax error"),
    };

    let key = args[0].clone();
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_existing_entry_mut_expiry(&key, |value, expires_at| match value {
        KVal::String(current) => {
            let result = current.clone();
            match expiry {
                GetExExpiry::Unchanged => {}
                GetExExpiry::Persist => *expires_at = None,
                GetExExpiry::At(at) => *expires_at = Some(at),
            }
            Ok(result)
        }
        _ => Err(()),
    });
    match result {
        Some(Ok(value)) => {
            if !matches!(expiry, GetExExpiry::Unchanged) {
                on_key_changed(ctx, state.db_index, key.clone());
                let event = if matches!(expiry, GetExExpiry::Persist) { "persist" } else { "expire" };
                notify_keyspace_event(ctx, state.db_index, '$', event, &key);
            }
            reply(Value::bulk(value))
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
pub(crate) fn format_float(n: f64) -> String {
    if n == n.trunc() && n >= i64::MIN as f64 && n < i64::MAX as f64 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

/// `SETEX` and `PSETEX` share the same replacement and notification behavior.
pub fn cmd_setex(ctx: &ServerContext, state: &ClientState, args: &[Bytes], millis: bool) -> Outcome {
    let [key, ttl, value] = args else { return wrong_args(if millis { "psetex" } else { "setex" }) };
    let ttl: u64 = match std::str::from_utf8(ttl).ok().and_then(|s| s.parse().ok()) {
        Some(n) if n > 0 => n,
        _ => return err("ERR invalid expire time in setex"),
    };
    let duration = if millis { Duration::from_millis(ttl) } else { Duration::from_secs(ttl) };
    let Some(expires_at) = Instant::now().checked_add(duration) else {
        return err("ERR invalid expire time in setex");
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    db.set(key.clone(), KVal::String(value.clone()), Some(expires_at));
    on_key_changed(ctx, state.db_index, key.clone());
    notify_keyspace_event(ctx, state.db_index, '$', "set", key);
    reply(Value::ok())
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

pub fn cmd_getbit(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, offset] = args else { return wrong_args("getbit") };
    let offset: usize = match std::str::from_utf8(offset).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR bit offset is not an integer or out of range"),
    };
    let byte_index = offset / 8;
    let bit_index = 7 - (offset % 8);
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::String(value) => Ok(value.get(byte_index).is_some_and(|byte| (byte >> bit_index) & 1 != 0)),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(bit)) => reply(Value::Integer(bit as i64)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_setbit(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, offset, bit] = args else { return wrong_args("setbit") };
    let offset: usize = match std::str::from_utf8(offset).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR bit offset is not an integer or out of range"),
    };
    let bit = match bit.as_ref() {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return err("ERR bit is not an integer or out of range"),
    };
    const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;
    let byte_index = offset / 8;
    if byte_index >= MAX_STRING_SIZE {
        return err("ERR bit offset is not an integer or out of range");
    }
    let bit_index = 7 - (offset % 8);
    let key = key.clone();
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let previous = db.with_entry_mut(
        key.clone(),
        || KVal::String(Bytes::new()),
        |v| match v {
            KVal::String(value) => {
                let mut bytes = value.to_vec();
                if bytes.len() <= byte_index {
                    bytes.resize(byte_index + 1, 0);
                }
                let mask = 1u8 << bit_index;
                let old = (bytes[byte_index] & mask) != 0;
                if bit == 0 {
                    bytes[byte_index] &= !mask;
                } else {
                    bytes[byte_index] |= mask;
                }
                *value = Bytes::from(bytes);
                Ok(old)
            }
            _ => Err(()),
        },
    );
    match previous {
        Ok(old) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, '$', "setbit", &key);
            reply(Value::Integer(old as i64))
        }
        Err(()) => wrong_type(),
    }
}

pub fn cmd_bitcount(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() || args.len() == 2 || args.len() > 3 {
        return wrong_args("bitcount");
    }
    let range = if args.len() == 3 {
        let start = match parse_i64(&args[1]) {
            Some(n) => n,
            None => return err("ERR value is not an integer or out of range"),
        };
        let end = match parse_i64(&args[2]) {
            Some(n) => n,
            None => return err("ERR value is not an integer or out of range"),
        };
        Some((start, end))
    } else {
        None
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(&args[0], |v| match v {
        KVal::String(value) => {
            let (start, end) = range.map_or((0, value.len() as i64 - 1), |(start, end)| {
                normalize(start, end, value.len() as i64)
            });
            let count = if start > end || value.is_empty() {
                0
            } else {
                value[start as usize..=end as usize].iter().map(|byte| byte.count_ones() as i64).sum()
            };
            Ok(count)
        }
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(count)) => reply(Value::Integer(count)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_setrange(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, offset, replacement] = args else { return wrong_args("setrange") };
    let offset: usize = match std::str::from_utf8(offset).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR offset is out of range"),
    };
    const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;
    let Some(end) = offset.checked_add(replacement.len()) else {
        return err("ERR string exceeds maximum allowed size");
    };
    if end > MAX_STRING_SIZE {
        return err("ERR string exceeds maximum allowed size");
    }

    let db = ctx.store.db(state.db_index).expect("valid db index");
    if replacement.is_empty() {
        return match db.with_value(key, |v| match v {
            KVal::String(value) => Ok(value.len()),
            _ => Err(()),
        }) {
            None => reply(Value::Integer(0)),
            Some(Ok(len)) => reply(Value::Integer(len as i64)),
            Some(Err(())) => wrong_type(),
        };
    }

    let key_owned = key.clone();
    let result = db.with_entry_mut(
        key_owned.clone(),
        || KVal::String(Bytes::new()),
        |v| match v {
            KVal::String(value) => {
                let mut updated = vec![0; value.len().max(end)];
                updated[..value.len()].copy_from_slice(value);
                updated[offset..end].copy_from_slice(replacement);
                *value = Bytes::from(updated);
                Ok(value.len())
            }
            _ => Err(()),
        },
    );
    match result {
        Ok(len) => {
            on_key_changed(ctx, state.db_index, key_owned.clone());
            notify_keyspace_event(ctx, state.db_index, '$', "setrange", &key_owned);
            reply(Value::Integer(len as i64))
        }
        Err(()) => wrong_type(),
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
