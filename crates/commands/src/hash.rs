use bytes::Bytes;
use keyspace::{Hash, Value as KVal};
use rand::seq::SliceRandom;
use rand::Rng;
use resp::Value;

use crate::dispatch::{err, reply, wrong_args, wrong_type, ClientState, Outcome, ServerContext};
use crate::dispatch::{notify_keyspace_event, on_key_changed};
use crate::scan::{self, ScanReply};

pub fn cmd_hset(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 3 || args.len() % 2 != 1 {
        return wrong_args("hset");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let key = args[0].clone();
    let pairs = &args[1..];

    let result = db.with_entry_mut(
        key.clone(),
        || KVal::Hash(Hash::new()),
        |v| match v {
            KVal::Hash(h) => {
                let mut added = 0;
                for chunk in pairs.chunks(2) {
                    if h.insert(chunk[0].clone(), chunk[1].clone()).is_none() {
                        added += 1;
                    }
                }
                Ok(added)
            }
            _ => Err(()),
        },
    );

    match result {
        Ok(added) => {
            on_key_changed(ctx, state, state.db_index, key.clone(), None);
            notify_keyspace_event(ctx, state.db_index, 'h', "hset", &key);
            reply(Value::Integer(added as i64))
        }
        Err(()) => wrong_type(),
    }
}

pub fn cmd_hsetnx(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, field, value] = args else { return wrong_args("hsetnx") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::Hash(Hash::new()),
        |v| match v {
            KVal::Hash(h) => {
                if h.contains_key(field.as_ref()) {
                    Ok(false)
                } else {
                    h.insert(field.clone(), value.clone());
                    Ok(true)
                }
            }
            _ => Err(()),
        },
    );
    match result {
        Ok(true) => {
            on_key_changed(ctx, state, state.db_index, key.clone(), None);
            notify_keyspace_event(ctx, state.db_index, 'h', "hset", key);
            reply(Value::Integer(1))
        }
        Ok(false) => reply(Value::Integer(0)),
        Err(()) => wrong_type(),
    }
}

enum HIncrFloatError {
    NotAFloat,
    WrongType,
}

pub fn cmd_hincrbyfloat(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, field, increment] = args else { return wrong_args("hincrbyfloat") };
    let delta: f64 = match std::str::from_utf8(increment).ok().and_then(|s| s.parse::<f64>().ok()) {
        Some(n) if n.is_finite() => n,
        _ => return err("ERR value is not a valid float"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::Hash(Hash::new()),
        |v| match v {
            KVal::Hash(h) => {
                let current = match h.get(field.as_ref()) {
                    Some(value) => {
                        match std::str::from_utf8(value).ok().and_then(|s| s.parse::<f64>().ok()) {
                            Some(n) if n.is_finite() => n,
                            _ => return Err(HIncrFloatError::NotAFloat),
                        }
                    }
                    None => 0.0,
                };
                let next = current + delta;
                if !next.is_finite() {
                    return Err(HIncrFloatError::NotAFloat);
                }
                let formatted = crate::string::format_float(next);
                h.insert(field.clone(), Bytes::from(formatted.clone()));
                Ok(formatted)
            }
            _ => Err(HIncrFloatError::WrongType),
        },
    );
    match result {
        Ok(formatted) => {
            on_key_changed(ctx, state, state.db_index, key.clone(), None);
            notify_keyspace_event(ctx, state.db_index, 'h', "hincrbyfloat", key);
            reply(Value::bulk(formatted))
        }
        Err(HIncrFloatError::NotAFloat) => err("ERR hash value is not a valid float"),
        Err(HIncrFloatError::WrongType) => wrong_type(),
    }
}

pub fn cmd_hstrlen(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, field] = args else { return wrong_args("hstrlen") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(h.get(field.as_ref()).map_or(0, Bytes::len)),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(len)) => reply(Value::Integer(len as i64)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_hrandfield(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() || args.len() > 3 {
        return wrong_args("hrandfield");
    }
    let count = if args.len() >= 2 {
        match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => Some(n),
            None => return err("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };
    let with_values = match args.get(2) {
        Some(option) if count.is_some() && option.eq_ignore_ascii_case(b"WITHVALUES") => true,
        Some(_) => return err("ERR syntax error"),
        None => false,
    };
    let max_count = 1_000_000i64;
    if count.is_some_and(|n| n.unsigned_abs() > max_count as u64) {
        return err("ERR count is out of range");
    }

    let db = ctx.store.db(state.db_index).expect("valid db index");
    let fields = match db.with_value(&args[0], |v| match v {
        KVal::Hash(hash) => {
            Ok(hash.iter().map(|(field, value)| (field.clone(), value.clone())).collect::<Vec<_>>())
        }
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(fields)) => fields,
        Some(Err(())) => return wrong_type(),
    };

    if let Some(count) = count {
        let requested = count.unsigned_abs() as usize;
        let selected = if count < 0 {
            let mut rng = rand::thread_rng();
            (0..requested)
                .filter_map(|_| fields.get(rng.gen_range(0..fields.len().max(1))).cloned())
                .collect::<Vec<_>>()
        } else {
            let mut sample = fields;
            sample.shuffle(&mut rand::thread_rng());
            sample.truncate(requested);
            sample
        };
        let mut values = Vec::with_capacity(selected.len() * if with_values { 2 } else { 1 });
        for (field, value) in selected {
            values.push(Value::bulk(field));
            if with_values {
                values.push(Value::bulk(value));
            }
        }
        reply(Value::Array(values))
    } else {
        let mut rng = rand::thread_rng();
        reply(fields.choose(&mut rng).map_or(Value::NullBulk, |(field, _)| Value::bulk(field.clone())))
    }
}

pub fn cmd_hget(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, field] = args else { return wrong_args("hget") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(h.get(field.as_ref()).cloned()),
        _ => Err(()),
    }) {
        None | Some(Ok(None)) => reply(Value::NullBulk),
        Some(Ok(Some(val))) => reply(Value::bulk(val)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_hdel(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("hdel");
    }
    let key = &args[0];
    let fields = &args[1..];
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let removed = db.with_value_mut(key, |v| match v {
        KVal::Hash(h) => Ok(fields.iter().filter(|f| h.remove(f.as_ref()).is_some()).count()),
        _ => Err(()),
    });
    let removed = match removed {
        None => return reply(Value::Integer(0)),
        Some(Ok(n)) => n,
        Some(Err(())) => return wrong_type(),
    };
    db.remove_if_empty(key);
    if removed > 0 {
        on_key_changed(ctx, state, state.db_index, key.clone(), None);
        notify_keyspace_event(ctx, state.db_index, 'h', "hdel", key);
    }
    reply(Value::Integer(removed as i64))
}

/// `HMGET key field [field ...]` — bulk read, one `NullBulk` per missing
/// field rather than failing the whole command.
pub fn cmd_hmget(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("hmget");
    }
    let key = &args[0];
    let fields = &args[1..];
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(fields.iter().map(|f| h.get(f.as_ref()).cloned()).collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => reply(Value::Array(fields.iter().map(|_| Value::NullBulk).collect())),
        Some(Ok(vals)) => {
            reply(Value::Array(vals.into_iter().map(|v| v.map_or(Value::NullBulk, Value::bulk)).collect()))
        }
        Some(Err(())) => wrong_type(),
    }
}

enum HIncrError {
    NotAnInteger,
    Overflow,
    WrongType,
}

/// `HINCRBY key field increment` — treats a missing field as `0`, creates
/// the hash if it doesn't exist (same create-on-first-write behavior as
/// `HSET`).
pub fn cmd_hincrby(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, field, amount] = args else { return wrong_args("hincrby") };
    let delta: i64 = match std::str::from_utf8(amount).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::Hash(Hash::new()),
        |v| match v {
            KVal::Hash(h) => {
                let current: i64 = match h.get(field.as_ref()) {
                    Some(s) => match std::str::from_utf8(s).ok().and_then(|x| x.parse().ok()) {
                        Some(n) => n,
                        None => return Err(HIncrError::NotAnInteger),
                    },
                    None => 0,
                };
                let next = current.checked_add(delta).ok_or(HIncrError::Overflow)?;
                h.insert(field.clone(), Bytes::from(next.to_string()));
                Ok(next)
            }
            _ => Err(HIncrError::WrongType),
        },
    );
    match result {
        Ok(n) => {
            on_key_changed(ctx, state, state.db_index, key.clone(), None);
            notify_keyspace_event(ctx, state.db_index, 'h', "hincrby", key);
            reply(Value::Integer(n))
        }
        Err(HIncrError::NotAnInteger) => err("ERR hash value is not an integer"),
        Err(HIncrError::Overflow) => err("ERR increment or decrement would overflow"),
        Err(HIncrError::WrongType) => wrong_type(),
    }
}

pub fn cmd_hkeys(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("hkeys") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(h.keys().cloned().collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => reply(Value::Array(vec![])),
        Some(Ok(keys)) => reply(Value::Array(keys.into_iter().map(Value::bulk).collect())),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_hvals(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("hvals") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(h.values().cloned().collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => reply(Value::Array(vec![])),
        Some(Ok(vals)) => reply(Value::Array(vals.into_iter().map(Value::bulk).collect())),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_hexists(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, field] = args else { return wrong_args("hexists") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(h.contains_key(field.as_ref())),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(found)) => reply(Value::Integer(found as i64)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_hlen(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("hlen") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(h.len()),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(len)) => reply(Value::Integer(len as i64)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_hgetall(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("hgetall") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(h.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => reply(Value::Map(vec![])),
        Some(Ok(pairs)) => {
            reply(Value::Map(pairs.into_iter().map(|(k, v)| (Value::bulk(k), Value::bulk(v))).collect()))
        }
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_hscan(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("hscan");
    }
    let key = &args[0];
    let opts = match scan::parse_opts(&args[2..]) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let fields = match db.with_value(key, |v| match v {
        KVal::Hash(h) => Ok(h.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(pairs)) => pairs,
        Some(Err(())) => return wrong_type(),
    };
    let mut flat = Vec::with_capacity(fields.len() * 2);
    for (field, value) in fields {
        if scan::matches_pattern(&opts, &field) {
            flat.push(Value::bulk(field));
            flat.push(Value::bulk(value));
        }
    }
    reply(ScanReply::done(flat).into_value())
}
