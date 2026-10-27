use std::collections::HashSet;

use bytes::Bytes;
use keyspace::{Set, Value as KVal};
use rand::seq::SliceRandom;
use resp::Value;

use crate::dispatch::{err, reply, wrong_args, wrong_type, ClientState, Outcome, ServerContext};
use crate::dispatch::{notify_keyspace_event, on_key_changed};
use crate::scan::{self, ScanReply};

pub fn cmd_sadd(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("sadd");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let key = args[0].clone();
    let members = &args[1..];

    let result = db.with_entry_mut(
        key.clone(),
        || KVal::Set(Set::new()),
        |v| match v {
            KVal::Set(s) => Ok(members.iter().filter(|m| s.insert((*m).clone())).count()),
            _ => Err(()),
        },
    );

    match result {
        Ok(added) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 's', "sadd", &key);
            reply(Value::Integer(added as i64))
        }
        Err(()) => wrong_type(),
    }
}

pub fn cmd_srem(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("srem");
    }
    let key = &args[0];
    let members = &args[1..];
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let removed = db.with_value_mut(key, |v| match v {
        KVal::Set(s) => Ok(members.iter().filter(|m| s.remove(m.as_ref())).count()),
        _ => Err(()),
    });
    let removed = match removed {
        None => return reply(Value::Integer(0)),
        Some(Ok(n)) => n,
        Some(Err(())) => return wrong_type(),
    };
    db.remove_if_empty(key);
    if removed > 0 {
        on_key_changed(ctx, state.db_index, key.clone());
        notify_keyspace_event(ctx, state.db_index, 's', "srem", key);
    }
    reply(Value::Integer(removed as i64))
}

pub fn cmd_smembers(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("smembers") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Set(s) => Ok(s.iter().cloned().collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => reply(Value::Set(vec![])),
        Some(Ok(members)) => reply(Value::Set(members.into_iter().map(Value::bulk).collect())),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_sismember(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, member] = args else { return wrong_args("sismember") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Set(s) => Ok(s.contains(member.as_ref())),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(found)) => reply(Value::Integer(found as i64)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_scard(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("scard") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::Set(s) => Ok(s.len()),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(len)) => reply(Value::Integer(len as i64)),
        Some(Err(())) => wrong_type(),
    }
}

/// `SINTERCARD numkeys key [key ...] [LIMIT limit]` — the cardinality of the
/// intersection, without materializing the result set.
pub fn cmd_sintercard(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("sintercard");
    }
    let numkeys: usize = match std::str::from_utf8(&args[0]).ok().and_then(|s| s.parse().ok()) {
        Some(n) if n > 0 => n,
        _ => return err("ERR numkeys should be greater than 0"),
    };
    if args.len() < 1 + numkeys {
        return err("ERR Number of keys can't be greater than number of args");
    }
    let keys = &args[1..1 + numkeys];
    let rest = &args[1 + numkeys..];
    let mut limit = usize::MAX;
    if !rest.is_empty() {
        match rest {
            [kw, n] if kw.eq_ignore_ascii_case(b"LIMIT") => {
                let n: i64 = match std::str::from_utf8(n).ok().and_then(|s| s.parse().ok()) {
                    Some(n) => n,
                    None => return err("ERR LIMIT can't be negative"),
                };
                if n < 0 {
                    return err("ERR LIMIT can't be negative");
                }
                limit = if n == 0 { usize::MAX } else { n as usize };
            }
            _ => return err("ERR syntax error"),
        }
    }

    let db = ctx.store.db(state.db_index).expect("valid db index");
    let mut sets: Vec<HashSet<Bytes>> = Vec::with_capacity(keys.len());
    for key in keys {
        match db.with_value(key, |v| match v {
            KVal::Set(s) => Ok(s.clone()),
            _ => Err(()),
        }) {
            None => return reply(Value::Integer(0)),
            Some(Ok(s)) => sets.push(s),
            Some(Err(())) => return wrong_type(),
        }
    }
    sets.sort_by_key(|s| s.len());
    let mut count = 0;
    'outer: for member in &sets[0] {
        if sets[1..].iter().all(|s| s.contains(member)) {
            count += 1;
            if count >= limit {
                break 'outer;
            }
        }
    }
    reply(Value::Integer(count as i64))
}

/// `SPOP key [count]` — removes and returns one (or `count`) random
/// member(s). With no `count`, replies with a single bulk string (or
/// `NullBulk`); with `count`, always replies with a set (possibly empty),
/// matching Redis.
pub fn cmd_spop(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, count) = match args {
        [key] => (key, None),
        [key, count] => {
            let n: i64 = match std::str::from_utf8(count).ok().and_then(|s| s.parse().ok()) {
                Some(n) if n >= 0 => n,
                _ => return err("ERR value is out of range, must be positive"),
            };
            (key, Some(n as usize))
        }
        _ => return wrong_args("spop"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let popped = db.with_value_mut(key, |v| match v {
        KVal::Set(s) => {
            let members: Vec<Bytes> = s.iter().cloned().collect();
            let n = count.unwrap_or(1).min(members.len());
            let mut rng = rand::thread_rng();
            let chosen: Vec<Bytes> = members.choose_multiple(&mut rng, n).cloned().collect();
            for m in &chosen {
                s.remove(m.as_ref());
            }
            Ok(chosen)
        }
        _ => Err(()),
    });
    let chosen = match popped {
        None => return reply(if count.is_some() { Value::Set(vec![]) } else { Value::NullBulk }),
        Some(Ok(c)) => c,
        Some(Err(())) => return wrong_type(),
    };
    db.remove_if_empty(key);
    if !chosen.is_empty() {
        on_key_changed(ctx, state.db_index, key.clone());
        notify_keyspace_event(ctx, state.db_index, 's', "spop", key);
    }
    match count {
        None => reply(chosen.into_iter().next().map_or(Value::NullBulk, Value::bulk)),
        Some(_) => reply(Value::Set(chosen.into_iter().map(Value::bulk).collect())),
    }
}

/// `SRANDMEMBER key [count]` — like `SPOP` but doesn't remove anything.
/// A negative `count` allows the same member to be returned more than
/// once (picks exactly `-count` times); a non-negative one returns
/// distinct members, capped at the set's size.
pub fn cmd_srandmember(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, count) = match args {
        [key] => (key, None),
        [key, count] => {
            let n: i64 = match std::str::from_utf8(count).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return err("ERR value is not an integer or out of range"),
            };
            (key, Some(n))
        }
        _ => return wrong_args("srandmember"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let members = match db.with_value(key, |v| match v {
        KVal::Set(s) => Ok(s.iter().cloned().collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => return reply(if count.is_some() { Value::Array(vec![]) } else { Value::NullBulk }),
        Some(Ok(m)) => m,
        Some(Err(())) => return wrong_type(),
    };
    let mut rng = rand::thread_rng();
    match count {
        None => reply(members.choose(&mut rng).cloned().map_or(Value::NullBulk, Value::bulk)),
        Some(n) if n >= 0 => {
            let n = (n as usize).min(members.len());
            let chosen: Vec<Bytes> = members.choose_multiple(&mut rng, n).cloned().collect();
            reply(Value::Array(chosen.into_iter().map(Value::bulk).collect()))
        }
        Some(n) => {
            let want = (-n) as usize;
            let mut out = Vec::with_capacity(want);
            for _ in 0..want {
                let Some(m) = members.choose(&mut rng) else { break };
                out.push(m.clone());
            }
            reply(Value::Array(out.into_iter().map(Value::bulk).collect()))
        }
    }
}

enum SetAlgebra {
    Union,
    Inter,
    Diff,
}

pub fn cmd_sunion(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    set_algebra(ctx, state, args, "sunion", SetAlgebra::Union)
}

pub fn cmd_sinter(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    set_algebra(ctx, state, args, "sinter", SetAlgebra::Inter)
}

pub fn cmd_sdiff(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    set_algebra(ctx, state, args, "sdiff", SetAlgebra::Diff)
}

/// Shared `SUNION`/`SINTER`/`SDIFF` implementation — a missing key is
/// treated as an empty set (matches Redis), not an error.
fn set_algebra(
    ctx: &ServerContext,
    state: &ClientState,
    args: &[Bytes],
    name: &str,
    op: SetAlgebra,
) -> Outcome {
    if args.is_empty() {
        return wrong_args(name);
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let mut sets: Vec<HashSet<Bytes>> = Vec::with_capacity(args.len());
    for key in args {
        match db.with_value(key, |v| match v {
            KVal::Set(s) => Ok(s.clone()),
            _ => Err(()),
        }) {
            None => sets.push(HashSet::new()),
            Some(Ok(s)) => sets.push(s),
            Some(Err(())) => return wrong_type(),
        }
    }
    let mut iter = sets.into_iter();
    let first = iter.next().unwrap_or_default();
    let result: HashSet<Bytes> = match op {
        SetAlgebra::Union => iter.fold(first, |mut acc, s| {
            acc.extend(s);
            acc
        }),
        SetAlgebra::Inter => iter.fold(first, |acc, s| acc.intersection(&s).cloned().collect()),
        SetAlgebra::Diff => iter.fold(first, |acc, s| acc.difference(&s).cloned().collect()),
    };
    reply(Value::Set(result.into_iter().map(Value::bulk).collect()))
}

pub fn cmd_sscan(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("sscan");
    }
    let key = &args[0];
    let opts = match scan::parse_opts(&args[2..]) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let members = match db.with_value(key, |v| match v {
        KVal::Set(s) => Ok(s.iter().cloned().collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(m)) => m,
        Some(Err(())) => return wrong_type(),
    };
    let items = members.into_iter().filter(|m| scan::matches_pattern(&opts, m)).map(Value::bulk).collect();
    reply(ScanReply::done(items).into_value())
}
