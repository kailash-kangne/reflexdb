use std::collections::HashSet;

use bytes::Bytes;
use keyspace::{Set, Value as KVal};
use rand::seq::SliceRandom;
use rand::Rng;
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
            on_key_changed(ctx, state, state.db_index, key.clone(), None);
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
        // `swap_remove` instead of plain `remove`: set member order is
        // never observable (matches Redis), and it's O(1) vs. the
        // order-preserving `shift_remove`'s O(n).
        KVal::Set(s) => Ok(members.iter().filter(|m| s.swap_remove(m.as_ref())).count()),
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
        notify_keyspace_event(ctx, state.db_index, 's', "srem", key);
    }
    reply(Value::Integer(removed as i64))
}

/// `SMOVE source destination member` — move `member` from the source set
/// to the destination set: 1 on move, 0 when the member isn't in `source`
/// (a no-op; the destination is never created). A same-key move is just a
/// membership check. Follows `lmove`'s restore-on-wrongtype pattern.
pub fn cmd_smove(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [source, destination, member] = args else { return wrong_args("smove") };
    let db = ctx.store.db(state.db_index).expect("valid db index");

    if source == destination {
        return match db.with_value(source, |v| match v {
            KVal::Set(s) => Ok(s.contains(member.as_ref())),
            _ => Err(()),
        }) {
            None | Some(Ok(false)) => reply(Value::Integer(0)),
            Some(Ok(true)) => reply(Value::Integer(1)),
            Some(Err(())) => wrong_type(),
        };
    }

    match db.with_value_mut(source, |v| match v {
        KVal::Set(s) => Ok(s.swap_remove(member.as_ref())),
        _ => Err(()),
    }) {
        None | Some(Ok(false)) => return reply(Value::Integer(0)),
        Some(Err(())) => return wrong_type(),
        Some(Ok(true)) => {}
    }
    db.remove_if_empty(source);

    let inserted = db.with_entry_mut(
        destination.clone(),
        || KVal::Set(Set::new()),
        |v| match v {
            KVal::Set(s) => {
                s.insert(member.clone());
                Ok(())
            }
            _ => Err(()),
        },
    );
    if inserted.is_err() {
        // Destination holds a non-set value: put the member back where it
        // came from instead of silently dropping it.
        db.with_entry_mut(
            source.clone(),
            || KVal::Set(Set::new()),
            |v| {
                if let KVal::Set(s) = v {
                    s.insert(member.clone());
                }
            },
        );
        return wrong_type();
    }

    on_key_changed(ctx, state, state.db_index, source.clone(), None);
    on_key_changed(ctx, state, state.db_index, destination.clone(), None);
    notify_keyspace_event(ctx, state.db_index, 's', "srem", source);
    notify_keyspace_event(ctx, state.db_index, 's', "sadd", destination);
    reply(Value::Integer(1))
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

pub fn cmd_smismember(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("smismember");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(&args[0], |v| match v {
        KVal::Set(set) => {
            Ok(args[1..].iter().map(|member| Value::Integer(set.contains(member.as_ref()) as i64)).collect())
        }
        _ => Err(()),
    }) {
        None => reply(Value::Array(args[1..].iter().map(|_| Value::Integer(0)).collect())),
        Some(Ok(values)) => reply(Value::Array(values)),
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
            KVal::Set(s) => Ok(s.iter().cloned().collect::<HashSet<Bytes>>()),
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
///
/// Picks each member by random index and `swap_remove_index`s it — O(1) per
/// pop (backed by `IndexSet`, not a plain `HashSet`) instead of cloning
/// every member into a `Vec` just to throw most of it away, which used to
/// make `SPOP` on a large set O(n) per call regardless of `count`.
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
            let n = count.unwrap_or(1).min(s.len());
            let mut rng = rand::thread_rng();
            let mut chosen = Vec::with_capacity(n);
            for _ in 0..n {
                let idx = rng.gen_range(0..s.len());
                if let Some(member) = s.swap_remove_index(idx) {
                    chosen.push(member);
                }
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
        on_key_changed(ctx, state, state.db_index, key.clone(), None);
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

pub(crate) enum SetAlgebra {
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

pub fn cmd_store(ctx: &ServerContext, state: &ClientState, args: &[Bytes], op: SetAlgebra) -> Outcome {
    if args.len() < 2 {
        return wrong_args(match op {
            SetAlgebra::Union => "sunionstore",
            SetAlgebra::Inter => "sinterstore",
            SetAlgebra::Diff => "sdiffstore",
        });
    }
    let destination = args[0].clone();
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let mut sets: Vec<HashSet<Bytes>> = Vec::with_capacity(args.len() - 1);
    for key in &args[1..] {
        match db.with_value(key, |v| match v {
            KVal::Set(s) => Ok(s.iter().cloned().collect()),
            _ => Err(()),
        }) {
            None => sets.push(HashSet::new()),
            Some(Ok(s)) => sets.push(s),
            Some(Err(())) => return wrong_type(),
        }
    }
    let mut sets = sets.into_iter();
    let first = sets.next().unwrap_or_default();
    let result: HashSet<Bytes> = match op {
        SetAlgebra::Union => sets.fold(first, |mut acc, s| {
            acc.extend(s);
            acc
        }),
        SetAlgebra::Inter => sets.fold(first, |acc, s| acc.intersection(&s).cloned().collect()),
        SetAlgebra::Diff => sets.fold(first, |acc, s| acc.difference(&s).cloned().collect()),
    };
    if result.is_empty() {
        db.del(std::slice::from_ref(&destination));
    } else {
        db.set(destination.clone(), KVal::Set(result.into_iter().collect()), None);
    }
    on_key_changed(ctx, state, state.db_index, destination.clone(), None);
    notify_keyspace_event(ctx, state.db_index, 's', "store", &destination);
    reply(Value::Integer(
        db.with_value(&destination, |v| match v {
            KVal::Set(s) => s.len(),
            _ => 0,
        })
        .unwrap_or(0) as i64,
    ))
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
            KVal::Set(s) => Ok(s.iter().cloned().collect::<HashSet<Bytes>>()),
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
