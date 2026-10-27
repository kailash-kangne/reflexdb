use bytes::Bytes;
use keyspace::{Value as KVal, ZSet};
use resp::Value;

use crate::dispatch::{
    blocking_deadline, check_write_allowed, clear_blocking_deadline, notify_keyspace_event, on_key_changed,
};
use crate::dispatch::{err, reply, wrong_args, wrong_type, ClientState, Outcome, ServerContext};
use crate::range::{normalize, parse_i64};
use crate::scan::{self, ScanReply};

pub fn cmd_zadd(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 3 || args.len() % 2 != 1 {
        return wrong_args("zadd");
    }
    let key = args[0].clone();
    let mut pairs = Vec::with_capacity(args.len() / 2);
    for chunk in args[1..].chunks(2) {
        let score: f64 = match std::str::from_utf8(&chunk[0]).ok().and_then(|s| s.parse().ok()) {
            Some(s) => s,
            None => return err("ERR value is not a valid float"),
        };
        pairs.push((chunk[1].clone(), score));
    }

    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::ZSet(ZSet::new()),
        |v| match v {
            KVal::ZSet(z) => {
                let mut added = 0;
                for (member, score) in pairs {
                    if z.insert(member, score) {
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
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'z', "zadd", &key);
            reply(Value::Integer(added as i64))
        }
        Err(()) => wrong_type(),
    }
}

/// `ZINCRBY key increment member` — adds `increment` to `member`'s current
/// score (treating a missing member as score `0`, same as `ZADD`'s own
/// create-on-first-write behavior), creating the key if it doesn't exist.
pub fn cmd_zincrby(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, increment, member] = args else { return wrong_args("zincrby") };
    let delta: f64 = match std::str::from_utf8(increment).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not a valid float"),
    };
    let key = key.clone();
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::ZSet(ZSet::new()),
        |v| match v {
            KVal::ZSet(z) => {
                let new_score = z.score(member).unwrap_or(0.0) + delta;
                z.insert(member.clone(), new_score);
                Ok(new_score)
            }
            _ => Err(()),
        },
    );
    match result {
        Ok(score) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'z', "zincr", &key);
            reply(Value::bulk(format_score(score)))
        }
        Err(()) => wrong_type(),
    }
}

pub fn cmd_zscore(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, member] = args else { return wrong_args("zscore") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.score(member)),
        _ => Err(()),
    }) {
        None | Some(Ok(None)) => reply(Value::NullBulk),
        Some(Ok(Some(score))) => reply(Value::bulk(format_score(score))),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_zrem(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("zrem");
    }
    let key = &args[0];
    let members = &args[1..];
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let removed = db.with_value_mut(key, |v| match v {
        KVal::ZSet(z) => Ok(members.iter().filter(|m| z.remove(m)).count()),
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
        notify_keyspace_event(ctx, state.db_index, 'z', "zrem", key);
    }
    reply(Value::Integer(removed as i64))
}

pub fn cmd_zcard(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("zcard") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.len()),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(len)) => reply(Value::Integer(len as i64)),
        Some(Err(())) => wrong_type(),
    }
}

/// `ZRANGE key start stop [WITHSCORES]` — index-based range only (no
/// `BYSCORE`/`BYLEX`/`REV` yet).
pub fn cmd_zrange(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, start, stop, withscores) = match args {
        [key, start, stop] => (key, start, stop, false),
        [key, start, stop, flag] if flag.eq_ignore_ascii_case(b"WITHSCORES") => (key, start, stop, true),
        _ => return wrong_args("zrange"),
    };
    let (start, stop): (i64, i64) = match (parse_i64(start), parse_i64(stop)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR value is not an integer or out of range"),
    };

    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted()),
        _ => Err(()),
    }) {
        None => reply(Value::Array(vec![])),
        Some(Ok(sorted)) => {
            let len = sorted.len() as i64;
            let (lo, hi) = normalize(start, stop, len);
            let slice = if lo > hi { &sorted[0..0] } else { &sorted[lo as usize..=(hi as usize)] };
            reply(Value::Array(render(slice, withscores)))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `ZRANGEBYSCORE key min max [WITHSCORES]` — `min`/`max` support `-inf`/`+inf`
/// but not the `(exclusive` prefix yet.
pub fn cmd_zrangebyscore(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, min, max, withscores) = match args {
        [key, min, max] => (key, min, max, false),
        [key, min, max, flag] if flag.eq_ignore_ascii_case(b"WITHSCORES") => (key, min, max, true),
        _ => return wrong_args("zrangebyscore"),
    };
    let (min, max) = match (parse_score_bound(min), parse_score_bound(max)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR min or max is not a float"),
    };

    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted()),
        _ => Err(()),
    }) {
        None => reply(Value::Array(vec![])),
        Some(Ok(sorted)) => {
            let filtered: Vec<(Bytes, f64)> =
                sorted.into_iter().filter(|(_, s)| *s >= min && *s <= max).collect();
            reply(Value::Array(render(&filtered, withscores)))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `ZRANK key member [WITHSCORE]` — ascending-order position (0-based).
pub fn cmd_zrank(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    zrank(ctx, state, args, "zrank", false)
}

/// `ZREVRANK key member [WITHSCORE]` — descending-order position (0-based).
pub fn cmd_zrevrank(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    zrank(ctx, state, args, "zrevrank", true)
}

fn zrank(ctx: &ServerContext, state: &ClientState, args: &[Bytes], name: &str, rev: bool) -> Outcome {
    let (key, member, withscore) = match args {
        [key, member] => (key, member, false),
        [key, member, flag] if flag.eq_ignore_ascii_case(b"WITHSCORE") => (key, member, true),
        _ => return wrong_args(name),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let not_found = || reply(if withscore { Value::NullArray } else { Value::NullBulk });
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted()),
        _ => Err(()),
    }) {
        None => not_found(),
        Some(Ok(mut sorted)) => {
            if rev {
                sorted.reverse();
            }
            match sorted.iter().position(|(m, _)| m.as_ref() == member.as_ref()) {
                None => not_found(),
                Some(idx) if withscore => reply(Value::Array(vec![
                    Value::Integer(idx as i64),
                    Value::bulk(format_score(sorted[idx].1)),
                ])),
                Some(idx) => reply(Value::Integer(idx as i64)),
            }
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `ZREVRANGE key start stop [WITHSCORES]` — same as `ZRANGE` but
/// descending by score.
pub fn cmd_zrevrange(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, start, stop, withscores) = match args {
        [key, start, stop] => (key, start, stop, false),
        [key, start, stop, flag] if flag.eq_ignore_ascii_case(b"WITHSCORES") => (key, start, stop, true),
        _ => return wrong_args("zrevrange"),
    };
    let (start, stop): (i64, i64) = match (parse_i64(start), parse_i64(stop)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR value is not an integer or out of range"),
    };

    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted()),
        _ => Err(()),
    }) {
        None => reply(Value::Array(vec![])),
        Some(Ok(mut sorted)) => {
            sorted.reverse();
            let len = sorted.len() as i64;
            let (lo, hi) = normalize(start, stop, len);
            let slice = if lo > hi { &sorted[0..0] } else { &sorted[lo as usize..=(hi as usize)] };
            reply(Value::Array(render(slice, withscores)))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `ZREVRANGEBYSCORE key max min [WITHSCORES]` — note the argument order is
/// `max` then `min` (reverse of `ZRANGEBYSCORE`), matching Redis.
pub fn cmd_zrevrangebyscore(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, raw_max, raw_min, withscores) = match args {
        [key, max, min] => (key, max, min, false),
        [key, max, min, flag] if flag.eq_ignore_ascii_case(b"WITHSCORES") => (key, max, min, true),
        _ => return wrong_args("zrevrangebyscore"),
    };
    let (min, max) = match (parse_score_bound(raw_min), parse_score_bound(raw_max)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR min or max is not a float"),
    };

    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted()),
        _ => Err(()),
    }) {
        None => reply(Value::Array(vec![])),
        Some(Ok(sorted)) => {
            let mut filtered: Vec<(Bytes, f64)> =
                sorted.into_iter().filter(|(_, s)| *s >= min && *s <= max).collect();
            filtered.reverse();
            reply(Value::Array(render(&filtered, withscores)))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `ZCOUNT key min max` — count of members whose score falls in `[min, max]`.
pub fn cmd_zcount(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, min, max] = args else { return wrong_args("zcount") };
    let (min, max) = match (parse_score_bound(min), parse_score_bound(max)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR min or max is not a float"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted()),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(sorted)) => {
            reply(Value::Integer(sorted.iter().filter(|(_, s)| *s >= min && *s <= max).count() as i64))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// `ZREMRANGEBYSCORE key min max` — removes every member scored in
/// `[min, max]`.
pub fn cmd_zremrangebyscore(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, min, max] = args else { return wrong_args("zremrangebyscore") };
    let (min, max) = match (parse_score_bound(min), parse_score_bound(max)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR min or max is not a float"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let removed = db.with_value_mut(key, |v| match v {
        KVal::ZSet(z) => {
            let to_remove: Vec<Bytes> =
                z.sorted().into_iter().filter(|(_, s)| *s >= min && *s <= max).map(|(m, _)| m).collect();
            for m in &to_remove {
                z.remove(m);
            }
            Ok(to_remove.len())
        }
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
        notify_keyspace_event(ctx, state.db_index, 'z', "zremrangebyscore", key);
    }
    reply(Value::Integer(removed as i64))
}

/// `ZREMRANGEBYRANK key start stop` — removes every member whose
/// ascending-order position falls in `[start, stop]` (negative indices
/// count from the end, same as `ZRANGE`).
pub fn cmd_zremrangebyrank(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, start, stop] = args else { return wrong_args("zremrangebyrank") };
    let (start, stop): (i64, i64) = match (parse_i64(start), parse_i64(stop)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR value is not an integer or out of range"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let removed = db.with_value_mut(key, |v| match v {
        KVal::ZSet(z) => {
            let sorted = z.sorted();
            let len = sorted.len() as i64;
            let (lo, hi) = normalize(start, stop, len);
            if lo > hi {
                return Ok(0);
            }
            let to_remove: Vec<Bytes> =
                sorted[lo as usize..=(hi as usize)].iter().map(|(m, _)| m.clone()).collect();
            for m in &to_remove {
                z.remove(m);
            }
            Ok(to_remove.len())
        }
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
        notify_keyspace_event(ctx, state.db_index, 'z', "zremrangebyrank", key);
    }
    reply(Value::Integer(removed as i64))
}

pub fn cmd_zpopmin(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    zpop(ctx, state, args, "zpopmin", true)
}

pub fn cmd_zpopmax(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    zpop(ctx, state, args, "zpopmax", false)
}

/// Shared `ZPOPMIN`/`ZPOPMAX` implementation. `min` picks the
/// lowest-scoring members; `!min` the highest. Uses `ZSet::pop_extreme`
/// (an `O(n)`-average partial selection), not `sorted()` — see its doc
/// comment for why a full sort here would be a real performance bug, not
/// just a style choice (it was, until `pop_extreme` replaced it).
fn zpop(ctx: &ServerContext, state: &ClientState, args: &[Bytes], name: &str, min: bool) -> Outcome {
    let (key, count) = match args {
        [key] => (key, 1usize),
        [key, count] => {
            let n: usize = match std::str::from_utf8(count).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return err("ERR value is not an integer or out of range"),
            };
            (key, n)
        }
        _ => return wrong_args(name),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let popped = db.with_value_mut(key, |v| match v {
        KVal::ZSet(z) => Ok(z.pop_extreme(count, min)),
        _ => Err(()),
    });
    match popped {
        None => reply(Value::Array(vec![])),
        Some(Ok(items)) => {
            db.remove_if_empty(key);
            if !items.is_empty() {
                on_key_changed(ctx, state.db_index, key.clone());
                notify_keyspace_event(ctx, state.db_index, 'z', name, key);
            }
            reply(Value::Array(render(&items, true)))
        }
        Some(Err(())) => wrong_type(),
    }
}

/// Non-replicated helper for `BZPOPMIN`/`BZPOPMAX`: pop exactly one member
/// from `key` without going through the full command-dispatch (and its
/// reply-shaping) path. Returns `None` if the key is missing/empty,
/// `Some(Err(()))` on a type mismatch.
pub(crate) fn pop_one(
    ctx: &ServerContext,
    state: &ClientState,
    key: &Bytes,
    min: bool,
) -> Option<Result<(Bytes, f64), ()>> {
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let popped = db.with_value_mut(key, |v| match v {
        // `pop_extreme`, not `sorted()` — same reasoning as `zpop`'s docs.
        KVal::ZSet(z) => Ok(z.pop_extreme(1, min).into_iter().next()),
        _ => Err(()),
    });
    match popped {
        None => None,
        Some(Err(())) => Some(Err(())),
        Some(Ok(None)) => None,
        Some(Ok(Some(item))) => {
            db.remove_if_empty(key);
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'z', if min { "zpopmin" } else { "zpopmax" }, key);
            Some(Ok(item))
        }
    }
}

pub fn cmd_bzpopmin(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    bzpop(ctx, state, args, "bzpopmin", true)
}

pub fn cmd_bzpopmax(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    bzpop(ctx, state, args, "bzpopmax", false)
}

/// `BZPOPMIN`/`BZPOPMAX key [key ...] timeout` — same shape, and the same
/// "replicate the resolved effect (`ZPOPMIN`/`ZPOPMAX`), not the raw
/// blocking command" reasoning, as `list::bpop`. Deliberately not in
/// `WRITE_COMMANDS` for the same reason that one isn't.
fn bzpop(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes], name: &str, min: bool) -> Outcome {
    let [keys @ .., timeout] = args else { return wrong_args(name) };
    if keys.is_empty() {
        return wrong_args(name);
    }
    let deadline = match blocking_deadline(state, timeout) {
        Ok(d) => d,
        Err(e) => return e,
    };
    if let Some(e) = check_write_allowed(ctx, state) {
        clear_blocking_deadline(state);
        return e;
    }
    for key in keys {
        match pop_one(ctx, state, key, min) {
            None => continue,
            Some(Err(())) => {
                clear_blocking_deadline(state);
                return wrong_type();
            }
            Some(Ok((member, score))) => {
                let effect_name: &[u8] = if min { b"ZPOPMIN" } else { b"ZPOPMAX" };
                let effect = vec![Bytes::from_static(effect_name), key.clone()];
                ctx.append_aof(state.db_index, &effect);
                ctx.replicate(state.db_index, &effect);
                clear_blocking_deadline(state);
                return reply(Value::Array(vec![
                    Value::bulk(key.clone()),
                    Value::bulk(member),
                    Value::bulk(format_score(score)),
                ]));
            }
        }
    }
    Outcome::BlockWait { deadline, on_timeout: Value::NullArray }
}

pub fn cmd_zscan(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("zscan");
    }
    let key = &args[0];
    let opts = match scan::parse_opts(&args[2..]) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let sorted = match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted()),
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(s)) => s,
        Some(Err(())) => return wrong_type(),
    };
    let mut flat = Vec::new();
    for (member, score) in sorted {
        if scan::matches_pattern(&opts, &member) {
            flat.push(Value::bulk(member));
            flat.push(Value::bulk(format_score(score)));
        }
    }
    reply(ScanReply::done(flat).into_value())
}

fn render(items: &[(Bytes, f64)], withscores: bool) -> Vec<Value> {
    if withscores {
        items.iter().flat_map(|(m, s)| [Value::bulk(m.clone()), Value::bulk(format_score(*s))]).collect()
    } else {
        items.iter().map(|(m, _)| Value::bulk(m.clone())).collect()
    }
}

pub(crate) fn format_score(score: f64) -> String {
    if score == score.trunc() && score.is_finite() {
        format!("{}", score as i64)
    } else {
        format!("{score}")
    }
}

fn parse_score_bound(b: &Bytes) -> Option<f64> {
    match b.as_ref() {
        b"-inf" => Some(f64::NEG_INFINITY),
        b"+inf" | b"inf" => Some(f64::INFINITY),
        _ => std::str::from_utf8(b).ok()?.parse().ok(),
    }
}
