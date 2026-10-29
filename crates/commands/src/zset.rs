use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use keyspace::{Value as KVal, ZSet};
use rand::seq::SliceRandom;
use rand::Rng;
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

pub fn cmd_zmscore(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("zmscore");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(&args[0], |v| match v {
        KVal::ZSet(zset) => Ok(args[1..]
            .iter()
            .map(|member| zset.score(member).map(|score| Value::bulk(format_score(score))))
            .collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => reply(Value::Array(args[1..].iter().map(|_| Value::NullBulk).collect())),
        Some(Ok(scores)) => {
            reply(Value::Array(scores.into_iter().map(|score| score.unwrap_or(Value::NullBulk)).collect()))
        }
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_algebra(
    ctx: &ServerContext,
    state: &ClientState,
    args: &[Bytes],
    op: ZAlgebra,
    store: bool,
    cardinality: bool,
) -> Outcome {
    let name = match op {
        ZAlgebra::Union => {
            if store {
                "zunionstore"
            } else {
                "zunion"
            }
        }
        ZAlgebra::Inter => {
            if store {
                "zinterstore"
            } else {
                "zinter"
            }
        }
        ZAlgebra::Diff => {
            if store {
                "zdiffstore"
            } else {
                "zdiff"
            }
        }
    };
    let base = if store { 2 } else { 1 };
    if args.len() < base + 1 {
        return wrong_args(name);
    }
    let numkeys = match parse_i64(&args[base - 1]) {
        Some(n) if n > 0 => n as usize,
        _ => return err("ERR numkeys should be greater than 0"),
    };
    if args.len() < base + numkeys {
        return err("ERR syntax error");
    }
    let destination = if store { Some(args[0].clone()) } else { None };
    let keys = &args[base..base + numkeys];
    let mut weights = vec![1.0f64; numkeys];
    let mut aggregate = b"SUM".to_vec();
    let mut withscores = false;
    let mut limit = usize::MAX;
    let mut i = base + numkeys;
    while i < args.len() {
        if !store && !cardinality && args[i].eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else if !cardinality && args[i].eq_ignore_ascii_case(b"WEIGHTS") && i + numkeys < args.len() {
            for (j, arg) in args[i + 1..i + 1 + numkeys].iter().enumerate() {
                let Some(weight) = std::str::from_utf8(arg)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|v| v.is_finite())
                else {
                    return err("ERR weight value is not a float");
                };
                weights[j] = weight;
            }
            i += numkeys + 1;
        } else if !cardinality && args[i].eq_ignore_ascii_case(b"AGGREGATE") && i + 1 < args.len() {
            aggregate = args[i + 1].to_ascii_uppercase();
            if !matches!(aggregate.as_slice(), b"SUM" | b"MIN" | b"MAX") {
                return err("ERR syntax error");
            }
            i += 2;
        } else if cardinality && args[i].eq_ignore_ascii_case(b"LIMIT") && i + 1 < args.len() {
            // `LIMIT 0` means "no limit" (matches ZINTERCARD's docs), not
            // "truncate the result to zero members".
            limit = match parse_i64(&args[i + 1]) {
                Some(0) => usize::MAX,
                Some(n) if n > 0 => n as usize,
                _ => return err("ERR value is out of range"),
            };
            i += 2;
        } else {
            return err("ERR syntax error");
        }
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let mut inputs = Vec::with_capacity(numkeys);
    for key in keys {
        match db.with_value(key, |v| match v {
            KVal::ZSet(z) => Ok(z.sorted().into_iter().collect::<HashMap<_, _>>()),
            _ => Err(()),
        }) {
            None => inputs.push(HashMap::new()),
            Some(Ok(z)) => inputs.push(z),
            Some(Err(())) => return wrong_type(),
        }
    }
    let mut scores = HashMap::<Bytes, f64>::new();
    match op {
        ZAlgebra::Union => {
            for (index, input) in inputs.iter().enumerate() {
                for (member, score) in input {
                    let weighted = score * weights[index];
                    scores
                        .entry(member.clone())
                        .and_modify(|old| *old = aggregate_score(*old, weighted, &aggregate))
                        .or_insert(weighted);
                }
            }
        }
        ZAlgebra::Inter => {
            if let Some(first) = inputs.first() {
                'member: for (member, score) in first {
                    let mut total = score * weights[0];
                    for (index, input) in inputs.iter().enumerate().skip(1) {
                        let Some(other) = input.get(member) else {
                            continue 'member;
                        };
                        total = aggregate_score(total, other * weights[index], &aggregate);
                    }
                    scores.insert(member.clone(), total);
                }
            }
        }
        ZAlgebra::Diff => {
            if let Some(first) = inputs.first() {
                let excluded: HashSet<&Bytes> = inputs.iter().skip(1).flat_map(HashMap::keys).collect();
                scores.extend(
                    first
                        .iter()
                        .filter(|(member, _)| !excluded.contains(member))
                        .map(|(m, s)| (m.clone(), *s)),
                );
            }
        }
    }
    let mut result: Vec<(Bytes, f64)> = scores.into_iter().collect();
    result.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    if cardinality {
        result.truncate(limit);
    }
    if let Some(destination) = destination {
        if result.is_empty() {
            db.del(std::slice::from_ref(&destination));
        } else {
            let mut z = ZSet::new();
            for (m, s) in &result {
                z.insert(m.clone(), *s);
            }
            db.set(destination.clone(), KVal::ZSet(z), None);
        }
        on_key_changed(ctx, state.db_index, destination.clone());
        notify_keyspace_event(ctx, state.db_index, 'z', "zstore", &destination);
        reply(Value::Integer(result.len() as i64))
    } else {
        let mut out = Vec::with_capacity(result.len() * if withscores { 2 } else { 1 });
        for (member, score) in result {
            out.push(Value::bulk(member));
            if withscores {
                out.push(Value::bulk(format_score(score)));
            }
        }
        reply(Value::Array(out))
    }
}

pub fn cmd_lex(
    ctx: &ServerContext,
    state: &ClientState,
    args: &[Bytes],
    reverse: bool,
    remove: bool,
) -> Outcome {
    let name = if remove {
        "zremrangebylex"
    } else if reverse {
        "zrevrangebylex"
    } else {
        "zrangebylex"
    };
    if args.len() < 3 {
        return wrong_args(name);
    }
    let key = &args[0];
    let (min, max) = if reverse { (&args[2], &args[1]) } else { (&args[1], &args[2]) };
    if !valid_lex_bound(min, true) || !valid_lex_bound(max, false) {
        return err("ERR min or max is not a valid string range item");
    }
    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut i = 3;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"LIMIT") && !remove && i + 2 < args.len() {
            let Some(o) = std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse::<i64>().ok()) else {
                return err("ERR value is not an integer or out of range");
            };
            let Some(c) = std::str::from_utf8(&args[i + 2]).ok().and_then(|s| s.parse::<i64>().ok()) else {
                return err("ERR value is not an integer or out of range");
            };
            if o < 0 || c < 0 {
                return err("ERR LIMIT can't be negative");
            }
            offset = o as usize;
            count = c as usize;
            i += 3;
        } else {
            return err("ERR syntax error");
        }
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let mut members = match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted().into_iter().map(|(m, _)| m).collect::<Vec<_>>()),
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(m)) => m,
        Some(Err(())) => return wrong_type(),
    };
    members.sort();
    let mut matches: Vec<Bytes> = members.into_iter().filter(|m| lex_in_range(m, min, max)).collect();
    if reverse {
        matches.reverse();
    }
    let selected: Vec<Bytes> = matches.into_iter().skip(offset).take(count).collect();
    if remove {
        if selected.is_empty() {
            return reply(Value::Integer(0));
        }
        let removed = db
            .with_value_mut(key, |v| match v {
                KVal::ZSet(z) => selected.iter().filter(|m| z.remove(m)).count(),
                _ => 0,
            })
            .unwrap_or(0);
        db.remove_if_empty(key);
        if removed > 0 {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'z', name, key);
        }
        reply(Value::Integer(removed as i64))
    } else {
        reply(Value::Array(selected.into_iter().map(Value::bulk).collect()))
    }
}

fn lex_in_range(member: &[u8], min: &[u8], max: &[u8]) -> bool {
    let lower = match min {
        b"-" => true,
        [b'[', ..] => member >= &min[1..],
        [b'(', ..] => member > &min[1..],
        _ => false,
    };
    let upper = match max {
        b"+" => true,
        [b'[', ..] => member <= &max[1..],
        [b'(', ..] => member < &max[1..],
        _ => false,
    };
    lower && upper
}

fn valid_lex_bound(bound: &[u8], lower: bool) -> bool {
    match bound {
        b"-" if lower => true,
        b"+" if !lower => true,
        [b'[', ..] | [b'(', ..] => true,
        _ => false,
    }
}

pub fn cmd_lexcount(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, min, max] = args else {
        return wrong_args("zlexcount");
    };
    if !valid_lex_bound(min, true) || !valid_lex_bound(max, false) {
        return err("ERR min or max is not a valid string range item");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted().into_iter().filter(|(m, _)| lex_in_range(m, min, max)).count()),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(n)) => reply(Value::Integer(n as i64)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_zrangestore(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 4 {
        return wrong_args("zrangestore");
    }
    let (dest, source, start, stop) = (&args[0], &args[1], &args[2], &args[3]);
    let mut mode = "rank";
    let mut reverse = false;
    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut i = 4;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"BYSCORE") {
            mode = "score";
            i += 1;
        } else if args[i].eq_ignore_ascii_case(b"BYLEX") {
            mode = "lex";
            i += 1;
        } else if args[i].eq_ignore_ascii_case(b"REV") {
            reverse = true;
            i += 1;
        } else if args[i].eq_ignore_ascii_case(b"LIMIT") && i + 2 < args.len() {
            let Some(o) = std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse::<i64>().ok()) else {
                return err("ERR value is not an integer or out of range");
            };
            let Some(c) = std::str::from_utf8(&args[i + 2]).ok().and_then(|s| s.parse::<i64>().ok()) else {
                return err("ERR value is not an integer or out of range");
            };
            if o < 0 || c < 0 {
                return err("ERR LIMIT can't be negative");
            }
            offset = o as usize;
            count = c as usize;
            i += 3;
        } else {
            return err("ERR syntax error");
        }
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let source_values = match db.with_value(source, |v| match v {
        KVal::ZSet(z) => Ok(z.sorted()),
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(v)) => v,
        Some(Err(())) => return wrong_type(),
    };
    let mut selected: Vec<(Bytes, f64)> = match mode {
        "rank" => {
            let Some(start) = parse_i64(start) else {
                return err("ERR value is not an integer or out of range");
            };
            let Some(stop) = parse_i64(stop) else {
                return err("ERR value is not an integer or out of range");
            };
            let len = source_values.len() as i64;
            let (lo, hi) = normalize(start, stop, len);
            if lo > hi {
                Vec::new()
            } else {
                source_values[lo as usize..=hi as usize].to_vec()
            }
        }
        "score" => {
            let (lower, upper) = if reverse {
                (parse_score(stop), parse_score(start))
            } else {
                (parse_score(start), parse_score(stop))
            };
            let (Some((lo, li)), Some((hi, ui))) = (lower, upper) else {
                return err("ERR min or max is not a float");
            };
            source_values
                .into_iter()
                .filter(|(_, s)| {
                    (if li { *s >= lo } else { *s > lo }) && (if ui { *s <= hi } else { *s < hi })
                })
                .collect()
        }
        _ => {
            let (min, max) =
                if reverse { (stop.as_ref(), start.as_ref()) } else { (start.as_ref(), stop.as_ref()) };
            if !valid_lex_bound(min, true) || !valid_lex_bound(max, false) {
                return err("ERR min or max is not a valid string range item");
            }
            source_values.into_iter().filter(|(m, _)| lex_in_range(m, min, max)).collect()
        }
    };
    if mode == "lex" {
        selected.sort_by(|a, b| a.0.cmp(&b.0));
    } else {
        selected.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    }
    if reverse {
        selected.reverse();
    }
    let selected: Vec<_> = selected.into_iter().skip(offset).take(count).collect();
    if selected.is_empty() {
        db.del(std::slice::from_ref(dest));
    } else {
        let mut z = ZSet::new();
        for (m, s) in &selected {
            z.insert(m.clone(), *s);
        }
        db.set(dest.clone(), KVal::ZSet(z), None);
    }
    on_key_changed(ctx, state.db_index, dest.clone());
    notify_keyspace_event(ctx, state.db_index, 'z', "rangestore", dest);
    reply(Value::Integer(selected.len() as i64))
}

fn parse_score(value: &[u8]) -> Option<(f64, bool)> {
    let (s, inclusive) = match value.first() {
        Some(b'(') => (&value[1..], false),
        _ => (value, true),
    };
    let n = match s {
        b"-inf" => f64::NEG_INFINITY,
        b"+inf" | b"inf" => f64::INFINITY,
        _ => std::str::from_utf8(s).ok()?.parse::<f64>().ok()?,
    };
    Some((n, inclusive))
}

pub fn cmd_zintercard(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    match cmd_algebra(ctx, state, args, ZAlgebra::Inter, false, true) {
        Outcome::Reply(Value::Array(items)) => reply(Value::Integer(items.len() as i64)),
        other => other,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZAlgebra {
    Union,
    Inter,
    Diff,
}

fn aggregate_score(a: f64, b: f64, aggregate: &[u8]) -> f64 {
    match aggregate {
        b"MIN" => a.min(b),
        b"MAX" => a.max(b),
        _ => a + b,
    }
}

pub fn cmd_zrandmember(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() || args.len() > 3 {
        return wrong_args("zrandmember");
    }
    let count = if args.len() >= 2 {
        match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => Some(n),
            None => return err("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };
    let with_scores = match args.get(2) {
        Some(option) if count.is_some() && option.eq_ignore_ascii_case(b"WITHSCORES") => true,
        Some(_) => return err("ERR syntax error"),
        None => false,
    };
    if count.is_some_and(|n| n.unsigned_abs() > 1_000_000) {
        return err("ERR count is out of range");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let members = match db.with_value(&args[0], |v| match v {
        KVal::ZSet(zset) => Ok(zset.sorted()),
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(members)) => members,
        Some(Err(())) => return wrong_type(),
    };
    if let Some(count) = count {
        let requested = count.unsigned_abs() as usize;
        let selected = if count < 0 {
            let mut rng = rand::thread_rng();
            (0..requested)
                .filter_map(|_| members.get(rng.gen_range(0..members.len().max(1))).cloned())
                .collect::<Vec<_>>()
        } else {
            let mut sample = members;
            sample.shuffle(&mut rand::thread_rng());
            sample.truncate(requested);
            sample
        };
        let mut result = Vec::with_capacity(selected.len() * if with_scores { 2 } else { 1 });
        for (member, score) in selected {
            result.push(Value::bulk(member));
            if with_scores {
                result.push(Value::bulk(format_score(score)));
            }
        }
        reply(Value::Array(result))
    } else {
        let mut rng = rand::thread_rng();
        reply(members.choose(&mut rng).map_or(Value::NullBulk, |(member, _)| Value::bulk(member.clone())))
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

pub fn cmd_zmpop(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes], blocking: bool) -> Outcome {
    let name = if blocking { "bzmpop" } else { "zmpop" };
    let offset = usize::from(blocking);
    if args.len() < offset + 3 {
        return wrong_args(name);
    }
    let timeout = if blocking { Some(&args[0]) } else { None };
    let numkeys = match std::str::from_utf8(&args[offset]).ok().and_then(|s| s.parse::<usize>().ok()) {
        Some(n) if n > 0 && args.len() >= offset + n + 2 => n,
        _ => return err("ERR numkeys should be greater than 0"),
    };
    let keys = &args[offset + 1..offset + 1 + numkeys];
    let mode_index = offset + numkeys + 1;
    let min = if args[mode_index].eq_ignore_ascii_case(b"MIN") {
        true
    } else if args[mode_index].eq_ignore_ascii_case(b"MAX") {
        false
    } else {
        return err("ERR syntax error");
    };
    let mut count = 1usize;
    if mode_index + 1 < args.len() {
        if mode_index + 3 != args.len() || !args[mode_index + 1].eq_ignore_ascii_case(b"COUNT") {
            return err("ERR syntax error");
        }
        count = match std::str::from_utf8(&args[mode_index + 2]).ok().and_then(|s| s.parse::<usize>().ok()) {
            Some(n) if n > 0 => n,
            _ => return err("ERR count should be greater than 0"),
        };
    }
    let deadline = if let Some(timeout) = timeout {
        match blocking_deadline(state, timeout) {
            Ok(d) => Some(d),
            Err(e) => return e,
        }
    } else {
        None
    };
    // Not in `WRITE_COMMANDS` (see `check_write_allowed`'s module docs): the
    // raw ZMPOP/BZMPOP request is never logged/replicated, only the
    // resolved ZPOPMIN/ZPOPMAX effect below, so the replica-read-only/
    // `maxmemory` guard has to be applied by hand here instead.
    if let Some(e) = check_write_allowed(ctx, state) {
        if blocking {
            clear_blocking_deadline(state);
        }
        return e;
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    for key in keys {
        let popped = db.with_value_mut(key, |v| match v {
            KVal::ZSet(z) => Ok(z.pop_extreme(count, min)),
            _ => Err(()),
        });
        match popped {
            None => continue,
            Some(Ok(values)) if values.is_empty() => continue,
            Some(Err(())) => {
                if blocking {
                    clear_blocking_deadline(state);
                }
                return wrong_type();
            }
            Some(Ok(values)) => {
                db.remove_if_empty(key);
                on_key_changed(ctx, state.db_index, key.clone());
                let command = if min { "zpopmin" } else { "zpopmax" };
                notify_keyspace_event(ctx, state.db_index, 'z', command, key);
                let mut effect =
                    vec![Bytes::from_static(if min { b"ZPOPMIN" } else { b"ZPOPMAX" }), key.clone()];
                if values.len() > 1 {
                    effect.push(Bytes::from(values.len().to_string()));
                }
                ctx.append_aof(state.db_index, &effect);
                ctx.replicate(state.db_index, &effect);
                let items = values
                    .into_iter()
                    .flat_map(|(member, score)| [Value::bulk(member), Value::bulk(format_score(score))])
                    .collect();
                if blocking {
                    clear_blocking_deadline(state);
                }
                return reply(Value::Array(vec![Value::bulk(key.clone()), Value::Array(items)]));
            }
        }
    }
    if blocking {
        Outcome::BlockWait { deadline: deadline.flatten(), on_timeout: Value::NullArray }
    } else {
        reply(Value::NullArray)
    }
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
