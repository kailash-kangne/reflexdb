use bytes::Bytes;
use keyspace::{List, Value as KVal};
use resp::Value;

use crate::dispatch::{
    blocking_deadline, check_write_allowed, clear_blocking_deadline, notify_keyspace_event, on_key_changed,
};
use crate::dispatch::{err, reply, wrong_args, wrong_type, ClientState, Outcome, ServerContext};
use crate::range::{normalize, parse_i64};

pub fn cmd_lpush(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    push(ctx, state, args, "lpush", true)
}

pub fn cmd_rpush(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    push(ctx, state, args, "rpush", false)
}

fn push(ctx: &ServerContext, state: &ClientState, args: &[Bytes], name: &str, front: bool) -> Outcome {
    if args.len() < 2 {
        return wrong_args(name);
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let key = args[0].clone();
    let values = &args[1..];

    let result = db.with_entry_mut(
        key.clone(),
        || KVal::List(List::new()),
        |v| match v {
            KVal::List(list) => {
                for value in values {
                    if front {
                        list.push_front(value.clone());
                    } else {
                        list.push_back(value.clone());
                    }
                }
                Ok(list.len())
            }
            _ => Err(()),
        },
    );

    match result {
        Ok(len) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'l', name, &key);
            reply(Value::Integer(len as i64))
        }
        Err(()) => wrong_type(),
    }
}

/// `LPUSHX`/`RPUSHX key value [value ...]` — like `LPUSH`/`RPUSH`, but a
/// no-op (replying `0`) if the key doesn't already exist, instead of
/// creating it.
pub fn cmd_lpushx(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    pushx(ctx, state, args, "lpushx", true)
}

pub fn cmd_rpushx(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    pushx(ctx, state, args, "rpushx", false)
}

fn pushx(ctx: &ServerContext, state: &ClientState, args: &[Bytes], name: &str, front: bool) -> Outcome {
    if args.len() < 2 {
        return wrong_args(name);
    }
    let key = &args[0];
    let values = &args[1..];
    let db = ctx.store.db(state.db_index).expect("valid db index");

    let result = db.with_value_mut(key, |v| match v {
        KVal::List(list) => {
            for value in values {
                if front {
                    list.push_front(value.clone());
                } else {
                    list.push_back(value.clone());
                }
            }
            Ok(list.len())
        }
        _ => Err(()),
    });

    match result {
        None => reply(Value::Integer(0)),
        Some(Ok(len)) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'l', name, key);
            reply(Value::Integer(len as i64))
        }
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_lpop(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    pop(ctx, state, args, "lpop", true)
}

pub fn cmd_rpop(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    pop(ctx, state, args, "rpop", false)
}

fn pop(ctx: &ServerContext, state: &ClientState, args: &[Bytes], name: &str, front: bool) -> Outcome {
    let (key, count) = match args {
        [key] => (key, None),
        [key, count] => {
            let n: i64 = match std::str::from_utf8(count).ok().and_then(|s| s.parse().ok()) {
                Some(n) if n >= 0 => n,
                _ => return err("ERR value is out of range, must be positive"),
            };
            (key, Some(n as usize))
        }
        _ => return wrong_args(name),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");

    let popped = db.with_value_mut(key, |v| match v {
        KVal::List(list) => {
            let n = count.unwrap_or(1).min(list.len());
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let item = if front { list.pop_front() } else { list.pop_back() };
                match item {
                    Some(i) => out.push(i),
                    None => break,
                }
            }
            Ok(out)
        }
        _ => Err(()),
    });

    let popped = match popped {
        None => None,
        Some(Ok(v)) => Some(v),
        Some(Err(())) => return wrong_type(),
    };
    db.remove_if_empty(key);
    if popped.as_ref().is_some_and(|v| !v.is_empty()) {
        on_key_changed(ctx, state.db_index, key.clone());
        notify_keyspace_event(ctx, state.db_index, 'l', name, key);
    }

    match (popped, count) {
        (None, _) => {
            if count.is_some() {
                reply(Value::NullArray)
            } else {
                reply(Value::NullBulk)
            }
        }
        (Some(items), None) => match items.into_iter().next() {
            Some(item) => reply(Value::bulk(item)),
            None => reply(Value::NullBulk),
        },
        (Some(items), Some(_)) => reply(Value::Array(items.into_iter().map(Value::bulk).collect())),
    }
}

pub fn cmd_llen(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("llen") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::List(list) => Ok(list.len()),
        _ => Err(()),
    }) {
        None => reply(Value::Integer(0)),
        Some(Ok(len)) => reply(Value::Integer(len as i64)),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_lpos(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("lpos");
    }
    let key = &args[0];
    let needle = &args[1];
    let mut rank = 1i64;
    let mut count = None;
    let mut maxlen = None;
    let mut i = 2;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"RANK") && i + 1 < args.len() {
            rank = match parse_i64(&args[i + 1]) {
                Some(n) if n != 0 => n,
                _ => return err(
                    "ERR RANK can't be zero: use 1 to start from the beginning or -1 to start from the end",
                ),
            };
            i += 2;
        } else if args[i].eq_ignore_ascii_case(b"COUNT") && i + 1 < args.len() {
            let n = match parse_i64(&args[i + 1]) {
                Some(n) if n >= 0 => n as usize,
                _ => return err("ERR COUNT can't be negative"),
            };
            count = Some(n);
            i += 2;
        } else if args[i].eq_ignore_ascii_case(b"MAXLEN") && i + 1 < args.len() {
            maxlen = match parse_i64(&args[i + 1]) {
                Some(n) if n >= 0 => Some(n as usize),
                _ => return err("ERR MAXLEN can't be negative"),
            };
            i += 2;
        } else {
            return err("ERR syntax error");
        }
    }

    let db = ctx.store.db(state.db_index).expect("valid db index");
    let indices = match db.with_value(key, |v| match v {
        KVal::List(list) => {
            let len = list.len();
            let reverse = rank < 0;
            let scan_limit = maxlen.unwrap_or(len).min(len);
            let wanted_rank = usize::try_from(rank.unsigned_abs()).unwrap_or(usize::MAX);
            let mut result = Vec::new();
            let mut matches_seen = 0usize;
            for offset in 0..scan_limit {
                let index = if reverse { len - 1 - offset } else { offset };
                if list[index] == *needle {
                    matches_seen += 1;
                    if matches_seen >= wanted_rank {
                        result.push(index);
                    }
                    if count.is_none() && matches_seen >= wanted_rank {
                        break;
                    }
                    if count.is_some_and(|n| n > 0 && result.len() >= n) {
                        break;
                    }
                }
            }
            Ok(result)
        }
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(indices)) => indices,
        Some(Err(())) => return wrong_type(),
    };

    if count.is_some() {
        reply(Value::Array(indices.into_iter().map(|n| Value::Integer(n as i64)).collect()))
    } else {
        reply(indices.first().map_or(Value::NullBulk, |n| Value::Integer(*n as i64)))
    }
}

pub fn cmd_lrange(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, start, stop] = args else { return wrong_args("lrange") };
    let (start, stop) = match (parse_i64(start), parse_i64(stop)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR value is not an integer or out of range"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    match db.with_value(key, |v| match v {
        KVal::List(list) => {
            let len = list.len() as i64;
            let (lo, hi) = normalize(start, stop, len);
            Ok(if lo > hi {
                Vec::new()
            } else {
                list.iter().skip(lo as usize).take((hi - lo + 1) as usize).cloned().collect::<Vec<_>>()
            })
        }
        _ => Err(()),
    }) {
        None => reply(Value::Array(vec![])),
        Some(Ok(items)) => reply(Value::Array(items.into_iter().map(Value::bulk).collect())),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_ltrim(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, start, stop] = args else { return wrong_args("ltrim") };
    let (start, stop) = match (parse_i64(start), parse_i64(stop)) {
        (Some(a), Some(b)) => (a, b),
        _ => return err("ERR value is not an integer or out of range"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_value_mut(key, |v| match v {
        KVal::List(list) => {
            let len = list.len() as i64;
            let (lo, hi) = normalize(start, stop, len);
            if lo > hi {
                list.clear();
            } else {
                // Drop the tail first so the earlier indices we want to keep
                // don't shift, then drop the head.
                list.truncate(hi as usize + 1);
                for _ in 0..lo {
                    list.pop_front();
                }
            }
            Ok(())
        }
        _ => Err(()),
    });
    match result {
        None => reply(Value::ok()),
        Some(Ok(())) => {
            db.remove_if_empty(key);
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'l', "ltrim", key);
            reply(Value::ok())
        }
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_lset(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, index, value] = args else { return wrong_args("lset") };
    let Some(index) = parse_i64(index) else {
        return err("ERR value is not an integer or out of range");
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_value_mut(key, |v| match v {
        KVal::List(list) => {
            let len = list.len() as i64;
            let idx = if index < 0 { index + len } else { index };
            if idx < 0 || idx >= len {
                return Ok(false);
            }
            list[idx as usize] = value.clone();
            Ok(true)
        }
        _ => Err(()),
    });
    match result {
        None => err("ERR no such key"),
        Some(Ok(true)) => {
            on_key_changed(ctx, state.db_index, key.clone());
            notify_keyspace_event(ctx, state.db_index, 'l', "lset", key);
            reply(Value::ok())
        }
        Some(Ok(false)) => err("ERR index out of range"),
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_linsert(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, where_, pivot, value] = args else { return wrong_args("linsert") };
    let before = match String::from_utf8_lossy(where_).to_ascii_uppercase().as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return err("ERR syntax error"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_value_mut(key, |v| match v {
        KVal::List(list) => {
            let Some(pos) = list.iter().position(|item| item.as_ref() == pivot.as_ref()) else {
                return Ok(-1);
            };
            let idx = if before { pos } else { pos + 1 };
            list.insert(idx, value.clone());
            Ok(list.len() as i64)
        }
        _ => Err(()),
    });
    match result {
        None => reply(Value::Integer(0)),
        Some(Ok(n)) => {
            if n > 0 {
                on_key_changed(ctx, state.db_index, key.clone());
                notify_keyspace_event(ctx, state.db_index, 'l', "linsert", key);
            }
            reply(Value::Integer(n))
        }
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_lrem(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, count, value] = args else { return wrong_args("lrem") };
    let Some(count) = parse_i64(count) else {
        return err("ERR value is not an integer or out of range");
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let result = db.with_value_mut(key, |v| match v {
        KVal::List(list) => {
            let mut removed = 0usize;
            if count >= 0 {
                let limit = if count == 0 { usize::MAX } else { count as usize };
                let mut i = 0;
                while i < list.len() && removed < limit {
                    if list[i].as_ref() == value.as_ref() {
                        list.remove(i);
                        removed += 1;
                    } else {
                        i += 1;
                    }
                }
            } else {
                let limit = (-count) as usize;
                let mut i = list.len();
                while i > 0 && removed < limit {
                    i -= 1;
                    if list[i].as_ref() == value.as_ref() {
                        list.remove(i);
                        removed += 1;
                    }
                }
            }
            Ok(removed)
        }
        _ => Err(()),
    });
    match result {
        None => reply(Value::Integer(0)),
        Some(Ok(removed)) => {
            db.remove_if_empty(key);
            if removed > 0 {
                on_key_changed(ctx, state.db_index, key.clone());
                notify_keyspace_event(ctx, state.db_index, 'l', "lrem", key);
            }
            reply(Value::Integer(removed as i64))
        }
        Some(Err(())) => wrong_type(),
    }
}

pub fn cmd_rpoplpush(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [source, destination] = args else { return wrong_args("rpoplpush") };
    lmove(ctx, state, source, destination, false, true)
}

pub fn cmd_lmove(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [source, destination, from, to] = args else { return wrong_args("lmove") };
    let from_left = match String::from_utf8_lossy(from).to_ascii_uppercase().as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return err("ERR syntax error"),
    };
    let to_left = match String::from_utf8_lossy(to).to_ascii_uppercase().as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return err("ERR syntax error"),
    };
    lmove(ctx, state, source, destination, from_left, to_left)
}

/// Shared implementation for `RPOPLPUSH`/`LMOVE`: pop one element from
/// `source` (from the left or right end) and push it onto `destination`
/// (onto the left or right end), atomically w.r.t. other list ops. When
/// `source == destination` this is done under a single lock as a rotate,
/// so the element never "disappears" between the pop and the push.
fn lmove(
    ctx: &ServerContext,
    state: &ClientState,
    source: &Bytes,
    destination: &Bytes,
    from_left: bool,
    to_left: bool,
) -> Outcome {
    let db = ctx.store.db(state.db_index).expect("valid db index");

    if source == destination {
        let result = db.with_value_mut(source, |v| match v {
            KVal::List(list) => {
                let item = if from_left { list.pop_front() } else { list.pop_back() };
                if let Some(item) = &item {
                    if to_left {
                        list.push_front(item.clone());
                    } else {
                        list.push_back(item.clone());
                    }
                }
                Ok(item)
            }
            _ => Err(()),
        });
        return match result {
            None => reply(Value::NullBulk),
            Some(Ok(None)) => reply(Value::NullBulk),
            Some(Ok(Some(item))) => {
                on_key_changed(ctx, state.db_index, source.clone());
                notify_keyspace_event(
                    ctx,
                    state.db_index,
                    'l',
                    if from_left { "lpop" } else { "rpop" },
                    source,
                );
                notify_keyspace_event(
                    ctx,
                    state.db_index,
                    'l',
                    if to_left { "lpush" } else { "rpush" },
                    source,
                );
                reply(Value::bulk(item))
            }
            Some(Err(())) => wrong_type(),
        };
    }

    let popped = db.with_value_mut(source, |v| match v {
        KVal::List(list) => Ok(if from_left { list.pop_front() } else { list.pop_back() }),
        _ => Err(()),
    });
    let item = match popped {
        None => return reply(Value::NullBulk),
        Some(Ok(None)) => return reply(Value::NullBulk),
        Some(Ok(Some(item))) => item,
        Some(Err(())) => return wrong_type(),
    };
    db.remove_if_empty(source);

    let pushed = db.with_entry_mut(
        destination.clone(),
        || KVal::List(List::new()),
        |v| match v {
            KVal::List(list) => {
                if to_left {
                    list.push_front(item.clone());
                } else {
                    list.push_back(item.clone());
                }
                Ok(())
            }
            _ => Err(()),
        },
    );
    if pushed.is_err() {
        // Destination holds a non-list value: put the popped element back
        // where it came from instead of silently dropping it.
        db.with_entry_mut(
            source.clone(),
            || KVal::List(List::new()),
            |v| {
                if let KVal::List(list) = v {
                    if from_left {
                        list.push_front(item.clone());
                    } else {
                        list.push_back(item.clone());
                    }
                }
            },
        );
        return wrong_type();
    }

    on_key_changed(ctx, state.db_index, source.clone());
    on_key_changed(ctx, state.db_index, destination.clone());
    notify_keyspace_event(ctx, state.db_index, 'l', if from_left { "lpop" } else { "rpop" }, source);
    notify_keyspace_event(ctx, state.db_index, 'l', if to_left { "lpush" } else { "rpush" }, destination);
    reply(Value::bulk(item))
}

pub fn cmd_blpop(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    bpop(ctx, state, args, "blpop", true)
}

pub fn cmd_lmpop(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes], blocking: bool) -> Outcome {
    let (timeout, offset) = if blocking {
        if args.len() < 4 {
            return wrong_args("blmpop");
        }
        (Some(&args[0]), 1)
    } else {
        (None, 0)
    };
    if args.len() < offset + 3 {
        return wrong_args(if blocking { "blmpop" } else { "lmpop" });
    }
    let numkeys = match std::str::from_utf8(&args[offset]).ok().and_then(|s| s.parse::<usize>().ok()) {
        Some(n) if n > 0 && args.len() >= offset + n + 2 => n,
        _ => return err("ERR numkeys should be greater than 0"),
    };
    let keys = &args[offset + 1..offset + 1 + numkeys];
    let dir_index = offset + 1 + numkeys;
    let from_left = if args[dir_index].eq_ignore_ascii_case(b"LEFT") {
        true
    } else if args[dir_index].eq_ignore_ascii_case(b"RIGHT") {
        false
    } else {
        return err("ERR syntax error");
    };
    let mut count = 1usize;
    let cursor = dir_index + 1;
    if cursor < args.len() {
        if cursor + 2 != args.len() || !args[cursor].eq_ignore_ascii_case(b"COUNT") {
            return err("ERR syntax error");
        }
        count = match std::str::from_utf8(&args[cursor + 1]).ok().and_then(|s| s.parse::<usize>().ok()) {
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
    // raw LMPOP/BLMPOP request is never logged/replicated, only the
    // resolved LPOP/RPOP effect below, so the replica-read-only/`maxmemory`
    // guard has to be applied by hand here instead.
    if let Some(e) = check_write_allowed(ctx, state) {
        if blocking {
            clear_blocking_deadline(state);
        }
        return e;
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    for key in keys {
        let popped = db.with_value_mut(key, |v| match v {
            KVal::List(list) => {
                let n = count.min(list.len());
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    out.push(
                        if from_left { list.pop_front() } else { list.pop_back() }
                            .expect("count bounded by len"),
                    );
                }
                Ok(out)
            }
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
                let event = if from_left { "lpop" } else { "rpop" };
                notify_keyspace_event(ctx, state.db_index, 'l', event, key);
                let mut effect =
                    vec![Bytes::from_static(if from_left { b"LPOP" } else { b"RPOP" }), key.clone()];
                if values.len() > 1 {
                    effect.push(Bytes::from(values.len().to_string()));
                }
                ctx.append_aof(state.db_index, &effect);
                ctx.replicate(state.db_index, &effect);
                if blocking {
                    clear_blocking_deadline(state);
                }
                return reply(Value::Array(vec![
                    Value::bulk(key.clone()),
                    Value::Array(values.into_iter().map(Value::bulk).collect()),
                ]));
            }
        }
    }
    if blocking {
        Outcome::BlockWait { deadline: deadline.flatten(), on_timeout: Value::NullArray }
    } else {
        reply(Value::NullArray)
    }
}

pub fn cmd_brpop(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    bpop(ctx, state, args, "brpop", false)
}

/// `BLPOP key [key ...] timeout` / `BRPOP key [key ...] timeout`: pop from
/// the first of `keys` that has something, blocking (via
/// `Outcome::BlockWait`, resolved by the connection loop) until one does or
/// `timeout` elapses. Deliberately **not** in `WRITE_COMMANDS`: the raw
/// `BLPOP`/`BRPOP` request is never itself AOF-logged or replicated — only
/// the fully-resolved `LPOP`/`RPOP` *effect* is, once a value actually
/// comes out, so a replica (which just `dispatch()`s whatever it's sent
/// and ignores the `Outcome`, see `server/replica_link.rs`) never has to
/// handle a `BlockWait` itself.
fn bpop(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes], name: &str, front: bool) -> Outcome {
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
    let db = ctx.store.db(state.db_index).expect("valid db index");
    for key in keys {
        let popped = db.with_value_mut(key, |v| match v {
            KVal::List(list) => Ok(if front { list.pop_front() } else { list.pop_back() }),
            _ => Err(()),
        });
        match popped {
            None | Some(Ok(None)) => continue,
            Some(Err(())) => {
                clear_blocking_deadline(state);
                return wrong_type();
            }
            Some(Ok(Some(item))) => {
                db.remove_if_empty(key);
                on_key_changed(ctx, state.db_index, key.clone());
                notify_keyspace_event(ctx, state.db_index, 'l', if front { "lpop" } else { "rpop" }, key);
                let effect_name: &[u8] = if front { b"LPOP" } else { b"RPOP" };
                let effect = vec![Bytes::from_static(effect_name), key.clone()];
                ctx.append_aof(state.db_index, &effect);
                ctx.replicate(state.db_index, &effect);
                clear_blocking_deadline(state);
                return reply(Value::Array(vec![Value::bulk(key.clone()), Value::bulk(item)]));
            }
        }
    }
    Outcome::BlockWait { deadline, on_timeout: Value::NullArray }
}

pub fn cmd_blmove(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [source, destination, from, to, timeout] = args else { return wrong_args("blmove") };
    let from_left = match String::from_utf8_lossy(from).to_ascii_uppercase().as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return err("ERR syntax error"),
    };
    let to_left = match String::from_utf8_lossy(to).to_ascii_uppercase().as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return err("ERR syntax error"),
    };

    let deadline = match blocking_deadline(state, timeout) {
        Ok(d) => d,
        Err(e) => return e,
    };
    if let Some(e) = check_write_allowed(ctx, state) {
        clear_blocking_deadline(state);
        return e;
    }

    // `lmove` already replies `NullBulk` (not an error) when `source` is
    // missing/empty, which is exactly the "nothing to do yet, keep
    // blocking" signal we need here. It handles `on_key_changed` itself,
    // but — like `lmove`'s other caller `cmd_lmove`/`cmd_rpoplpush` isn't
    // the one that AOF-logs/replicates either, `dispatch()`'s generic
    // `WRITE_COMMANDS` path is — we still log the effect here by hand.
    let outcome = lmove(ctx, state, source, destination, from_left, to_left);
    match outcome {
        Outcome::Reply(Value::NullBulk) => Outcome::BlockWait { deadline, on_timeout: Value::NullBulk },
        Outcome::Reply(Value::Bulk(_)) => {
            let effect = vec![
                Bytes::from_static(b"LMOVE"),
                source.clone(),
                destination.clone(),
                Bytes::from_static(if from_left { b"LEFT" } else { b"RIGHT" }),
                Bytes::from_static(if to_left { b"LEFT" } else { b"RIGHT" }),
            ];
            ctx.append_aof(state.db_index, &effect);
            ctx.replicate(state.db_index, &effect);
            clear_blocking_deadline(state);
            outcome
        }
        other => {
            clear_blocking_deadline(state);
            other
        }
    }
}
