//! Compatibility commands whose implementation spans key types or does not
//! fit naturally in one collection module.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use keyspace::Value as KVal;
use rand::seq::SliceRandom;
use resp::Value;

use crate::dispatch::{err, reply, wrong_args, wrong_type, ClientState, Outcome, ServerContext};
use crate::dispatch::{notify_keyspace_event, on_key_changed};

pub fn cmd_time(args: &[Bytes]) -> Outcome {
    if !args.is_empty() {
        return wrong_args("time");
    }
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return err("ERR system clock is before the Unix epoch");
    };
    reply(Value::Array(vec![
        Value::bulk(now.as_secs().to_string()),
        Value::bulk(format!("{:06}", now.subsec_micros())),
    ]))
}

pub fn cmd_config(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let Some(subcommand) = args.first() else {
        return wrong_args("config");
    };
    match subcommand.to_ascii_uppercase().as_slice() {
        b"GET" if args.len() == 2 => {
            let pattern = String::from_utf8_lossy(&args[1]).to_ascii_lowercase();
            let maxmemory = ctx.effective_maxmemory().unwrap_or(0).to_string();
            let policy = policy_name(ctx.effective_maxmemory_policy()).to_string();
            let values = [("maxmemory", maxmemory), ("maxmemory-policy", policy)];
            let mut out = Vec::new();
            for (key, value) in values {
                if glob_matches(&pattern, key) {
                    out.push(Value::bulk(key));
                    out.push(Value::bulk(value));
                }
            }
            reply(Value::Array(out))
        }
        b"SET" if args.len() == 3 => {
            let key = String::from_utf8_lossy(&args[1]).to_ascii_lowercase();
            let value = String::from_utf8_lossy(&args[2]).into_owned();
            match key.as_str() {
                "maxmemory" => match parse_memory(&value) {
                    Some(n) => {
                        ctx.config_overrides.insert(key, n.to_string());
                        reply(Value::ok())
                    }
                    None => err("ERR value is not an integer or out of range"),
                },
                "maxmemory-policy" => match crate::dispatch::MaxMemoryPolicy::parse(&value) {
                    Some(_) => {
                        ctx.config_overrides.insert(key, value.to_ascii_lowercase());
                        reply(Value::ok())
                    }
                    None => err("ERR Invalid argument 'maxmemory-policy' for CONFIG SET"),
                },
                _ => err("ERR Unsupported CONFIG parameter"),
            }
        }
        _ => err("ERR Unknown subcommand or wrong number of arguments for CONFIG"),
    }
}

fn parse_memory(value: &str) -> Option<u64> {
    let lower = value.to_ascii_lowercase();
    let (number, multiplier) = if let Some(v) = lower.strip_suffix("gb") {
        (v, 1024u64.checked_pow(3)?)
    } else if let Some(v) = lower.strip_suffix("mb") {
        (v, 1024u64.checked_pow(2)?)
    } else if let Some(v) = lower.strip_suffix("kb") {
        (v, 1024)
    } else {
        (lower.as_str(), 1)
    };
    let n: u64 = number.parse().ok()?;
    n.checked_mul(multiplier)
}

fn policy_name(policy: crate::dispatch::MaxMemoryPolicy) -> &'static str {
    use crate::dispatch::MaxMemoryPolicy::*;
    match policy {
        NoEviction => "noeviction",
        AllKeysRandom => "allkeys-random",
        VolatileRandom => "volatile-random",
        AllKeysLru => "allkeys-lru",
        VolatileLru => "volatile-lru",
        AllKeysLfu => "allkeys-lfu",
        VolatileLfu => "volatile-lfu",
        VolatileTtl => "volatile-ttl",
    }
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let mut rest = value;
    let parts: Vec<&str> = pattern.split('*').collect();
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        let Some(at) = rest.find(part) else {
            return false;
        };
        if index == 0 && !pattern.starts_with('*') && at != 0 {
            return false;
        }
        rest = &rest[at + part.len()..];
    }
    pattern.ends_with('*') || parts.last().is_some_and(|part| rest.is_empty() && value.ends_with(part))
}

pub fn cmd_randomkey(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if !args.is_empty() {
        return wrong_args("randomkey");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let keys = db.keys_snapshot();
    let mut rng = rand::thread_rng();
    for key in keys.choose_multiple(&mut rng, keys.len().min(32)) {
        if db.exists(std::slice::from_ref(key)) > 0 {
            return reply(Value::bulk(key.clone()));
        }
    }
    // A bounded sample can miss live keys when many expired entries exist;
    // fall back to a full pass only in that rare case.
    for key in keys {
        if db.exists(std::slice::from_ref(&key)) > 0 {
            return reply(Value::bulk(key));
        }
    }
    reply(Value::NullBulk)
}

pub fn cmd_touch(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("touch");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let count = args.iter().filter(|k| db.touch_key(k)).count();
    reply(Value::Integer(count as i64))
}

pub fn cmd_copy(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [source, destination, rest @ ..] = args else {
        return wrong_args("copy");
    };
    let mut replace = false;
    let mut target_db = state.db_index;
    let mut i = 0;
    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            i += 1;
        } else if rest[i].eq_ignore_ascii_case(b"DB") && i + 1 < rest.len() {
            let Some(index) = parse_db_index(&rest[i + 1], ctx.store.len()) else {
                return err("ERR DB index is out of range");
            };
            target_db = index;
            i += 2;
        } else {
            return err("ERR syntax error");
        }
    }
    let source_db = ctx.store.db(state.db_index).expect("valid db index");
    let Some(value) = source_db.get(source) else {
        return reply(Value::Integer(0));
    };
    let Some(dest_db) = ctx.store.db(target_db) else {
        return err("ERR DB index is out of range");
    };
    if target_db == state.db_index && source == destination {
        return reply(Value::Integer(0));
    }
    if dest_db.exists(std::slice::from_ref(destination)) > 0 {
        if !replace {
            return reply(Value::Integer(0));
        }
        dest_db.del(std::slice::from_ref(destination));
    }
    let expires_at = match source_db.ttl(source) {
        keyspace::Ttl::Expires(ttl) => Instant::now().checked_add(ttl),
        _ => None,
    };
    dest_db.set(destination.clone(), value, expires_at);
    on_key_changed(ctx, state, target_db, destination.clone(), None);
    notify_keyspace_event(ctx, target_db, 'g', "copy_to", destination);
    reply(Value::Integer(1))
}

pub fn cmd_move(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key, db_arg] = args else {
        return wrong_args("move");
    };
    let Some(target) = parse_db_index(db_arg, ctx.store.len()) else {
        return err("ERR DB index is out of range");
    };
    if target == state.db_index {
        return reply(Value::Integer(0));
    }
    let source_db = ctx.store.db(state.db_index).expect("valid db index");
    if source_db.exists(std::slice::from_ref(key)) == 0 {
        return reply(Value::Integer(0));
    }
    let Some(value) = source_db.get(key) else {
        return reply(Value::Integer(0));
    };
    let expires_at = match source_db.ttl(key) {
        keyspace::Ttl::Expires(ttl) => Instant::now().checked_add(ttl),
        _ => None,
    };
    let dest_db = ctx.store.db(target).expect("validated db index");
    if dest_db.exists(std::slice::from_ref(key)) > 0 {
        return reply(Value::Integer(0));
    }
    source_db.del(std::slice::from_ref(key));
    dest_db.set(key.clone(), value, expires_at);
    on_key_changed(ctx, state, state.db_index, key.clone(), None);
    on_key_changed(ctx, state, target, key.clone(), None);
    notify_keyspace_event(ctx, state.db_index, 'g', "move_from", key);
    notify_keyspace_event(ctx, target, 'g', "move_to", key);
    reply(Value::Integer(1))
}

fn parse_db_index(arg: &[u8], count: usize) -> Option<usize> {
    std::str::from_utf8(arg).ok()?.parse::<usize>().ok().filter(|i| *i < count)
}

pub fn cmd_bitop(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 3 {
        return wrong_args("bitop");
    }
    let op = args[0].to_ascii_uppercase();
    let dest = args[1].clone();
    let sources = &args[2..];
    if op.as_slice() == b"NOT" && sources.len() != 1 {
        return err("ERR BITOP NOT must be called with a single source key.");
    }
    if !matches!(op.as_slice(), b"AND" | b"OR" | b"XOR" | b"NOT") {
        return err("ERR syntax error");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let mut values = Vec::with_capacity(sources.len());
    for key in sources {
        match db.with_value(key, |v| v.string_bytes().ok_or(())) {
            None => values.push(Bytes::new()),
            Some(Ok(v)) => values.push(v),
            Some(Err(())) => return wrong_type(),
        }
    }
    let len = if op.as_slice() == b"NOT" {
        values[0].len()
    } else {
        values.iter().map(Bytes::len).max().unwrap_or(0)
    };
    let mut out = vec![0u8; len];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = match op.as_slice() {
            b"NOT" => !values[0][i],
            b"AND" => values.iter().fold(0xff, |a, v| a & v.get(i).copied().unwrap_or(0)),
            b"OR" => values.iter().fold(0, |a, v| a | v.get(i).copied().unwrap_or(0)),
            _ => values.iter().fold(0, |a, v| a ^ v.get(i).copied().unwrap_or(0)),
        };
    }
    db.set(dest.clone(), KVal::String(Bytes::from(out)), None);
    on_key_changed(ctx, state, state.db_index, dest.clone(), None);
    notify_keyspace_event(ctx, state.db_index, '$', "set", &dest);
    reply(Value::Integer(len as i64))
}

pub fn cmd_bitpos(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 || args.len() > 5 {
        return wrong_args("bitpos");
    }
    let bit = match args[1].as_ref() {
        b"0" => false,
        b"1" => true,
        _ => return err("ERR bit is not an integer or out of range"),
    };
    let parsed = match args.len() {
        2 => Ok((0i64, None, false)),
        3 => parse_num(&args[2]).map(|start| (start, None, false)),
        4 => parse_num(&args[2]).and_then(|start| parse_num(&args[3]).map(|end| (start, Some(end), false))),
        5 if args[4].eq_ignore_ascii_case(b"BIT") => {
            parse_num(&args[2]).and_then(|start| parse_num(&args[3]).map(|end| (start, Some(end), true)))
        }
        5 => return err("ERR syntax error"),
        _ => unreachable!(),
    };
    let (start, end, bitunit) = match parsed {
        Ok(value) => value,
        Err(error) => return error,
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let value = match db.with_value(&args[0], |v| v.string_bytes().ok_or(())) {
        None => Bytes::new(),
        Some(Ok(v)) => v,
        Some(Err(())) => return wrong_type(),
    };
    let total = (value.len() as i64).saturating_mul(8);
    let unit = if bitunit { 1 } else { 8 };
    let start_bits = normalize_index(start, total / unit).saturating_mul(unit);
    let end_bits =
        end.map(|e| normalize_index(e, total / unit).saturating_mul(unit).saturating_add(unit - 1));
    let from = start_bits;
    let to = end_bits.unwrap_or(total.saturating_sub(1)).min(total.saturating_sub(1));
    let found = if total == 0 || from > to {
        None
    } else {
        (from..=to).find(|p| {
            let byte = value.get((*p / 8) as usize).copied().unwrap_or(0);
            ((byte >> (7 - (*p % 8))) & 1 == 1) == bit
        })
    };
    // BITPOS with a zero search and no explicit end includes the implicit
    // zero bytes after the string, so the first zero is at len * 8.
    let pos = found
        .or_else(|| if !bit && end.is_none() { Some(value.len() as i64 * 8) } else { None })
        .unwrap_or(-1);
    reply(Value::Integer(pos))
}

pub fn cmd_bitfield(ctx: &ServerContext, state: &ClientState, args: &[Bytes], read_only: bool) -> Outcome {
    if args.is_empty() {
        return wrong_args(if read_only { "bitfield_ro" } else { "bitfield" });
    }
    let mut ops = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut i = 1;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"OVERFLOW") {
            if i + 1 >= args.len() {
                return err("ERR syntax error");
            }
            overflow = if args[i + 1].eq_ignore_ascii_case(b"WRAP") {
                Overflow::Wrap
            } else if args[i + 1].eq_ignore_ascii_case(b"SAT") {
                Overflow::Sat
            } else if args[i + 1].eq_ignore_ascii_case(b"FAIL") {
                Overflow::Fail
            } else {
                return err("ERR syntax error");
            };
            i += 2;
            continue;
        }
        let op = if args[i].eq_ignore_ascii_case(b"GET") {
            FieldOp::Get
        } else if args[i].eq_ignore_ascii_case(b"SET") {
            FieldOp::Set
        } else if args[i].eq_ignore_ascii_case(b"INCRBY") {
            FieldOp::Incr
        } else {
            return err("ERR syntax error");
        };
        if read_only && op != FieldOp::Get {
            return err("ERR BITFIELD_RO only supports the GET subcommand");
        }
        let need = if op == FieldOp::Get { 3 } else { 4 };
        if i + need > args.len() {
            return err("ERR syntax error");
        }
        let (signed, width) = match parse_encoding(&args[i+1]) { Some(v) => v, None => return err("ERR Invalid bitfield type. Use something like i16 or u8. Note that u64 is not supported but i64 is.") };
        let Some(offset) = parse_bit_offset(&args[i + 2], width) else {
            return err("ERR bit offset is not an integer or out of range");
        };
        // Match the configured `proto-max-bulk-len` and prevent a tiny
        // command from forcing an outsized allocation for one bitfield offset.
        if offset.saturating_add(width as u64) > (ctx.proto_max_bulk_len as u64 * 8) {
            return err("ERR bit offset is out of range");
        }
        let value = if op == FieldOp::Set {
            match parse_field_value(&args[i + 3], signed, width) {
                Some(v) => Some(v),
                None => return err("ERR value is out of range"),
            }
        } else {
            None
        };
        let delta = if op == FieldOp::Incr {
            match std::str::from_utf8(&args[i + 3]).ok().and_then(|s| s.parse::<i64>().ok()) {
                Some(n) => Some(n),
                None => return err("ERR value is not an integer or out of range"),
            }
        } else {
            None
        };
        ops.push(FieldCommand { op, signed, width, offset, value, delta, overflow });
        i += need;
    }
    if ops.is_empty() {
        return err("ERR syntax error");
    }
    let key = args[0].clone();
    let db = ctx.store.db(state.db_index).expect("valid db index");
    if read_only {
        let result = match db.with_value(&key, |v| v.string_bytes().ok_or(())) {
            None => Bytes::new(),
            Some(Ok(v)) => v,
            Some(Err(())) => return wrong_type(),
        };
        let out = ops
            .iter()
            .map(|cmd| {
                Value::Integer(decode_field(read_bits(&result, cmd.offset, cmd.width), cmd.signed, cmd.width))
            })
            .collect();
        return reply(Value::Array(out));
    }
    let mut changed = false;
    let result = db.with_entry_mut(
        key.clone(),
        || KVal::String(Bytes::new()),
        |v| {
            let Some(bytes) = v.string_bytes_mut() else { return Err(()) };
            let mut data = bytes.to_vec();
            let mut replies = Vec::with_capacity(ops.len());
            for cmd in &ops {
                let old = read_bits(&data, cmd.offset, cmd.width);
                match cmd.op {
                    FieldOp::Get => replies.push(Value::Integer(decode_field(old, cmd.signed, cmd.width))),
                    FieldOp::Set => {
                        write_bits(&mut data, cmd.offset, cmd.width, cmd.value.unwrap_or(0));
                        replies.push(Value::Integer(decode_field(old, cmd.signed, cmd.width)));
                        changed = true;
                    }
                    FieldOp::Incr => {
                        let next =
                            increment_field(old, cmd.signed, cmd.width, cmd.delta.unwrap_or(0), cmd.overflow);
                        match next {
                            None => replies.push(Value::NullBulk),
                            Some(raw) => {
                                write_bits(&mut data, cmd.offset, cmd.width, raw);
                                replies.push(Value::Integer(decode_field(raw, cmd.signed, cmd.width)));
                                changed = true;
                            }
                        }
                    }
                }
            }
            *bytes = Bytes::from(data);
            Ok(replies)
        },
    );
    match result {
        Err(()) => wrong_type(),
        Ok(replies) => {
            if changed {
                on_key_changed(ctx, state, state.db_index, key.clone(), None);
                notify_keyspace_event(ctx, state.db_index, '$', "bitfield", &key);
            }
            reply(Value::Array(replies))
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FieldOp {
    Get,
    Set,
    Incr,
}
#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}
struct FieldCommand {
    op: FieldOp,
    signed: bool,
    width: u8,
    offset: u64,
    value: Option<u64>,
    delta: Option<i64>,
    overflow: Overflow,
}

fn parse_encoding(value: &[u8]) -> Option<(bool, u8)> {
    let (signed, n) = match value.first()? {
        b'i' | b'I' => (true, &value[1..]),
        b'u' | b'U' => (false, &value[1..]),
        _ => return None,
    };
    let width = std::str::from_utf8(n).ok()?.parse::<u8>().ok()?;
    if width == 0 || width > 64 || (!signed && width == 64) {
        return None;
    }
    Some((signed, width))
}
fn parse_bit_offset(value: &[u8], width: u8) -> Option<u64> {
    let (raw, multiplier) =
        if value.first() == Some(&b'#') { (&value[1..], width as u64) } else { (value, 1) };
    std::str::from_utf8(raw).ok()?.parse::<u64>().ok()?.checked_mul(multiplier)
}
fn parse_field_value(value: &[u8], signed: bool, width: u8) -> Option<u64> {
    if signed {
        let n = std::str::from_utf8(value).ok()?.parse::<i64>().ok()?;
        let min = if width == 64 { i64::MIN } else { -(1i64 << (width - 1)) };
        let max = if width == 64 { i64::MAX } else { (1i64 << (width - 1)) - 1 };
        if n < min || n > max {
            return None;
        }
        Some(n as u64 & bit_mask(width))
    } else {
        let n = std::str::from_utf8(value).ok()?.parse::<u64>().ok()?;
        if n > bit_mask(width) {
            return None;
        }
        Some(n)
    }
}
fn bit_mask(width: u8) -> u64 {
    if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    }
}
fn read_bits(bytes: &[u8], offset: u64, width: u8) -> u64 {
    let mut value = 0u64;
    for n in 0..width as u64 {
        let pos = offset + n;
        let bit = bytes.get((pos / 8) as usize).copied().unwrap_or(0) >> (7 - (pos % 8));
        value = (value << 1) | u64::from(bit & 1);
    }
    value
}
fn write_bits(bytes: &mut Vec<u8>, offset: u64, width: u8, value: u64) {
    let end = offset + width as u64;
    bytes.resize(bytes.len().max(end.div_ceil(8) as usize), 0);
    for n in 0..width as u64 {
        let pos = offset + n;
        let mask = 1u8 << (7 - (pos % 8));
        let bit = (value >> (width as u64 - 1 - n)) & 1;
        if bit == 1 {
            bytes[(pos / 8) as usize] |= mask;
        } else {
            bytes[(pos / 8) as usize] &= !mask;
        }
    }
}
fn decode_field(raw: u64, signed: bool, width: u8) -> i64 {
    if !signed {
        return raw as i64;
    }
    if width == 64 {
        return raw as i64;
    }
    let sign = 1u64 << (width - 1);
    if raw & sign != 0 {
        (raw | !bit_mask(width)) as i64
    } else {
        raw as i64
    }
}
fn increment_field(raw: u64, signed: bool, width: u8, delta: i64, mode: Overflow) -> Option<u64> {
    if signed {
        let old = decode_field(raw, true, width);
        if let Some(value) = old.checked_add(delta) {
            let min = if width == 64 { i64::MIN } else { -(1i64 << (width - 1)) };
            let max = if width == 64 { i64::MAX } else { (1i64 << (width - 1)) - 1 };
            if value >= min && value <= max {
                return Some(value as u64 & bit_mask(width));
            }
        }
        match mode {
            Overflow::Fail => None,
            Overflow::Wrap => Some((old.wrapping_add(delta) as u64) & bit_mask(width)),
            Overflow::Sat => {
                let min = if width == 64 { i64::MIN } else { -(1i64 << (width - 1)) };
                let max = if width == 64 { i64::MAX } else { (1i64 << (width - 1)) - 1 };
                Some(if delta >= 0 { max } else { min } as u64 & bit_mask(width))
            }
        }
    } else {
        let max = bit_mask(width) as i128;
        let next = raw as i128 + delta as i128;
        if next >= 0 && next <= max {
            return Some(next as u64);
        }
        match mode {
            Overflow::Fail => None,
            Overflow::Wrap => Some((next.rem_euclid(max + 1)) as u64),
            Overflow::Sat => Some(if next < 0 { 0 } else { max as u64 }),
        }
    }
}

fn parse_num(arg: &[u8]) -> Result<i64, Outcome> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| err("ERR value is not an integer or out of range"))
}
fn normalize_index(i: i64, len: i64) -> i64 {
    if i < 0 {
        len.saturating_add(i).max(0)
    } else {
        i.min(len)
    }
}

pub fn cmd_msetnx(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return wrong_args("msetnx");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let keys: Vec<Bytes> = args.chunks(2).map(|p| p[0].clone()).collect();
    if keys.iter().any(|key| db.exists(std::slice::from_ref(key)) > 0) {
        return reply(Value::Integer(0));
    }
    for pair in args.chunks(2) {
        db.set(pair[0].clone(), KVal::String(pair[1].clone()), None);
    }
    for key in keys {
        on_key_changed(ctx, state, state.db_index, key.clone(), None);
        notify_keyspace_event(ctx, state.db_index, '$', "mset", &key);
    }
    reply(Value::Integer(1))
}

pub fn cmd_sort(ctx: &ServerContext, state: &ClientState, args: &[Bytes], read_only: bool) -> Outcome {
    if args.is_empty() {
        return wrong_args(if read_only { "sort_ro" } else { "sort" });
    }
    let source = &args[0];
    let mut desc = false;
    let mut alpha = false;
    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut store = None;
    let mut by = None;
    let mut gets = Vec::new();
    let mut i = 1;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"ASC") {
            desc = false;
            i += 1;
        } else if args[i].eq_ignore_ascii_case(b"DESC") {
            desc = true;
            i += 1;
        } else if args[i].eq_ignore_ascii_case(b"ALPHA") {
            alpha = true;
            i += 1;
        } else if args[i].eq_ignore_ascii_case(b"LIMIT") && i + 2 < args.len() {
            let Some(o) = std::str::from_utf8(&args[i + 1]).ok().and_then(|s| s.parse::<i64>().ok()) else {
                return err("ERR value is not an integer or out of range");
            };
            let Some(c) = std::str::from_utf8(&args[i + 2]).ok().and_then(|s| s.parse::<i64>().ok()) else {
                return err("ERR value is not an integer or out of range");
            };
            offset = if o < 0 { 0 } else { o as usize };
            count = if c < 0 { usize::MAX } else { c as usize };
            i += 3;
        } else if args[i].eq_ignore_ascii_case(b"STORE") && i + 1 < args.len() {
            if read_only {
                return err("ERR SORT_RO does not allow the STORE option");
            }
            store = Some(args[i + 1].clone());
            i += 2;
        } else if args[i].eq_ignore_ascii_case(b"BY") && i + 1 < args.len() {
            by = Some(args[i + 1].clone());
            i += 2;
        } else if args[i].eq_ignore_ascii_case(b"GET") && i + 1 < args.len() {
            gets.push(args[i + 1].clone());
            i += 2;
        } else {
            return err("ERR syntax error");
        }
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let values = match db.with_value(source, |v| match v {
        KVal::List(l) => Ok(l.iter().cloned().collect::<Vec<_>>()),
        KVal::Set(s) => Ok(s.iter().cloned().collect()),
        KVal::ZSet(z) => Ok(z.sorted().into_iter().map(|(m, _)| m).collect()),
        _ => Err(()),
    }) {
        None => Vec::new(),
        Some(Ok(values)) => values,
        Some(Err(())) => return wrong_type(),
    };
    let mut decorated: Vec<(Bytes, f64, Bytes)> = Vec::with_capacity(values.len());
    for value in values {
        let sort_value = match by.as_ref() {
            Some(pattern) if pattern.eq_ignore_ascii_case(b"nosort") => value.clone(),
            Some(pattern) => sort_lookup(db, pattern, &value).unwrap_or_else(|| Bytes::from_static(b"0")),
            None => value.clone(),
        };
        let score = if alpha {
            0.0
        } else {
            match std::str::from_utf8(&sort_value)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|v| v.is_finite())
            {
                Some(n) => n,
                None if by.is_some() => 0.0,
                None => return err("ERR One or more scores can't be converted into double"),
            }
        };
        decorated.push((value, score, sort_value));
    }
    if !by.as_ref().is_some_and(|pattern| pattern.eq_ignore_ascii_case(b"nosort")) {
        decorated.sort_by(|a, b| {
            let order = if alpha {
                a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0))
            } else {
                a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
            };
            if desc {
                order.reverse()
            } else {
                order
            }
        });
    }
    let selected: Vec<Bytes> = decorated.into_iter().skip(offset).take(count).map(|(v, _, _)| v).collect();
    let output: Vec<Option<Bytes>> = if gets.is_empty() {
        selected.iter().cloned().map(Some).collect()
    } else {
        selected
            .iter()
            .flat_map(|member| {
                gets.iter().map(|pattern| {
                    if pattern.as_ref() == b"#" {
                        Some(member.clone())
                    } else {
                        sort_lookup(db, pattern, member)
                    }
                })
            })
            .collect()
    };
    if let Some(destination) = store {
        if output.is_empty() {
            db.del(std::slice::from_ref(&destination));
        } else {
            let stored = output.iter().map(|v| v.clone().unwrap_or_default()).collect();
            db.set(destination.clone(), KVal::List(stored), None);
        }
        on_key_changed(ctx, state, state.db_index, destination.clone(), None);
        notify_keyspace_event(ctx, state.db_index, 'l', "sortstore", &destination);
        reply(Value::Integer(output.len() as i64))
    } else {
        reply(Value::Array(
            output.into_iter().map(|v| v.map(Value::bulk).unwrap_or(Value::NullBulk)).collect(),
        ))
    }
}

fn sort_lookup(db: &keyspace::Db, pattern: &[u8], member: &[u8]) -> Option<Bytes> {
    let mut key = Vec::with_capacity(pattern.len() + member.len());
    if let Some(star) = pattern.iter().position(|byte| *byte == b'*') {
        key.extend_from_slice(&pattern[..star]);
        key.extend_from_slice(member);
        key.extend_from_slice(&pattern[star + 1..]);
    } else {
        key.extend_from_slice(pattern);
    }
    db.with_value(&key, |value| value.string_bytes()).flatten()
}
