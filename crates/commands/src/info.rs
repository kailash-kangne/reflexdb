//! `INFO [section]`, `SLOWLOG`, `LATENCY`, `MEMORY` — server introspection.
//! Real Redis's `INFO` reply is a single bulk string of `# Section` headers
//! and `key:value` lines (not an array), so clients that parse it
//! line-by-line work unmodified.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use keyspace::Value as KVal;
use resp::Value;

use crate::dispatch::{err, reply, wrong_args, ClientState, MaxMemoryPolicy, Outcome, ServerContext};

/// One `SLOWLOG` entry: a command that took at least `slowlog_threshold_us`.
pub struct SlowLogEntry {
    pub id: u64,
    pub timestamp_secs: u64,
    pub duration_us: u64,
    pub args: Vec<Bytes>,
}

pub fn cmd_info(ctx: &ServerContext, _args: &[Bytes]) -> Outcome {
    let mut out = String::new();

    out.push_str("# Server\r\n");
    out.push_str("redis_version:7.4.0\r\n"); // version string clients sniff for feature checks
    out.push_str("reflexdb_version:0.1.0\r\n");
    out.push_str(&format!("process_id:{}\r\n", std::process::id()));
    out.push_str(&format!("uptime_in_seconds:{}\r\n", ctx.start_time.elapsed().as_secs()));
    out.push_str("\r\n"); // tcp_port omitted — the request's own connection doesn't know its listen port

    out.push_str("# Clients\r\n");
    out.push_str(&format!("connected_clients:{}\r\n", ctx.connected_clients()));
    out.push_str(&format!("blocked_clients:{}\r\n", ctx.singleflight_waiters()));
    out.push_str("\r\n");

    out.push_str("# Memory\r\n");
    let used = ctx.store.approx_bytes();
    out.push_str(&format!("used_memory:{used}\r\n"));
    out.push_str(&format!("used_memory_human:{}\r\n", human_bytes(used as u64)));
    match ctx.effective_maxmemory() {
        Some(limit) => {
            out.push_str(&format!("maxmemory:{limit}\r\n"));
            out.push_str(&format!("maxmemory_human:{}\r\n", human_bytes(limit)));
        }
        None => out.push_str("maxmemory:0\r\n"),
    }
    let policy = match ctx.effective_maxmemory_policy() {
        MaxMemoryPolicy::NoEviction => "noeviction",
        MaxMemoryPolicy::AllKeysRandom => "allkeys-random",
        MaxMemoryPolicy::VolatileRandom => "volatile-random",
        MaxMemoryPolicy::AllKeysLru => "allkeys-lru",
        MaxMemoryPolicy::VolatileLru => "volatile-lru",
        MaxMemoryPolicy::AllKeysLfu => "allkeys-lfu",
        MaxMemoryPolicy::VolatileLfu => "volatile-lfu",
        MaxMemoryPolicy::VolatileTtl => "volatile-ttl",
    };
    out.push_str(&format!("maxmemory_policy:{policy}\r\n"));
    out.push_str("\r\n");

    out.push_str("# Tiering\r\n");
    out.push_str(&format!("tiered_memory_limit:{}\r\n", ctx.tiered_limit.unwrap_or(0)));
    match ctx.store.tiered_disk_usage_bytes() {
        // `du`-style directory size includes fjall's pre-allocated journal
        // segments (can be tens of MB even near-empty) — this is the same
        // self-tracked payload-byte estimate `tiered_disk_limit`
        // enforcement actually checks against, so it's the number to
        // compare against the limit, not a filesystem walk.
        Some(used) => out.push_str(&format!("tiered_disk_used_bytes:{used}\r\n")),
        None => out.push_str("tiered_disk_used_bytes:0\r\n"),
    }
    out.push_str(&format!("tiered_disk_limit:{}\r\n", ctx.tiered_disk_limit.unwrap_or(0)));
    let policy = match ctx.tiered_overflow_policy {
        crate::dispatch::TieredOverflowPolicy::RejectWrites => "reject-writes",
        crate::dispatch::TieredOverflowPolicy::EvictOldest => "evict-oldest",
        crate::dispatch::TieredOverflowPolicy::NoLimit => "no-limit",
    };
    out.push_str(&format!("tiered_overflow_policy:{policy}\r\n"));
    out.push_str(&format!("tiered_evictions:{}\r\n", ctx.tiered_evictions.load(Ordering::Relaxed)));
    out.push_str(&format!("tiered_disk_drops:{}\r\n", ctx.tiered_disk_drops.load(Ordering::Relaxed)));
    out.push_str(&format!(
        "tiered_rejected_writes:{}\r\n",
        ctx.tiered_full_rejections.load(Ordering::Relaxed)
    ));
    out.push_str(&format!("tiered_spill_failures:{}\r\n", ctx.tiered_spill_failures.load(Ordering::Relaxed)));
    out.push_str(&format!(
        "tiered_expired_dropped:{}\r\n",
        ctx.tiered_expired_dropped.load(Ordering::Relaxed)
    ));
    let stats = ctx.store.tiered_stats();
    out.push_str(&format!("tiered_entries_removed:{}\r\n", stats.entries_removed));
    out.push_str(&format!("tiered_compactions:{}\r\n", stats.compactions_completed));
    out.push_str(&format!("tiered_compacting:{}\r\n", stats.compacting as i32));
    out.push_str("\r\n");

    out.push_str("# Persistence\r\n");
    out.push_str(&format!("rdb_last_save_time:{}\r\n", ctx.last_save()));
    out.push_str(&format!("aof_enabled:{}\r\n", ctx.aof_enabled() as i32));
    out.push_str("\r\n");

    out.push_str("# Replication\r\n");
    match ctx.replica_of_addr() {
        Some((host, port)) => {
            out.push_str("role:slave\r\n");
            out.push_str(&format!("master_host:{host}\r\n"));
            out.push_str(&format!("master_port:{port}\r\n"));
            out.push_str("master_link_status:up\r\n");
        }
        None => out.push_str("role:master\r\n"),
    }
    out.push_str(&format!("connected_slaves:{}\r\n", ctx.connected_replicas()));
    out.push_str(&format!("master_replid:{}\r\n", ctx.replid));
    out.push_str(&format!("master_repl_offset:{}\r\n", ctx.master_repl_offset()));
    out.push_str("\r\n");

    out.push_str("# Keyspace\r\n");
    for i in 0..ctx.store.len() {
        if let Some(db) = ctx.store.db(i) {
            let size = db.dbsize();
            if size > 0 {
                out.push_str(&format!("db{i}:keys={size},expires=0,avg_ttl=0\r\n"));
            }
        }
    }

    reply(Value::bulk(out))
}

pub fn cmd_slowlog(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let [sub, rest @ ..] = args else { return wrong_args("slowlog") };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "GET" => {
            let count: i64 = match rest {
                [] => 10,
                [n] => std::str::from_utf8(n).ok().and_then(|s| s.parse().ok()).unwrap_or(10),
                _ => return wrong_args("slowlog|get"),
            };
            let log = ctx.slowlog.lock().unwrap();
            let take = if count < 0 { log.len() } else { count as usize };
            let items = log
                .iter()
                .take(take)
                .map(|e| {
                    Value::Array(vec![
                        Value::Integer(e.id as i64),
                        Value::Integer(e.timestamp_secs as i64),
                        Value::Integer(e.duration_us as i64),
                        Value::Array(e.args.iter().cloned().map(Value::bulk).collect()),
                    ])
                })
                .collect();
            reply(Value::Array(items))
        }
        "LEN" => reply(Value::Integer(ctx.slowlog.lock().unwrap().len() as i64)),
        "RESET" => {
            ctx.slowlog.lock().unwrap().clear();
            reply(Value::ok())
        }
        other => err(format!("ERR Unknown SLOWLOG subcommand '{}'", other.to_ascii_lowercase())),
    }
}

/// Minimal `LATENCY` support built on the same timing data `SLOWLOG` keeps,
/// rather than separately tracked per-event-type latency spikes (fork time,
/// AOF fsync stalls, etc.) like real Redis — there's only one "event" here,
/// `"command"`.
pub fn cmd_latency(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let [sub, rest @ ..] = args else { return wrong_args("latency") };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "HISTORY" => {
            let [event] = rest else { return wrong_args("latency|history") };
            if !event.eq_ignore_ascii_case(b"command") {
                return reply(Value::Array(vec![]));
            }
            let log = ctx.slowlog.lock().unwrap();
            let items = log
                .iter()
                .map(|e| {
                    Value::Array(vec![
                        Value::Integer(e.timestamp_secs as i64),
                        Value::Integer((e.duration_us / 1000) as i64),
                    ])
                })
                .collect();
            reply(Value::Array(items))
        }
        "LATEST" => {
            let log = ctx.slowlog.lock().unwrap();
            match log.front() {
                Some(latest) => {
                    let max_ms = log.iter().map(|e| e.duration_us / 1000).max().unwrap_or(0);
                    reply(Value::Array(vec![Value::Array(vec![
                        Value::bulk(&b"command"[..]),
                        Value::Integer(latest.timestamp_secs as i64),
                        Value::Integer((latest.duration_us / 1000) as i64),
                        Value::Integer(max_ms as i64),
                    ])]))
                }
                None => reply(Value::Array(vec![])),
            }
        }
        "RESET" => {
            let mut log = ctx.slowlog.lock().unwrap();
            let count = log.len();
            log.clear();
            reply(Value::Integer(count as i64))
        }
        other => err(format!("ERR Unknown LATENCY subcommand '{}'", other.to_ascii_lowercase())),
    }
}

pub fn cmd_memory(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [sub, rest @ ..] = args else { return wrong_args("memory") };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "USAGE" => {
            let [key] = rest else { return wrong_args("memory|usage") };
            let db = ctx.store.db(state.db_index).expect("valid db index");
            match db.with_value(key, |v: &KVal| v.approx_bytes()) {
                Some(bytes) => reply(Value::Integer((bytes + 48) as i64)), // + a small per-entry overhead estimate
                None => reply(Value::NullBulk),
            }
        }
        "DOCTOR" => reply(Value::bulk(
            &b"Sage advice: looks fine, but this is a from-scratch MVP, not a tuned allocator report."[..],
        )),
        "STATS" => {
            let used = ctx.store.approx_bytes();
            let total_keys: usize =
                (0..ctx.store.len()).filter_map(|i| ctx.store.db(i)).map(|db| db.dbsize()).sum();
            reply(Value::Array(vec![
                Value::bulk(&b"total.allocated"[..]),
                Value::Integer(used as i64),
                Value::bulk(&b"keys.count"[..]),
                Value::Integer(total_keys as i64),
            ]))
        }
        other => err(format!("ERR Unknown MEMORY subcommand '{}'", other.to_ascii_lowercase())),
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "K", "M", "G", "T"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2}{}", UNITS[unit])
}
