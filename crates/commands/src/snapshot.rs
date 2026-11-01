//! `SAVE`/`BGSAVE`/`LASTSAVE`/`DUMP`/`RESTORE` — point-in-time snapshotting
//! built on the `persistence` crate's RDB-style format. `DUMP`'s payload is
//! reflexdb's own serialization, not byte-compatible with external RDB
//! blobs (see `persistence::rdb`'s module docs).

use std::time::Duration;

use bincode::Options;
use bytes::Bytes;
use keyspace::Value as KVal;
use resp::Value;

use crate::dispatch::{
    err, notify_keyspace_event, on_key_changed, reply, saturating_deadline, wrong_args, ClientState, Outcome,
    ServerContext,
};

pub fn cmd_save(ctx: &ServerContext) -> Outcome {
    let Some(path) = ctx.rdb_path() else {
        return err("ERR no RDB file configured (set `dir`/`dbfilename` or pass --dbfilename)");
    };
    match persistence::rdb::save(&path, &ctx.store) {
        Ok(()) => {
            ctx.mark_saved();
            reply(Value::ok())
        }
        Err(e) => err(format!("ERR {e}")),
    }
}

pub fn cmd_bgsave(ctx: &ServerContext) -> Outcome {
    let Some(path) = ctx.rdb_path() else {
        return err("ERR no RDB file configured (set `dir`/`dbfilename` or pass --dbfilename)");
    };
    // Unlike `SAVE`, `BGSAVE` must not block the caller for the write+rename
    // — only `serialize` (an in-memory keyspace walk) has to happen here,
    // since it needs `&ctx.store`, which doesn't outlive this call. The
    // resulting `payload` is owned, so the actual disk I/O can move to a
    // background thread with nothing left borrowed from `ctx`.
    match persistence::rdb::serialize(&ctx.store) {
        Ok(payload) => {
            let path = path.clone();
            let spawn_result = persistence::rdb::save_in_background(path, payload, |result| {
                if let Err(e) = result {
                    tracing::warn!(error = %e, "BGSAVE: background write failed");
                }
            });
            match spawn_result {
                // `dispatch()` only ever borrows `&ServerContext` for one
                // call (see its module docs), so there's no way to hand
                // the background thread a handle back to `ctx` to update
                // `last_save` once the write actually finishes — mark it
                // now instead. Same fire-and-forget trade-off as the AOF
                // writer: `LASTSAVE` can report a timestamp a few
                // milliseconds ahead of the file actually landing on disk.
                Ok(()) => {
                    ctx.mark_saved();
                    reply(Value::Simple("Background saving started".into()))
                }
                Err(e) => err(format!("ERR {e}")),
            }
        }
        Err(e) => err(format!("ERR {e}")),
    }
}

pub fn cmd_lastsave(ctx: &ServerContext) -> Outcome {
    reply(Value::Integer(ctx.last_save() as i64))
}

/// `BGREWRITEAOF` — replaces the AOF's ever-growing command history with
/// just enough commands to recreate the current keyspace (see
/// `persistence::aof::build_rewrite_base`'s docs). `AofWriter::begin_rewrite`
/// is sent *before* walking the keyspace, not after — that ordering (not a
/// `fork()`-based point-in-time snapshot, which this codebase has no
/// equivalent of) is what guarantees a write landing concurrently with the
/// rewrite is never lost; see its docs for exactly why. Like `BGSAVE`,
/// replies as soon as the rewrite is queued, not once the file swap has
/// actually happened on the writer thread.
pub fn cmd_bgrewriteaof(ctx: &ServerContext) -> Outcome {
    let aof = ctx.aof.read().unwrap_or_else(std::sync::PoisonError::into_inner);
    let (Some(writer), Some(path)) = (&*aof, &ctx.aof_path) else {
        return err("ERR no append only file configured (set `appendonly yes`)");
    };
    if let Err(e) = writer.begin_rewrite() {
        return err(format!("ERR {e}"));
    }
    let base = persistence::aof::build_rewrite_base(&ctx.store);
    if let Err(e) = writer.finish_rewrite(base, path.clone()) {
        return err(format!("ERR {e}"));
    }
    reply(Value::Simple("Background append only file rewriting started".into()))
}

pub fn cmd_dump(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("dump") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: `bincode::serialize` only needs `&Value`,
    // so there's no reason to clone the value (potentially a huge
    // List/Hash/Set/ZSet) before handing it to the serializer.
    match db.with_value(key, bincode::serialize) {
        None => reply(Value::NullBulk),
        Some(Ok(payload)) => reply(Value::bulk(payload)),
        Some(Err(e)) => err(format!("ERR {e}")),
    }
}

pub fn cmd_restore(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let (key, ttl_raw, payload, replace) = match args {
        [k, ttl, payload] => (k, ttl, payload, false),
        [k, ttl, payload, flag] if flag.eq_ignore_ascii_case(b"REPLACE") => (k, ttl, payload, true),
        _ => return wrong_args("restore"),
    };
    let ttl_ms: u64 = match std::str::from_utf8(ttl_raw).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR Invalid TTL value, must be >= 0"),
    };

    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: only existence matters here, not a clone of
    // a potentially huge List/Hash/Set/ZSet value.
    if !replace && db.with_value(key, |_| ()).is_some() {
        return err("BUSYKEY Target key name already exists.");
    }
    // `payload` is attacker-controlled (any client can send `RESTORE`).
    // `bincode::deserialize`'s default config has no cap on how many
    // bytes a declared collection length can claim to need — serde's
    // `size_hint`-based sequence checks already reject the obvious "huge
    // declared length, near-empty payload" attacks for the types `Value`
    // is built from (verified manually: a `Vec`/`VecDeque` claiming tens
    // of millions of elements backed by a 12-byte payload errors in
    // microseconds, no large allocation), but a `with_limit` here is
    // cheap, correct defense-in-depth against anything that specific
    // check doesn't happen to cover (nested collections, future
    // bincode/serde behavior changes) — capped at the payload's own size,
    // since nothing legitimately encoded in it can need to claim more
    // bytes than that. `with_fixint_encoding()` is required alongside
    // `with_limit()`: the `Options` trait's `DefaultOptions` defaults to
    // *varint* integer encoding, while the plain `bincode::serialize`/
    // `deserialize` free functions this payload was produced with (and
    // that every other caller of `keyspace::Value` (de)serialization in
    // this codebase uses) default to *fixint* — mixing the two doesn't
    // error cleanly, it silently misreads every length-prefixed field,
    // so without this every legitimate `RESTORE` would fail with "Bad
    // data format" (caught by `dump_and_restore_round_trip_a_value`).
    let value: KVal = match bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(payload.len() as u64)
        .deserialize(payload)
    {
        Ok(v) => v,
        Err(_) => return err("ERR Bad data format"),
    };

    let expires_at =
        if ttl_ms == 0 { None } else { Some(saturating_deadline(Duration::from_millis(ttl_ms))) };
    db.set(key.clone(), value, expires_at);
    on_key_changed(ctx, state, state.db_index, key.clone(), None);
    notify_keyspace_event(ctx, state.db_index, 'g', "restore", key);
    reply(Value::ok())
}
