//! `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`/`SCRIPT` — Lua 5.1 scripting via
//! `mlua` (vendored, so no system Lua is required). See
//! `docs/08-scripting-modules.md`.
//!
//! Each invocation gets a fresh `Lua` VM (simpler and safer than reusing one
//! sandboxed global state, at some performance cost we're fine paying at
//! this stage) with `KEYS`/`ARGV` populated and a `redis.call`/`redis.pcall`
//! bridge back into `dispatch()`.
//!
//! ## Determinism
//!
//! Real Redis replicates the whole script (or re-runs it against the AOF)
//! and so has to work hard to make every non-deterministic Lua primitive
//! (`math.random`, wall-clock time, table iteration order, ...) behave
//! identically everywhere it's replayed. We sidestep almost all of that by
//! construction: `call_inner` dispatches each `redis.call` through the real
//! `dispatch()`, which AOF-logs/replicates *that one resolved write* as it
//! happens (see `dispatch()`'s "effects replication" comment) — never the
//! script source. A replica never re-runs the script at all, so most
//! classic determinism bugs (two primaries' `math.random()` diverging,
//! etc.) can't arise here.
//!
//! What's left, and what this module does about it:
//! - `math.random`/`math.randomseed` are overridden with a PRNG seeded from
//!   the script's own source (same script text -> same random sequence),
//!   so a given script's *Lua-side* control flow — which branch it takes,
//!   not the writes it issues — is reproducible across runs for debugging,
//!   even though nothing here depends on that for correctness.
//! - The `os` and `io` libraries (wall-clock reads, file/process access)
//!   are removed from the sandbox entirely, matching real Redis's script
//!   sandboxing — not just a determinism concern, but also why `FUNCTION`/
//!   `FCALL` are still not implemented (out of scope, see
//!   `docs/10-rust-plan.md`'s "What NOT to build").

use std::cell::RefCell;

use bytes::Bytes;
use mlua::{Lua, Value as LuaValue, Variadic};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sha1::{Digest, Sha1};

use crate::dispatch::{
    dispatch, err, reply, wrong_args, ClientState, Outcome, ServerContext, WRITE_COMMANDS,
};

/// Commands that make no sense (or are actively unsafe) to call recursively
/// from inside a script.
const SCRIPT_DISALLOWED: &[&str] = &[
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "LIVE",
    "UNLIVE",
    "EVAL",
    "EVAL_RO",
    "EVALSHA",
    "EVALSHA_RO",
    "SCRIPT",
    "AUTH",
    "HELLO",
    "ACL",
    "RESET",
    "QUIT",
    "SELECT",
    "SCHEDULE",
    "REPLICAOF",
    "SLAVEOF",
    "PSYNC",
    "REPLCONF",
    "WAIT",
    "WAITAOF",
];

pub fn cmd_eval(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    eval(ctx, state, args, false)
}

pub fn cmd_eval_ro(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    eval(ctx, state, args, true)
}

pub fn cmd_evalsha(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    evalsha(ctx, state, args, false)
}

pub fn cmd_evalsha_ro(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    evalsha(ctx, state, args, true)
}

fn eval(ctx: &ServerContext, state: &ClientState, args: &[Bytes], read_only: bool) -> Outcome {
    let [source, rest @ ..] = args else { return wrong_args("eval") };
    let sha = sha1_hex(source);
    ctx.scripts.insert(sha, String::from_utf8_lossy(source).into_owned());
    run_with_numkeys(ctx, state, &String::from_utf8_lossy(source), rest, read_only)
}

fn evalsha(ctx: &ServerContext, state: &ClientState, args: &[Bytes], read_only: bool) -> Outcome {
    let [sha, rest @ ..] = args else { return wrong_args("evalsha") };
    let sha = String::from_utf8_lossy(sha).to_ascii_lowercase();
    let Some(source) = ctx.scripts.get(&sha).map(|s| s.clone()) else {
        return err("NOSCRIPT No matching script. Please use EVAL.");
    };
    run_with_numkeys(ctx, state, &source, rest, read_only)
}

fn run_with_numkeys(
    ctx: &ServerContext,
    state: &ClientState,
    source: &str,
    rest: &[Bytes],
    read_only: bool,
) -> Outcome {
    let [numkeys_raw, tail @ ..] = rest else { return wrong_args("eval") };
    let numkeys: i64 = match std::str::from_utf8(numkeys_raw).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    if numkeys < 0 {
        return err("ERR Number of keys can't be negative");
    }
    let numkeys = numkeys as usize;
    if tail.len() < numkeys {
        return err("ERR Number of keys can't be greater than number of args");
    }
    let keys = tail[..numkeys].to_vec();
    let argv = tail[numkeys..].to_vec();

    match run_script(ctx, state.db_index, source, keys, argv, read_only) {
        Ok(v) => reply(v),
        Err(msg) => err(msg),
    }
}

pub fn cmd_script(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let [sub, rest @ ..] = args else { return wrong_args("script") };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "LOAD" => {
            let [source] = rest else { return wrong_args("script|load") };
            let sha = sha1_hex(source);
            ctx.scripts.insert(sha.clone(), String::from_utf8_lossy(source).into_owned());
            reply(resp::Value::bulk(sha))
        }
        "EXISTS" => {
            let items = rest
                .iter()
                .map(|s| {
                    let sha = String::from_utf8_lossy(s).to_ascii_lowercase();
                    resp::Value::Integer(ctx.scripts.contains_key(&sha) as i64)
                })
                .collect();
            reply(resp::Value::Array(items))
        }
        "FLUSH" => {
            ctx.scripts.clear();
            reply(resp::Value::ok())
        }
        other => err(format!(
            "ERR Unknown SCRIPT subcommand or wrong number of arguments for '{}'",
            other.to_ascii_lowercase()
        )),
    }
}

fn sha1_hex(data: &[u8]) -> String {
    let digest = Sha1::digest(data);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Deterministic `math.random` seed for a script: first 8 bytes of its own
/// SHA-1, so identical script text always starts from the same sequence.
fn seed_from_source(source: &str) -> u64 {
    let digest = Sha1::digest(source.as_bytes());
    u64::from_le_bytes(digest[..8].try_into().expect("SHA-1 digest is >= 8 bytes"))
}

fn lua_int(v: &LuaValue) -> Option<i64> {
    match v {
        LuaValue::Integer(i) => Some(*i),
        LuaValue::Number(n) => Some(*n as i64),
        _ => None,
    }
}

/// Run `source` in a fresh Lua VM with `KEYS`/`ARGV` bound and `redis.call`/
/// `redis.pcall` wired back into `dispatch()` against a scratch client
/// state scoped to this one script invocation.
fn run_script(
    ctx: &ServerContext,
    db_index: usize,
    source: &str,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    read_only: bool,
) -> Result<resp::Value, String> {
    let lua = Lua::new();
    let mut scratch = ClientState::with_id(0, false);
    scratch.db_index = db_index;
    let scratch = RefCell::new(scratch);
    // Same script text -> same random sequence; see this module's doc
    // comment on determinism for why that's the bar here, not "identical
    // across primary and replica" (we never re-run the script at all).
    let rng = RefCell::new(StdRng::seed_from_u64(seed_from_source(source)));

    let outcome = lua.scope(|scope| {
        let globals = lua.globals();

        let keys_table = lua.create_table()?;
        for (i, k) in keys.iter().enumerate() {
            keys_table.set(i + 1, lua.create_string(k)?)?;
        }
        globals.set("KEYS", keys_table)?;

        let argv_table = lua.create_table()?;
        for (i, a) in argv.iter().enumerate() {
            argv_table.set(i + 1, lua.create_string(a)?)?;
        }
        globals.set("ARGV", argv_table)?;

        // Sandboxing: no wall-clock reads, no file/process access.
        globals.set("os", LuaValue::Nil)?;
        globals.set("io", LuaValue::Nil)?;

        let math_table: mlua::Table = globals.get("math")?;
        math_table.set(
            "random",
            scope.create_function(move |_, args: Variadic<LuaValue>| {
                let mut rng = rng.borrow_mut();
                match args.as_slice() {
                    [] => Ok(LuaValue::Number(rng.gen::<f64>())),
                    [m] => {
                        let m = lua_int(m)
                            .filter(|m| *m >= 1)
                            .ok_or_else(|| mlua::Error::RuntimeError("bad argument #1 to 'random'".into()))?;
                        Ok(LuaValue::Integer(rng.gen_range(1..=m)))
                    }
                    [lo, hi] => {
                        let (lo, hi) = (lua_int(lo), lua_int(hi));
                        let (Some(lo), Some(hi)) = (lo, hi) else {
                            return Err(mlua::Error::RuntimeError("bad argument to 'random'".into()));
                        };
                        if lo > hi {
                            return Err(mlua::Error::RuntimeError(
                                "bad argument #2 to 'random' (interval is empty)".into(),
                            ));
                        }
                        Ok(LuaValue::Integer(rng.gen_range(lo..=hi)))
                    }
                    _ => Err(mlua::Error::RuntimeError("wrong number of arguments".into())),
                }
            })?,
        )?;
        math_table.set(
            "randomseed",
            // Our RNG is already deterministically seeded from the script's
            // own source; accept (and ignore) the call so scripts that call
            // it the way real Redis expects don't just error out.
            scope.create_function(|_, _: Variadic<LuaValue>| Ok(()))?,
        )?;

        let redis_table = lua.create_table()?;
        redis_table.set(
            "call",
            scope.create_function(|lua_ctx, args: Variadic<LuaValue>| {
                let req = lua_args_to_request(&args)?;
                match call_inner(ctx, &scratch, req, read_only) {
                    Ok(v) => resp_to_lua(lua_ctx, &v),
                    Err(msg) => Err(mlua::Error::RuntimeError(msg)),
                }
            })?,
        )?;
        redis_table.set(
            "pcall",
            scope.create_function(|lua_ctx, args: Variadic<LuaValue>| {
                let req = match lua_args_to_request(&args) {
                    Ok(r) => r,
                    Err(e) => return error_table(lua_ctx, &e.to_string()),
                };
                match call_inner(ctx, &scratch, req, read_only) {
                    Ok(v) => resp_to_lua(lua_ctx, &v),
                    Err(msg) => error_table(lua_ctx, &msg),
                }
            })?,
        )?;
        redis_table
            .set("sha1hex", scope.create_function(|_, s: mlua::String| Ok(sha1_hex(s.as_bytes())))?)?;
        globals.set("redis", redis_table)?;

        lua.load(source).eval::<LuaValue>()
    });

    match outcome {
        Ok(v) => Ok(lua_to_resp(v)),
        Err(e) => Err(format_script_error(&e)),
    }
}

fn clean_error(e: &mlua::Error) -> String {
    // mlua wraps our RuntimeError messages with extra "runtime error: "
    // prefixes and, for load/parse errors, a multi-line stack traceback.
    // RESP error replies are a single line (no embedded CR/LF), so keep
    // only the first line of whichever message mlua gives us.
    let raw = e.to_string();
    let first_line = raw.lines().next().unwrap_or(&raw);
    first_line.strip_prefix("runtime error: ").unwrap_or(first_line).to_string()
}

/// `call_inner`'s errors (propagated through Lua as a `RuntimeError`) are
/// already proper `"CODE message"` Redis errors (e.g. `"WRONGTYPE ..."`,
/// `"ERR ..."`); genuine Lua errors (syntax errors, a script's own
/// `error("...")`) aren't, so give those a generic `ERR` prefix. Avoids
/// double-prefixing like `"ERR ERR Write commands ..."`.
fn format_script_error(e: &mlua::Error) -> String {
    let msg = clean_error(e);
    let already_has_code = msg
        .split(' ')
        .next()
        .is_some_and(|word| !word.is_empty() && word.chars().all(|c| c.is_ascii_uppercase()));
    if already_has_code {
        msg
    } else {
        format!("ERR {msg}")
    }
}

fn call_inner(
    ctx: &ServerContext,
    scratch: &RefCell<ClientState>,
    req: Vec<Bytes>,
    read_only: bool,
) -> Result<resp::Value, String> {
    let Some(name_bytes) = req.first() else {
        return Err("ERR Please specify at least one argument for this redis lib call".into());
    };
    let name = String::from_utf8_lossy(name_bytes).to_ascii_uppercase();
    if SCRIPT_DISALLOWED.contains(&name.as_str()) {
        return Err(format!(
            "ERR This Redis command is not allowed from script: {}",
            name.to_ascii_lowercase()
        ));
    }
    if read_only && WRITE_COMMANDS.contains(name.as_str()) {
        return Err("ERR Write commands are not allowed from read-only scripts.".into());
    }

    let mut state = scratch.borrow_mut();
    let value = match dispatch(ctx, &mut state, req) {
        Outcome::Reply(v) => v,
        Outcome::Multi(mut vs) => vs.pop().unwrap_or(resp::Value::NullBulk),
        Outcome::CloseConnection(v) => v,
        // GET ... SINGLEFLIGHT can't block inside a script (dispatch() has
        // to stay synchronous there) — treat a would-be wait as a miss.
        Outcome::SingleFlightWait(_) => resp::Value::NullBulk,
        // Neither makes sense mid-script: REPLCONF isn't script-callable
        // (it's in SCRIPT_DISALLOWED... actually it isn't, but scripts have
        // no reason to call it) and WAIT can't block here either.
        Outcome::NoReply => resp::Value::ok(),
        Outcome::WaitForReplicas { .. } => resp::Value::Integer(0),
        // `BLPOP`-family commands can't block inside a script either —
        // treat a would-be block as an immediate timeout, same idea.
        Outcome::BlockWait { on_timeout, .. } => on_timeout,
    };
    match value {
        resp::Value::Error(msg) => Err(msg),
        other => Ok(other),
    }
}

fn lua_args_to_request(args: &[LuaValue]) -> mlua::Result<Vec<Bytes>> {
    args.iter()
        .map(|v| match v {
            LuaValue::String(s) => Ok(Bytes::copy_from_slice(s.as_bytes())),
            LuaValue::Integer(i) => Ok(Bytes::from(i.to_string())),
            LuaValue::Number(n) => Ok(Bytes::from(n.to_string())),
            _ => Err(mlua::Error::RuntimeError(
                "Lua redis lib command arguments must be strings or integers".into(),
            )),
        })
        .collect()
}

fn error_table<'lua>(lua: &'lua Lua, msg: &str) -> mlua::Result<LuaValue<'lua>> {
    let t = lua.create_table()?;
    t.set("err", msg)?;
    Ok(LuaValue::Table(t))
}

/// Redis's reply -> Lua value conversion: status replies become `{ok=...}`
/// tables, nil/false replies become Lua `false`, everything else maps
/// structurally (arrays/sets/pushes -> 1-indexed tables, maps -> flattened
/// 1-indexed tables).
fn resp_to_lua<'lua>(lua: &'lua Lua, value: &resp::Value) -> mlua::Result<LuaValue<'lua>> {
    use resp::Value as V;
    Ok(match value {
        V::Simple(s) => {
            let t = lua.create_table()?;
            t.set("ok", s.clone())?;
            LuaValue::Table(t)
        }
        V::Error(e) => return Err(mlua::Error::RuntimeError(e.clone())),
        V::Integer(i) => LuaValue::Integer(*i as mlua::Integer),
        V::Bulk(b) => LuaValue::String(lua.create_string(b)?),
        V::NullBulk | V::NullArray | V::Null => LuaValue::Boolean(false),
        V::Array(items) | V::Set(items) | V::Push(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                t.set(i + 1, resp_to_lua(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
        V::Map(pairs) => {
            let t = lua.create_table()?;
            let mut i = 1;
            for (k, v) in pairs {
                t.set(i, resp_to_lua(lua, k)?)?;
                i += 1;
                t.set(i, resp_to_lua(lua, v)?)?;
                i += 1;
            }
            LuaValue::Table(t)
        }
        V::Boolean(b) => LuaValue::Boolean(*b),
        V::Double(d) => LuaValue::Number(*d),
        V::BigNumber(s) => LuaValue::String(lua.create_string(s)?),
    })
}

/// A script's final return value -> Redis reply, per the same conversion
/// table (in reverse): `nil`/`false` -> nil reply, `true` -> `1`, numbers
/// truncate to integers, `{err=...}`/`{ok=...}` tables become error/status
/// replies, other tables become arrays read until the first `nil` hole.
fn lua_to_resp(value: LuaValue) -> resp::Value {
    match value {
        LuaValue::Nil | LuaValue::Boolean(false) => resp::Value::NullBulk,
        LuaValue::Boolean(true) => resp::Value::Integer(1),
        LuaValue::Integer(i) => resp::Value::Integer(i),
        LuaValue::Number(n) => resp::Value::Integer(n as i64),
        LuaValue::String(s) => resp::Value::Bulk(Bytes::copy_from_slice(s.as_bytes())),
        LuaValue::Table(t) => {
            if let Ok(msg) = t.get::<_, mlua::String>("err") {
                return resp::Value::Error(String::from_utf8_lossy(msg.as_bytes()).into_owned());
            }
            if let Ok(msg) = t.get::<_, mlua::String>("ok") {
                return resp::Value::Simple(String::from_utf8_lossy(msg.as_bytes()).into_owned());
            }
            let mut items = Vec::new();
            let mut i = 1;
            loop {
                let v: LuaValue = t.get(i).unwrap_or(LuaValue::Nil);
                if matches!(v, LuaValue::Nil) {
                    break;
                }
                items.push(lua_to_resp(v));
                i += 1;
            }
            resp::Value::Array(items)
        }
        _ => resp::Value::NullBulk,
    }
}
