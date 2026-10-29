//! `CLIENT` — connection introspection/management, plus `CLIENT TRACKING`
//! (client-side caching invalidation). See `ServerContext::tracking_table`'s
//! docs for the scope of what's tracked.
//!
//! Scoped down: `CLIENT KILL`/`PAUSE`/`UNPAUSE`/`REPLY` are
//! not implemented (they'd need the connection loop to reach back into a
//! *different* connection's socket, which `dispatch()`'s `&ServerContext`
//! can't do); `CLIENT LIST`/`INFO` report a plausible-but-simplified line
//! (no per-connection address/db-index/age tracking yet).

use bytes::Bytes;
use resp::{Protocol, Value};

use crate::dispatch::{err, reply, wrong_args, ClientState, Outcome, ServerContext};

pub fn cmd_client(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let Some(sub) = args.first() else { return wrong_args("client") };
    let sub = String::from_utf8_lossy(sub).to_ascii_uppercase();
    let rest = &args[1..];

    match sub.as_str() {
        "ID" => reply(Value::Integer(state.id as i64)),
        "GETNAME" => reply(Value::bulk(state.client_name.clone())),
        "SETNAME" => {
            let [name] = rest else { return wrong_args("client|setname") };
            if name.iter().any(|b| b.is_ascii_whitespace()) {
                return err("ERR Client names cannot contain spaces, newlines or special characters.");
            }
            let name = String::from_utf8_lossy(name).into_owned();
            if name.is_empty() {
                ctx.client_names.remove(&state.id);
            } else {
                ctx.client_names.insert(state.id, name.clone());
            }
            state.client_name = name;
            reply(Value::ok())
        }
        "LIST" => reply(Value::bulk(client_list_line(ctx))),
        "INFO" => reply(Value::bulk(one_client_line(ctx, state))),
        "NO-EVICT" | "NO-TOUCH" => match rest {
            [flag] if flag.eq_ignore_ascii_case(b"ON") || flag.eq_ignore_ascii_case(b"OFF") => {
                reply(Value::ok())
            }
            _ => err("ERR syntax error"),
        },
        "TRACKING" => cmd_tracking(state, rest),
        "UNPAUSE" => reply(Value::ok()),
        _ => err(format!(
            "ERR Unknown CLIENT subcommand or wrong number of arguments for '{}'",
            sub.to_ascii_lowercase()
        )),
    }
}

fn cmd_tracking(state: &mut ClientState, rest: &[Bytes]) -> Outcome {
    let Some(mode) = rest.first() else { return wrong_args("client|tracking") };
    let on = if mode.eq_ignore_ascii_case(b"ON") {
        true
    } else if mode.eq_ignore_ascii_case(b"OFF") {
        false
    } else {
        return err("ERR syntax error");
    };
    if on && state.protocol != Protocol::Resp3 {
        return err("ERR Client tracking is only supported in RESP3 mode (see `HELLO 3`)");
    }
    // `BCAST`/`PREFIX`/`REDIRECT`/`OPTIN`/`OPTOUT`/`NOLOOP` aren't
    // implemented — accepted syntactically (so clients that always send
    // them don't just fail outright) but have no effect beyond plain ON/OFF.
    state.tracking = on;
    reply(Value::ok())
}

fn one_client_line(_ctx: &ServerContext, state: &ClientState) -> String {
    format!(
        "id={} addr=? laddr=? fd=? name={} age=0 idle=0 flags=N db={} sub=0 psub=0 ssub=0 multi=-1 \
         watch={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 \
         events=r cmd=client|info user={} redir=-1 resp={} tot-net-in=0 tot-net-out=0",
        state.id,
        state.client_name,
        state.db_index,
        state.watched.len(),
        state.acl_user,
        if state.protocol == Protocol::Resp3 { 3 } else { 2 },
    )
}

fn client_list_line(ctx: &ServerContext) -> String {
    let mut lines = String::new();
    for entry in ctx.clients.iter() {
        let id = *entry.key();
        let name = ctx.client_names.get(&id).map(|n| n.clone()).unwrap_or_default();
        lines.push_str(&format!(
            "id={id} addr=? laddr=? fd=? name={name} age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 \
             multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=0 rbs=0 rbp=0 obl=0 \
             oll=0 omem=0 events=r cmd=NULL user=default redir=-1 resp=2 tot-net-in=0 tot-net-out=0\n"
        ));
    }
    lines
}
