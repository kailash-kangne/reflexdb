//! `PUBLISH`/`SUBSCRIBE`/`PSUBSCRIBE`/`PUBSUB` — classic Redis pub/sub.
//! (The sharded `SPUBLISH`/`SSUBSCRIBE`/`SUNSUBSCRIBE` variants are a
//! cluster-mode feature and are deliberately deferred — see Phase 7.)

use std::collections::HashSet;

use bytes::Bytes;
use resp::Value;

use crate::dispatch::{reply, wrong_args, ClientState, Outcome, ServerContext};
use crate::pattern;

pub fn cmd_subscribe(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("subscribe");
    }
    let mut replies = Vec::with_capacity(args.len());
    for channel in args {
        ctx.channels.entry(channel.clone()).or_default().insert(state.id);
        state.subscriptions += 1;
        replies.push(confirmation("subscribe", channel, state.subscriptions));
    }
    Outcome::Multi(replies)
}

pub fn cmd_unsubscribe(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let channels: Vec<Bytes> = if args.is_empty() {
        ctx.channels.iter().filter(|e| e.value().contains(&state.id)).map(|e| e.key().clone()).collect()
    } else {
        args.to_vec()
    };
    if channels.is_empty() {
        return Outcome::Multi(vec![confirmation("unsubscribe", b"", state.subscriptions)]);
    }
    let mut replies = Vec::with_capacity(channels.len());
    for channel in &channels {
        let mut now_empty = false;
        if let Some(mut subs) = ctx.channels.get_mut(channel.as_ref()) {
            if subs.remove(&state.id) {
                state.subscriptions = state.subscriptions.saturating_sub(1);
            }
            now_empty = subs.is_empty();
        }
        if now_empty {
            ctx.channels.remove(channel.as_ref());
        }
        replies.push(confirmation("unsubscribe", channel, state.subscriptions));
    }
    Outcome::Multi(replies)
}

pub fn cmd_psubscribe(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("psubscribe");
    }
    let mut replies = Vec::with_capacity(args.len());
    for pattern in args {
        ctx.patterns.entry(pattern.clone()).or_default().insert(state.id);
        state.subscriptions += 1;
        replies.push(confirmation("psubscribe", pattern, state.subscriptions));
    }
    Outcome::Multi(replies)
}

pub fn cmd_punsubscribe(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let patterns: Vec<Bytes> = if args.is_empty() {
        ctx.patterns.iter().filter(|e| e.value().contains(&state.id)).map(|e| e.key().clone()).collect()
    } else {
        args.to_vec()
    };
    if patterns.is_empty() {
        return Outcome::Multi(vec![confirmation("punsubscribe", b"", state.subscriptions)]);
    }
    let mut replies = Vec::with_capacity(patterns.len());
    for pat in &patterns {
        let mut now_empty = false;
        if let Some(mut subs) = ctx.patterns.get_mut(pat.as_ref()) {
            if subs.remove(&state.id) {
                state.subscriptions = state.subscriptions.saturating_sub(1);
            }
            now_empty = subs.is_empty();
        }
        if now_empty {
            ctx.patterns.remove(pat.as_ref());
        }
        replies.push(confirmation("punsubscribe", pat, state.subscriptions));
    }
    Outcome::Multi(replies)
}

pub fn cmd_publish(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let [channel, message] = args else { return wrong_args("publish") };
    reply(Value::Integer(ctx.publish(channel, message.clone()) as i64))
}

pub fn cmd_pubsub(ctx: &ServerContext, args: &[Bytes]) -> Outcome {
    let [sub, rest @ ..] = args else { return wrong_args("pubsub") };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "CHANNELS" => {
            let pat = rest.first();
            let names: Vec<Value> = ctx
                .channels
                .iter()
                .filter(|e| !e.value().is_empty())
                .filter(|e| pat.map(|p| pattern::matches(p, e.key())).unwrap_or(true))
                .map(|e| Value::bulk(e.key().clone()))
                .collect();
            reply(Value::Array(names))
        }
        "NUMSUB" => {
            let mut out = Vec::with_capacity(rest.len() * 2);
            for channel in rest {
                let n = ctx.channels.get(channel.as_ref()).map(|s| s.len()).unwrap_or(0);
                out.push(Value::bulk(channel.clone()));
                out.push(Value::Integer(n as i64));
            }
            reply(Value::Array(out))
        }
        "NUMPAT" => reply(Value::Integer(ctx.patterns.len() as i64)),
        _ => wrong_args("pubsub"),
    }
}

fn confirmation(kind: &'static str, channel: &[u8], count: usize) -> Value {
    Value::Push(vec![
        Value::bulk(kind.to_string()),
        Value::bulk(channel.to_vec()),
        Value::Integer(count as i64),
    ])
}

impl ServerContext {
    /// Deliver `message` on `channel` to every direct subscriber and every
    /// pattern subscriber whose pattern matches. Returns the receiver count.
    pub(crate) fn publish(&self, channel: &[u8], message: Bytes) -> usize {
        let mut delivered: HashSet<u64> = HashSet::new();

        if let Some(subs) = self.channels.get(channel) {
            let payload = Value::Push(vec![
                Value::bulk(&b"message"[..]),
                Value::bulk(channel.to_vec()),
                Value::bulk(message.clone()),
            ]);
            for &client_id in subs.iter() {
                self.push_to(client_id, payload.clone());
                delivered.insert(client_id);
            }
        }

        for entry in self.patterns.iter() {
            if !pattern::matches(entry.key(), channel) {
                continue;
            }
            let payload = Value::Push(vec![
                Value::bulk(&b"pmessage"[..]),
                Value::bulk(entry.key().clone()),
                Value::bulk(channel.to_vec()),
                Value::bulk(message.clone()),
            ]);
            for &client_id in entry.value().iter() {
                self.push_to(client_id, payload.clone());
                delivered.insert(client_id);
            }
        }

        delivered.len()
    }
}
