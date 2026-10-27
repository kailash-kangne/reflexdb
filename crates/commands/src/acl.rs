//! `ACL SETUSER`/`GETUSER`/`DELUSER`/`LIST`/`USERS`/`WHOAMI`/`CAT`/`GENPASS`
//! and the permission checks `dispatch()` runs against the authenticated
//! user on every command. See `docs/07-security.md` §2-3.
//!
//! Scope, deliberately: command-level allow/deny (`+cmd`/`-cmd`/
//! `allcommands`/`nocommands`) and key-glob patterns (`~pattern`/`allkeys`)
//! are enforced. Command **categories** (`+@read`, `+@write`, ...) and
//! read/write-scoped key patterns (`%R~pattern`/`%W~pattern`) are parsed
//! (so `ACL SETUSER` doesn't error on them) but don't do real category or
//! read-vs-write matching — nothing in reflexdb's command table is tagged
//! with categories, and `%R~`/`%W~` patterns are just folded into the same
//! full-access `key_patterns` list as plain `~pattern`. Pub/Sub channel
//! patterns (`&pattern`) and `ACL LOG`/`LOAD`/`SAVE`/`DRYRUN`/`HELP` aren't
//! implemented. Key-pattern enforcement only runs for a known list of
//! single-key commands (`KEY_COMMANDS` below), not every command that
//! happens to take a key as an argument.
//!
//! This is a real subset of Redis/Valkey ACL, not "full ACL" — see
//! `docs/07-security.md` for the honest scope.

use std::collections::HashSet;

use bytes::Bytes;
use dashmap::DashMap;
use rand::Rng;
use resp::Value;
use sha2::{Digest, Sha256};

use crate::dispatch::{err, reply, wrong_args, ClientState, Outcome, ServerContext};
use crate::pattern;

#[derive(Clone, Default)]
pub struct AclUser {
    pub enabled: bool,
    pub nopass: bool,
    pub password_hashes: HashSet<String>,
    pub allkeys: bool,
    pub key_patterns: Vec<String>,
    pub allcommands: bool,
    pub allowed_commands: HashSet<String>,
    pub denied_commands: HashSet<String>,
}

impl AclUser {
    /// The `default` user as it behaves today: equivalent to the plain
    /// `requirepass` flow, so existing single-user setups keep working
    /// unmodified when nobody touches ACL at all.
    pub fn default_user(requirepass: Option<&str>) -> Self {
        Self {
            enabled: true,
            nopass: requirepass.is_none(),
            password_hashes: requirepass
                .map(|p| [sha256_hex(p.as_bytes())].into_iter().collect())
                .unwrap_or_default(),
            allkeys: true,
            key_patterns: Vec::new(),
            allcommands: true,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
        }
    }

    pub fn can_run(&self, command: &str) -> bool {
        if self.denied_commands.contains(command) {
            return false;
        }
        self.allcommands || self.allowed_commands.contains(command)
    }

    pub fn can_access_key(&self, key: &[u8]) -> bool {
        self.allkeys || self.key_patterns.iter().any(|p| pattern::matches(p.as_bytes(), key))
    }
}

pub fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Commands that take a single key as their first argument — the only ones
/// `~pattern` key restrictions are checked against (see module docs).
const KEY_COMMANDS: &[&str] = &[
    "GET",
    "SET",
    "DEL",
    "UNLINK",
    "EXISTS",
    "TTL",
    "PTTL",
    "EXPIRE",
    "PEXPIRE",
    "PERSIST",
    "TYPE",
    "OBJECT",
    "DUMP",
    "RESTORE",
    "LPUSH",
    "RPUSH",
    "LPOP",
    "RPOP",
    "LLEN",
    "LRANGE",
    "HSET",
    "HGET",
    "HDEL",
    "HEXISTS",
    "HLEN",
    "HGETALL",
    "HSCAN",
    "SADD",
    "SREM",
    "SMEMBERS",
    "SISMEMBER",
    "SCARD",
    "SSCAN",
    "ZADD",
    "ZSCORE",
    "ZREM",
    "ZCARD",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZSCAN",
];

pub fn is_key_command(name: &str) -> bool {
    KEY_COMMANDS.contains(&name)
}

fn new_acl_users(requirepass: Option<&str>) -> DashMap<String, AclUser> {
    let users = DashMap::new();
    users.insert("default".to_string(), AclUser::default_user(requirepass));
    users
}

impl ServerContext {
    pub(crate) fn init_acl(requirepass: Option<&str>) -> DashMap<String, AclUser> {
        new_acl_users(requirepass)
    }
}

pub fn cmd_acl(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [sub, rest @ ..] = args else { return wrong_args("acl") };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "WHOAMI" => reply(Value::bulk(state.acl_user.clone())),
        "USERS" => reply(Value::Array(ctx.acl_users.iter().map(|e| Value::bulk(e.key().clone())).collect())),
        "LIST" => reply(Value::Array(
            ctx.acl_users.iter().map(|e| Value::bulk(describe_user(e.key(), e.value()))).collect(),
        )),
        "CAT" => reply(Value::Array(
            [
                "keyspace",
                "read",
                "write",
                "string",
                "list",
                "hash",
                "set",
                "sortedset",
                "connection",
                "admin",
            ]
            .iter()
            .map(|c| Value::bulk(*c))
            .collect(),
        )),
        "GENPASS" => {
            let mut rng = rand::thread_rng();
            let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
            reply(Value::bulk(bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()))
        }
        "GETUSER" => {
            let [name] = rest else { return wrong_args("acl|getuser") };
            let name = String::from_utf8_lossy(name).to_string();
            match ctx.acl_users.get(&name) {
                Some(user) => reply(describe_user_map(&user.value().clone())),
                None => reply(Value::NullBulk),
            }
        }
        "DELUSER" => {
            if rest.is_empty() {
                return wrong_args("acl|deluser");
            }
            let mut removed = 0;
            for name in rest {
                let name = String::from_utf8_lossy(name).to_string();
                if name == "default" {
                    continue; // matches Redis: the default user can't be deleted
                }
                if ctx.acl_users.remove(&name).is_some() {
                    removed += 1;
                }
            }
            reply(Value::Integer(removed))
        }
        "SETUSER" => {
            let [name, tokens @ ..] = rest else { return wrong_args("acl|setuser") };
            let name = String::from_utf8_lossy(name).to_string();
            let mut user = ctx.acl_users.get(&name).map(|u| u.clone()).unwrap_or_default();
            if let Err(e) = apply_setuser_tokens(&mut user, tokens) {
                return err(e);
            }
            ctx.acl_users.insert(name, user);
            reply(Value::ok())
        }
        other => err(format!(
            "ERR Unknown ACL subcommand or wrong number of arguments for '{}'",
            other.to_ascii_lowercase()
        )),
    }
}

fn apply_setuser_tokens(user: &mut AclUser, tokens: &[Bytes]) -> Result<(), String> {
    for token in tokens {
        let text = String::from_utf8_lossy(token);
        match text.as_ref() {
            "on" => user.enabled = true,
            "off" => user.enabled = false,
            "nopass" => {
                user.nopass = true;
                user.password_hashes.clear();
            }
            "resetpass" => {
                user.nopass = false;
                user.password_hashes.clear();
            }
            "allkeys" => {
                user.allkeys = true;
                user.key_patterns.clear();
            }
            "resetkeys" => {
                user.allkeys = false;
                user.key_patterns.clear();
            }
            "allcommands" => {
                user.allcommands = true;
                user.allowed_commands.clear();
                user.denied_commands.clear();
            }
            "nocommands" => {
                user.allcommands = false;
                user.allowed_commands.clear();
                user.denied_commands.clear();
            }
            "reset" => *user = AclUser::default(),
            _ if text.starts_with('>') => {
                user.nopass = false;
                user.password_hashes.insert(sha256_hex(text[1..].as_bytes()));
            }
            _ if text.starts_with('<') => {
                user.password_hashes.remove(&sha256_hex(text[1..].as_bytes()));
            }
            _ if text.starts_with('#') => {
                user.nopass = false;
                user.password_hashes.insert(text[1..].to_ascii_lowercase());
            }
            "!" => {
                user.nopass = false;
                user.password_hashes.clear();
            }
            _ if text.starts_with('~') => {
                user.allkeys = false;
                user.key_patterns.push(text[1..].to_string());
            }
            _ if text.starts_with("%RW~") => {
                user.allkeys = false;
                user.key_patterns.push(text[4..].to_string());
            }
            _ if text.starts_with("%R~") || text.starts_with("%W~") => {
                // Read-only/write-only key patterns: accepted (so `SETUSER`
                // doesn't error on otherwise-valid syntax) but, like command
                // categories, not enforced separately — treated as full
                // (read+write) key access on this pattern. See module docs.
                user.allkeys = false;
                user.key_patterns.push(text[3..].to_string());
            }
            _ if text.starts_with("+@") || text.starts_with("-@") => {
                // Command categories: parsed but not enforced — see module docs.
            }
            _ if text.starts_with('+') => {
                let cmd = text[1..].to_ascii_uppercase();
                user.denied_commands.remove(&cmd);
                user.allowed_commands.insert(cmd);
            }
            _ if text.starts_with('-') => {
                let cmd = text[1..].to_ascii_uppercase();
                user.allowed_commands.remove(&cmd);
                user.denied_commands.insert(cmd);
            }
            _ if text.starts_with('&') => {
                // Pub/Sub channel patterns: not enforced yet — see module docs.
            }
            other => return Err(format!("ERR Error in ACL SETUSER modifier '{other}'")),
        }
    }
    Ok(())
}

fn describe_user(name: &str, user: &AclUser) -> String {
    let status = if user.enabled { "on" } else { "off" };
    let pass = if user.nopass {
        "nopass".to_string()
    } else {
        user.password_hashes.iter().map(|h| format!("#{h}")).collect::<Vec<_>>().join(" ")
    };
    let keys = if user.allkeys {
        "~*".to_string()
    } else {
        user.key_patterns.iter().map(|p| format!("~{p}")).collect::<Vec<_>>().join(" ")
    };
    let commands = if user.allcommands {
        "+@all".to_string()
    } else {
        let mut parts: Vec<String> =
            user.allowed_commands.iter().map(|c| format!("+{}", c.to_ascii_lowercase())).collect();
        parts.extend(user.denied_commands.iter().map(|c| format!("-{}", c.to_ascii_lowercase())));
        if parts.is_empty() {
            "-@all".to_string()
        } else {
            parts.join(" ")
        }
    };
    format!("user {name} {status} {pass} {keys} {commands}")
}

fn describe_user_map(user: &AclUser) -> Value {
    let mut flags = vec![Value::bulk(if user.enabled { "on" } else { "off" })];
    if user.allkeys {
        flags.push(Value::bulk("allkeys"));
    }
    if user.nopass {
        flags.push(Value::bulk("nopass"));
    }
    Value::Map(vec![
        (Value::bulk("flags"), Value::Array(flags)),
        (
            Value::bulk("passwords"),
            Value::Array(user.password_hashes.iter().map(|h| Value::bulk(h.clone())).collect()),
        ),
        (
            Value::bulk("commands"),
            Value::bulk(if user.allcommands { "+@all".to_string() } else { "-@all".to_string() }),
        ),
        (
            Value::bulk("keys"),
            Value::bulk(if user.allkeys {
                "~*".to_string()
            } else {
                user.key_patterns.iter().map(|p| format!("~{p}")).collect::<Vec<_>>().join(" ")
            }),
        ),
        (Value::bulk("channels"), Value::bulk("")),
    ])
}
