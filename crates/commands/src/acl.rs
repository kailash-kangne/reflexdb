//! `ACL SETUSER`/`GETUSER`/`DELUSER`/`LIST`/`USERS`/`WHOAMI`/`CAT`/`GENPASS`
//! and the permission checks `dispatch()` runs against the authenticated
//! user on every command. See `docs/SECURITY.md`.
//!
//! Enforced: command-level allow/deny (`+cmd`/`-cmd`/`allcommands`/
//! `nocommands`), command categories (`+@read`, `-@write`, ... — see
//! `command_categories`), key globs (`~pattern`/`allkeys`), and
//! read/write-scoped key patterns (`%R~pattern`/`%W~pattern`).
//! Rule precedence: explicit `+cmd`/`-cmd` beats categories, categories beat
//! `allcommands`/`nocommands`; this differs from Redis's last-token-wins
//! order but keeps `+get -@read` (grant one read) unsurprising.
//!
//! Rejected rather than silently accepted: pub/sub channel patterns
//! (`&pattern`), `(...)` selectors, and unknown `+@category` names — a
//! configuration that can't be enforced must fail loudly.
//! `ACL LOG`/`DRYRUN`/`HELP` aren't implemented.
//!
//! Persistence: the `aclfile` config directive names a file in Redis's
//! aclfile format (`user <name> <rules>` per line — exactly what
//! `describe_user` emits). It's loaded once at startup; `ACL SAVE` writes
//! the live table back to it and `ACL LOAD` swaps the table with the
//! file's contents. A file without a `default` user gets a disabled one —
//! a missing `default` must never fail open.
//!
//! This is a subset of the ACL command surface, not "full ACL" — see
//! `docs/SECURITY.md` for the honest scope.

use std::collections::HashSet;
use std::path::Path;

use bytes::{Bytes, BytesMut};
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
    /// `~pattern` — read and write access.
    pub key_patterns: Vec<String>,
    /// `%R~pattern` — read-only access.
    pub read_patterns: Vec<String>,
    /// `%W~pattern` — write-only access.
    pub write_patterns: Vec<String>,
    pub allcommands: bool,
    pub allowed_commands: HashSet<String>,
    pub denied_commands: HashSet<String>,
    pub allowed_categories: u32,
    pub denied_categories: u32,
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
            read_patterns: Vec::new(),
            write_patterns: Vec::new(),
            allcommands: true,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            allowed_categories: 0,
            denied_categories: 0,
        }
    }

    /// Explicit `+cmd`/`-cmd` rules win over categories; a denied category
    /// still blocks `allcommands`. See module docs.
    pub fn can_run(&self, command: &str) -> bool {
        if self.denied_commands.contains(command) {
            return false;
        }
        if self.allowed_commands.contains(command) {
            return true;
        }
        // The overwhelmingly common shape (`allcommands`, no denied
        // categories) skips the category-table lookup entirely.
        if self.allcommands && self.denied_categories == 0 {
            return true;
        }
        let cats = command_categories(command);
        if cats & self.denied_categories != 0 {
            return false;
        }
        self.allcommands || cats & self.allowed_categories != 0
    }

    pub fn can_access_key(&self, key: &[u8], write: bool) -> bool {
        let scoped = if write { &self.write_patterns } else { &self.read_patterns };
        self.allkeys
            || self.key_patterns.iter().chain(scoped.iter()).any(|p| pattern::matches(p.as_bytes(), key))
    }
}

pub fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Command categories understood by `+@name`/`-@name`. `@all` is special.
pub mod cat {
    pub const READ: u32 = 1 << 0;
    pub const WRITE: u32 = 1 << 1;
    pub const KEYSPACE: u32 = 1 << 2;
    pub const GENERIC: u32 = 1 << 3;
    pub const ADMIN: u32 = 1 << 4;
    pub const DANGEROUS: u32 = 1 << 5;
    pub const CONNECTION: u32 = 1 << 6;
    pub const PUBSUB: u32 = 1 << 7;
    pub const SCRIPTING: u32 = 1 << 8;
    pub const TRANSACTION: u32 = 1 << 9;
    pub const BLOCKING: u32 = 1 << 10;
    pub const SLOW: u32 = 1 << 11;
    pub const FAST: u32 = 1 << 12;
    pub const STRING: u32 = 1 << 13;
    pub const HASH: u32 = 1 << 14;
    pub const LIST: u32 = 1 << 15;
    pub const SET: u32 = 1 << 16;
    pub const SORTEDSET: u32 = 1 << 17;
    pub const STREAM: u32 = 1 << 18;
    pub const ALL: u32 = (1 << 19) - 1;
}

/// `+@name` → category bits; `None` for names we don't enforce so
/// `SETUSER` rejects them instead of silently granting nothing.
pub fn category_mask(name: &str) -> Option<u32> {
    use cat::*;
    Some(match name {
        "all" => ALL,
        "read" => READ,
        "write" => WRITE,
        "keyspace" => KEYSPACE,
        "generic" => GENERIC,
        "admin" => ADMIN,
        "dangerous" => DANGEROUS,
        "connection" => CONNECTION,
        "pubsub" => PUBSUB,
        "scripting" => SCRIPTING,
        "transaction" => TRANSACTION,
        "blocking" => BLOCKING,
        "slow" => SLOW,
        "fast" => FAST,
        "string" => STRING,
        "hash" => HASH,
        "list" => LIST,
        "set" => SET,
        "sortedset" => SORTEDSET,
        "stream" => STREAM,
        _ => return None,
    })
}

/// Inverse of `category_mask` for `ACL CAT`/`GETUSER` output.
pub fn category_names(mask: u32) -> Vec<&'static str> {
    [
        ("read", cat::READ),
        ("write", cat::WRITE),
        ("keyspace", cat::KEYSPACE),
        ("generic", cat::GENERIC),
        ("admin", cat::ADMIN),
        ("dangerous", cat::DANGEROUS),
        ("connection", cat::CONNECTION),
        ("pubsub", cat::PUBSUB),
        ("scripting", cat::SCRIPTING),
        ("transaction", cat::TRANSACTION),
        ("blocking", cat::BLOCKING),
        ("slow", cat::SLOW),
        ("fast", cat::FAST),
        ("string", cat::STRING),
        ("hash", cat::HASH),
        ("list", cat::LIST),
        ("set", cat::SET),
        ("sortedset", cat::SORTEDSET),
        ("stream", cat::STREAM),
    ]
    .into_iter()
    .filter_map(|(name, bit)| (mask & bit != 0).then_some(name))
    .collect()
}

/// Every dispatched command's category bits — the single source used by
/// both `command_categories` and `ACL CAT <category>`.
const COMMAND_CATEGORY_TABLE: &[(&str, u32)] = &[
    // connection
    ("PING", cat::CONNECTION | cat::FAST),
    ("ECHO", cat::CONNECTION | cat::FAST),
    ("QUIT", cat::CONNECTION | cat::FAST),
    ("AUTH", cat::CONNECTION | cat::FAST),
    ("HELLO", cat::CONNECTION | cat::FAST),
    ("RESET", cat::CONNECTION | cat::FAST),
    ("SELECT", cat::CONNECTION | cat::FAST),
    ("TIME", cat::FAST),
    // pub/sub
    ("PUBLISH", cat::PUBSUB | cat::FAST),
    ("SUBSCRIBE", cat::PUBSUB),
    ("PSUBSCRIBE", cat::PUBSUB),
    ("UNSUBSCRIBE", cat::PUBSUB),
    ("PUNSUBSCRIBE", cat::PUBSUB),
    ("PUBSUB", cat::PUBSUB | cat::SLOW),
    ("LIVE", cat::PUBSUB | cat::SLOW),
    ("UNLIVE", cat::PUBSUB | cat::SLOW),
    // transactions
    ("MULTI", cat::TRANSACTION | cat::FAST),
    ("DISCARD", cat::TRANSACTION | cat::FAST),
    ("WATCH", cat::TRANSACTION | cat::FAST),
    ("UNWATCH", cat::TRANSACTION | cat::FAST),
    ("EXEC", cat::TRANSACTION | cat::SLOW),
    // scripting
    ("EVAL", cat::SCRIPTING | cat::WRITE | cat::SLOW),
    ("EVALSHA", cat::SCRIPTING | cat::WRITE | cat::SLOW),
    ("EVAL_RO", cat::SCRIPTING | cat::READ | cat::SLOW),
    ("EVALSHA_RO", cat::SCRIPTING | cat::READ | cat::SLOW),
    ("SCRIPT", cat::SCRIPTING | cat::SLOW),
    // server admin / dangerous
    ("CONFIG", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("ACL", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("CLIENT", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("INFO", cat::ADMIN | cat::SLOW),
    ("COMMAND", cat::ADMIN | cat::SLOW),
    ("LATENCY", cat::ADMIN | cat::SLOW),
    ("SLOWLOG", cat::ADMIN | cat::SLOW),
    ("MEMORY", cat::ADMIN | cat::SLOW),
    ("OBJECT", cat::KEYSPACE | cat::READ | cat::SLOW),
    ("DBSIZE", cat::KEYSPACE | cat::READ | cat::FAST),
    ("SHUTDOWN", cat::ADMIN | cat::DANGEROUS),
    ("SAVE", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("BGSAVE", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("BGREWRITEAOF", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("LASTSAVE", cat::ADMIN | cat::FAST),
    ("FLUSHDB", cat::KEYSPACE | cat::WRITE | cat::SLOW | cat::DANGEROUS),
    ("FLUSHALL", cat::KEYSPACE | cat::WRITE | cat::SLOW | cat::DANGEROUS),
    ("REPLICAOF", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("ROLE", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("SLAVEOF", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("REPLCONF", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("PSYNC", cat::ADMIN | cat::SLOW | cat::DANGEROUS),
    ("WAIT", cat::ADMIN | cat::BLOCKING | cat::SLOW),
    ("WAITAOF", cat::ADMIN | cat::BLOCKING | cat::SLOW),
    ("SCHEDULE", cat::ADMIN | cat::WRITE | cat::DANGEROUS),
    // generic keyspace
    ("DEL", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::FAST),
    ("UNLINK", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::FAST),
    ("EXISTS", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::FAST),
    ("TTL", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::FAST),
    ("PTTL", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::FAST),
    ("EXPIRETIME", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::FAST),
    ("PEXPIRETIME", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::FAST),
    ("TYPE", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::FAST),
    ("TOUCH", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::FAST),
    ("EXPIRE", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::FAST),
    ("PEXPIRE", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::FAST),
    ("EXPIREAT", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::FAST),
    ("PEXPIREAT", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::FAST),
    ("PERSIST", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::FAST),
    ("KEYS", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::SLOW | cat::DANGEROUS),
    ("SCAN", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::SLOW),
    ("RANDOMKEY", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::SLOW),
    ("RENAME", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::SLOW),
    ("RENAMENX", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::SLOW),
    ("COPY", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::SLOW),
    ("MOVE", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::FAST),
    ("DUMP", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::SLOW),
    ("RESTORE", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::SLOW | cat::DANGEROUS),
    ("SORT", cat::GENERIC | cat::WRITE | cat::SLOW | cat::DANGEROUS),
    ("SORT_RO", cat::GENERIC | cat::READ | cat::SLOW),
    ("GETAT", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::SLOW),
    ("HISTORY", cat::KEYSPACE | cat::GENERIC | cat::READ | cat::SLOW),
    // `RUN RESTORE` wipes a key prefix, so the whole family is dangerous.
    ("RUN", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::SLOW | cat::DANGEROUS),
    ("IDEMPOTENT", cat::KEYSPACE | cat::GENERIC | cat::WRITE | cat::SLOW),
    // strings
    ("GET", cat::STRING | cat::READ | cat::FAST),
    ("STRLEN", cat::STRING | cat::READ | cat::FAST),
    ("GETBIT", cat::STRING | cat::READ | cat::FAST),
    ("GETEX", cat::STRING | cat::READ | cat::FAST),
    ("BITFIELD_RO", cat::STRING | cat::READ | cat::FAST),
    ("GETRANGE", cat::STRING | cat::READ | cat::SLOW),
    ("MGET", cat::STRING | cat::READ | cat::SLOW),
    ("BITCOUNT", cat::STRING | cat::READ | cat::SLOW),
    ("BITPOS", cat::STRING | cat::READ | cat::SLOW),
    ("SET", cat::STRING | cat::WRITE | cat::FAST),
    ("SETEX", cat::STRING | cat::WRITE | cat::FAST),
    ("PSETEX", cat::STRING | cat::WRITE | cat::FAST),
    ("SETNX", cat::STRING | cat::WRITE | cat::FAST),
    ("APPEND", cat::STRING | cat::WRITE | cat::FAST),
    ("INCR", cat::STRING | cat::WRITE | cat::FAST),
    ("INCRBY", cat::STRING | cat::WRITE | cat::FAST),
    ("DECR", cat::STRING | cat::WRITE | cat::FAST),
    ("DECRBY", cat::STRING | cat::WRITE | cat::FAST),
    ("INCRBYFLOAT", cat::STRING | cat::WRITE | cat::FAST),
    ("GETDEL", cat::STRING | cat::WRITE | cat::FAST),
    ("GETSET", cat::STRING | cat::WRITE | cat::FAST),
    ("SETBIT", cat::STRING | cat::WRITE | cat::FAST),
    ("MSET", cat::STRING | cat::WRITE | cat::SLOW),
    ("MSETNX", cat::STRING | cat::WRITE | cat::SLOW),
    ("SETRANGE", cat::STRING | cat::WRITE | cat::SLOW),
    ("BITFIELD", cat::STRING | cat::WRITE | cat::SLOW),
    ("BITOP", cat::STRING | cat::WRITE | cat::SLOW | cat::DANGEROUS),
    // hashes
    ("HGET", cat::HASH | cat::READ | cat::FAST),
    ("HEXISTS", cat::HASH | cat::READ | cat::FAST),
    ("HLEN", cat::HASH | cat::READ | cat::FAST),
    ("HMGET", cat::HASH | cat::READ | cat::FAST),
    ("HSTRLEN", cat::HASH | cat::READ | cat::FAST),
    ("HRANDFIELD", cat::HASH | cat::READ | cat::FAST),
    ("HGETALL", cat::HASH | cat::READ | cat::SLOW),
    ("HKEYS", cat::HASH | cat::READ | cat::SLOW),
    ("HVALS", cat::HASH | cat::READ | cat::SLOW),
    ("HSCAN", cat::HASH | cat::READ | cat::SLOW),
    ("HSET", cat::HASH | cat::WRITE | cat::FAST),
    ("HMSET", cat::HASH | cat::WRITE | cat::FAST),
    ("HSETNX", cat::HASH | cat::WRITE | cat::FAST),
    ("HDEL", cat::HASH | cat::WRITE | cat::FAST),
    ("HINCRBY", cat::HASH | cat::WRITE | cat::FAST),
    ("HINCRBYFLOAT", cat::HASH | cat::WRITE | cat::FAST),
    // lists
    ("LLEN", cat::LIST | cat::READ | cat::FAST),
    ("LINDEX", cat::LIST | cat::READ | cat::FAST),
    ("LRANGE", cat::LIST | cat::READ | cat::SLOW),
    ("LPOS", cat::LIST | cat::READ | cat::SLOW),
    ("LPUSH", cat::LIST | cat::WRITE | cat::FAST),
    ("RPUSH", cat::LIST | cat::WRITE | cat::FAST),
    ("LPUSHX", cat::LIST | cat::WRITE | cat::FAST),
    ("RPUSHX", cat::LIST | cat::WRITE | cat::FAST),
    ("LPOP", cat::LIST | cat::WRITE | cat::FAST),
    ("RPOP", cat::LIST | cat::WRITE | cat::FAST),
    ("LSET", cat::LIST | cat::WRITE | cat::FAST),
    ("LINSERT", cat::LIST | cat::WRITE | cat::FAST),
    ("LMOVE", cat::LIST | cat::WRITE | cat::FAST),
    ("LMPOP", cat::LIST | cat::WRITE | cat::FAST),
    ("LTRIM", cat::LIST | cat::WRITE | cat::SLOW),
    ("LREM", cat::LIST | cat::WRITE | cat::SLOW),
    ("RPOPLPUSH", cat::LIST | cat::WRITE | cat::SLOW),
    ("BLPOP", cat::LIST | cat::WRITE | cat::BLOCKING | cat::SLOW),
    ("BRPOP", cat::LIST | cat::WRITE | cat::BLOCKING | cat::SLOW),
    ("BLMPOP", cat::LIST | cat::WRITE | cat::BLOCKING | cat::SLOW),
    ("BLMOVE", cat::LIST | cat::WRITE | cat::BLOCKING | cat::SLOW),
    // sets
    ("SISMEMBER", cat::SET | cat::READ | cat::FAST),
    ("SMISMEMBER", cat::SET | cat::READ | cat::FAST),
    ("SCARD", cat::SET | cat::READ | cat::FAST),
    ("SRANDMEMBER", cat::SET | cat::READ | cat::FAST),
    ("SINTERCARD", cat::SET | cat::READ | cat::FAST),
    ("SMEMBERS", cat::SET | cat::READ | cat::SLOW),
    ("SUNION", cat::SET | cat::READ | cat::SLOW),
    ("SINTER", cat::SET | cat::READ | cat::SLOW),
    ("SDIFF", cat::SET | cat::READ | cat::SLOW),
    ("SSCAN", cat::SET | cat::READ | cat::SLOW),
    ("SADD", cat::SET | cat::WRITE | cat::FAST),
    ("SREM", cat::SET | cat::WRITE | cat::FAST),
    ("SPOP", cat::SET | cat::WRITE | cat::FAST),
    ("SMOVE", cat::SET | cat::WRITE | cat::FAST),
    ("SUNIONSTORE", cat::SET | cat::WRITE | cat::SLOW),
    ("SINTERSTORE", cat::SET | cat::WRITE | cat::SLOW),
    ("SDIFFSTORE", cat::SET | cat::WRITE | cat::SLOW),
    // sorted sets
    ("ZSCORE", cat::SORTEDSET | cat::READ | cat::FAST),
    ("ZMSCORE", cat::SORTEDSET | cat::READ | cat::FAST),
    ("ZCARD", cat::SORTEDSET | cat::READ | cat::FAST),
    ("ZRANK", cat::SORTEDSET | cat::READ | cat::FAST),
    ("ZREVRANK", cat::SORTEDSET | cat::READ | cat::FAST),
    ("ZCOUNT", cat::SORTEDSET | cat::READ | cat::FAST),
    ("ZLEXCOUNT", cat::SORTEDSET | cat::READ | cat::FAST),
    ("ZRANDMEMBER", cat::SORTEDSET | cat::READ | cat::FAST),
    ("ZRANGE", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZRANGEBYSCORE", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZREVRANGE", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZREVRANGEBYSCORE", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZRANGEBYLEX", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZREVRANGEBYLEX", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZSCAN", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZDIFF", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZINTER", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZINTERCARD", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZUNION", cat::SORTEDSET | cat::READ | cat::SLOW),
    ("ZADD", cat::SORTEDSET | cat::WRITE | cat::FAST),
    ("ZINCRBY", cat::SORTEDSET | cat::WRITE | cat::FAST),
    ("ZREM", cat::SORTEDSET | cat::WRITE | cat::FAST),
    ("ZPOPMIN", cat::SORTEDSET | cat::WRITE | cat::FAST),
    ("ZPOPMAX", cat::SORTEDSET | cat::WRITE | cat::FAST),
    ("ZMPOP", cat::SORTEDSET | cat::WRITE | cat::FAST),
    ("ZREMRANGEBYSCORE", cat::SORTEDSET | cat::WRITE | cat::SLOW),
    ("ZREMRANGEBYRANK", cat::SORTEDSET | cat::WRITE | cat::SLOW),
    ("ZREMRANGEBYLEX", cat::SORTEDSET | cat::WRITE | cat::SLOW),
    ("ZRANGESTORE", cat::SORTEDSET | cat::WRITE | cat::SLOW),
    ("ZDIFFSTORE", cat::SORTEDSET | cat::WRITE | cat::SLOW),
    ("ZINTERSTORE", cat::SORTEDSET | cat::WRITE | cat::SLOW),
    ("ZUNIONSTORE", cat::SORTEDSET | cat::WRITE | cat::SLOW),
    ("BZPOPMIN", cat::SORTEDSET | cat::WRITE | cat::BLOCKING | cat::SLOW),
    ("BZPOPMAX", cat::SORTEDSET | cat::WRITE | cat::BLOCKING | cat::SLOW),
    ("BZMPOP", cat::SORTEDSET | cat::WRITE | cat::BLOCKING | cat::SLOW),
    // streams
    ("XLEN", cat::STREAM | cat::READ | cat::FAST),
    ("XPENDING", cat::STREAM | cat::READ | cat::FAST),
    ("XINFO", cat::STREAM | cat::READ | cat::FAST),
    ("XRANGE", cat::STREAM | cat::READ | cat::SLOW),
    ("XREVRANGE", cat::STREAM | cat::READ | cat::SLOW),
    ("XREAD", cat::STREAM | cat::READ | cat::BLOCKING | cat::SLOW),
    ("XADD", cat::STREAM | cat::WRITE | cat::SLOW),
    ("XACK", cat::STREAM | cat::WRITE | cat::SLOW),
    ("XAUTOCLAIM", cat::STREAM | cat::WRITE | cat::SLOW),
    ("XCLAIM", cat::STREAM | cat::WRITE | cat::SLOW),
    ("XDEL", cat::STREAM | cat::WRITE | cat::SLOW),
    ("XGROUP", cat::STREAM | cat::WRITE | cat::SLOW),
    ("XTRIM", cat::STREAM | cat::WRITE | cat::SLOW),
    ("XREADGROUP", cat::STREAM | cat::WRITE | cat::BLOCKING | cat::SLOW),
];

/// `COMMAND_CATEGORY_TABLE` as a map — `command_categories` runs on every
/// command for users with real ACL rules, so the linear scan over ~250
/// entries per call is wasted work.
static COMMAND_CATEGORY_MAP: std::sync::LazyLock<std::collections::HashMap<&'static str, u32>> =
    std::sync::LazyLock::new(|| COMMAND_CATEGORY_TABLE.iter().copied().collect());

/// Category bits for `name`. Commands not in the table get `0`: categories
/// neither grant nor deny them (unknown commands anyway, or reachable only
/// via explicit `+cmd`/`allcommands`).
pub fn command_categories(name: &str) -> u32 {
    COMMAND_CATEGORY_MAP.get(name).copied().unwrap_or(0)
}

/// Commands whose key operand is in the first argument position. Multi-key
/// and non-leading-key layouts are handled explicitly by `command_keys`.
const KEY_COMMANDS: &[&str] = &[
    "APPEND",
    "BITCOUNT",
    "BITFIELD",
    "BITFIELD_RO",
    "BITPOS",
    "DECR",
    "DECRBY",
    "EXPIRETIME",
    "GETDEL",
    "GETEX",
    "GETRANGE",
    "GETSET",
    "GETAT",
    "HISTORY",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "PEXPIRETIME",
    "PEXPIREAT",
    "RENAME",
    "RENAMENX",
    "SETRANGE",
    "SETEX",
    "SETNX",
    "PSETEX",
    "STRLEN",
    "TOUCH",
    "UNLINK",
    "WATCH",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XCLAIM",
    "XDEL",
    "XGROUP",
    "XLEN",
    "XPENDING",
    "XRANGE",
    "XREVRANGE",
    "XTRIM",
    "ZCOUNT",
    "ZINCRBY",
    "ZLEXCOUNT",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANK",
    "ZREVRANK",
    "ZSCORE",
    "BLPOP",
    "BRPOP",
    "EXPIREAT",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HMGET",
    "HMSET",
    "HRANDFIELD",
    "HSTRLEN",
    "HVALS",
    "LINSERT",
    "LINDEX",
    "LPOS",
    "LREM",
    "LSET",
    "LTRIM",
    "PEXPIREAT",
    "RPOPLPUSH",
    "SETBIT",
    "GETBIT",
    "GETAT",
    "HISTORY",
    "LPUSHX",
    "RPUSHX",
    "LMOVE",
    "LMPOP",
    "LMPOP",
    "ZUNION",
    "ZINTER",
    "ZDIFF",
    "ZINTERCARD",
    "ZUNIONSTORE",
    "ZINTERSTORE",
    "ZDIFFSTORE",
    "ZRANDMEMBER",
    "SRANDMEMBER",
    "SPOP",
    "SDIFFSTORE",
    "SINTERSTORE",
    "SUNIONSTORE",
    "MEMORY",
    "MOVE",
    "XREAD",
    "XREADGROUP",
    "XINFO",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZREVRANGEBYLEX",
    "ZRANGEBYLEX",
    "ZRANGESTORE",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "GET",
    "SET",
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
    "IDEMPOTENT",
];

/// `KEY_COMMANDS` as a set — the `_ =>` fallback in `command_keys` checks
/// membership for every restricted-ACL command.
static KEY_COMMANDS_SET: std::sync::LazyLock<HashSet<&'static str>> =
    std::sync::LazyLock::new(|| KEY_COMMANDS.iter().copied().collect());

/// Return every key operand understood by the ACL key-pattern checks.
/// Commands whose key positions depend on a count are decoded from their
/// own argument layout; option/value operands are never treated as keys.
/// Owned `Bytes` (refcount clones) because `RUN`'s effective key is
/// synthesized from the run id, not an argument.
pub fn command_keys(name: &str, args: &[Bytes]) -> Vec<Bytes> {
    let all = |args: &[Bytes]| args.to_vec();
    match name {
        "DEL" | "UNLINK" | "EXISTS" | "MGET" | "SDIFF" | "SINTER" | "SUNION" | "WATCH" | "TOUCH" => all(args),
        "SINTERCARD" => {
            let count =
                args.first().and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(1).take(count).cloned().collect()
        }
        "MSET" | "MSETNX" => args.iter().step_by(2).cloned().collect(),
        "RENAME" | "RENAMENX" | "COPY" | "RPOPLPUSH" | "LMOVE" | "BLMOVE" | "SMOVE" => {
            args.iter().take(2).cloned().collect()
        }
        "OBJECT" | "XGROUP" | "XINFO" => args.get(1).cloned().into_iter().collect(),
        "MEMORY" if args.first().is_some_and(|b| b.eq_ignore_ascii_case(b"USAGE")) => {
            args.get(1).cloned().into_iter().collect()
        }
        "BITOP" => args.iter().skip(1).cloned().collect(),
        "BLPOP" | "BRPOP" | "BZPOPMAX" | "BZPOPMIN" => {
            args.iter().take(args.len().saturating_sub(1)).cloned().collect()
        }
        "LMPOP" | "ZMPOP" => {
            let count =
                args.first().and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(1).take(count).cloned().collect()
        }
        "BZMPOP" | "BLMPOP" => {
            let count =
                args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(2).take(count).cloned().collect()
        }
        "SORT" | "SORT_RO" => {
            let mut keys: Vec<Bytes> = args.first().cloned().into_iter().collect();
            for pair in args.windows(2) {
                if pair[0].eq_ignore_ascii_case(b"STORE") {
                    keys.push(pair[1].clone());
                }
            }
            keys
        }
        "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" => all(args),
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" => {
            let count =
                args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(2).take(count).cloned().collect()
        }
        "ZINTER" | "ZUNION" | "ZDIFF" | "ZINTERCARD" => {
            let count =
                args.first().and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(1).take(count).cloned().collect()
        }
        "ZINTERSTORE" | "ZUNIONSTORE" | "ZDIFFSTORE" => {
            let count =
                args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().take(1).chain(args.iter().skip(2).take(count)).cloned().collect()
        }
        "XREAD" | "XREADGROUP" => {
            let Some(streams) = args.iter().position(|b| b.eq_ignore_ascii_case(b"STREAMS")) else {
                return Vec::new();
            };
            let tail = &args[streams + 1..];
            let count = tail.len() / 2;
            tail.iter().take(count).cloned().collect()
        }
        // `RUN <sub> <id>` touches `run:<id>` plus its `~`/`:`-suffixed
        // sys and state keys — all covered by a `~run:*` pattern check on
        // the record itself. `RUN LIST` has no id; `run:` keeps the same
        // gate. Wrapped keys of `IDEMPOTENT`'s inner command are checked
        // by the inner dispatch.
        "RUN" => match args.get(1) {
            Some(id) => {
                let mut k = BytesMut::with_capacity(4 + id.len());
                k.extend_from_slice(b"run:");
                k.extend_from_slice(id);
                vec![k.freeze()]
            }
            None => vec![Bytes::from_static(b"run:")],
        },
        _ if KEY_COMMANDS_SET.contains(name) => args.first().cloned().into_iter().collect(),
        _ => Vec::new(),
    }
}

pub fn authorize(user: &AclUser, name: &str, args: &[Bytes]) -> Result<(), String> {
    if !user.enabled {
        return Err("NOAUTH Authentication required.".into());
    }
    if !user.can_run(name) {
        return Err(format!(
            "NOPERM User has no permissions to run the '{}' command",
            name.to_ascii_lowercase()
        ));
    }
    // `allkeys` (the default user) can skip extracting key operands — the
    // per-key check below would pass unconditionally anyway. Saves a Vec
    // allocation plus the category lookup on every command.
    if user.allkeys {
        return Ok(());
    }
    let write = command_categories(name) & cat::WRITE != 0;
    if command_keys(name, args).into_iter().any(|key| !user.can_access_key(&key, write)) {
        return Err("NOPERM No permissions to access a key for this user".into());
    }
    Ok(())
}

fn new_acl_users(requirepass: Option<&str>) -> DashMap<String, AclUser> {
    let users = DashMap::new();
    users.insert("default".to_string(), AclUser::default_user(requirepass));
    users
}

/// Parse an aclfile into `(name, user)` pairs. Blank lines and `#`
/// comments are skipped; anything else that isn't `user <name> <rules>`
/// is an error — a security file that's half-understood must fail loudly.
/// A file with no `default` user yields one with every flag off (disabled,
/// no passwords, no keys, no commands).
pub fn parse_acl_file(path: &Path) -> Result<Vec<(String, AclUser)>, String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("cannot read ACL file {}: {e}", path.display()))?;
    parse_acl_content(&content).map_err(|e| format!("{}: {e}", path.display()))
}

fn parse_acl_content(content: &str) -> Result<Vec<(String, AclUser)>, String> {
    let mut users = Vec::new();
    for (index, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut tokens = line.split_whitespace();
        if tokens.next() != Some("user") {
            return Err(format!("line {}: expected 'user <name> <rules>'", index + 1));
        }
        let Some(name) = tokens.next() else {
            return Err(format!("line {}: missing user name", index + 1));
        };
        let mut user = AclUser::default();
        let rest: Vec<Bytes> = tokens.map(|t| Bytes::copy_from_slice(t.as_bytes())).collect();
        apply_setuser_tokens(&mut user, &rest).map_err(|e| format!("line {}: {e}", index + 1))?;
        users.push((name.to_string(), user));
    }
    if !users.iter().any(|(name, _)| name == "default") {
        users.push(("default".to_string(), AclUser::default()));
    }
    Ok(users)
}

/// Serialize the user table and swap it into `path` atomically (write a
/// sibling temp file, then rename), so a crash mid-save can't leave a
/// truncated aclfile behind. Lines are sorted for a stable, diffable file.
fn write_acl_file(path: &Path, users: &DashMap<String, AclUser>) -> std::io::Result<()> {
    use std::io::Write;
    let mut lines: Vec<String> = users.iter().map(|e| describe_user(e.key(), e.value())).collect();
    lines.sort();
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(lines.join("\n").as_bytes())?;
        file.write_all(b"\n")?;
    }
    std::fs::rename(&tmp, path)
}

impl ServerContext {
    pub(crate) fn init_acl(requirepass: Option<&str>) -> DashMap<String, AclUser> {
        new_acl_users(requirepass)
    }

    /// Whether a brand-new connection must `AUTH` before running commands —
    /// derived from the effective `default` user rather than `requirepass`
    /// directly, so an aclfile that gives `default` a password (or disables
    /// it) is enforced even when `requirepass` is unset. Fails closed if
    /// `default` is somehow absent.
    pub fn auth_required(&self) -> bool {
        self.acl_users.get("default").map(|u| !(u.enabled && u.nopass)).unwrap_or(true)
    }

    /// Replace the whole user table with the aclfile at `path`; returns how
    /// many users were loaded. Entries dropped by the file are removed
    /// first so they lose access immediately — a client authenticated as a
    /// deleted user gets `NOAUTH` on its next command via the per-command
    /// `acl_users` lookup.
    pub fn load_acl_users(&self, path: &Path) -> Result<usize, String> {
        let users = parse_acl_file(path)?;
        let count = users.len();
        self.acl_users.retain(|name, _| users.iter().any(|(n, _)| n == name));
        for (name, user) in users {
            self.acl_users.insert(name, user);
        }
        Ok(count)
    }

    /// `ACL SAVE` — persist the live table to the configured `aclfile`.
    pub fn save_acl_users(&self) -> Result<(), String> {
        let guard = self.acl_file.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(path) = &*guard else {
            return Err(
                "ERR This server is not configured to use an ACL file (missing 'aclfile' directive)".into()
            );
        };
        write_acl_file(path, &self.acl_users)
            .map_err(|e| format!("ERR could not write ACL file {}: {e}", path.display()))
    }

    /// `ACL LOAD` — swap the live table for the configured `aclfile`'s.
    pub fn reload_acl_users(&self) -> Result<(), String> {
        let Some(path) = self.acl_file.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
        else {
            return Err(
                "ERR This server is not configured to use an ACL file (missing 'aclfile' directive)".into()
            );
        };
        self.load_acl_users(&path).map(|_| ())
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
        "CAT" => {
            if let [name] = rest {
                let name = String::from_utf8_lossy(name).to_ascii_lowercase();
                return match category_mask(&name) {
                    Some(mask) => reply(Value::Array(
                        COMMAND_CATEGORY_TABLE
                            .iter()
                            .filter(|(_, bits)| bits & mask != 0)
                            .map(|(c, _)| Value::bulk(c.to_ascii_lowercase()))
                            .collect(),
                    )),
                    None => err(format!("ERR Unknown category '{name}'")),
                };
            }
            reply(Value::Array(
                std::iter::once(Value::bulk("all"))
                    .chain(category_names(cat::ALL).into_iter().map(Value::bulk))
                    .collect(),
            ))
        }
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
        "SAVE" => {
            if !rest.is_empty() {
                return wrong_args("acl|save");
            }
            match ctx.save_acl_users() {
                Ok(()) => reply(Value::ok()),
                Err(e) => err(e),
            }
        }
        "LOAD" => {
            if !rest.is_empty() {
                return wrong_args("acl|load");
            }
            match ctx.reload_acl_users() {
                Ok(()) => reply(Value::ok()),
                Err(e) => err(e),
            }
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
                user.read_patterns.clear();
                user.write_patterns.clear();
            }
            "resetkeys" => {
                user.allkeys = false;
                user.key_patterns.clear();
                user.read_patterns.clear();
                user.write_patterns.clear();
            }
            "allcommands" | "+@all" => {
                user.allcommands = true;
                user.allowed_commands.clear();
                user.denied_commands.clear();
                user.allowed_categories = 0;
                user.denied_categories = 0;
            }
            "nocommands" | "-@all" => {
                user.allcommands = false;
                user.allowed_commands.clear();
                user.denied_commands.clear();
                user.allowed_categories = 0;
                user.denied_categories = 0;
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
            _ if text.starts_with("%R~") => {
                user.allkeys = false;
                user.read_patterns.push(text[3..].to_string());
            }
            _ if text.starts_with("%W~") => {
                user.allkeys = false;
                user.write_patterns.push(text[3..].to_string());
            }
            _ if text.starts_with("+@") => {
                let name = &text[2..];
                let Some(mask) = category_mask(name) else {
                    return Err(format!("ERR Unrecognized ACL category '{name}'"));
                };
                user.allowed_categories |= mask;
                user.denied_categories &= !mask;
            }
            _ if text.starts_with("-@") => {
                let name = &text[2..];
                let Some(mask) = category_mask(name) else {
                    return Err(format!("ERR Unrecognized ACL category '{name}'"));
                };
                user.denied_categories |= mask;
                user.allowed_categories &= !mask;
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
                return Err(format!(
                    "ERR Error in ACL SETUSER modifier '{text}': channel patterns are not supported"
                ));
            }
            _ if text.starts_with('(') => {
                return Err(format!(
                    "ERR Error in ACL SETUSER modifier '{text}': selectors are not supported"
                ));
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
        let mut hashes: Vec<String> = user.password_hashes.iter().map(|h| format!("#{h}")).collect();
        hashes.sort();
        hashes.join(" ")
    };
    let keys = describe_keys(user);
    let commands = describe_commands(user);
    // Skip empty sections (e.g. a password-less non-nopass user, or a
    // resetkeys user) instead of emitting double spaces.
    let mut line = format!("user {name} {status}");
    for section in [pass, keys, commands] {
        if !section.is_empty() {
            line.push(' ');
            line.push_str(&section);
        }
    }
    line
}

fn describe_keys(user: &AclUser) -> String {
    if user.allkeys {
        return "~*".to_string();
    }
    user.key_patterns
        .iter()
        .map(|p| format!("~{p}"))
        .chain(user.read_patterns.iter().map(|p| format!("%R~{p}")))
        .chain(user.write_patterns.iter().map(|p| format!("%W~{p}")))
        .collect::<Vec<_>>()
        .join(" ")
}

fn describe_commands(user: &AclUser) -> String {
    if user.allcommands {
        return "+@all".to_string();
    }
    let mut parts: Vec<String> =
        user.allowed_commands.iter().map(|c| format!("+{}", c.to_ascii_lowercase())).collect();
    parts.extend(user.denied_commands.iter().map(|c| format!("-{}", c.to_ascii_lowercase())));
    parts.extend(category_names(user.allowed_categories).into_iter().map(|c| format!("+@{c}")));
    parts.extend(category_names(user.denied_categories).into_iter().map(|c| format!("-@{c}")));
    if parts.is_empty() {
        "-@all".to_string()
    } else {
        parts.sort();
        parts.join(" ")
    }
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
        (Value::bulk("commands"), Value::bulk(describe_commands(user))),
        (Value::bulk("keys"), Value::bulk(describe_keys(user))),
        (Value::bulk("channels"), Value::bulk("")),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::{dispatch, ClientState, Outcome, ServerContext};

    fn run(ctx: &ServerContext, state: &mut ClientState, words: &[&str]) -> Value {
        let req: Vec<Bytes> = words.iter().map(|w| Bytes::from(w.to_string())).collect();
        match dispatch(ctx, state, req) {
            Outcome::Reply(v) => v,
            _ => panic!("expected Outcome::Reply"),
        }
    }

    fn temp_acl(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("reflexdb-acl-{name}-{}", std::process::id()))
    }

    /// The aclfile format `ACL SAVE` writes must parse back into an
    /// equivalent table — exercise every token class `describe_user` emits.
    #[test]
    fn save_then_load_round_trips_the_user_table() {
        let path = temp_acl("roundtrip.acl");
        let ctx = ServerContext::new(1, None).with_acl_file(Some(path.clone()));
        let mut st = ClientState::new(false);
        assert_eq!(
            run(
                &ctx,
                &mut st,
                &[
                    "ACL",
                    "SETUSER",
                    "alice",
                    "on",
                    ">pw1",
                    ">pw2",
                    "~cache:*",
                    "%R~public:*",
                    "%W~scratch",
                    "+get",
                    "+set",
                    "+@read",
                    "-@dangerous"
                ]
            ),
            Value::ok()
        );
        assert_eq!(
            run(
                &ctx,
                &mut st,
                &["ACL", "SETUSER", "bob", "off", "nopass", "resetkeys", "nocommands", "+ping"]
            ),
            Value::ok()
        );
        assert_eq!(run(&ctx, &mut st, &["ACL", "SAVE"]), Value::ok());

        // Mutate the table away, then reload: LOAD must fully restore it.
        run(&ctx, &mut st, &["ACL", "DELUSER", "alice", "bob"]);
        assert_eq!(run(&ctx, &mut st, &["ACL", "LOAD"]), Value::ok());

        let mut lines: Vec<String> =
            ctx.acl_users.iter().map(|e| describe_user(e.key(), e.value())).collect();
        lines.sort();
        let on_disk: Vec<String> =
            std::fs::read_to_string(&path).unwrap().lines().map(str::to_string).collect();
        assert_eq!(lines, on_disk);

        // And the reloaded rules must actually enforce.
        let mut alice = ClientState::new(false);
        assert_eq!(run(&ctx, &mut alice, &["AUTH", "alice", "pw1"]), Value::ok());
        assert_eq!(run(&ctx, &mut alice, &["SET", "cache:a", "1"]), Value::ok());
        assert!(matches!(run(&ctx, &mut alice, &["GET", "other:b"]), Value::Error(_)));
        assert!(matches!(run(&ctx, &mut alice, &["SHUTDOWN"]), Value::Error(_)));
        let _ = std::fs::remove_file(&path);
    }

    /// A file with no `default` user must not authenticate new clients:
    /// the loader inserts a disabled default, so `auth_required` stays on.
    #[test]
    fn aclfile_without_default_fails_closed() {
        let path = temp_acl("nodefault.acl");
        std::fs::write(&path, "user alice on nopass ~* +@all\n").unwrap();
        let ctx = ServerContext::new(1, None).with_acl_file(Some(path.clone()));
        ctx.load_acl_users(&path).unwrap();
        assert!(ctx.auth_required());
        assert!(ctx.acl_users.contains_key("default"));
        assert!(!ctx.acl_users.get("default").unwrap().enabled);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn aclfile_rejects_malformed_lines() {
        assert!(parse_acl_content("user alice on ~* +@all\nbogus line\n").is_err());
        assert!(parse_acl_content("user\n").is_err());
        assert!(parse_acl_content("user alice on +@nosuchcat\n").is_err());
        assert!(parse_acl_content("user alice &chan\n").is_err());
    }

    /// `auth_required` tracks the *effective* default user: an aclfile that
    /// gives it a password must force AUTH even with `requirepass` unset.
    #[test]
    fn auth_required_follows_the_default_user() {
        let ctx = ServerContext::new(1, None);
        assert!(!ctx.auth_required());
        ctx.acl_users.get_mut("default").unwrap().nopass = false;
        assert!(ctx.auth_required());
        let ctx = ServerContext::new(1, Some("pw".into()));
        assert!(ctx.auth_required());
    }

    #[test]
    fn save_and_load_error_without_aclfile() {
        let ctx = ServerContext::new(1, None);
        let mut st = ClientState::new(false);
        assert!(matches!(run(&ctx, &mut st, &["ACL", "SAVE"]), Value::Error(_)));
        assert!(matches!(run(&ctx, &mut st, &["ACL", "LOAD"]), Value::Error(_)));
    }
}
