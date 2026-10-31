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
//! `ACL LOG`/`LOAD`/`SAVE`/`DRYRUN`/`HELP` aren't implemented.
//!
//! This is a subset of the ACL command surface, not "full ACL" — see
//! `docs/SECURITY.md` for the honest scope.

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
    ("SUBSTR", cat::STRING | cat::READ | cat::SLOW),
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

/// Category bits for `name`. Commands not in the table get `0`: categories
/// neither grant nor deny them (unknown commands anyway, or reachable only
/// via explicit `+cmd`/`allcommands`).
pub fn command_categories(name: &str) -> u32 {
    COMMAND_CATEGORY_TABLE.iter().find_map(|(n, bits)| (*n == name).then_some(*bits)).unwrap_or(0)
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
    "SUBSTR",
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
    "BRPOPLPUSH",
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
];

/// Return every key operand understood by the ACL key-pattern checks.
/// Commands whose key positions depend on a count are decoded from their
/// own argument layout; option/value operands are never treated as keys.
pub fn command_keys<'a>(name: &str, args: &'a [Bytes]) -> Vec<&'a [u8]> {
    let all = |args: &'a [Bytes]| args.iter().map(Bytes::as_ref).collect();
    match name {
        "DEL" | "UNLINK" | "EXISTS" | "MGET" | "SDIFF" | "SINTER" | "SUNION" | "WATCH" | "TOUCH" => all(args),
        "SINTERCARD" => {
            let count =
                args.first().and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(1).take(count).map(Bytes::as_ref).collect()
        }
        "MSET" | "MSETNX" => args.iter().step_by(2).map(Bytes::as_ref).collect(),
        "RENAME" | "RENAMENX" | "COPY" | "RPOPLPUSH" | "BRPOPLPUSH" | "LMOVE" | "BLMOVE" | "SMOVE" => {
            args.iter().take(2).map(Bytes::as_ref).collect()
        }
        "OBJECT" | "XGROUP" | "XINFO" => args.get(1).map(|b| vec![b.as_ref()]).unwrap_or_default(),
        "MEMORY" if args.first().is_some_and(|b| b.eq_ignore_ascii_case(b"USAGE")) => {
            args.get(1).map(|b| vec![b.as_ref()]).unwrap_or_default()
        }
        "BITOP" => args.iter().skip(1).map(Bytes::as_ref).collect(),
        "BLPOP" | "BRPOP" | "BZPOPMAX" | "BZPOPMIN" => {
            args.iter().take(args.len().saturating_sub(1)).map(Bytes::as_ref).collect()
        }
        "LMPOP" | "ZMPOP" => {
            let count =
                args.first().and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(1).take(count).map(Bytes::as_ref).collect()
        }
        "BZMPOP" | "BLMPOP" => {
            let count =
                args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(2).take(count).map(Bytes::as_ref).collect()
        }
        "SORT" | "SORT_RO" => {
            let mut keys = args.first().map(|b| vec![b.as_ref()]).unwrap_or_default();
            for pair in args.windows(2) {
                if pair[0].eq_ignore_ascii_case(b"STORE") {
                    keys.push(pair[1].as_ref());
                }
            }
            keys
        }
        "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" => all(args),
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" => {
            let count =
                args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(2).take(count).map(Bytes::as_ref).collect()
        }
        "ZINTER" | "ZUNION" | "ZDIFF" | "ZINTERCARD" => {
            let count =
                args.first().and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().skip(1).take(count).map(Bytes::as_ref).collect()
        }
        "ZINTERSTORE" | "ZUNIONSTORE" | "ZDIFFSTORE" => {
            let count =
                args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(0);
            args.iter().take(1).chain(args.iter().skip(2).take(count)).map(Bytes::as_ref).collect()
        }
        "XREAD" | "XREADGROUP" => {
            let Some(streams) = args.iter().position(|b| b.eq_ignore_ascii_case(b"STREAMS")) else {
                return Vec::new();
            };
            let tail = &args[streams + 1..];
            let count = tail.len() / 2;
            tail.iter().take(count).map(Bytes::as_ref).collect()
        }
        _ if KEY_COMMANDS.contains(&name) => args.first().map(|b| vec![b.as_ref()]).unwrap_or_default(),
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
    let write = command_categories(name) & cat::WRITE != 0;
    if command_keys(name, args).into_iter().any(|key| !user.can_access_key(key, write)) {
        return Err("NOPERM No permissions to access a key for this user".into());
    }
    Ok(())
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
        user.password_hashes.iter().map(|h| format!("#{h}")).collect::<Vec<_>>().join(" ")
    };
    let keys = describe_keys(user);
    let commands = describe_commands(user);
    format!("user {name} {status} {pass} {keys} {commands}")
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
