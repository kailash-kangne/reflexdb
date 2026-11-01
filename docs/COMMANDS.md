# Commands — full reference

Commands tagged **[cut]** are deliberately not being built (see [ROADMAP.md](ROADMAP.md) for
why). Commands tagged **[planned]** are designed but not implemented yet (see
[ROADMAP.md](ROADMAP.md) for the design). Every command below works over RESP2/RESP3 and the
optional [RFX](RFX.md) binary framing — RFX changes only the wire encoding, not command
semantics.

## Generic (keyspace)

`COPY`, `DEL`, `DUMP`, `EXISTS`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`
(`NX|XX|GT|LT`, plus reflexdb `SLIDING`), `EXPIRETIME`, `KEYS`, `MOVE`, `OBJECT ENCODING`,
`PERSIST`, `PEXPIRETIME`,
`PTTL`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `RESTORE`, `SCAN`, `SORT`, `SORT_RO`,
`TOUCH`, `TTL`, `TYPE`, `UNLINK`, `WAIT`, `WAITAOF`

`EVICT`, `MIGRATE`, `LCS`, `NO-TOUCH` — **[planned]**, not implemented.

`SUBSTR`, `DEBUG` — **[cut]** (`SUBSTR` is a deprecated alias of `GETRANGE`).

## String

`APPEND`, `DECR`, `DECRBY`, `GET`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`,
`INCRBYFLOAT`, `MGET`, `MSET`, `MSETNX`, `PSETEX`, `SET` (`NX|XX|GET|KEEPTTL|EX|PX|EXAT|PXAT`),
`SETEX`, `SETNX`, `SETRANGE`, `STRLEN`

## List

`BLMOVE`, `BLMPOP`, `BLPOP`, `BRPOP`, `LINDEX`, `LINSERT`, `LLEN`, `LMOVE`,
`LMPOP`, `LPOP [count]`, `LPOS`, `LPUSH`, `LPUSHX`, `LRANGE`, `LREM`, `LSET`, `LTRIM`,
`RPOP [count]`, `RPOPLPUSH`, `RPUSH`, `RPUSHX`

`BRPOPLPUSH` — **[cut]** (deprecated since Redis 6.2; use `BLMOVE src dst RIGHT LEFT`).

## Hash

`HDEL`, `HEXISTS`, `HGET`, `HGETALL`, `HINCRBY`, `HINCRBYFLOAT`, `HKEYS`, `HLEN`, `HMGET`,
`HMSET`, `HRANDFIELD`, `HSCAN`, `HSET` (`HSETNX` variant), `HSTRLEN`, `HVALS`

Iteration order is randomized per-process — never assert ordering from `HKEYS`/`HVALS`/
`HGETALL` (same for `SMEMBERS`).

## Set

`SADD`, `SCARD`, `SDIFF`, `SDIFFSTORE`, `SINTER`, `SINTERCARD`, `SINTERSTORE`, `SISMEMBER`,
`SMEMBERS`, `SMISMEMBER`, `SMOVE`, `SPOP [count]`, `SRANDMEMBER [count]`, `SREM`, `SSCAN`,
`SUNION`, `SUNIONSTORE`

## Sorted Set

`BZMPOP`, `BZPOPMAX`, `BZPOPMIN`, `ZADD` (`NX|XX|GT|LT|CH|INCR`), `ZCARD`, `ZCOUNT`, `ZDIFF`,
`ZDIFFSTORE`, `ZINCRBY`, `ZINTER`, `ZINTERCARD`, `ZINTERSTORE`, `ZLEXCOUNT`, `ZMPOP`, `ZMSCORE`,
`ZPOPMAX`, `ZPOPMIN`, `ZRANDMEMBER`, `ZRANGE [BYSCORE|BYLEX] [REV] [LIMIT] [WITHSCORES]`,
`ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANGESTORE`, `ZRANK`, `ZREM`, `ZREMRANGEBYLEX`,
`ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYLEX`, `ZREVRANGEBYSCORE`,
`ZREVRANK`, `ZSCAN`, `ZSCORE`, `ZUNION`, `ZUNIONSTORE`

## Stream

`XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XDEL`, `XTRIM`, `XGROUP`, `XREADGROUP`, `XACK`,
`XPENDING`, `XREAD`, `XCLAIM`, `XAUTOCLAIM`

`XINFO`, standalone `XSETID`, `BLOCK`ing reads, `MINID` trimming — **[cut]**.

## Geo, HyperLogLog — **[cut]**

Not being built.

## Bitmap

`BITCOUNT`, `BITFIELD`, `BITFIELD_RO`, `BITOP` (`AND|OR|XOR|NOT`), `BITPOS`, `GETBIT`, `SETBIT`

## Pub/Sub + reflexdb LIVE

`PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBSUB` (`CHANNELS`,
`NUMSUB`, `NUMPAT`)

`SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, `PUBSUB SHARDCHANNELS/SHARDNUMSUB` — **[cut]**
(cluster-mode feature).

**reflexdb-native**: `LIVE`, `UNLIVE` — see [FEATURES.md](FEATURES.md#reactivity--live--unlive).

## Connection / Client

`AUTH`, `CLIENT` (`CACHING`, `CAPA`, `GETNAME`, `GETREDIR`, `ID`, `INFO`, `KILL`, `LIST`,
`PAUSE`/`UNPAUSE`, `REPLY`, `SETINFO`, `SETNAME`, `TRACKING`, `TRACKINGINFO`, `UNBLOCK`),
`ECHO`, `HELLO`, `PING`, `QUIT`, `RESET`, `SELECT`

`CLIENT NO-EVICT` — **[planned]**.

## Server

`ACL` (see [FEATURES.md](FEATURES.md#security)), `BGREWRITEAOF`, `BGSAVE`, `COMMAND` (`COUNT`,
`DOCS`, `INFO`), `CONFIG` (`GET`, `SET` — runtime-settable: `maxmemory`,
`maxmemory-policy`, `appendonly`, `appendfsync`, `history-enabled`,
`slowlog-log-slower-than`, `slowlog-max-len`, `notify-keyspace-events`,
`min-replicas-to-write`, `requirepass`, `aclfile`, `dir`, `dbfilename`),
`DBSIZE`, `FLUSHALL`, `FLUSHDB`, `INFO`, `LASTSAVE`, `LATENCY` (`DOCTOR`,
`GRAPH`, `HISTOGRAM`, `HISTORY`, `LATEST`, `RESET`), `MEMORY` (`DOCTOR`,
`MALLOC-STATS`, `PURGE`, `STATS`, `USAGE`), `SAVE`, `SHUTDOWN` (`[NOSAVE|SAVE]` —
flushes the AOF, writes a final RDB when AOF is off unless `NOSAVE`, then exits),
`SLOWLOG` (`GET`, `HELP`, `LEN`, `RESET`), `TIME`

`FAILOVER`, `LOLWUT`, `MONITOR`, `SWAPDB` — **[planned]** (listed in Redis'
server group; not implemented yet — verified 2026-11-01, they return `ERR unknown command`).

`MODULE` — **[cut]** (extensions are compiled-in Rust crates, not `.so` modules; returns
`ERR unknown command`).

## Transactions

`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

## Scripting

`EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT` (`EXISTS`, `FLUSH`, `LOAD`)

`FCALL`, `FCALL_RO`, `FUNCTION *` — **[planned]**.

## HA / Replication

`PSYNC`, `REPLICAOF` (`SLAVEOF` alias), `REPLCONF`, `ROLE`, `WAIT`, `WAITAOF`

`FAILOVER`, `SYNC` — **[planned]** (`PSYNC` covers the replica handshake; plain `SYNC` and
`FAILOVER` return `ERR unknown command` — verified 2026-11-01).

`ASKING`/`READONLY`/`READWRITE`/`RESTORE-ASKING` — **[cut]** (cluster-mode).

## Cluster — **[cut]**

Not being built.

## Sentinel mode

Active only under `--sentinel` mode (`reflexdb-sentinel` binary): `CKQUORUM`,
`GET-MASTER-ADDR-BY-NAME`, `IS-MASTER-DOWN-BY-ADDR`, `MASTER`, `MASTERS`, `REPLICAS`

`SENTINEL CONFIG`/`DEBUG`/`FAILOVER`/`MONITOR`/`REMOVE`/`RESET`/`SET` and the
`SLAVES`/`PRIMARIES`-style aliases — **[cut]**, not implemented.

## Debug commands — **[cut]**

`DEBUG` returns `ERR unknown command` (no `enable-debug-command` config exists).

## reflexdb-native commands

| Command | Arity | Notes |
|---------|-------|-------|
| `LIVE` | `-3` | wraps any read-only command as a push-stream |
| `UNLIVE` | `-2` | unsubscribe by fingerprint |
| `GETAT` | `3` | point-in-time read (needs `history-enabled yes`) |
| `HISTORY` | `-2` | per-key version log (needs `history-enabled yes`) |
| `SCHEDULE` | `-2` | delayed command execution |
| `RUN` | `-2` | run registry + checkpoints — `BEGIN`/`STEP`/`CHECKPOINT`/`RESTORE`/`END`/`STATUS`/`LIST`/`AUDIT` |
| `IDEMPOTENT` | `-4` | `IDEMPOTENT <key> <ttl_ms> <cmd> <args...>` — execute once, replay stored reply on retry |

`JSON.*`/`BF.*` extension families are cut entirely — see [ROADMAP.md](ROADMAP.md).
