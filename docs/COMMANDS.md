# Commands — full reference

Commands tagged **[cut]** are deliberately not being built (see [ROADMAP.md](ROADMAP.md) for
why). Commands tagged **[planned]** are designed but not implemented yet (see
[ROADMAP.md](ROADMAP.md) for the design).

## Generic (keyspace)

`COPY`, `DEL`, `DUMP`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `EXPIRETIME`, `KEYS`, `MOVE`, `OBJECT`
(`ENCODING`, `FREQ`, `IDLETIME`, `REFCOUNT`), `PERSIST`, `PEXPIRE`, `PEXPIREAT`, `PEXPIRETIME`,
`PTTL`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `RESTORE`, `SCAN`, `SORT`, `SORT_RO`, `SUBSTR`
(alias of `GETRANGE`), `TOUCH`, `TTL`, `TYPE`, `UNLINK`, `WAIT`, `WAITAOF`, `DEBUG` (subset)

`EVICT`, `MIGRATE`, `LCS`, `NO-TOUCH` — **[planned]**, not implemented.

## String

`APPEND`, `DECR`, `DECRBY`, `GET`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`,
`INCRBYFLOAT`, `MGET`, `MSET`, `MSETNX`, `PSETEX`, `SET` (`NX|XX|GET|KEEPTTL|EX|PX|EXAT|PXAT`),
`SETEX`, `SETNX`, `SETRANGE`, `STRLEN`

## List

`BLMOVE`, `BLMPOP`, `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `LINDEX`, `LINSERT`, `LLEN`, `LMOVE`,
`LMPOP`, `LPOP [count]`, `LPOS`, `LPUSH`, `LPUSHX`, `LRANGE`, `LREM`, `LSET`, `LTRIM`,
`RPOP [count]`, `RPOPLPUSH`, `RPUSH`, `RPUSHX`

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
`XPENDING`, `XREAD`

`XCLAIM`/`XAUTOCLAIM`, `XINFO`, standalone `XSETID`, `BLOCK`ing reads, `MINID` trimming —
**[cut]**.

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
`DOCS`, `GETKEYS`, `GETKEYSANDFLAGS`, `INFO`, `LIST`), `CONFIG` (`GET`, `REWRITE`, `RESETSTAT`,
`SET`), `DBSIZE`, `FAILOVER`, `FLUSHALL`, `FLUSHDB`, `INFO`, `LASTSAVE`, `LATENCY` (`DOCTOR`,
`GRAPH`, `HISTOGRAM`, `HISTORY`, `LATEST`, `RESET`), `LOLWUT`, `MEMORY` (`DOCTOR`,
`MALLOC-STATS`, `PURGE`, `STATS`, `USAGE`), `MONITOR`, `ROLE`, `SAVE`, `SHUTDOWN`, `SLOWLOG`
(`GET`, `HELP`, `LEN`, `RESET`), `SWAPDB`, `TIME`

`MODULE` — parsed but no-op (extensions are compiled-in Rust crates, not `.so` modules).

## Transactions

`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

## Scripting

`EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT` (`DEBUG`, `EXISTS`, `FLUSH`, `KILL`,
`LOAD`, `SHOW`)

`FCALL`, `FCALL_RO`, `FUNCTION *` — **[planned]**.

## HA / Replication

`PSYNC`, `REPLICAOF` (`SLAVEOF` alias), `REPLCONF`, `ROLE`, `SYNC`, `WAIT`, `WAITAOF`,
`FAILOVER`

`ASKING`/`READONLY`/`READWRITE`/`RESTORE-ASKING` — **[cut]** (cluster-mode).

## Cluster — **[cut]**

Not being built.

## Sentinel mode

Active only under `--sentinel` mode (`reflexdb-sentinel` binary): `CKQUORUM`, `CONFIG`,
`DEBUG`, `FAILOVER`, `FLUSHCONFIG`, `GET-MASTER-ADDR-BY-NAME`, `GET-PRIMARY-ADDR-BY-NAME`,
`INFO-CACHE`, `IS-MASTER-DOWN-BY-ADDR`, `IS-PRIMARY-DOWN-BY-ADDR`, `MASTER(S)`, `MONITOR`,
`MYID`, `PENDING-SCRIPTS`, `PRIMAR(Y/IES)`, `REMOVE`, `REPLICAS`, `RESET`, `SENTINELS`, `SET`,
`SIMULATE-FAILURE`, `SLAVES`

## Debug-only (gated behind `enable-debug-command yes`)

`DEBUG SET-ACTIVE-EXPIRE`, `QUICKLIST-PACKED-THRESHOLD`, `STRINGMATCH-LEN`, `SLEEP`, `OBJECT`,
`DIGEST`, `DIGEST-VALUE`, `RELOAD`, `CHANGE-REPL-ID`, `JMAP`

## reflexdb-native commands

| Command | Arity | Notes |
|---------|-------|-------|
| `LIVE` | `-3` | wraps any read-only command as a push-stream |
| `UNLIVE` | `-2` | unsubscribe by fingerprint |
| `GETAT` | `3` | point-in-time read (needs `history-enabled yes`) |
| `HISTORY` | `-2` | per-key version log (needs `history-enabled yes`) |
| `SCHEDULE` | `-2` | delayed command execution |

`JSON.*`/`BF.*` extension families are cut entirely — see [ROADMAP.md](ROADMAP.md).
