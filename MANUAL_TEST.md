# Manual Test Guide

Quick smoke test for a running `reflexdb-server`. All of the "huge duration" checks below are
also covered by an automated test:

```bash
cargo test -p commands huge_client_supplied_durations_saturate_instead_of_panicking
```

## Build & run

```bash
cargo install --path crates/server --force   # installs reflexdb-server/-sentinel/-cli to ~/.cargo/bin
reflexdb-server --port 7777                  # leave running in its own terminal
reflexdb-cli -p 7777 PING                    # expect: PONG
```

## Huge client-supplied TTL/timeout values don't crash the connection

A client-supplied TTL/timeout of `9223372036854775807` (i64::MAX) or
`18446744073709551615` (u64::MAX) must saturate, not panic:

```bash
reflexdb-cli -p 7777 SET k v
reflexdb-cli -p 7777 EXPIRE k 9223372036854775807          # -> (integer) 1
reflexdb-cli -p 7777 PEXPIRE k 9223372036854775807         # -> (integer) 1
reflexdb-cli -p 7777 EXPIRE k 9223372036854775807 SLIDING  # -> (integer) 1
reflexdb-cli -p 7777 SET k2 v EX 9223372036854775807       # -> OK
reflexdb-cli -p 7777 GET k4 SINGLEFLIGHT 18446744073709551615  # -> (nil)
reflexdb-cli -p 7777 SCHEDULE IN 18446744073709551615 PING # -> schedule id, no crash
reflexdb-cli -p 7777 EVAL "local d=redis.call('DUMP',KEYS[1]); return redis.call('RESTORE',KEYS[2],ARGV[1],d,'REPLACE')" 2 k k5 18446744073709551615  # -> OK
reflexdb-cli -p 7777 BLPOP nosuchlist inf                  # blocks forever (correct) — Ctrl+C after a couple seconds
reflexdb-cli -p 7777 PING                                  # -> PONG (server survived)
```

## `LIVE` (reactivity)

```bash
# Terminal 1
reflexdb-cli -p 7777 LIVE GET watched
# Terminal 2
reflexdb-cli -p 7777 SET watched hello
# Terminal 1 should print a new push with the updated result, unprompted
```

`SUBSCRIBE`/`PSUBSCRIBE` work the same way via `reflexdb-cli -p 7777 SUBSCRIBE mychannel`.

## Interactive prompt

`reflexdb-cli -p 7777` with no trailing command drops into a `redis-cli`-style prompt
(`exit`/`quit`/Ctrl+D to leave).

## Clean up

```bash
pkill reflexdb-server
```
