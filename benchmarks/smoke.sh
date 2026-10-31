#!/bin/sh
# Feature smoke test for any RESP-compatible server (ReflexDB, Redis, Dragonfly).
# Usage: HOST=127.0.0.1 PORT=6379 ./benchmarks/smoke.sh
# Uses only redis-cli. Keys are prefixed smoke: and cleaned up at the end;
# FLUSHALL is intentionally not called.
set -u

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6379}"
R="redis-cli -h $HOST -p $PORT"
pass=0; fail=0; skip=0

check() { # name expected actual
  if [ "$2" = "$3" ]; then pass=$((pass+1)); else fail=$((fail+1)); echo "FAIL: $1 -> got '$3', want '$2'"; fi
}
skip() { skip=$((skip+1)); echo "SKIP: $1 ($2)"; }
cleanup() { $R del $(for i in 1 2 3 4 5 6 7 8 9 10; do printf 'smoke:%s ' "$i"; done) smoke:h smoke:l smoke:st smoke:z smoke:x smoke:tk smoke:ex smoke:m1 smoke:lk smoke:sched smoke:live smoke:bm 2>/dev/null >/dev/null; }
trap cleanup EXIT

check "PING"           PONG    "$($R ping)"
check "SET"            OK      "$($R set smoke:1 v)"
check "GET"            v       "$($R get smoke:1)"
check "INCR"           1       "$($R incr smoke:2)"
check "APPEND+GET"     hello   "$($R append smoke:3 he >/dev/null; $R append smoke:3 llo >/dev/null; $R get smoke:3)"
check "GETDEL"         v       "$($R getdel smoke:1)"
check "GETSET"         5       "$($R set smoke:1 5 >/dev/null; $R getset smoke:1 9)"
check "SET NX"         ""      "$($R set smoke:1 x nx)"

$R setbit smoke:bm 7 1 >/dev/null
check "BITCOUNT"       1       "$($R bitcount smoke:bm)"

$R hset smoke:h f1 v1 f2 v2 >/dev/null
check "HGET"           v1      "$($R hget smoke:h f1)"
check "HLEN"           2       "$($R hlen smoke:h)"
check "HINCRBY"        5       "$($R hset smoke:h n 2 >/dev/null; $R hincrby smoke:h n 3)"

$R rpush smoke:l a b c >/dev/null
check "LRANGE"         "$(printf 'a\nb\nc')" "$($R lrange smoke:l 0 -1)"
check "LPOP"           a       "$($R lpop smoke:l)"
check "LLEN"           2       "$($R llen smoke:l)"
check "BLPOP-timeout"  ""      "$($R blpop smoke:nokey 0.1 2>/dev/null)"

$R sadd smoke:st m1 m2 m3 >/dev/null
check "SCARD"          3       "$($R scard smoke:st)"
check "SISMEMBER"      1       "$($R sismember smoke:st m2)"
check "SPOP-removes"   2       "$($R spop smoke:st >/dev/null; $R scard smoke:st)"

$R zadd smoke:z 1 a 2 b 3 c >/dev/null
check "ZCARD"          3       "$($R zcard smoke:z)"
check "ZRANGE"         "$(printf 'a\nb')" "$($R zrange smoke:z 0 1)"
check "ZINCRBY"        5       "$($R zincrby smoke:z 3 b)"
check "ZPOPMIN"        "$(printf 'a\n1')" "$($R zpopmin smoke:z)"

xid=$($R xadd smoke:x '*' f v 2>/dev/null)
if echo "$xid" | grep -q -- '-'; then
  check "XRANGE"       v       "$($R xrange smoke:x - + | grep -c '^v$' >/dev/null && echo v)"
  check "XDEL"         1       "$($R xdel smoke:x "$xid")"
else
  skip "Streams" "XADD returned '$xid'"
fi

$R set smoke:tk tv ex 10 >/dev/null
ttl=$($R ttl smoke:tk)
case "$ttl" in ''|*[!0-9-]*) r=bad;; *) { [ "$ttl" -gt 0 ] && [ "$ttl" -le 10 ]; } && r=ok || r=bad;; esac
check "TTL-range"      ok      "$r"
check "EXPIRE"         1       "$($R expire smoke:tk 5)"
check "PERSIST"        1       "$($R persist smoke:tk)"
$R set smoke:ex e px 100 >/dev/null; sleep 0.3
check "PX-expiry"      ""      "$($R get smoke:ex)"

check "MULTI"          a       "$(printf 'multi\nset smoke:m1 a\nget smoke:m1\nexec\n' | $R | tail -1)"

if $R eval 'return 1' 0 2>/dev/null | grep -q 1; then
  check "EVAL"         hello   "$($R eval 'redis.call("set", KEYS[1], "hello"); return redis.call("get", KEYS[1])' 1 smoke:lk)"
else
  skip "Lua" "EVAL unsupported or disabled"
fi

$R subscribe smoke:ch > /tmp/smoke-sub.$$ 2>&1 &
subpid=$!
sleep 0.4; $R publish smoke:ch msg1 >/dev/null; sleep 0.5; { kill $subpid; wait $subpid; } 2>/dev/null
check "PUBSUB"         msg1    "$(grep -q msg1 /tmp/smoke-sub.$$ && echo msg1)"
rm -f /tmp/smoke-sub.$$

$R set smoke:4 dv >/dev/null
# redis-cli mangles binary DUMP payloads; use a raw RESP client instead.
python3 - "$HOST" "$PORT" >/dev/null 2>&1 <<'EOF'
import socket, sys
def enc(*parts):
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        p = p if isinstance(p, bytes) else str(p).encode()
        out += [f"${len(p)}\r\n".encode(), p, b"\r\n"]
    return b"".join(out)
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), 3)
f = s.makefile("rb")
s.sendall(enc("DUMP", "smoke:4"))
f.readline()  # $len
payload = f.readline()[:-2]
s.sendall(enc("RESTORE", "smoke:5", 0, payload))
assert f.readline() == b"+OK\r\n", "RESTORE failed"
EOF
check "DUMP/RESTORE"   dv      "$($R get smoke:5)"

check "TYPE"           string  "$($R type smoke:5)"
check "EXISTS"         1       "$($R exists smoke:5)"
check "SCAN"           ok      "$($R scan 0 match 'smoke:*' count 100 | grep -q smoke:5 && echo ok)"
check "INFO"           ok      "$($R info server 2>/dev/null | grep -qi version && echo ok)"
check "CONFIG-GET"     ok      "$($R config get maxmemory 2>/dev/null | head -1 | grep -q . && echo ok)"
check "MEMORY-USAGE"   ok      "$($R memory usage smoke:5 2>/dev/null | grep -q '^[0-9]' && echo ok)"
check "SLOWLOG"        ok      "$($R slowlog get 1 >/dev/null 2>&1 && echo ok)"

# --- ReflexDB-only features: run only when the server identifies as ReflexDB ---
if $R info server 2>/dev/null | grep -qi reflexdb; then
  python3 - "$HOST" "$PORT" > /tmp/smoke-live.$$ 2>/dev/null <<'EOF' &
import socket, sys, time
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), 3)
s.sendall(b"*3\r\n$4\r\nLIVE\r\n$3\r\nGET\r\n$10\r\nsmoke:live\r\n")
s.settimeout(2.0)
try:
    while True:
        data = s.recv(65536)
        if not data:
            break
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
except OSError:
    pass
EOF
  livepid=$!
  sleep 0.4; $R set smoke:live liveval >/dev/null; wait $livepid 2>/dev/null
  check "LIVE"         liveval "$(grep -q liveval /tmp/smoke-live.$$ && echo liveval)"
  rm -f /tmp/smoke-live.$$

  check "SINGLEFLIGHT" ""      "$($R del smoke:6 >/dev/null; $R get smoke:6 singleflight 2000 | head -1)"

  $R schedule in 100 set smoke:sched done >/dev/null; sleep 0.4
  check "SCHEDULE"     done    "$($R get smoke:sched)"

  check "SLIDING-TTL"  1       "$($R set smoke:7 sv >/dev/null; $R expire smoke:7 5 sliding)"

  if $R getat smoke:4 now >/dev/null 2>&1; then
    check "HISTORY"    ok      "$($R history smoke:4 2>/dev/null | grep -q . && echo ok)"
  else
    skip "GETAT/HISTORY" "history-enabled is off"
  fi
else
  skip "ReflexDB extras" "LIVE/SCHEDULE/single-flight/GETAT not supported by this engine"
fi

echo ""
echo "=== $pass passed, $fail failed, $skip skipped ==="
[ "$fail" -eq 0 ]
