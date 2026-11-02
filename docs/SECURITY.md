# Threat model

What reflexdb defends against, what it deliberately doesn't, and where the sharp edges are.
Read this before deploying it anywhere traffic you don't control can reach.

## Trust boundaries

- **Client connections** — RESP2/RESP3/RFX on the plaintext listener, optionally a TLS
  listener (`tls-port` + `tls-cert-file`/`tls-key-file`; `port 0` disables plaintext for
  TLS-only mode). RFX is auto-detected by a leading `!` byte on the same port — it is a wire
  encoding, not an authentication path, and gets the same ACL checks.
- **Replication link** — outbound replica connection to a primary, plaintext by default;
  `tls-replication yes` wraps it in TLS verified against `tls-ca-cert-file` (the replica's
  `tls-cert-file`/`tls-key-file` double as its client certificate when the primary runs
  `tls-auth-clients yes`). `masterauth` sends `AUTH` on connect when the primary sets
  `requirepass`. A replica trusts everything the primary sends: keep the link protected.
- **Persistence files** — AOF/RDB/history/tiered files under `dir`/`tiered-dir`. Anyone with
  filesystem access reads the dataset; nothing at rest is encrypted.
- **Lua** — `EVAL`/`EVALSHA` run in an `mlua` Lua 5.1 sandbox with `os`/`io` removed and
  `math.random` seeded.
- **Config file** — directives are trusted operator input; unknown directives warn and skip.

## Defenses implemented

- Unauthenticated connections may only run `AUTH`/`HELLO`/`QUIT`/`RESET`; everything else
  gets `NOAUTH` (`dispatch.rs`).
- ACL: per-user command allow/deny (`+cmd`/`-cmd`/`allcommands`/`nocommands`), command
  categories (`+@read`, `-@write`, ...), key globs (`~pattern`), read/write-scoped
  patterns (`%R~`/`%W~`), and pub/sub channel globs (`&pattern`/`allchannels`/
  `resetchannels`, enforced on every channel operand of `(P)SUBSCRIBE`/`PUBLISH`).
  Rules that can't be enforced (`(...)` selectors, unknown category names) are
  **rejected** at `SETUSER` time rather than silently accepted. Passwords are stored
  as salted, iterated SHA-256 (`s$` entries written by `>password` and `requirepass`);
  legacy `#sha256` aclfile entries still verify so stock Redis files load. The
  `default` user mirrors plain `requirepass` semantics.
- Denied commands and failed authentications are logged at `warn` level (user,
  command, reason) — scrape the server log for an audit trail; `ACL LOG` isn't
  implemented.
- Key-pattern checks decode the key operands of every known command shape
  (`acl::command_keys`), including multi-key and `EVAL`/`SORT`/`XREAD` layouts.
- Connection cap: `maxclients` semaphore; excess sockets are dropped at accept.
- Protocol limits: `MAX_MULTIBULK_COUNT` and `MAX_BULK_LEN` (64 MiB) cap declared lengths
  before allocation in the codec; oversized buffers are dropped.
- Pub/sub + LIVE push queues are bounded; overflowing subscribers are disconnected.
- `RESTORE` payloads are bounded by the codec's `MAX_BULK_LEN`.

## Known gaps (do not treat as closed)

- **Bounded `AUTH` attempts** — 16 failed authentications on one connection drop the socket,
  capping online brute force per socket (an attacker can still reconnect; use long passwords
  (`ACL GENPASS`) and network-level controls). Passwords are also masked in `CONFIG GET`.
- **Password hashing is still SHA-256-based** — salted and iterated (`s$` format), which
  kills rainbow tables and raises offline-crack cost, but it is not a memory-hard KDF.
  Legacy `#sha256` aclfile entries still verify (needed to read stock Redis files) and are
  unsalted — rewrite them with `ACL SETUSER … resetpass >newpass`. `ACL SAVE` emits `!s$…`
  tokens that stock Redis will not parse. Treat hash disclosure as password disclosure
  regardless.
- **ACLs persist only via `aclfile`** — without the directive users are in-memory and vanish
  on restart; `ACL SAVE`/`LOAD` exist.
- **Plaintext listener is on by default** — TLS is additive (`tls-port`); set `port 0`
  explicitly for TLS-only.
- **Metrics listener is separate and unauthenticated by default** — `metrics-port` serves
  keyspace/statistics to anyone who can reach it. Set `metrics-token` to require
  `Authorization: Bearer <token>` (401 otherwise), or keep it loopback/network-guarded.
- **`KEYS`/`CONFIG`/`ACL` are @dangerous but enabled for `allcommands` users** — grant them
  deliberately.
- **One writer per data dir** — enforced: startup takes an exclusive `flock` on
  `<dir>/reflexdb.lock` (released automatically on exit, including SIGKILL), so a second
  server on the same `dir` refuses to boot.
- **`LIVE`/`SCHEDULE` execute on behalf of the subscriber/issuer** — scheduled commands run
  later with server-side privileges; restrict `SCHEDULE` (`-SCHEDULE` or `-@admin`).
- **`protected-mode` is on by default** — while no password is configured, a listener that
  accepts non-loopback traffic (e.g. `bind 0.0.0.0`) refuses off-loopback clients with DENIED;
  loopback stays open. `protected-mode no` disables it. Note `CONFIG SET requirepass` lifts
  the fence live. Any client that can reach a `PSYNC`-capable listener and knows the password
  can still pull the whole dataset — replication auth is the same credential.
- **No `rename-command`** — dangerous commands can't be hidden, only ACL-denied.
- **`GETAT`/`HISTORY` and tiered storage** keep copies of values outside the in-memory
  keyspace — key ACLs don't retroactively hide already-persisted bytes.
