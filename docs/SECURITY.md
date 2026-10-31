# Threat model

What reflexdb defends against, what it deliberately doesn't, and where the sharp edges are.
Read this before deploying it anywhere traffic you don't control can reach.

## Trust boundaries

- **Client connections** — RESP2/RESP3/RFX on the plaintext listener, optionally a TLS
  listener (`tls-port` + `tls-cert-file`/`tls-key-file`; `port 0` disables plaintext for
  TLS-only mode). RFX is auto-detected by a leading `!` byte on the same port — it is a wire
  encoding, not an authentication path, and gets the same ACL checks.
- **Replication link** — outbound replica connection to a primary, **plaintext only**
  (`tls-replication` is not implemented). A replica trusts everything the primary sends:
  run replication only on trusted networks.
- **Persistence files** — AOF/RDB/history/tiered files under `dir`/`tiered-dir`. Anyone with
  filesystem access reads the dataset; nothing at rest is encrypted.
- **Lua** — `EVAL`/`EVALSHA` run in an `mlua` Lua 5.1 sandbox with `os`/`io` removed and
  `math.random` seeded.
- **Config file** — directives are trusted operator input; unknown directives warn and skip.

## Defenses implemented

- Unauthenticated connections may only run `AUTH`/`HELLO`/`QUIT`/`RESET`; everything else
  gets `NOAUTH` (`dispatch.rs`).
- ACL: per-user command allow/deny (`+cmd`/`-cmd`/`allcommands`/`nocommands`), command
  categories (`+@read`, `-@write`, ...), key globs (`~pattern`), and read/write-scoped
  patterns (`%R~`/`%W~`). Rules that can't be enforced (`&channel` patterns, `(...)`
  selectors, unknown category names) are **rejected** at `SETUSER` time rather than silently
  accepted. Passwords stored as SHA-256 hashes; the `default` user mirrors plain
  `requirepass` semantics.
- Key-pattern checks decode the key operands of every known command shape
  (`acl::command_keys`), including multi-key and `EVAL`/`SORT`/`XREAD` layouts.
- Connection cap: `maxclients` semaphore; excess sockets are dropped at accept.
- Protocol limits: `MAX_MULTIBULK_COUNT` and `MAX_BULK_LEN` (64 MiB) cap declared lengths
  before allocation in the codec; oversized buffers are dropped.
- Pub/sub + LIVE push queues are bounded; overflowing subscribers are disconnected.
- `RESTORE` payloads are bounded by the codec's `MAX_BULK_LEN`.

## Known gaps (do not treat as closed)

- **No `AUTH` rate limiting** — online brute-force against `requirepass`/user passwords is
  possible. Use long passwords (`ACL GENPASS`) and network-level controls.
- **Passwords are unsalted SHA-256** — Redis-compatible but weak against offline attacks if
  an attacker reads process memory or `ACL LIST`/`GETUSER` output. Treat hash disclosure as
  password disclosure.
- **ACLs are in-memory only** — no `aclfile`, `ACL LOAD`/`SAVE`, or `ACL LOG`; users vanish
  on restart and there is no audit trail of denied commands.
- **Plaintext listener is on by default** — TLS is additive (`tls-port`); set `port 0`
  explicitly for TLS-only.
- **No channel-pattern ACLs** — pub/sub channel restrictions (`&pattern`) are rejected, not
  enforced; any authenticated user can subscribe to any channel.
- **`KEYS`/`CONFIG`/`ACL` are @dangerous but enabled for `allcommands` users** — grant them
  deliberately.
- **One writer per data dir** — two servers sharing a `dir` will corrupt the AOF (verified in
  testing). There is no lockfile protecting against this.
- **`LIVE`/`SCHEDULE` execute on behalf of the subscriber/issuer** — scheduled commands run
  later with server-side privileges; restrict `SCHEDULE` (`-SCHEDULE` or `-@admin`).
- **No `rename-command` or `protected-mode`** — dangerous commands can't be hidden, only
  ACL-denied, and there is no loopback-only safety net: bind addresses and auth are your job.
- **`GETAT`/`HISTORY` and tiered storage** keep copies of values outside the in-memory
  keyspace — key ACLs don't retroactively hide already-persisted bytes.
