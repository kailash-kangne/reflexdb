# RFX wire protocol

RFX is ReflexDB's optional binary request/reply format. It runs on the **same port** as RESP —
the server sniffs the first byte of a connection (`!` = RFX, anything else = RESP) and latches
that format for the connection's lifetime. No config, no handshake: a client picks RFX simply
by sending `!`-framed requests. Mixing formats on one connection is a protocol error.

All commands, replies, pub/sub messages, and `LIVE` pushes work identically in both formats —
RFX only changes how bytes are framed on the wire.

## Request frame

```
!  ver:u8  nargs:u16be   then nargs × ( arg_len:u32be + arg_bytes )
```

`ver` is the frame version — currently `0x01`. Any other value is a protocol error and closes
the connection, so a future RFX2 can't be silently mis-parsed by an older server.

Example, `SET foo bar`:

```
21 01 00 03  00 00 00 03 53 45 54  00 00 00 03 66 6f 6f  00 00 00 03 62 61 72
!  v1 nargs=3  len=3     "SET"      len=3     "foo"      len=3     "bar"
```

Limits: `nargs` ≤ 65535 (u16), each `arg_len` ≤ the server's `proto-max-bulk-len` (default
64 MiB), total frame ≤ the same cap. Violations are a protocol error and close the connection.

## Reply frames

One-byte tag (same characters as RESP where they exist) + binary fields, no `\r\n`:

| Tag | Type    | Body                                          |
|-----|---------|-----------------------------------------------|
| `+` | simple string | `len:u32` + bytes                         |
| `-` | error   | `len:u32` + bytes                               |
| `:` | integer | `i64` big-endian                                |
| `$` | bulk string | `len:u32` + bytes                           |
| `_` | null    | (no body — covers null bulk/array/scalar)       |
| `*` | array   | `count:u32` + count nested reply frames         |
| `>` | push    | `count:u32` + count nested reply frames         |
| `~` | set     | `count:u32` + count nested reply frames         |
| `%` | map     | `count:u32` + count × (key frame + value frame) |
| `#` | boolean | `u8` (0 = false, 1 = true)                      |
| `,` | double  | `f64` big-endian                                |
| `(` | big number | `len:u32` + bytes                          |

Push-type replies (pub/sub messages, `LIVE` updates) arrive unsolicited, exactly as in RESP —
a client reading replies must be prepared for `>`/`*` frames that don't correspond to a
request it sent.

## Design notes

- **No request-id**: replies are strictly ordered like RESP — the Nth reply answers the Nth
  request — so pipelining needs no per-request correlation field. A request-id would cost
  4–8 bytes per frame plus client-side correlation state, and only buys out-of-order or
  interleaved replies, which a command/response store doesn't need. Unsolicited pushes are
  distinguishable by their `>` tag rather than a sequence number.
- **Zero-copy decode**: the server validates the whole frame in place, then slices each arg
  out of the socket buffer — no per-argument copies, one `Vec` per request.
- **Why a version byte rather than versioned first-request handshakes**: it keeps every
  frame self-describing and the decoder stateless beyond the `!` latch — costs one byte per
  request (~1–3% on tiny commands) in exchange for never mis-parsing a newer frame.
- **Metadata/extensibility**: there is no per-frame metadata field by design; anything that
  needs out-of-band data today (auth, `HELLO`-style negotiation) already rides in-band as
  ordinary commands. If a real need emerges, a version bump gets a clean cut rather than a
  permanently-reserved field every client pays for.
