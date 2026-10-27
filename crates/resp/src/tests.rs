use bytes::{Bytes, BytesMut};
use tokio_util::codec::Decoder;

use crate::codec::RequestDecoder;
use crate::value::{Protocol, Value};

#[test]
fn decodes_multibulk_set_command() {
    let mut buf = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..]);
    let mut decoder = RequestDecoder;
    let req = decoder.decode(&mut buf).unwrap().unwrap();
    assert_eq!(req.len(), 3);
    assert_eq!(&req[0][..], b"SET");
    assert_eq!(&req[1][..], b"foo");
    assert_eq!(&req[2][..], b"bar");
    assert!(buf.is_empty());
}

#[test]
fn decodes_inline_command() {
    let mut buf = BytesMut::from(&b"PING\r\n"[..]);
    let mut decoder = RequestDecoder;
    let req = decoder.decode(&mut buf).unwrap().unwrap();
    assert_eq!(req.len(), 1);
    assert_eq!(&req[0][..], b"PING");
}

#[test]
fn decodes_inline_command_with_bare_lf() {
    // `redis-cli --pipe` (and the inline protocol spec) terminate lines with
    // a bare `\n`, not `\r\n` — regression test for a bug where multiple
    // `\n`-separated commands got glued into one bad command.
    let mut buf = BytesMut::from(&b"SET a 1\nSET b 2\nGET a\n"[..]);
    let mut decoder = RequestDecoder;

    let req = decoder.decode(&mut buf).unwrap().unwrap();
    assert_eq!(req, vec![Bytes::from_static(b"SET"), Bytes::from_static(b"a"), Bytes::from_static(b"1")]);

    let req = decoder.decode(&mut buf).unwrap().unwrap();
    assert_eq!(req, vec![Bytes::from_static(b"SET"), Bytes::from_static(b"b"), Bytes::from_static(b"2")]);

    let req = decoder.decode(&mut buf).unwrap().unwrap();
    assert_eq!(req, vec![Bytes::from_static(b"GET"), Bytes::from_static(b"a")]);
    assert!(buf.is_empty());
}

#[test]
fn skips_a_blank_inline_line_without_waiting_on_more_io() {
    // A blank line (just `\r\n`) followed immediately by a real request in
    // the same buffer must not make `decode` return `None` — that would
    // stall the connection until more bytes arrive from the socket, even
    // though the next request is already fully buffered.
    let mut buf = BytesMut::from(&b"\r\n*1\r\n$4\r\nPING\r\n"[..]);
    let mut decoder = RequestDecoder;
    let req = decoder.decode(&mut buf).unwrap().unwrap();
    assert_eq!(req, vec![Bytes::from_static(b"PING")]);
    assert!(buf.is_empty());
}

#[test]
fn returns_none_on_partial_frame() {
    let mut buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfo"[..]);
    let mut decoder = RequestDecoder;
    assert!(decoder.decode(&mut buf).unwrap().is_none());
}

/// Regression test for a real out-of-memory bug found by fuzzing
/// (`fuzz/fuzz_targets/resp_decode.rs`, artifact `oom-56e331660...`), not
/// by design review: a declared multibulk array count fed straight into
/// `Vec::with_capacity` with no upper bound meant a client could send a
/// ~13-byte header (`*99999999999999\r\n`) and make the decoder try to
/// allocate gigabytes before a single array element arrived. Must now
/// return a protocol error instead of panicking/aborting/hanging.
#[test]
fn rejects_an_absurd_multibulk_count_instead_of_allocating_it() {
    let mut buf = BytesMut::from(&b"*99999999999999\r\n"[..]);
    let mut decoder = RequestDecoder;
    assert!(decoder.decode(&mut buf).is_err());
}

/// Same amplification shape as the multibulk-count bug above, but for a
/// single bulk string's declared length (`$<len>`) — added alongside it as
/// defense-in-depth, see `MAX_BULK_LEN`'s doc comment for why this one
/// isn't actually exploitable the same way (no pre-allocation happens),
/// but should still error promptly rather than waiting forever for bytes
/// that may never come.
#[test]
fn rejects_an_absurd_bulk_length() {
    let mut buf = BytesMut::from(&b"*1\r\n$99999999999999\r\n"[..]);
    let mut decoder = RequestDecoder;
    assert!(decoder.decode(&mut buf).is_err());
}

#[test]
fn encodes_simple_and_bulk_resp2() {
    let mut out = BytesMut::new();
    Value::ok().encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b"+OK\r\n");

    out.clear();
    Value::bulk(&b"hello"[..]).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b"$5\r\nhello\r\n");

    out.clear();
    Value::NullBulk.encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b"$-1\r\n");
}

#[test]
fn encodes_null_resp3() {
    let mut out = BytesMut::new();
    Value::NullBulk.encode(&mut out, Protocol::Resp3);
    assert_eq!(&out[..], b"_\r\n");
}

#[test]
fn encodes_boolean_downgrades_on_resp2() {
    let mut out = BytesMut::new();
    Value::Boolean(true).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b":1\r\n");

    out.clear();
    Value::Boolean(true).encode(&mut out, Protocol::Resp3);
    assert_eq!(&out[..], b"#t\r\n");
}

/// `encode`'s integer/length writer is a hand-rolled, allocation-free
/// digit writer (replacing `n.to_string()`) — exercise the edge cases
/// that kind of code tends to get wrong: zero, a negative value, and
/// `i64::MIN` (whose magnitude doesn't fit in an `i64`, only a `u64`).
#[test]
fn encodes_integer_edge_cases_without_the_to_string_helper() {
    let mut out = BytesMut::new();
    Value::Integer(0).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b":0\r\n");

    out.clear();
    Value::Integer(-42).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b":-42\r\n");

    out.clear();
    Value::Integer(i64::MIN).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], format!(":{}\r\n", i64::MIN).as_bytes());

    out.clear();
    Value::Integer(i64::MAX).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], format!(":{}\r\n", i64::MAX).as_bytes());
}

/// `Map`/`Set`/`Push` downgrade to a plain RESP2 array by writing items
/// directly instead of cloning into a throwaway `Value::Array` — check the
/// wire output still matches what that clone-based path used to produce.
#[test]
fn map_set_push_downgrade_to_the_same_resp2_array_shape() {
    let mut out = BytesMut::new();
    Value::Map(vec![(Value::bulk(&b"a"[..]), Value::Integer(1))]).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b"*2\r\n$1\r\na\r\n:1\r\n");

    out.clear();
    Value::Set(vec![Value::Integer(1), Value::Integer(2)]).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b"*2\r\n:1\r\n:2\r\n");

    out.clear();
    Value::Push(vec![Value::bulk(&b"msg"[..])]).encode(&mut out, Protocol::Resp2);
    assert_eq!(&out[..], b"*1\r\n$3\r\nmsg\r\n");
}
