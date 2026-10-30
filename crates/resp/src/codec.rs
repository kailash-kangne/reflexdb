use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

use crate::value::{Protocol, Value};

#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A client request: a command name plus its arguments, both raw bytes.
///
/// Every real Redis request is a multibulk array of bulk strings (or, for
/// interactive `redis-cli`/`telnet` use, a single "inline" line of
/// space-separated words). Nothing else is a valid *request* shape, even
/// though RESP itself can represent richer values.
pub type Request = Vec<Bytes>;

/// Decodes a stream of client bytes into discrete `Request`s.
#[derive(Debug, Default)]
pub struct RequestDecoder;

impl Decoder for RequestDecoder {
    type Item = Request;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if src.is_empty() {
                return Ok(None);
            }

            if src[0] == b'*' {
                return decode_multibulk(src);
            }

            // A blank inline line is consumed and discarded, but that's not
            // the same as "need more bytes from the socket": the next real
            // request (inline or multibulk) may already be sitting right
            // behind it in `src`, so keep going instead of returning `None`
            // and stalling until more I/O happens.
            match decode_inline(src)? {
                InlineResult::Request(req) => return Ok(Some(req)),
                InlineResult::Blank => continue,
                InlineResult::NeedMoreData => return Ok(None),
            }
        }
    }
}

/// Find `\r\n` starting at `from`, returning the index of `\r`.
fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    if from >= buf.len() {
        return None;
    }
    buf[from..].windows(2).position(|w| w == b"\r\n").map(|p| from + p)
}

/// Result of trying to pull one inline line out of the buffer.
enum InlineResult {
    /// A real, non-blank request.
    Request(Request),
    /// A blank line was found and consumed; there may be more already
    /// buffered right behind it.
    Blank,
    /// No full line is buffered yet; need more bytes from the socket.
    NeedMoreData,
}

fn decode_inline(src: &mut BytesMut) -> Result<InlineResult, RespError> {
    // Inline commands are normally `\r\n`-terminated, but the protocol (and
    // real clients like `redis-cli --pipe`) also allow a bare `\n` with the
    // `\r` simply omitted. Find the `\n` and strip an optional trailing `\r`
    // ourselves, rather than requiring the full `\r\n` pair.
    let line_end = match src.iter().position(|&b| b == b'\n') {
        Some(idx) => idx,
        None => {
            if src.len() > 64 * 1024 {
                return Err(RespError::Protocol("inline request too long".into()));
            }
            return Ok(InlineResult::NeedMoreData);
        }
    };
    let line = src.split_to(line_end + 1);
    let mut line = &line[..line.len() - 1];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    let words: Vec<Bytes> = std::str::from_utf8(line)
        .map_err(|_| RespError::Protocol("invalid inline request encoding".into()))?
        .split_whitespace()
        .map(|w| Bytes::copy_from_slice(w.as_bytes()))
        .collect();
    if words.is_empty() {
        return Ok(InlineResult::Blank);
    }
    Ok(InlineResult::Request(words))
}

/// Hard cap on a single request's declared array element count. Without
/// this, a client can send just `*99999999999\r\n` — a 13-byte header,
/// zero actual elements — and make `Vec::with_capacity(count)` below try
/// to allocate gigabytes (or more) before a single array element has
/// arrived: an out-of-memory crash from a handful of bytes. Found by
/// fuzzing (`fuzz/fuzz_targets/resp_decode.rs`), not by design review —
/// see its `oom-*` artifact. `1024 * 1024` matches what real Redis itself
/// treats as an "invalid multibulk length" (no legitimate client command
/// has anywhere near a million top-level arguments).
const MAX_MULTIBULK_COUNT: i64 = 1024 * 1024;

/// Hard cap on a single bulk string's declared length — same
/// amplification shape as `MAX_MULTIBULK_COUNT` but for `$<len>`, and the
/// same value real Redis's `proto-max-bulk-len` defaults to. Unlike the
/// array-count case this one can't actually over-allocate (the data isn't
/// copied until it's confirmed already present in `src`, so growth is
/// naturally bounded by bytes the client has actually sent) — this is
/// belt-and-suspenders validation, rejecting an obviously-bogus length
/// instead of patiently waiting forever for 512MB+ that may never arrive.
const MAX_BULK_LEN: i64 = 64 * 1024 * 1024;
/// Maximum encoded size of one request frame. Prevents a client from
/// retaining arbitrarily many individually valid bulk strings in a frame.
const MAX_REQUEST_LEN: usize = 64 * 1024 * 1024;

fn decode_multibulk(src: &mut BytesMut) -> Result<Option<Request>, RespError> {
    let mut pos = 0usize;

    let header_end = match find_crlf(src, pos) {
        Some(i) => i,
        None => return Ok(None),
    };
    let count: i64 = parse_int(&src[1..header_end])?;
    pos = header_end + 2;

    if count < 0 {
        src.advance(pos);
        return Ok(Some(Vec::new()));
    }
    if count > MAX_MULTIBULK_COUNT {
        return Err(RespError::Protocol(format!("invalid multibulk length: {count}")));
    }
    let count = count as usize;
    // Do not trust the declared count for eager allocation: a tiny header
    // must not reserve millions of `Bytes` slots before any arrive.
    let mut items = Vec::new();

    for _ in 0..count {
        if pos >= src.len() {
            return Ok(None);
        }
        if src[pos] != b'$' {
            return Err(RespError::Protocol(format!("expected '$', got '{}'", src[pos] as char)));
        }
        let bulk_header_end = match find_crlf(src, pos + 1) {
            Some(i) => i,
            None => return Ok(None),
        };
        let len: i64 = parse_int(&src[pos + 1..bulk_header_end])?;
        if len < 0 {
            return Err(RespError::Protocol("invalid bulk length".into()));
        }
        if len > MAX_BULK_LEN {
            return Err(RespError::Protocol(format!("invalid bulk length: {len}")));
        }
        let len = len as usize;
        let data_start = bulk_header_end + 2;
        let data_end = data_start + len;
        // `data_end` is already an absolute offset from the frame start,
        // including every prior header and bulk payload.
        if data_end.saturating_add(2) > MAX_REQUEST_LEN {
            return Err(RespError::Protocol("request exceeds maximum size".into()));
        }
        if src.len() < data_end + 2 {
            return Ok(None);
        }
        if &src[data_end..data_end + 2] != b"\r\n" {
            return Err(RespError::Protocol("missing CRLF after bulk data".into()));
        }
        items.push(Bytes::copy_from_slice(&src[data_start..data_end]));
        pos = data_end + 2;
    }

    src.advance(pos);
    Ok(Some(items))
}

fn parse_int(buf: &[u8]) -> Result<i64, RespError> {
    std::str::from_utf8(buf)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .ok_or_else(|| RespError::Protocol("invalid integer".into()))
}

/// Encode a reply `Value` for the given negotiated protocol version.
pub fn encode_reply(value: &Value, proto: Protocol, out: &mut BytesMut) {
    value.encode(out, proto);
}
