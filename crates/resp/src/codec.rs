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

/// Which wire format a connection negotiated. `Rfx` frames start with
/// `!` (never valid at the start of a RESP request), so the decoder
/// sniffs the first byte and latches the format for the connection's
/// lifetime — see `docs/RFX.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Wire {
    #[default]
    Resp,
    Rfx,
}

/// Decodes a stream of client bytes into discrete `Request`s.
///
/// `max_bulk_len`/`max_request_len` default to `64 MiB` (`Default`,
/// real Redis's own `proto-max-bulk-len` default) and can be overridden
/// with `with_max_bulk_len` — see `server`'s `proto-max-bulk-len` config
/// directive for the operational trade-off. The same caps cover RFX
/// frames.
#[derive(Debug)]
pub struct RequestDecoder {
    max_bulk_len: i64,
    max_request_len: usize,
    /// `None` until the first frame identifies the connection's format;
    /// latched so the connection can't mix protocols mid-stream.
    wire: Option<Wire>,
}

impl Default for RequestDecoder {
    fn default() -> Self {
        Self {
            max_bulk_len: DEFAULT_MAX_BULK_LEN,
            max_request_len: DEFAULT_MAX_BULK_LEN as usize,
            wire: None,
        }
    }
}

impl RequestDecoder {
    /// Both the largest single bulk-string element and the largest total
    /// request frame this decoder will accept. Tied together (rather than
    /// two independent knobs) since real clients never need a request
    /// frame bigger than its own biggest element by more than a small,
    /// fixed overhead — splitting them would just be two numbers an
    /// operator has to keep in sync for no practical benefit.
    pub fn with_max_bulk_len(max_bulk_len: usize) -> Self {
        Self { max_bulk_len: max_bulk_len as i64, max_request_len: max_bulk_len, wire: None }
    }

    /// The negotiated wire format — `Resp` until a `!`-prefixed frame
    /// arrives (or while nothing has been decoded yet, in which case
    /// there's nothing to reply to anyway).
    pub fn wire(&self) -> Wire {
        self.wire.unwrap_or_default()
    }
}

impl Decoder for RequestDecoder {
    type Item = Request;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if src.is_empty() {
                return Ok(None);
            }

            match self.wire {
                Some(Wire::Rfx) => {
                    if src[0] != b'!' {
                        return Err(RespError::Protocol("expected '!' frame on RFX connection".into()));
                    }
                    return decode_rfx(src, self.max_bulk_len, self.max_request_len);
                }
                Some(Wire::Resp) => {
                    if src[0] == b'!' {
                        return Err(RespError::Protocol("unexpected '!' frame on RESP connection".into()));
                    }
                }
                None => {
                    self.wire = Some(if src[0] == b'!' { Wire::Rfx } else { Wire::Resp });
                    continue;
                }
            }

            if src[0] == b'*' {
                return decode_multibulk(src, self.max_bulk_len, self.max_request_len);
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

/// Default hard cap on a single bulk string's declared length — same
/// amplification shape as `MAX_MULTIBULK_COUNT` but for `$<len>`, and the
/// same value real Redis's `proto-max-bulk-len` defaults to. Unlike the
/// array-count case this one can't actually over-allocate (the data isn't
/// copied until it's confirmed already present in `src`, so growth is
/// naturally bounded by bytes the client has actually sent) — this is
/// belt-and-suspenders validation, rejecting an obviously-bogus length
/// instead of patiently waiting forever for 512MB+ that may never arrive.
/// Configurable per `RequestDecoder` (see `with_max_bulk_len`) — this is
/// just the `Default` value.
const DEFAULT_MAX_BULK_LEN: i64 = 64 * 1024 * 1024;

fn decode_multibulk(
    src: &mut BytesMut,
    max_bulk_len: i64,
    max_request_len: usize,
) -> Result<Option<Request>, RespError> {
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
        if len > max_bulk_len {
            return Err(RespError::Protocol(format!("invalid bulk length: {len}")));
        }
        let len = len as usize;
        let data_start = bulk_header_end + 2;
        let data_end = data_start + len;
        // `data_end` is already an absolute offset from the frame start,
        // including every prior header and bulk payload.
        if data_end.saturating_add(2) > max_request_len {
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

/// Current RFX frame version — bump when the request layout changes.
pub const RFX_VERSION: u8 = 1;

/// Decode one RFX frame — `! ver:u8 nargs:u16be` followed by `nargs` pairs
/// of `len:u32be` + raw bytes. Two passes: the first bounds-checks the
/// whole frame before touching `src`, the second splits the frame out
/// once and slices each arg from it — zero copies, one allocation for the
/// `Vec`. Same caps as `decode_multibulk` (`nargs` is bounded by u16,
/// per-arg and total sizes rejected before the frame is consumed).
fn decode_rfx(
    src: &mut BytesMut,
    max_bulk_len: i64,
    max_request_len: usize,
) -> Result<Option<Request>, RespError> {
    const HEADER: usize = 1 + 1 + 2;
    if src.len() < HEADER {
        return Ok(None);
    }
    if src[1] != RFX_VERSION {
        return Err(RespError::Protocol(format!("unsupported RFX version: {}", src[1])));
    }
    let count = u16::from_be_bytes([src[2], src[3]]) as usize;
    let mut pos = HEADER;
    for _ in 0..count {
        if pos + 4 > src.len() {
            return Ok(None);
        }
        let len = u32::from_be_bytes([src[pos], src[pos + 1], src[pos + 2], src[pos + 3]]) as usize;
        if len as i64 > max_bulk_len {
            return Err(RespError::Protocol(format!("invalid bulk length: {len}")));
        }
        pos += 4;
        if pos.saturating_add(len) > max_request_len {
            return Err(RespError::Protocol("request exceeds maximum size".into()));
        }
        if src.len() < pos + len {
            return Ok(None);
        }
        pos += len;
    }
    // Frame is fully buffered and validated — `with_capacity` can't be
    // abused here the way a bare `*<count>` header could.
    let frame = src.split_to(pos).freeze();
    let mut items = Vec::with_capacity(count);
    let mut off = HEADER;
    for _ in 0..count {
        let len = u32::from_be_bytes([frame[off], frame[off + 1], frame[off + 2], frame[off + 3]]) as usize;
        off += 4;
        items.push(frame.slice(off..off + len));
        off += len;
    }
    Ok(Some(items))
}

/// Encode a reply `Value` for the given negotiated protocol version.
pub fn encode_reply(value: &Value, proto: Protocol, out: &mut BytesMut) {
    value.encode(out, proto);
}

/// Encode a reply `Value` in RFX binary format (`docs/RFX.md`).
pub fn encode_rfx_reply(value: &Value, out: &mut BytesMut) {
    value.encode_rfx(out);
}
