//! A minimal RESP client — connect, send one command, read one reply.
//! Used by `reflexdb-sentinel` to `PING`/`INFO` the nodes it monitors and
//! to issue `REPLICAOF`/`PUBLISH` during failover. Not exposed to end
//! users; just enough of the protocol for Sentinel's own needs (no RESP3
//! push types, no pipelining).

use std::io;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

const MAX_REPLY_LINE: usize = 64 * 1024;
const MAX_BULK_LEN: usize = 64 * 1024 * 1024;
const MAX_ARRAY_LEN: usize = 1_000_000;

/// A parsed reply. Flat enough for Sentinel's needs — `PING`/`REPLICAOF`
/// (simple string), `PUBLISH` (integer), `INFO` (bulk string), `SUBSCRIBE`
/// (array of scalars).
#[derive(Debug)]
pub enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

pub struct RespClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl RespClient {
    pub async fn connect(host: &str, port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true).ok();
        let (r, w) = stream.into_split();
        Ok(Self { reader: BufReader::new(r), writer: w })
    }

    pub async fn call(&mut self, args: &[&str]) -> io::Result<Reply> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&out).await?;
        self.read_reply().await
    }

    /// Reads one more reply frame off the wire without sending anything —
    /// for commands like `SUBSCRIBE`/`PSUBSCRIBE` (one confirmation frame
    /// per channel/pattern argument, on top of `call`'s first read) and
    /// `LIVE`/pub-sub (unsolicited frames pushed later, whenever the
    /// subscribed key/channel changes). `reflexdb-cli` is the only caller.
    pub async fn read_next(&mut self) -> io::Result<Reply> {
        self.read_reply().await
    }

    async fn read_line(&mut self) -> io::Result<String> {
        let mut line = String::new();
        loop {
            let available = tokio::time::timeout(std::time::Duration::from_secs(30), self.reader.fill_buf())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "reply line timed out"))??;
            if available.is_empty() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed"));
            }
            let take = available.iter().position(|b| *b == b'\n').map_or(available.len(), |p| p + 1);
            if line.len().saturating_add(take) > MAX_REPLY_LINE {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "reply line exceeds limit"));
            }
            line.push_str(
                std::str::from_utf8(&available[..take])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "reply line is not UTF-8"))?,
            );
            self.reader.consume(take);
            if line.ends_with('\n') {
                break;
            }
        }
        if !line.ends_with("\r\n") {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "reply line exceeds limit or lacks CRLF"));
        }
        Ok(line.trim_end().to_string())
    }

    /// Boxed because this recurses one level for `Array` replies — plain
    /// `async fn`s can't be directly recursive in Rust.
    fn read_reply(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Reply>> + Send + '_>> {
        self.read_reply_depth(0)
    }

    fn read_reply_depth(
        &mut self,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Reply>> + Send + '_>> {
        Box::pin(async move {
            const MAX_REPLY_DEPTH: usize = 64;
            if depth > MAX_REPLY_DEPTH {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "reply nesting exceeds limit"));
            }
            let line = self.read_line().await?;
            if line.is_empty() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "empty reply line"));
            }
            let (tag, rest) = line.split_at(1);
            match tag {
                "+" => Ok(Reply::Simple(rest.to_string())),
                "-" => Ok(Reply::Error(rest.to_string())),
                ":" => Ok(Reply::Integer(
                    rest.parse()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid integer reply"))?,
                )),
                "$" => {
                    let len: i64 = rest
                        .parse()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid bulk length"))?;
                    if len < 0 {
                        return Ok(Reply::Bulk(None));
                    }
                    if len as u64 > MAX_BULK_LEN as u64 {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "bulk reply exceeds limit"));
                    }
                    let mut buf = vec![0u8; len as usize];
                    tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        self.reader.read_exact(&mut buf),
                    )
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "bulk reply timed out"))??;
                    let mut crlf = [0u8; 2];
                    self.reader.read_exact(&mut crlf).await?;
                    if crlf != *b"\r\n" {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid bulk terminator"));
                    }
                    Ok(Reply::Bulk(Some(buf)))
                }
                "*" => {
                    let len: i64 = rest
                        .parse()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid array length"))?;
                    if len < 0 {
                        return Ok(Reply::Array(None));
                    }
                    if len as u64 > MAX_ARRAY_LEN as u64 {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "array reply exceeds limit"));
                    }
                    let mut items = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        items.push(self.read_reply_depth(depth + 1).await?);
                    }
                    Ok(Reply::Array(Some(items)))
                }
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unexpected reply: {line}"))),
            }
        })
    }
}
