//! Outbound replication client: the actual TCP connection to a primary,
//! plus the supervisor that watches `REPLICAOF` changes. This lives in the
//! `server` crate (not `commands`) because it needs the long-lived
//! `Arc<ServerContext>` a background task requires — `commands::dispatch()`
//! only ever borrows `&ServerContext` for the duration of one call, so it
//! can flip `ServerContext::replica_of`/`replication_epoch` but can't itself
//! hold a connection open.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use commands::{ClientState, ServerContext};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

/// Runs forever: whenever `REPLICAOF` changes `replication_epoch`, (re)dials
/// whatever `replica_of_addr()` now says (or does nothing for `NO ONE`).
pub async fn supervise(ctx: Arc<ServerContext>) {
    let mut last_seen_epoch = u64::MAX; // guarantees the first tick checks
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        tick.tick().await;
        let epoch = ctx.replication_epoch();
        if epoch == last_seen_epoch {
            continue;
        }
        last_seen_epoch = epoch;
        if let Some((host, port)) = ctx.replica_of_addr() {
            tracing::info!(%host, port, "REPLICAOF: starting replication link");
            let ctx = ctx.clone();
            tokio::spawn(async move { run_link(ctx, host, port, epoch).await });
        } else {
            tracing::info!("REPLICAOF NO ONE: promoted back to primary");
        }
    }
}

/// Keeps a replication link to `host:port` alive — full resync, then apply
/// the replicated stream — reconnecting with a short backoff on failure, for
/// as long as `epoch` is still the current one (i.e. `REPLICAOF` hasn't
/// pointed elsewhere, or back to `NO ONE`, since this task started).
async fn run_link(ctx: Arc<ServerContext>, host: String, port: u16, epoch: u64) {
    while ctx.replication_epoch() == epoch {
        if let Err(e) = try_link_once(&ctx, &host, port, epoch).await {
            tracing::warn!(%host, port, error = %e, "replication link dropped; retrying");
        }
        if ctx.replication_epoch() != epoch {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn try_link_once(ctx: &Arc<ServerContext>, host: &str, port: u16, epoch: u64) -> io::Result<()> {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    const FULL_RESYNC_TIMEOUT: Duration = Duration::from_secs(300);
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "replication connect timed out"))??;
    stream.set_nodelay(true).ok();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let (mut applied, snapshot) = tokio::time::timeout(FULL_RESYNC_TIMEOUT, async {
        write_half.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
        let offset = read_fullresync_line(&mut reader).await?;
        let snapshot = read_bulk_payload(&mut reader).await?;
        io::Result::Ok((offset, snapshot))
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "replication full resync timed out"))??;

    for i in 0..ctx.store.len() {
        if let Some(db) = ctx.store.db(i) {
            db.flush();
        }
    }
    persistence::rdb::deserialize(&snapshot, &ctx.store)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    tracing::info!(%host, port, offset = applied, "replica: full resync complete");

    // From here, the primary streams plain RESP command arrays — identical
    // framing to a normal client request, so the same decoder applies.
    let mut commands = FramedRead::new(reader, resp::RequestDecoder::default());
    let mut scratch = ClientState::for_replication_link();

    loop {
        if ctx.replication_epoch() != epoch {
            return Ok(());
        }
        tokio::select! {
            frame = commands.next() => {
                match frame {
                    Some(Ok(req)) => {
                        crate::dispatch_offloaded(ctx.clone(), &mut scratch, req).await;
                        applied += 1;
                    }
                    Some(Err(e)) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
                    None => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "primary closed the connection")),
                }
            }
            // Idle tick: still gives us a chance to notice an epoch change
            // and to send a heartbeat ACK even when nothing's been written.
            () = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
        send_ack(&mut write_half, applied).await?;
    }
}

/// Reads the `+FULLRESYNC <replid> <offset>\r\n` line and returns the offset.
async fn read_fullresync_line(reader: &mut (impl AsyncBufReadExt + Unpin)) -> io::Result<u64> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let line = line.trim_end();
    let rest = line.strip_prefix('+').ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, format!("expected PSYNC reply, got: {line}"))
    })?;
    let mut parts = rest.split_whitespace();
    match (parts.next(), parts.next(), parts.next()) {
        (Some("FULLRESYNC"), Some(_replid), Some(offset)) => offset
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("bad offset in: {line}"))),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unexpected PSYNC reply: {line}"))),
    }
}

/// Reads a `$<len>\r\n<raw bytes>` bulk payload (no trailing `\r\n` — this
/// isn't a reply to a request, it's the raw RDB-style snapshot stream).
async fn read_bulk_payload(
    reader: &mut (impl AsyncBufReadExt + AsyncReadExt + Unpin),
) -> io::Result<Vec<u8>> {
    const MAX_RDB_SIZE: u64 = 512 * 1024 * 1024;
    const MAX_HEADER_LEN: usize = 64;
    let mut header = Vec::with_capacity(16);
    loop {
        if header.len() >= MAX_HEADER_LEN {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "RDB bulk header too long"));
        }
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        if byte[0] == b'\n' {
            break;
        }
        header.push(byte[0]);
    }
    let header = std::str::from_utf8(&header)
        .ok()
        .and_then(|s| s.strip_suffix('\r'))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid RDB bulk header"))?;
    let len: usize = header
        .strip_prefix('$')
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|n| *n <= MAX_RDB_SIZE)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid or oversized RDB bulk length"))?;
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn send_ack(write_half: &mut (impl AsyncWriteExt + Unpin), offset: u64) -> io::Result<()> {
    let offset_str = offset.to_string();
    let mut out = BytesMut::new();
    out.extend_from_slice(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$");
    out.extend_from_slice(offset_str.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(offset_str.as_bytes());
    out.extend_from_slice(b"\r\n");
    write_half.write_all(&out).await
}
