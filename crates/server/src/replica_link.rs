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
use rustls::pki_types::ServerName;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

/// What a link needs to resume after a reconnect: the primary's replid
/// and the stream offset already consumed, plus the `ClientState` that
/// applies the stream (it carries the current db selection, which a
/// `+CONTINUE` resume must keep). Lives in `supervise` so a `REPLICAOF`
/// re-issue to the same primary resumes with `PSYNC <replid> <offset>`
/// instead of re-fetching a full snapshot. Cleared on `REPLICAOF NO ONE`
/// — a promoted replica's data has diverged, so resuming is unsafe.
struct LinkCursor {
    replid: String,
    offset: u64,
    /// `replica_generation` when this cursor was created — a promotion
    /// (`REPLICAOF NO ONE`) bumps the generation, so a stale cursor is
    /// dropped instead of resuming over diverged data.
    generation: u64,
    scratch: ClientState,
}

/// Runs forever: whenever `REPLICAOF` changes `replication_epoch`, (re)dials
/// whatever `replica_of_addr()` now says (or does nothing for `NO ONE`).
/// `tls` is `Some` when `tls-replication yes` — the link to the primary is
/// then wrapped in TLS, verified against `tls-ca-cert-file`.
pub async fn supervise(ctx: Arc<ServerContext>, tls: Option<TlsConnector>) {
    let mut last_seen_epoch = u64::MAX; // guarantees the first tick checks
    let mut cursor: Option<LinkCursor> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        tick.tick().await;
        let epoch = ctx.replication_epoch();
        if epoch == last_seen_epoch {
            continue;
        }
        last_seen_epoch = epoch;
        match ctx.replica_of_addr() {
            Some((host, port)) => {
                tracing::info!(%host, port, "REPLICAOF: starting replication link");
                // A promotion since this cursor was born invalidates it —
                // also covers NO ONE→Some within one tick, which the
                // `None` branch below can never observe.
                if cursor.as_ref().is_some_and(|c| c.generation != ctx.replica_generation()) {
                    cursor = None;
                }
                // Inline, not spawned: `run_link` exits exactly when the
                // epoch changes, and keeping `cursor` in this frame lets a
                // re-issued REPLICAOF to the same primary partial-resync.
                run_link(&ctx, host, port, epoch, tls.as_ref(), &mut cursor).await;
            }
            None => {
                cursor = None;
                tracing::info!("REPLICAOF NO ONE: promoted back to primary");
                // A promoted replica's armed timers were never firing —
                // re-arm (idempotently) from `~sched:*` markers now that
                // this node owns them.
                let armed = ctx.load_scheduled();
                if armed > 0 {
                    tracing::info!(armed, "re-armed scheduled commands after promotion");
                }
            }
        }
    }
}

/// Keeps a replication link to `host:port` alive — resync, then apply
/// the replicated stream — reconnecting with a short backoff on failure, for
/// as long as `epoch` is still the current one (i.e. `REPLICAOF` hasn't
/// pointed elsewhere, or back to `NO ONE`, since this task started).
async fn run_link(
    ctx: &Arc<ServerContext>,
    host: String,
    port: u16,
    epoch: u64,
    tls: Option<&TlsConnector>,
    cursor: &mut Option<LinkCursor>,
) {
    while ctx.replication_epoch() == epoch {
        if let Err(e) = try_link_once(ctx, &host, port, epoch, tls, cursor).await {
            tracing::warn!(%host, port, error = %e, "replication link dropped; retrying");
        }
        if ctx.replication_epoch() != epoch {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn try_link_once(
    ctx: &Arc<ServerContext>,
    host: &str,
    port: u16,
    epoch: u64,
    tls: Option<&TlsConnector>,
    cursor: &mut Option<LinkCursor>,
) -> io::Result<()> {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    const FULL_RESYNC_TIMEOUT: Duration = Duration::from_secs(300);
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "replication connect timed out"))??;
    stream.set_nodelay(true).ok();
    // Plain TCP and TLS share the rest of the link path via boxed halves —
    // trait-object overhead is irrelevant next to TLS itself, and boxing
    // keeps one `BufReader`/`send_ack` implementation for both.
    let (read_half, mut write_half): (Box<dyn AsyncRead + Unpin + Send>, Box<dyn AsyncWrite + Unpin + Send>) =
        match tls {
            Some(connector) => {
                let server_name = ServerName::try_from(host.to_string()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("invalid TLS server name: {host}"))
                })?;
                let tls_stream = connector.connect(server_name, stream).await.map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("replication TLS handshake failed: {e}"),
                    )
                })?;
                let (r, w) = tokio::io::split(tls_stream);
                (Box::new(r), Box::new(w))
            }
            None => {
                let (r, w) = stream.into_split();
                (Box::new(r), Box::new(w))
            }
        };
    let mut reader = BufReader::new(read_half);

    // `masterauth`: a password-protected primary NOAUTHs PSYNC, so AUTH
    // first. Wrong/missing credentials surface as a link error and the
    // supervisor retries (logging the primary's reply).
    if let Some(password) = ctx.masterauth.as_deref() {
        write_half.write_all(&auth_request(password)).await?;
        read_simple_reply(&mut reader).await?;
    }

    // Resume when we have a cursor from a previous link (`PSYNC <replid>
    // <offset>`); a first-time link asks for everything (`PSYNC ? -1`).
    let psync = match cursor.as_ref() {
        Some(c) => psync_request(&c.replid, &c.offset.to_string()),
        None => psync_request("?", "-1"),
    };
    let handshake = tokio::time::timeout(FULL_RESYNC_TIMEOUT, async {
        write_half.write_all(&psync).await?;
        match read_psync_reply(&mut reader).await? {
            PsyncReply::Full { replid, offset } => {
                let snapshot = read_bulk_payload(&mut reader).await?;
                io::Result::Ok(Handshake::Full { replid, offset, snapshot })
            }
            PsyncReply::Continue => io::Result::Ok(Handshake::Continue),
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "replication resync timed out"))??;

    match handshake {
        Handshake::Full { replid, offset, snapshot } => {
            for i in 0..ctx.store.len() {
                if let Some(db) = ctx.store.db(i) {
                    db.flush();
                }
            }
            persistence::rdb::deserialize(&snapshot, &ctx.store)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            *cursor = Some(LinkCursor {
                replid,
                offset,
                generation: ctx.replica_generation(),
                scratch: ClientState::for_replication_link(),
            });
            tracing::info!(%host, port, offset, "replica: full resync complete");
        }
        // A `+CONTINUE` resume keeps both the data and the cursor's
        // scratch state (its db selection is still valid — the catch-up
        // stream re-selects dbs as needed).
        Handshake::Continue => {
            let Some(c) = cursor.as_ref() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "primary sent CONTINUE to a first-time PSYNC",
                ));
            };
            tracing::info!(%host, port, offset = c.offset, "replica: partial resync");
        }
    }
    let cursor = cursor.as_mut().expect("cursor set by handshake");

    // From here, the primary streams plain RESP command arrays — identical
    // framing to a normal client request, so the same decoder applies.
    let mut commands = FramedRead::new(reader, resp::RequestDecoder::default());

    loop {
        if ctx.replication_epoch() != epoch {
            return Ok(());
        }
        tokio::select! {
            frame = commands.next() => {
                match frame {
                    Some(Ok(req)) => {
                        // `SELECT`s are primary-side stream bookkeeping,
                        // not replicated writes — they must not advance
                        // the offset or the next `PSYNC` would resume
                        // past commands it never received.
                        let counts = !req.first().is_some_and(|c| c.eq_ignore_ascii_case(b"SELECT"));
                        crate::dispatch_offloaded(ctx.clone(), &mut cursor.scratch, req).await;
                        if counts {
                            cursor.offset += 1;
                        }
                    }
                    Some(Err(e)) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
                    None => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "primary closed the connection")),
                }
            }
            // Idle tick: still gives us a chance to notice an epoch change
            // and to send a heartbeat ACK even when nothing's been written.
            () = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
        send_ack(&mut write_half, cursor.offset).await?;
    }
}

enum Handshake {
    Full { replid: String, offset: u64, snapshot: Vec<u8> },
    Continue,
}

enum PsyncReply {
    Full { replid: String, offset: u64 },
    Continue,
}

/// `*2\r\n$4\r\nAUTH\r\n$<n>\r\n<password>\r\n` — the `default`-user form
/// (matches `requirepass`, which is what `masterauth` pairs with).
fn auth_request(password: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(24 + password.len());
    out.extend_from_slice(b"*2\r\n$4\r\nAUTH\r\n$");
    out.extend_from_slice(password.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(password.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

/// Reads one `+`/`-` line reply — used for the `AUTH` handshake where
/// only success vs. failure matters.
async fn read_simple_reply(reader: &mut (impl AsyncBufReadExt + Unpin)) -> io::Result<()> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let line = line.trim_end();
    match line.as_bytes().first() {
        Some(b'+') => Ok(()),
        _ => Err(io::Error::new(io::ErrorKind::PermissionDenied, format!("primary rejected AUTH: {line}"))),
    }
}

/// `*3\r\n$5\r\nPSYNC\r\n$<n>\r\n<replid>\r\n$<n>\r\n<offset>\r\n`.
fn psync_request(replid: &str, offset: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + replid.len() + offset.len());
    out.extend_from_slice(b"*3\r\n$5\r\nPSYNC\r\n$");
    out.extend_from_slice(replid.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(replid.as_bytes());
    out.extend_from_slice(b"\r\n$");
    out.extend_from_slice(offset.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(offset.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

/// Reads the `+FULLRESYNC <replid> <offset>\r\n` or `+CONTINUE\r\n` line
/// the primary answers a `PSYNC` with.
async fn read_psync_reply(reader: &mut (impl AsyncBufReadExt + Unpin)) -> io::Result<PsyncReply> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let line = line.trim_end();
    let rest = line.strip_prefix('+').ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, format!("expected PSYNC reply, got: {line}"))
    })?;
    let mut parts = rest.split_whitespace();
    match parts.next() {
        Some("FULLRESYNC") => match (parts.next(), parts.next()) {
            (Some(replid), Some(offset)) => Ok(PsyncReply::Full {
                replid: replid.to_string(),
                offset: offset.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("bad offset in: {line}"))
                })?,
            }),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unexpected PSYNC reply: {line}"))),
        },
        Some("CONTINUE") => Ok(PsyncReply::Continue),
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
