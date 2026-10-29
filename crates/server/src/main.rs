mod replica_link;
mod tls;

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use commands::{dispatch, ClientState, Outcome, ServerContext, SingleFlightLease};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::codec::FramedRead;

use reflexdb_server::config::Config;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = match Config::from_args(std::env::args().skip(1)) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("reflexdb-server: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = std::fs::create_dir_all(&cfg.dir) {
        tracing::warn!(dir = %cfg.dir.display(), error = %e, "could not create data dir");
    }

    let store = if cfg.tiered_storage {
        match tiered::FjallTier::open(&cfg.tiered_dir_path()) {
            Ok(tier) => {
                tracing::info!(dir = %cfg.tiered_dir_path().display(), "tiered storage enabled");
                keyspace::Store::new_with_tiering(cfg.databases, std::sync::Arc::new(tier))
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to open tiered storage; continuing RAM-only");
                keyspace::Store::new(cfg.databases)
            }
        }
    } else {
        keyspace::Store::new(cfg.databases)
    };

    let mut ctx = ServerContext::with_store(store, cfg.requirepass.clone())
        .with_rdb_path(cfg.rdb_path())
        .with_maxmemory(cfg.maxmemory, cfg.maxmemory_policy)
        .with_tiered_limit(cfg.tiered_storage.then_some(cfg.tiered_memory_limit))
        .with_singleflight_max_wait(Duration::from_millis(cfg.singleflight_max_wait_ms))
        .with_notify_keyspace_events(commands::NotifyFlags::parse(&cfg.notify_keyspace_events));

    if cfg.history_enabled {
        match persistence::history::HistoryWriter::open(&cfg.history_dir_path()) {
            Ok(writer) => ctx = ctx.with_history(writer),
            Err(e) => tracing::warn!(error = %e, "failed to open history store; GETAT/HISTORY disabled"),
        }
    }

    // Restore prior state before wiring up the AOF writer below — replaying
    // into a context that's already AOF-attached would just re-log every
    // replayed command right back into the same file.
    if cfg.appendonly {
        match persistence::aof::replay(&cfg.aof_path()) {
            Ok(requests) => {
                let mut scratch = ClientState::with_id(0, false);
                let count = requests.len();
                for req in requests {
                    dispatch(&ctx, &mut scratch, req);
                }
                tracing::info!(commands = count, "replayed AOF");
            }
            Err(e) => tracing::warn!(error = %e, "failed to replay AOF"),
        }
    } else {
        match persistence::rdb::load(&cfg.rdb_path(), &ctx.store) {
            Ok(true) => tracing::info!("loaded RDB snapshot"),
            Ok(false) => {}
            Err(e) => tracing::warn!(error = %e, "failed to load RDB snapshot"),
        }
    }

    if cfg.appendonly {
        match persistence::aof::AofWriter::open(&cfg.aof_path(), cfg.appendfsync) {
            Ok(writer) => ctx = ctx.with_aof(writer, cfg.aof_path()),
            Err(e) => tracing::warn!(error = %e, "failed to open AOF for writing; continuing without it"),
        }
    }

    let ctx = Arc::new(ctx);

    // Background active-expire cycle, sampling a bounded number of keys.
    // The same tick also refreshes the `maxmemory` estimate and evicts keys
    // if it's exceeded (both no-ops unless `maxmemory` is configured), and
    // fires any `SCHEDULE AT`/`SCHEDULE IN` commands whose time has come.
    {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            loop {
                tick.tick().await;
                ctx.store.active_expire_cycle(20);
                ctx.run_eviction_tick();
                ctx.run_due_scheduled();
            }
        });
    }

    // Periodic `GETAT`/`HISTORY` retention sweep: reclaims every recorded
    // version older than `history-retention-ms`, regardless of whether its
    // live key still exists (see `persistence::history`'s module docs —
    // this is what bounds the store's size, replacing the old in-RAM
    // per-key version cap). A coarse interval is fine: unlike active
    // expiry, nothing is waiting on this to free up a hot key. No-op
    // (and basically free) while `history-enabled no`.
    if cfg.history_enabled {
        let ctx = ctx.clone();
        let retention_ms = cfg.history_retention_ms;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(300));
            loop {
                tick.tick().await;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let cutoff = now_ms.saturating_sub(retention_ms);
                let removed = ctx.history_sweep(cutoff);
                if removed > 0 {
                    tracing::debug!(removed, "swept expired history versions");
                }
            }
        });
    }

    // `appendfsync everysec`: fsync the AOF on a timer instead of after
    // every single write.
    if cfg.appendonly && cfg.appendfsync == persistence::aof::Fsync::EverySec {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                ctx.aof_fsync();
            }
        });
    }

    // Replication-link supervisor: watches `REPLICAOF`/`replication_epoch`
    // for the lifetime of the server and owns the actual outbound
    // connection to a primary, since that needs the `Arc` this task has
    // but `dispatch()` (called synchronously from any connection) doesn't.
    {
        let ctx = ctx.clone();
        tokio::spawn(replica_link::supervise(ctx));
    }
    if let Some((host, port)) = cfg.replicaof.clone() {
        let mut scratch = ClientState::with_id(0, false);
        dispatch(
            &ctx,
            &mut scratch,
            vec![
                bytes::Bytes::from_static(b"REPLICAOF"),
                bytes::Bytes::from(host),
                bytes::Bytes::from(port.to_string()),
            ],
        );
    }

    if cfg.tls_port != 0 {
        match (&cfg.tls_cert_file, &cfg.tls_key_file) {
            (Some(cert), Some(key)) => match tls::build_acceptor(cert, key) {
                Ok(acceptor) => {
                    let ctx = ctx.clone();
                    let bind = cfg.bind.clone();
                    let tls_port = cfg.tls_port;
                    tokio::spawn(async move {
                        if let Err(e) = run_tls_listener(ctx, &bind, tls_port, acceptor).await {
                            tracing::error!(error = %e, "TLS listener failed");
                        }
                    });
                }
                Err(e) => tracing::warn!(error = %e, "failed to configure TLS; continuing without it"),
            },
            _ => tracing::warn!("tls-port set but tls-cert-file/tls-key-file missing; TLS disabled"),
        }
    }

    let addr = format!("{}:{}", cfg.bind, cfg.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, databases = cfg.databases, "reflexdb-server listening");

    loop {
        let (socket, peer) = listener.accept().await?;
        socket.set_nodelay(true).ok();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(ctx, socket).await {
                tracing::debug!(%peer, error = %e, "connection closed");
            }
        });
    }
}

async fn run_tls_listener(
    ctx: Arc<ServerContext>,
    bind: &str,
    port: u16,
    acceptor: tokio_rustls::TlsAcceptor,
) -> std::io::Result<()> {
    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "reflexdb-server listening (TLS)");
    loop {
        let (socket, peer) = listener.accept().await?;
        socket.set_nodelay(true).ok();
        let ctx = ctx.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            match acceptor.accept(socket).await {
                Ok(tls_stream) => {
                    if let Err(e) = handle_connection(ctx, tls_stream).await {
                        tracing::debug!(%peer, error = %e, "TLS connection closed");
                    }
                }
                Err(e) => tracing::debug!(%peer, error = %e, "TLS handshake failed"),
            }
        });
    }
}

/// Drops a client's pub/sub + LIVE subscriptions when its connection ends,
/// however that happens (clean QUIT, protocol error, or the socket just
/// going away).
struct ClientGuard {
    ctx: Arc<ServerContext>,
    id: u64,
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.ctx.disconnect_client(self.id);
    }
}

/// Generic over the stream type so both plain `TcpStream` and a TLS-wrapped
/// one (`tokio_rustls::server::TlsStream`) share this same connection loop —
/// `tokio::io::split` works for any `AsyncRead + AsyncWrite`, unlike
/// `TcpStream::into_split`, which is TCP-specific.
async fn handle_connection<S>(ctx: Arc<ServerContext>, stream: S) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = FramedRead::new(read_half, resp::RequestDecoder);

    let (id, mut pushes) = ctx.register_client();
    let _guard = ClientGuard { ctx: ctx.clone(), id };

    let mut state = ClientState::with_id(id, ctx.requirepass.is_some());
    let mut out = BytesMut::new();

    loop {
        tokio::select! {
            // Pub/Sub message or LIVE update pushed from another connection.
            Some(value) = pushes.recv() => {
                out.clear();
                resp::encode_reply(&value, state.protocol, &mut out);
                write_half.write_all(&out).await?;
            }
            frame = tokio_stream_next(&mut reader) => {
                let req = match frame {
                    Some(Ok(req)) => req,
                    Some(Err(e)) => {
                        let value = resp::Value::error(format!("ERR Protocol error: {e}"));
                        out.clear();
                        resp::encode_reply(&value, state.protocol, &mut out);
                        write_half.write_all(&out).await?;
                        return Ok(());
                    }
                    None => return Ok(()),
                };

                // A pipelining client can have many requests already sitting
                // in `reader`'s internal buffer from one `read(2)`. Drain and
                // dispatch all of them into `out` before doing a single
                // `write_all` — flushing per-command here would otherwise
                // turn a pipelined burst into one syscall (and, with
                // `TCP_NODELAY`, one network packet) per command.
                out.clear();
                let mut next = Some(req);
                let mut eof = false;
                while let Some(req) = next.take() {
                    if !req.is_empty() {
                        // Single-flight / blocking waits are the cases
                        // dispatch() can't resolve synchronously — it needs
                        // this task to actually `.await` something, and that
                        // retry needs the original request bytes. Only the
                        // commands that can actually produce one of those
                        // outcomes are worth the clone — skipping it for
                        // plain `SET`/`GET`/etc. avoids an allocation on
                        // every single command, not just the rare blocking
                        // ones. Flush what's batched so far first on a
                        // retry, so reply order on the wire still matches
                        // request order.
                        let req_for_retry = if might_need_retry(&req) { Some(req.clone()) } else { None };
                        match dispatch(&ctx, &mut state, req) {
                            Outcome::Reply(value) => resp::encode_reply(&value, state.protocol, &mut out),
                            Outcome::Multi(values) => {
                                for value in &values {
                                    resp::encode_reply(value, state.protocol, &mut out);
                                }
                            }
                            Outcome::CloseConnection(value) => {
                                resp::encode_reply(&value, state.protocol, &mut out);
                                write_half.write_all(&out).await?;
                                return Ok(());
                            }
                            Outcome::SingleFlightWait(lease) => {
                                write_half.write_all(&out).await?;
                                out.clear();
                                let req_for_retry = req_for_retry
                                    .expect("SingleFlightWait only returned for commands might_need_retry covers");
                                let value = resolve_singleflight(&ctx, &mut state, req_for_retry, lease).await;
                                resp::encode_reply(&value, state.protocol, &mut out);
                            }
                            // `REPLCONF ACK` — deliberately no reply; see `Outcome::NoReply`'s docs.
                            Outcome::NoReply => {}
                            Outcome::WaitForReplicas { numreplicas, target_offset, timeout, numlocal } => {
                                write_half.write_all(&out).await?;
                                out.clear();
                                let value = resolve_wait(&ctx, numreplicas, target_offset, timeout, numlocal).await;
                                resp::encode_reply(&value, state.protocol, &mut out);
                            }
                            Outcome::BlockWait { deadline, on_timeout } => {
                                write_half.write_all(&out).await?;
                                out.clear();
                                let req_for_retry = req_for_retry
                                    .expect("BlockWait only returned for commands might_need_retry covers");
                                let deadline = deadline.map(tokio::time::Instant::from_std);
                                let value = resolve_block_wait(&ctx, &mut state, req_for_retry, deadline, on_timeout).await;
                                resp::encode_reply(&value, state.protocol, &mut out);
                            }
                        }
                    }

                    // Keep draining whatever's already buffered, without
                    // waiting on more I/O — that's what makes this a batch
                    // instead of just moving the same one write later.
                    match try_poll_frame(&mut reader) {
                        PolledFrame::Ready(Ok(more)) => next = Some(more),
                        PolledFrame::Ready(Err(e)) => {
                            let value = resp::Value::error(format!("ERR Protocol error: {e}"));
                            resp::encode_reply(&value, state.protocol, &mut out);
                            eof = true;
                        }
                        PolledFrame::Eof => eof = true,
                        PolledFrame::Pending => {}
                    }
                }

                write_half.write_all(&out).await?;
                if eof {
                    return Ok(());
                }
            }
        }
    }
}

/// Whether `req` could plausibly resolve to `Outcome::SingleFlightWait` or
/// `Outcome::BlockWait`, i.e. whether it's worth cloning so a retry has the
/// original bytes to re-dispatch. Only `GET` with a trailing modifier (the
/// `SINGLEFLIGHT` flag) and the blocking pop family can produce those —
/// everything else (plain `SET`/`GET`/`INCR`/...) never needs the clone,
/// which otherwise would've been a wasted allocation on every command.
fn might_need_retry(req: &resp::Request) -> bool {
    let Some(cmd) = req.first() else { return false };
    if cmd.eq_ignore_ascii_case(b"GET") {
        // Plain `GET key` can't take the SINGLEFLIGHT path; only `GET key
        // SINGLEFLIGHT [...]` can.
        return req.len() > 2;
    }
    [&b"BLPOP"[..], b"BRPOP", b"BLMOVE", b"BZPOPMIN", b"BZPOPMAX"]
        .iter()
        .any(|name| cmd.eq_ignore_ascii_case(name))
}

/// Outcome of a non-blocking poll for another already-buffered request.
enum PolledFrame {
    Ready(Result<resp::Request, resp::RespError>),
    /// The connection is closed and no more requests are coming.
    Eof,
    /// Nothing fully buffered right now; would need to wait on more I/O.
    Pending,
}

/// Like `tokio_stream_next`, but never waits on actual I/O: returns
/// `Pending` immediately if a full request isn't already sitting in
/// `reader`'s buffer, instead of registering for a wakeup. Used to batch a
/// pipelined client's backlog into one `write_all` instead of one per
/// command.
fn try_poll_frame<R>(reader: &mut FramedRead<R, resp::RequestDecoder>) -> PolledFrame
where
    R: AsyncRead + Unpin,
{
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};
    use tokio_stream::Stream;

    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(reader).poll_next(&mut cx) {
        Poll::Ready(Some(item)) => PolledFrame::Ready(item),
        Poll::Ready(None) => PolledFrame::Eof,
        Poll::Pending => PolledFrame::Pending,
    }
}

/// Repeatedly wait on a `GET ... SINGLEFLIGHT` lease and retry the original
/// request, until either a real value comes back (the leaseholder `SET` the
/// key, or this caller became the new leaseholder after the old lease
/// expired) or `singleflight_max_wait` is exceeded.
async fn resolve_singleflight(
    ctx: &ServerContext,
    state: &mut ClientState,
    original_req: resp::Request,
    mut lease: std::sync::Arc<SingleFlightLease>,
) -> resp::Value {
    let deadline = tokio::time::Instant::now() + ctx.singleflight_max_wait;
    loop {
        let wait_for = lease.remaining().min(deadline.saturating_duration_since(tokio::time::Instant::now()));
        if wait_for.is_zero() {
            return resp::Value::error("ERR SINGLEFLIGHT wait timed out");
        }
        tokio::select! {
            () = lease.notify.notified() => {}
            () = tokio::time::sleep(wait_for) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return resp::Value::error("ERR SINGLEFLIGHT wait timed out");
        }
        match dispatch(ctx, state, original_req.clone()) {
            Outcome::Reply(v) => return v,
            Outcome::Multi(mut vs) => return vs.pop().unwrap_or(resp::Value::NullBulk),
            Outcome::CloseConnection(v) => return v,
            Outcome::SingleFlightWait(new_lease) => lease = new_lease,
            // Can't actually happen for a retried `GET` — neither command
            // produces these — but the match has to be exhaustive.
            Outcome::NoReply => return resp::Value::NullBulk,
            Outcome::WaitForReplicas { .. } => {
                return resp::Value::error("ERR unexpected WAIT during SINGLEFLIGHT retry")
            }
            Outcome::BlockWait { .. } => {
                return resp::Value::error("ERR unexpected blocking command during SINGLEFLIGHT retry")
            }
        }
    }
}

/// Repeatedly wait on `blocking_notify` (or the command's own bounded
/// timeout, whichever comes first) and re-dispatch the exact same request —
/// same retry-the-original-request shape as `resolve_singleflight`/
/// `resolve_wait`. `deadline: None` means "block forever" (the `BLPOP`-
/// family's `timeout 0`).
async fn resolve_block_wait(
    ctx: &ServerContext,
    state: &mut ClientState,
    original_req: resp::Request,
    mut deadline: Option<tokio::time::Instant>,
    on_timeout: resp::Value,
) -> resp::Value {
    loop {
        let wait_for = match deadline {
            Some(d) => {
                let remaining = d.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return on_timeout;
                }
                Some(remaining)
            }
            None => None,
        };
        match wait_for {
            Some(d) => {
                tokio::select! {
                    () = ctx.blocking_notify.notified() => {}
                    () = tokio::time::sleep(d) => {}
                }
            }
            None => ctx.blocking_notify.notified().await,
        }
        match dispatch(ctx, state, original_req.clone()) {
            Outcome::Reply(v) => return v,
            Outcome::Multi(mut vs) => return vs.pop().unwrap_or(resp::Value::NullBulk),
            Outcome::CloseConnection(v) => return v,
            Outcome::BlockWait { deadline: new_deadline, .. } => {
                deadline = new_deadline.map(tokio::time::Instant::from_std);
            }
            // None of these make sense for a retried blocking command, but
            // the match has to be exhaustive.
            Outcome::SingleFlightWait(_) => {
                return resp::Value::error("ERR unexpected SINGLEFLIGHT during blocking retry")
            }
            Outcome::NoReply => return resp::Value::NullBulk,
            Outcome::WaitForReplicas { .. } => {
                return resp::Value::error("ERR unexpected WAIT during blocking retry")
            }
        }
    }
}

/// Poll until at least `numreplicas` have acked `target_offset` (woken by
/// `REPLCONF ACK` via `replica_ack_notify`, same bounded-wait pattern as
/// `resolve_singleflight`), or `timeout` elapses. `timeout: None` means
/// `WAIT numreplicas 0` — block indefinitely, matching Redis's `WAIT`.
/// On timeout, replies with however many replicas are caught up so far
/// (Redis's `WAIT` doesn't error on timeout, it returns the best-effort
/// count), not an error.
async fn resolve_wait(
    ctx: &ServerContext,
    numreplicas: usize,
    target_offset: u64,
    timeout: Option<Duration>,
    numlocal: Option<i64>,
) -> resp::Value {
    let finish = |satisfied: usize| match numlocal {
        Some(local) => {
            resp::Value::Array(vec![resp::Value::Integer(local), resp::Value::Integer(satisfied as i64)])
        }
        None => resp::Value::Integer(satisfied as i64),
    };
    let deadline = timeout.map(|d| tokio::time::Instant::now() + d);
    loop {
        let satisfied = ctx.count_replicas_at_least(target_offset);
        if satisfied >= numreplicas {
            return finish(satisfied);
        }
        let wait_for = match deadline {
            Some(d) => {
                let remaining = d.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return finish(satisfied);
                }
                Some(remaining)
            }
            None => None,
        };
        match wait_for {
            Some(d) => {
                tokio::select! {
                    () = ctx.replica_ack_notify.notified() => {}
                    () = tokio::time::sleep(d) => {}
                }
            }
            None => ctx.replica_ack_notify.notified().await,
        }
    }
}

/// Small shim so we don't need to pull in `futures::StreamExt` just for `.next()`.
async fn tokio_stream_next<R>(
    reader: &mut FramedRead<R, resp::RequestDecoder>,
) -> Option<Result<resp::Request, resp::RespError>>
where
    R: AsyncRead + Unpin,
{
    use tokio_stream::StreamExt;
    reader.next().await
}
