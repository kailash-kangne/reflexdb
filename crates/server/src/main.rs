mod replica_link;
mod tls;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use commands::{dispatch, ClientState, Outcome, ServerContext, SingleFlightLease};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
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

    // TLS directives that can't be satisfied are a broken security
    // configuration — fail startup rather than silently downgrade.
    if cfg.tls_ca_cert_file.is_none() && (cfg.tls_auth_clients || cfg.tls_replication) {
        eprintln!("reflexdb-server: tls-auth-clients and tls-replication both require tls-ca-cert-file");
        std::process::exit(1);
    }
    if cfg.tls_auth_clients && cfg.tls_port == 0 {
        eprintln!("reflexdb-server: tls-auth-clients set but tls-port is disabled");
        std::process::exit(1);
    }

    let replica_tls = if cfg.tls_replication {
        // `tls_ca_cert_file` presence was validated above.
        let ca = cfg.tls_ca_cert_file.as_deref().expect("validated above");
        // The replica's own tls-cert-file/tls-key-file double as its client
        // certificate for primaries running tls-auth-clients.
        match tls::build_connector(ca, cfg.tls_cert_file.as_deref(), cfg.tls_key_file.as_deref()) {
            Ok(connector) => Some(connector),
            Err(e) => {
                eprintln!("reflexdb-server: tls-replication: {e}");
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    if let Err(e) = std::fs::create_dir_all(&cfg.dir) {
        tracing::warn!(dir = %cfg.dir.display(), error = %e, "could not create data dir");
    }

    // Held for the process lifetime; the OS releases the flock on any exit
    // (including SIGKILL), so a crashed server can't leave the dir locked.
    let _dir_lock = acquire_dir_lock(&cfg.dir);

    let store = if cfg.tiered_storage {
        match tiered::FjallTier::open(&cfg.tiered_dir_path()) {
            Ok(tier) => {
                tier.set_disk_limit(cfg.tiered_disk_limit.unwrap_or(0));
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
        .with_tiered_disk_limit(cfg.tiered_storage.then_some(cfg.tiered_disk_limit).flatten())
        .with_tiered_overflow_policy(cfg.tiered_overflow_policy)
        .with_slowlog(cfg.slowlog_log_slower_than_us, cfg.slowlog_max_len)
        .with_singleflight_max_wait(Duration::from_millis(cfg.singleflight_max_wait_ms))
        .with_notify_keyspace_events(commands::NotifyFlags::parse(&cfg.notify_keyspace_events))
        .with_proto_max_bulk_len(cfg.proto_max_bulk_len)
        .with_eviction_tuning(cfg.eviction_low_watermark, cfg.max_evictions_per_tick)
        .with_replica_backpressure(
            cfg.replica_backlog_size,
            cfg.replica_backlog_max_bytes,
            Duration::from_millis(cfg.replica_slow_disconnect_ms),
        )
        .with_min_replicas_to_write(cfg.min_replicas_to_write)
        .with_partial_resync_backlog(cfg.partial_resync_backlog)
        .with_live_pacing(cfg.live_sweep_ms, cfg.live_pace_chunk, cfg.live_pace_ms)
        .with_acl_file(cfg.acl_file.clone())
        .with_maxclients(cfg.maxclients)
        .with_masterauth(cfg.masterauth.clone())
        // Always record where these would live so `CONFIG SET
        // appendonly|history-enabled yes` can open them lazily.
        .with_aof_config(cfg.aof_path(), cfg.appendfsync)
        .with_auto_aof_rewrite(cfg.auto_aof_rewrite_percentage, cfg.auto_aof_rewrite_min_size)
        .with_history_config(cfg.history_dir_path(), cfg.history_max_versions_per_key)
        // Startup-only directives `CONFIG GET *` reports read-only (the
        // runtime-settable ones are emitted from their live state instead).
        .with_config_display({
            let yn = |b: bool| if b { "yes" } else { "no" }.to_string();
            let opt = |o: Option<u64>| o.map(|n| n.to_string()).unwrap_or_default();
            let path = |o: &Option<PathBuf>| o.as_ref().map(|p| p.display().to_string()).unwrap_or_default();
            vec![
                ("bind".into(), cfg.bind.clone()),
                ("port".into(), cfg.port.to_string()),
                ("proto-max-bulk-len".into(), cfg.proto_max_bulk_len.to_string()),
                ("protected-mode".into(), yn(cfg.protected_mode)),
                (
                    "masterauth".into(),
                    cfg.masterauth.as_ref().map(|_| "********".to_string()).unwrap_or_default(),
                ),
                ("profile".into(), cfg.profile.clone().unwrap_or_default()),
                ("tiered-storage".into(), yn(cfg.tiered_storage)),
                ("tiered-memory-limit".into(), cfg.tiered_memory_limit.to_string()),
                ("tiered-dir".into(), cfg.tiered_dir_path().display().to_string()),
                ("tiered-disk-limit".into(), opt(cfg.tiered_disk_limit)),
                ("tiered-overflow-policy".into(), cfg.tiered_overflow_policy.name().to_string()),
                ("eviction-tick-ms".into(), cfg.eviction_tick_ms.to_string()),
                ("eviction-low-watermark".into(), cfg.eviction_low_watermark.to_string()),
                ("eviction-max-per-tick".into(), cfg.max_evictions_per_tick.to_string()),
                ("singleflight-max-wait-ms".into(), cfg.singleflight_max_wait_ms.to_string()),
                (
                    "replicaof".into(),
                    cfg.replicaof.as_ref().map(|(h, p)| format!("{h} {p}")).unwrap_or_default(),
                ),
                ("tls-port".into(), cfg.tls_port.to_string()),
                ("tls-cert-file".into(), path(&cfg.tls_cert_file)),
                ("tls-key-file".into(), path(&cfg.tls_key_file)),
                ("tls-ca-cert-file".into(), path(&cfg.tls_ca_cert_file)),
                ("tls-auth-clients".into(), yn(cfg.tls_auth_clients)),
                ("tls-replication".into(), yn(cfg.tls_replication)),
                ("metrics-port".into(), cfg.metrics_port.to_string()),
                (
                    "metrics-token".into(),
                    cfg.metrics_token.as_ref().map(|_| "******".to_string()).unwrap_or_default(),
                ),
                ("history-dir".into(), cfg.history_dir_path().display().to_string()),
                ("history-retention-ms".into(), cfg.history_retention_ms.to_string()),
                ("history-max-versions-per-key".into(), cfg.history_max_versions_per_key.to_string()),
                ("repl-backlog-size".into(), cfg.replica_backlog_size.to_string()),
                ("repl-backlog-max-bytes".into(), cfg.replica_backlog_max_bytes.to_string()),
                ("partial-resync-backlog".into(), cfg.partial_resync_backlog.to_string()),
                ("repl-slow-disconnect-delay-ms".into(), cfg.replica_slow_disconnect_ms.to_string()),
                ("live-sweep-ms".into(), cfg.live_sweep_ms.to_string()),
                ("live-pace-chunk".into(), cfg.live_pace_chunk.to_string()),
                ("live-pace-ms".into(), cfg.live_pace_ms.to_string()),
            ]
        });

    // `aclfile`: a missing file just means "not saved yet" — the in-memory
    // defaults (requirepass-derived `default` user) still apply and the
    // first `ACL SAVE` creates it. A file that exists but won't parse is a
    // broken security configuration: refuse to start rather than silently
    // boot with different users than the operator asked for.
    if let Some(path) = &cfg.acl_file {
        if path.exists() {
            match ctx.load_acl_users(path) {
                Ok(count) => {
                    tracing::info!(users = count, path = %path.display(), "loaded ACL file")
                }
                Err(e) => {
                    eprintln!("reflexdb-server: {e}");
                    std::process::exit(1);
                }
            }
        } else {
            tracing::warn!(path = %path.display(), "aclfile does not exist; using in-memory users (ACL SAVE will create it)");
        }
    }

    if cfg.history_enabled {
        match persistence::history::HistoryWriter::open_with_limit(
            &cfg.history_dir_path(),
            cfg.history_max_versions_per_key,
        ) {
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
                // Internal apply: exempt from client-facing write gates
                // (READONLY on replicas, min-replicas-to-write) so replayed
                // data always lands.
                let mut scratch = ClientState::for_replication_link();
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

    // Re-arm `SCHEDULE` timers from the `~sched:*` markers persistence
    // just restored — runs again on replica promotion (replica_link).
    let rearmed = ctx.load_scheduled();
    if rearmed > 0 {
        tracing::info!(rearmed, "restored scheduled commands");
    }

    if let Some(profile) = &cfg.profile {
        tracing::info!(%profile, appendonly = cfg.appendonly, appendfsync = ?cfg.appendfsync,
            min_replicas_to_write = cfg.min_replicas_to_write, "durability profile applied");
    }

    let ctx = Arc::new(ctx);
    ctx.bind_live_fanout();

    // Background active-expire cycle, sampling a bounded number of keys.
    // The same tick also refreshes the `maxmemory` estimate and evicts keys
    // if it's exceeded (both no-ops unless `maxmemory` is configured), and
    // fires any `SCHEDULE AT`/`SCHEDULE IN` commands whose time has come.
    {
        let ctx = ctx.clone();
        let tick_ms = cfg.eviction_tick_ms.max(1);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(tick_ms));
            loop {
                tick.tick().await;
                let maintenance_ctx = ctx.clone();
                if let Err(error) = tokio::task::spawn_blocking(move || {
                    maintenance_ctx.store.active_expire_cycle(20);
                    maintenance_ctx.run_eviction_tick();
                    maintenance_ctx.run_due_scheduled();
                    maintenance_ctx.maybe_rewrite_aof();
                })
                .await
                {
                    tracing::warn!(%error, "maintenance task failed");
                }
            }
        });
    }

    // Periodic tiered-storage maintenance: GCs entries that expired while
    // down or never got touched, and major-compacts the LSM once enough
    // tombstones pile up — deletes don't reclaim file space until then.
    // Off the latency path; coarse 60s interval is fine. No-op (and ~free)
    // while `tiered-storage no`.
    if cfg.tiered_storage {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                let maintenance_ctx = ctx.clone();
                if let Err(error) =
                    tokio::task::spawn_blocking(move || maintenance_ctx.run_tiered_maintenance()).await
                {
                    tracing::warn!(%error, "tiered maintenance task failed");
                }
            }
        });
    }

    // Graceful shutdown on SIGTERM/SIGINT: same ritual `SHUTDOWN` runs —
    // final AOF fsync, plus an RDB snapshot when no AOF covers the writes.
    // persist_for_shutdown blocks the caller for the duration; it's a
    // dedicated task, so nothing else is harmed.
    {
        let shutdown_ctx = ctx.clone();
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received; flushing persistence");
            shutdown_ctx.persist_for_shutdown(!shutdown_ctx.aof_enabled());
            std::process::exit(0);
        });
    }

    // Periodic `GETAT`/`HISTORY` retention sweep: reclaims every recorded
    // version older than `history-retention-ms`, regardless of whether its
    // live key still exists. A zero retention disables age expiry; the
    // configured per-key version cap still applies. A coarse interval is
    // fine: unlike active
    // expiry, nothing is waiting on this to free up a hot key. No-op
    // (and basically free) while `history-enabled no`.
    if cfg.history_enabled && cfg.history_retention_ms > 0 {
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
                let sweep_ctx = ctx.clone();
                let removed = match tokio::task::spawn_blocking(move || sweep_ctx.history_sweep(cutoff)).await
                {
                    Ok(removed) => removed,
                    Err(error) => {
                        tracing::warn!(%error, "history sweep task failed");
                        0
                    }
                };
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
                let fsync_ctx = ctx.clone();
                if let Err(error) = tokio::task::spawn_blocking(move || fsync_ctx.aof_fsync()).await {
                    tracing::warn!(%error, "AOF fsync enqueue task failed");
                }
            }
        });
    }

    // Replication-link supervisor: watches `REPLICAOF`/`replication_epoch`
    // for the lifetime of the server and owns the actual outbound
    // connection to a primary, since that needs the `Arc` this task has
    // but `dispatch()` (called synchronously from any connection) doesn't.
    {
        let ctx = ctx.clone();
        tokio::spawn(replica_link::supervise(ctx, replica_tls));
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

    // `protected-mode` (default on, like Redis): while no password is
    // configured, a listener reachable off-host refuses non-loopback
    // clients with DENIED. `auth_required()` is re-checked per connection,
    // so a later `CONFIG SET requirepass` lifts the fence automatically.
    let protected = cfg.protected_mode && bind_accepts_non_loopback(&cfg.bind);

    let connection_slots = Arc::new(Semaphore::new(cfg.maxclients));
    let mut tls_listening = false;
    if cfg.tls_port != 0 {
        match (&cfg.tls_cert_file, &cfg.tls_key_file) {
            (Some(cert), Some(key)) => match tls::build_acceptor(
                cert,
                key,
                cfg.tls_auth_clients.then_some(cfg.tls_ca_cert_file.as_deref()).flatten(),
            ) {
                Ok(acceptor) => {
                    tls_listening = true;
                    let ctx = ctx.clone();
                    let bind = cfg.bind.clone();
                    let tls_port = cfg.tls_port;
                    let connection_slots = connection_slots.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            run_tls_listener(ctx, &bind, tls_port, acceptor, connection_slots, protected)
                                .await
                        {
                            tracing::error!(error = %e, "TLS listener failed");
                        }
                    });
                }
                Err(e) => tracing::warn!(error = %e, "failed to configure TLS; continuing without it"),
            },
            _ => tracing::warn!("tls-port set but tls-cert-file/tls-key-file missing; TLS disabled"),
        }
    }

    if cfg.metrics_port != 0 {
        let ctx = ctx.clone();
        let bind = cfg.bind.clone();
        let port = cfg.metrics_port;
        let token = cfg.metrics_token.clone();
        tokio::spawn(async move {
            if let Err(e) = run_metrics_listener(ctx, &bind, port, token).await {
                tracing::error!(error = %e, "metrics listener failed");
            }
        });
    }

    // `port 0` disables the plaintext listener (TLS-only mode).
    if cfg.port == 0 {
        if tls_listening {
            tracing::info!("plaintext listener disabled (port 0); serving TLS only");
            return std::future::pending().await;
        }
        eprintln!("reflexdb-server: both `port` and `tls-port` are disabled or TLS failed to start; nothing to listen on");
        std::process::exit(1);
    }

    let addr = format!("{}:{}", cfg.bind, cfg.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, databases = cfg.databases, "reflexdb-server listening");

    loop {
        let (mut socket, peer) = listener.accept().await?;
        if protected && !peer.ip().is_loopback() && !ctx.auth_required() {
            deny_protected(&mut socket, &peer).await;
            continue;
        }
        let Ok(slot) = connection_slots.clone().try_acquire_owned() else {
            drop(socket);
            tracing::debug!(%peer, "connection limit reached");
            continue;
        };
        socket.set_nodelay(true).ok();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _slot = slot;
            if let Err(e) = handle_connection(ctx, socket).await {
                tracing::debug!(%peer, error = %e, "connection closed");
            }
        });
    }
}

/// Whether `bind` admits off-host clients — `protected-mode` only needs
/// to engage when the listener is actually reachable non-locally. A
/// hostname we can't parse locally is treated as open (fail-safe).
fn bind_accepts_non_loopback(bind: &str) -> bool {
    match bind.parse::<std::net::IpAddr>() {
        Ok(ip) => !ip.is_loopback(),
        Err(_) => true,
    }
}

/// Sends the one-shot DENIED error, then drains until the client closes
/// (bounded) — a bare drop right after `write_all` discards the queued
/// reply into an RST on some stacks, so the client owns the close.
async fn deny_protected(socket: &mut tokio::net::TcpStream, peer: &std::net::SocketAddr) {
    let msg = "-DENIED ReflexDB is running in protected mode because protected-mode is enabled \
               and no password is configured, while the listener accepts non-loopback traffic. \
               Only loopback connections are served. Set `requirepass` (or `CONFIG SET requirepass` \
               from loopback) to authenticate, or run with `--protected-mode no` to disable.\r\n";
    let _ = socket.write_all(msg.as_bytes()).await;
    let mut buf = [0u8; 512];
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match socket.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    tracing::warn!(%peer, "protected-mode: refused non-loopback connection");
}

/// `metrics-port` listener: minimal HTTP/1.0 — read one request (any
/// path answers the same exposition, so `GET /` and `GET /metrics` both
/// work), respond, close. No keep-alive. `metrics-token` optionally
/// requires `Authorization: Bearer <token>`; with no token configured the
/// port is meant for loopback or a network-guarded boundary.
async fn run_metrics_listener(
    ctx: Arc<ServerContext>,
    bind: &str,
    port: u16,
    token: Option<String>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("{bind}:{port}")).await?;
    tracing::info!(bind, port, "metrics endpoint listening (Prometheus /metrics)");
    let expected = token.map(|t| format!("Bearer {t}"));
    loop {
        let (mut socket, _peer) = listener.accept().await?;
        let ctx = ctx.clone();
        let expected = expected.clone();
        tokio::spawn(async move {
            // Bound the request read — a scrape is one small request;
            // anything larger is not a scraper.
            let mut req = [0u8; 4096];
            let mut filled = 0;
            let headers_done = loop {
                match tokio::time::timeout(Duration::from_secs(5), socket.read(&mut req[filled..])).await {
                    Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return,
                    Ok(Ok(n)) => {
                        filled += n;
                        if req[..filled].windows(4).any(|w| w == b"\r\n\r\n") {
                            break true;
                        }
                        if filled == req.len() {
                            break false;
                        }
                    }
                }
            };
            if !headers_done {
                return;
            }
            if let Some(expected) = &expected {
                let authorized = String::from_utf8_lossy(&req[..filled]).lines().any(|line| {
                    line.split_once(':').is_some_and(|(name, value)| {
                        name.trim().eq_ignore_ascii_case("authorization") && value.trim() == expected
                    })
                });
                if !authorized {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.0 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        )
                        .await;
                    return;
                }
            }
            let body = commands::render_metrics(&ctx);
            let resp = format!(
                "HTTP/1.0 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        });
    }
}

async fn run_tls_listener(
    ctx: Arc<ServerContext>,
    bind: &str,
    port: u16,
    acceptor: tokio_rustls::TlsAcceptor,
    connection_slots: Arc<Semaphore>,
    protected: bool,
) -> std::io::Result<()> {
    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "reflexdb-server listening (TLS)");
    loop {
        let (mut socket, peer) = listener.accept().await?;
        if protected && !peer.ip().is_loopback() && !ctx.auth_required() {
            deny_protected(&mut socket, &peer).await;
            continue;
        }
        let Ok(slot) = connection_slots.clone().try_acquire_owned() else {
            drop(socket);
            tracing::debug!(%peer, "TLS connection limit reached");
            continue;
        };
        socket.set_nodelay(true).ok();
        let ctx = ctx.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let _slot = slot;
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
    let (read_half, write_half) = tokio::io::split(stream);
    let mut reader =
        FramedRead::new(read_half, resp::RequestDecoder::with_max_bulk_len(ctx.proto_max_bulk_len));

    let (id, mut pushes) = ctx.register_client();
    // The socket stays owned by this task until the client takes out a
    // `LIVE` subscription — only then does it move into the shared writer
    // the live-fanout thread `try_lock`s, so plain clients keep the direct
    // `write_all` fast path (no mutex + no `dyn` per reply). `push_live`
    // falls back to the `pushes` queue while no writer is attached, so a
    // subscriber can't miss pushes in the gap.
    let mut write_half = Some(write_half);
    let mut writer: Option<Arc<commands::ClientWriter>> = None;
    let _guard = ClientGuard { ctx: ctx.clone(), id };

    let mut state = ClientState::with_id(id, ctx.auth_required());
    let mut out = BytesMut::new();
    let mut replica_pushes: Option<tokio::sync::mpsc::Receiver<commands::ReplicaMessage>> = None;

    loop {
        // Hand the socket to the shared writer once this client has LIVE
        // subscriptions — the only client type the fan-out thread pushes to.
        if writer.is_none() && !state.live_fingerprints.is_empty() {
            let w = ctx.attach_writer(
                id,
                write_half.take().expect("write half is moved into the writer exactly once"),
            );
            w.note_format(state.wire, state.protocol);
            writer = Some(w);
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(25)), if ctx.is_slow_client(id) && !ctx.has_replica_client(id) => {
                return Ok(());
            }
            _ = tokio::time::sleep(ctx.slow_replica_remaining(id).unwrap_or(Duration::from_secs(5))), if ctx.is_slow_replica(id) => {
                tracing::debug!(client_id = id, "disconnecting replica after its output buffer overflowed");
                return Ok(());
            }
            // Out-of-band pushes: pub/sub messages (encoded per this
            // connection's protocol) and `LIVE` updates (already encoded
            // once per wire variant — just pick the matching bytes). The
            // queue is bounded; overflow flags the client slow instead of
            // growing memory, same as pub/sub.
            Some(push) = pushes.recv() => {
                out.clear();
                let mut push = Some(push);
                // Drain whatever else is already queued — one syscall for
                // the whole batch instead of one per push.
                while let Some(p) = push.take() {
                    match p {
                        commands::ClientPush::Value(value) => {
                            encode_out(&value, &state, &mut out);
                        }
                        commands::ClientPush::Live(encoded) => {
                            out.extend_from_slice(encoded.for_wire(state.wire, state.protocol));
                        }
                    }
                    push = pushes.try_recv().ok();
                }
                flush(&mut writer, &mut write_half, &out).await?;
            }
            replica_push = next_replica_push(&mut replica_pushes) => {
                if let Some(push) = replica_push {
                    out.clear();
                    // Replication frames arrive pre-encoded (the stream is
                    // always RESP2 regardless of this conn's negotiated wire).
                    out.extend_from_slice(&push.data);
                    // Same batching: pull any already-queued replication
                    // frames into this one write.
                    while let Ok(next) = replica_pushes.as_mut().unwrap().try_recv() {
                        out.extend_from_slice(&next.data);
                    }
                    flush(&mut writer, &mut write_half, &out).await?;
                } else {
                    return Ok(());
                }
            }
            // `LIVE` pushes arrive on the shared `pushes` queue above.
            frame = tokio_stream_next(&mut reader) => {
                let req = match frame {
                    Some(Ok(req)) => req,
                    Some(Err(e)) => {
                        state.wire = reader.decoder().wire();
                        if let Some(w) = &writer {
                            w.note_format(state.wire, state.protocol);
                        }
                        let value = resp::Value::error(format!("ERR Protocol error: {e}"));
                        out.clear();
                        encode_out(&value, &state, &mut out);
                        flush(&mut writer, &mut write_half, &out).await?;
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
                        let is_psync = req.first().is_some_and(|name| name.eq_ignore_ascii_case(b"PSYNC"));
                        let outcome = dispatch_offloaded(ctx.clone(), &mut state, req).await;
                        if is_psync {
                            replica_pushes = ctx.take_replica_receiver(id);
                        }
                        state.wire = reader.decoder().wire();
                        if let Some(w) = &writer {
                            w.note_format(state.wire, state.protocol);
                        }
                        match outcome {
                            Outcome::Reply(value) => encode_out(&value, &state, &mut out),
                            Outcome::Multi(values) => {
                                for value in &values {
                                    encode_out(value, &state, &mut out);
                                }
                            }
                            Outcome::CloseConnection(value) => {
                                encode_out(&value, &state, &mut out);
                                flush(&mut writer, &mut write_half, &out).await?;
                                return Ok(());
                            }
                            // No reply first — like Redis, a client that
                            // SHUTDOWNs just sees the connection die.
                            Outcome::Shutdown { save } => {
                                ctx.persist_for_shutdown(save);
                                std::process::exit(0);
                            }
                            Outcome::SingleFlightWait(lease) => {
                                flush(&mut writer, &mut write_half, &out).await?;
                                out.clear();
                                let req_for_retry = req_for_retry
                                    .expect("SingleFlightWait only returned for commands might_need_retry covers");
                                let value = resolve_singleflight(&ctx, &mut state, req_for_retry, lease).await;
                                encode_out(&value, &state, &mut out);
                            }
                            // `REPLCONF ACK` — deliberately no reply; see `Outcome::NoReply`'s docs.
                            Outcome::NoReply => {}
                            Outcome::WaitForReplicas { numreplicas, target_offset, timeout, numlocal } => {
                                flush(&mut writer, &mut write_half, &out).await?;
                                out.clear();
                                let value = resolve_wait(&ctx, numreplicas, target_offset, timeout, numlocal).await;
                                encode_out(&value, &state, &mut out);
                            }
                            Outcome::BlockWait { deadline, on_timeout } => {
                                flush(&mut writer, &mut write_half, &out).await?;
                                out.clear();
                                let req_for_retry = req_for_retry
                                    .expect("BlockWait only returned for commands might_need_retry covers");
                                let deadline = deadline.map(tokio::time::Instant::from_std);
                                let value = resolve_block_wait(&ctx, &mut state, req_for_retry, deadline, on_timeout).await;
                                encode_out(&value, &state, &mut out);
                            }
                        }
                    }

                    // Keep draining whatever's already buffered, without
                    // waiting on more I/O — that's what makes this a batch
                    // instead of just moving the same one write later.
                    match try_poll_frame(&mut reader) {
                        PolledFrame::Ready(Ok(more)) => next = Some(more),
                        PolledFrame::Ready(Err(e)) => {
                            state.wire = reader.decoder().wire();
                            if let Some(w) = &writer {
                                w.note_format(state.wire, state.protocol);
                            }
                            let value = resp::Value::error(format!("ERR Protocol error: {e}"));
                            encode_out(&value, &state, &mut out);
                            eof = true;
                        }
                        PolledFrame::Eof => eof = true,
                        PolledFrame::Pending => {}
                    }
                }

                flush(&mut writer, &mut write_half, &out).await?;
                if eof {
                    return Ok(());
                }
            }
        }
    }
}

/// Encodes a reply in whichever wire format this connection negotiated —
/// RFX binary once the decoder latched `!`-framed requests, RESP
/// (respecting the `HELLO`-negotiated version) otherwise.
fn encode_out(value: &resp::Value, state: &ClientState, out: &mut BytesMut) {
    match state.wire {
        resp::Wire::Rfx => resp::encode_rfx_reply(value, out),
        resp::Wire::Resp => resp::encode_reply(value, state.protocol, out),
    }
}

/// Flushes `bytes` to the client: a direct write while the connection
/// still owns its socket half, a locked shared-writer write once it's
/// attached — the `live-fanout` thread `try_lock`s the same mutex for
/// direct LIVE pushes, so reply and push bytes can never interleave
/// mid-frame.
async fn flush<S>(
    writer: &mut Option<Arc<commands::ClientWriter>>,
    write_half: &mut Option<tokio::io::WriteHalf<S>>,
    bytes: &[u8],
) -> std::io::Result<()>
where
    S: AsyncWrite + Send + Unpin + 'static,
{
    match (writer, write_half) {
        (Some(w), _) => w.w.lock().await.write_all(bytes).await,
        (None, Some(h)) => h.write_all(bytes).await,
        (None, None) => unreachable!("write half is moved into the writer exactly once"),
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

async fn next_replica_push(
    receiver: &mut Option<tokio::sync::mpsc::Receiver<commands::ReplicaMessage>>,
) -> Option<commands::ReplicaMessage> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

/// Run persistence writes and snapshot commands on Tokio's blocking pool.
/// Tier storage I/O yields the worker at the Fjall/keyspace boundary instead,
/// so RAM-hit commands avoid a per-command pool handoff.
pub(crate) async fn dispatch_offloaded(
    ctx: Arc<ServerContext>,
    state: &mut ClientState,
    req: resp::Request,
) -> Outcome {
    let offload = ((ctx.aof_enabled() || ctx.history_enabled()) && commands::request_may_write(&req))
        || req.first().is_some_and(|name| {
            matches!(
                name.to_ascii_uppercase().as_slice(),
                b"SAVE" | b"BGSAVE" | b"BGREWRITEAOF" | b"WAITAOF" | b"EXEC"
            )
        });
    if !offload {
        return dispatch(&ctx, state, req);
    }

    // While the owned state is on the blocking pool, this placeholder is
    // never observed by another task; the connection loop awaits completion.
    // Preserve protocol so a worker panic can still close cleanly.
    let mut placeholder = ClientState::with_id(state.id, ctx.auth_required());
    placeholder.protocol = state.protocol;
    let owned_state = std::mem::replace(state, placeholder);
    match tokio::task::spawn_blocking(move || {
        let mut owned_state = owned_state;
        let outcome = dispatch(&ctx, &mut owned_state, req);
        (owned_state, outcome)
    })
    .await
    {
        Ok((owned_state, outcome)) => {
            *state = owned_state;
            outcome
        }
        Err(error) => {
            tracing::error!(%error, "blocking command dispatch failed");
            Outcome::CloseConnection(resp::Value::error("ERR blocking command dispatch failed"))
        }
    }
}

/// Repeatedly wait on a `GET ... SINGLEFLIGHT` lease and retry the original
/// request, until either a real value comes back (the leaseholder `SET` the
/// key, or this caller became the new leaseholder after the old lease
/// expired) or `singleflight_max_wait` is exceeded.
async fn resolve_singleflight(
    ctx: &Arc<ServerContext>,
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
        match dispatch_offloaded(ctx.clone(), state, original_req.clone()).await {
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
            Outcome::Shutdown { .. } => {
                return resp::Value::error("ERR unexpected SHUTDOWN during SINGLEFLIGHT retry")
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
    ctx: &Arc<ServerContext>,
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
        match dispatch_offloaded(ctx.clone(), state, original_req.clone()).await {
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
            Outcome::Shutdown { .. } => {
                return resp::Value::error("ERR unexpected SHUTDOWN during blocking retry")
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

/// Opens `<dir>/reflexdb.lock` and takes an exclusive non-blocking flock on
/// it. A second server pointed at the same data dir gets a clear refusal
/// instead of two writers corrupting the same AOF/RDB. Unix only — on other
/// platforms the pid file is still written but no lock is taken.
fn acquire_dir_lock(dir: &std::path::Path) -> std::fs::File {
    use std::io::Write;
    let path = dir.join("reflexdb.lock");
    let file =
        std::fs::OpenOptions::new().create(true).truncate(true).write(true).open(&path).unwrap_or_else(|e| {
            eprintln!("reflexdb-server: cannot open {}: {e}", path.display());
            std::process::exit(1);
        });
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            eprintln!(
                "reflexdb-server: {} is locked by another reflexdb-server (or a stale mount); \
                 refusing to share a data dir",
                path.display()
            );
            std::process::exit(1);
        }
    }
    let _ = (&file).write_all(std::process::id().to_string().as_bytes());
    file
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
