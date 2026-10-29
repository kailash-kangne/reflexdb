//! `reflexdb-proxy --listen <host:port> --upstream <host:port>` — a thin
//! pass-through proxy that sits in front of an existing RESP-compatible
//! server (see `docs/ROADMAP.md`: "distribution wedge"). Every byte a client
//! sends is relayed straight to `--upstream`, and every byte `--upstream`
//! replies with is relayed straight back — the proxy doesn't parse RESP at
//! all for this path, since a dumb bidirectional splice is both the
//! simplest and the fastest thing that can do it correctly for every
//! command, including ones this project hasn't implemented yet.
//!
//! Adoption cost for whoever puts this in front of an existing RESP server is
//! "change a port number," not "migrate your data" — nothing here touches
//! the upstream's protocol or data at all. Follow-up work (handling `LIVE`
//! locally, coalescing concurrent cache-miss reads, etc.) means actually
//! understanding RESP requests/replies and is deliberately not part of
//! this first pass.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};

struct Args {
    listen: String,
    upstream: String,
}

fn parse_args() -> Result<Args, String> {
    let mut listen = "127.0.0.1:7380".to_string();
    let mut upstream = None;
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        let flag = raw[i].trim_start_matches("--");
        let value = raw.get(i + 1).ok_or_else(|| format!("missing value for --{flag}"))?;
        match flag {
            "listen" => listen = value.clone(),
            "upstream" => upstream = Some(value.clone()),
            other => return Err(format!("unknown flag --{other}")),
        }
        i += 2;
    }
    let upstream = upstream.ok_or_else(|| "missing required --upstream <host:port>".to_string())?;
    Ok(Args { listen, upstream })
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("reflexdb-proxy: {e}");
            eprintln!("usage: reflexdb-proxy --listen <host:port> --upstream <host:port>");
            std::process::exit(1);
        }
    };

    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!(listen = %args.listen, upstream = %args.upstream, "reflexdb-proxy listening");

    // Just for log correlation across concurrent connections — not used
    // for anything functional.
    static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

    loop {
        let (client, peer) = listener.accept().await?;
        client.set_nodelay(true).ok();
        let upstream_addr = args.upstream.clone();
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            if let Err(e) = relay(client, &upstream_addr).await {
                tracing::debug!(conn_id, %peer, error = %e, "proxy connection closed");
            }
        });
    }
}

/// Dial `upstream` and splice it with `client` in both directions until
/// either side closes or errors.
async fn relay(mut client: TcpStream, upstream: &str) -> std::io::Result<()> {
    let mut upstream_conn = TcpStream::connect(upstream).await?;
    upstream_conn.set_nodelay(true).ok();
    copy_bidirectional(&mut client, &mut upstream_conn).await?;
    Ok(())
}
