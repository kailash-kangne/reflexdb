//! Sentinel's own RESP listener: `PING` plus a `SENTINEL` subcommand
//! surface (`MASTERS`, `MASTER`, `REPLICAS`, `GET-MASTER-ADDR-BY-NAME`,
//! `IS-MASTER-DOWN-BY-ADDR`, `CKQUORUM`). Reuses `resp::RequestDecoder`/
//! `resp::encode_reply` directly — same wire format as the main server,
//! just a much smaller command set.
//!
//! Not implemented: `SENTINEL MONITOR`/`REMOVE`/`SET` (dynamic
//! reconfiguration — masters come only from `sentinel.conf` at startup),
//! `SENTINEL SENTINELS` (listing peers), and the `__sentinel__:hello`
//! pub/sub discovery channel real Sentinel uses instead of this file's
//! explicit `known-sentinel`/`known-replica` config.

use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use resp::{Protocol, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::state::MasterState;

pub async fn run(
    bind: &str,
    port: u16,
    masters: Arc<DashMap<String, Arc<MasterState>>>,
) -> std::io::Result<()> {
    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr).await?;
    const MAX_CONNECTIONS: usize = 1024;
    let connection_slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    tracing::info!(%addr, "reflexdb-sentinel listening");
    loop {
        let (socket, peer) = listener.accept().await?;
        let Ok(slot) = connection_slots.clone().try_acquire_owned() else {
            drop(socket);
            tracing::debug!(%peer, "sentinel connection limit reached");
            continue;
        };
        socket.set_nodelay(true).ok();
        let masters = masters.clone();
        tokio::spawn(async move {
            let _slot = slot;
            if let Err(e) = handle_connection(socket, masters).await {
                tracing::debug!(%peer, error = %e, "sentinel connection closed");
            }
        });
    }
}

async fn handle_connection(
    socket: tokio::net::TcpStream,
    masters: Arc<DashMap<String, Arc<MasterState>>>,
) -> std::io::Result<()> {
    let (read_half, mut write_half) = tokio::io::split(socket);
    let mut reader = FramedRead::new(read_half, resp::RequestDecoder);
    let mut out = BytesMut::new();

    while let Some(frame) = reader.next().await {
        let req = match frame {
            Ok(req) if !req.is_empty() => req,
            Ok(_) => continue,
            Err(_) => return Ok(()),
        };
        let value = dispatch(&masters, &req);
        out.clear();
        resp::encode_reply(&value, Protocol::Resp2, &mut out);
        write_half.write_all(&out).await?;
    }
    Ok(())
}

fn dispatch(masters: &DashMap<String, Arc<MasterState>>, req: &[bytes::Bytes]) -> Value {
    let name = String::from_utf8_lossy(&req[0]).to_ascii_uppercase();
    match name.as_str() {
        "PING" => Value::Simple("PONG".to_string()),
        "SENTINEL" => cmd_sentinel(masters, &req[1..]),
        other => Value::error(format!("ERR unknown command '{}'", other.to_ascii_lowercase())),
    }
}

fn cmd_sentinel(masters: &DashMap<String, Arc<MasterState>>, args: &[bytes::Bytes]) -> Value {
    let Some(sub) = args.first() else {
        return Value::error("ERR wrong number of arguments for 'sentinel' command");
    };
    match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
        "MASTERS" => Value::Array(masters.iter().map(|e| describe_master(e.value())).collect()),
        "MASTER" => {
            let Some(name) = args.get(1) else { return Value::error("ERR sentinel master <name>") };
            let name = String::from_utf8_lossy(name).to_string();
            match masters.get(&name) {
                Some(m) => describe_master(&m),
                None => Value::error(format!("ERR No such master '{name}'")),
            }
        }
        "REPLICAS" => {
            let Some(name) = args.get(1) else { return Value::error("ERR sentinel replicas <name>") };
            let name = String::from_utf8_lossy(name).to_string();
            match masters.get(&name) {
                Some(m) => Value::Array(
                    m.config
                        .known_replicas
                        .iter()
                        .map(|(h, p)| {
                            Value::Map(vec![
                                (Value::bulk(&b"ip"[..]), Value::bulk(h.clone())),
                                (Value::bulk(&b"port"[..]), Value::bulk(p.to_string())),
                            ])
                        })
                        .collect(),
                ),
                None => Value::error(format!("ERR No such master '{name}'")),
            }
        }
        "GET-MASTER-ADDR-BY-NAME" => {
            let Some(name) = args.get(1) else {
                return Value::error("ERR sentinel get-master-addr-by-name <name>");
            };
            let name = String::from_utf8_lossy(name).to_string();
            match masters.get(&name) {
                Some(m) => {
                    let (host, port) = m.current_addr.lock().unwrap().clone();
                    Value::Array(vec![Value::bulk(host), Value::bulk(port.to_string())])
                }
                None => Value::NullArray,
            }
        }
        "IS-MASTER-DOWN-BY-ADDR" => {
            let (Some(host), Some(port)) = (args.get(1), args.get(2)) else {
                return Value::error("ERR sentinel is-master-down-by-addr <ip> <port>");
            };
            let host = String::from_utf8_lossy(host).to_string();
            let port: u16 = String::from_utf8_lossy(port).parse().unwrap_or(0);
            let down = masters.iter().any(|e| {
                let addr = e.value().current_addr.lock().unwrap().clone();
                addr == (host.clone(), port) && e.value().is_subjectively_down()
            });
            Value::Integer(down as i64)
        }
        "CKQUORUM" => {
            let Some(name) = args.get(1) else { return Value::error("ERR sentinel ckquorum <name>") };
            let name = String::from_utf8_lossy(name).to_string();
            match masters.get(&name) {
                Some(m) => {
                    Value::Simple(format!("OK {} usable Sentinels", m.config.known_sentinels.len() + 1))
                }
                None => Value::error(format!("ERR No such master '{name}'")),
            }
        }
        other => Value::error(format!("ERR Unknown SENTINEL subcommand '{}'", other.to_ascii_lowercase())),
    }
}

fn describe_master(m: &MasterState) -> Value {
    let (host, port) = m.current_addr.lock().unwrap().clone();
    Value::Map(vec![
        (Value::bulk(&b"name"[..]), Value::bulk(m.config.name.clone())),
        (Value::bulk(&b"ip"[..]), Value::bulk(host)),
        (Value::bulk(&b"port"[..]), Value::bulk(port.to_string())),
        (Value::bulk(&b"quorum"[..]), Value::bulk(m.config.quorum.to_string())),
        (
            Value::bulk(&b"flags"[..]),
            Value::bulk(if m.is_subjectively_down() { "s_down,master" } else { "master" }),
        ),
    ])
}
