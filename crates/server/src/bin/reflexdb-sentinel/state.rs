//! Per-master runtime state: the liveness watcher loop, quorum voting
//! against peer Sentinels, and failover execution.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use reflexdb_server::resp_client::{Reply, RespClient};

use crate::config::MonitoredMaster;

pub struct MasterState {
    pub config: MonitoredMaster,
    /// Updated after a successful failover — `SENTINEL GET-MASTER-ADDR-BY-NAME`
    /// and subsequent liveness checks use this, not `config`'s original address.
    pub current_addr: Mutex<(String, u16)>,
    /// When this Sentinel first noticed the master not responding; `None`
    /// once it answers again. This is this Sentinel's own "SDOWN" (subjective
    /// down) state — reaching quorum across Sentinels promotes it to ODOWN.
    sdown_since: Mutex<Option<Instant>>,
    failover_in_progress: AtomicBool,
}

impl MasterState {
    pub fn new(config: MonitoredMaster) -> Self {
        let addr = (config.host.clone(), config.port);
        Self {
            config,
            current_addr: Mutex::new(addr),
            sdown_since: Mutex::new(None),
            failover_in_progress: AtomicBool::new(false),
        }
    }

    pub fn is_subjectively_down(&self) -> bool {
        self.sdown_since.lock().unwrap().is_some()
    }
}

async fn ping(host: &str, port: u16) -> bool {
    match RespClient::connect(host, port).await {
        Ok(mut client) => matches!(client.call(&["PING"]).await, Ok(Reply::Simple(s)) if s == "PONG"),
        Err(_) => false,
    }
}

/// Runs forever for one monitored master: pings it on an interval, tracks
/// how long it's been unresponsive, and once that exceeds
/// `down-after-milliseconds`, asks peer Sentinels whether they see the same
/// thing (`SENTINEL IS-MASTER-DOWN-BY-ADDR`). Reaching `quorum` votes
/// (counting itself) triggers `failover`.
pub async fn watch(sentinel_port: u16, state: std::sync::Arc<MasterState>) {
    let mut tick = tokio::time::interval(Duration::from_millis(1_000));
    loop {
        tick.tick().await;
        let (host, port) = state.current_addr.lock().unwrap().clone();
        let alive = ping(&host, port).await;

        if alive {
            *state.sdown_since.lock().unwrap() = None;
            continue;
        }

        let since = {
            let mut guard = state.sdown_since.lock().unwrap();
            *guard.get_or_insert_with(Instant::now)
        };
        if since.elapsed().as_millis() < state.config.down_after_ms as u128 {
            continue; // not down long enough yet to even ask peers
        }

        tracing::warn!(master = %state.config.name, %host, port, "SDOWN: master not responding");
        let votes = count_down_votes(sentinel_port, &state, &host, port).await;
        if votes >= state.config.quorum {
            tracing::warn!(master = %state.config.name, votes, quorum = state.config.quorum, "ODOWN reached");
            failover(&state).await;
        } else {
            tracing::info!(master = %state.config.name, votes, quorum = state.config.quorum, "SDOWN but quorum not yet reached");
        }
    }
}

/// Counts this Sentinel's own vote plus however many configured peers
/// agree the master at `host:port` is down right now.
async fn count_down_votes(sentinel_port: u16, state: &MasterState, host: &str, port: u16) -> usize {
    let mut votes = 1; // this Sentinel's own SDOWN state, already established by the caller
    for (peer_host, peer_port) in &state.config.known_sentinels {
        if *peer_port == sentinel_port && peer_host == "127.0.0.1" {
            continue; // never ask ourselves if we happened to list our own address
        }
        let Ok(mut client) = RespClient::connect(peer_host, *peer_port).await else { continue };
        if let Ok(Reply::Integer(1)) =
            client.call(&["SENTINEL", "IS-MASTER-DOWN-BY-ADDR", host, &port.to_string()]).await
        {
            votes += 1;
        }
    }
    votes
}

/// Promotes the first configured replica, re-points the rest at it, and
/// updates this Sentinel's own view of where the master now lives.
///
/// Deliberately simplified vs. real Sentinel: replica selection is just
/// "the first one listed" rather than comparing replication offsets/
/// priorities, re-pointing replicas happens sequentially rather than
/// honoring `parallel-syncs`, and the old (presumably dead) master is never
/// told to become a replica of the new one if it comes back — no
/// split-brain "walk-back" handling. All noted in `docs/06-ha-replication-
/// cluster.md`'s scope note for this feature.
async fn failover(state: &MasterState) {
    if state.failover_in_progress.swap(true, Ordering::SeqCst) {
        return; // another tick already kicked this off
    }

    let Some((new_host, new_port)) = state.config.known_replicas.first().cloned() else {
        tracing::error!(master = %state.config.name, "ODOWN but no known-replica configured; cannot fail over");
        state.failover_in_progress.store(false, Ordering::SeqCst);
        return;
    };

    tracing::warn!(master = %state.config.name, new_primary = %format!("{new_host}:{new_port}"), "starting failover");

    match RespClient::connect(&new_host, new_port).await {
        Ok(mut client) => {
            if let Err(e) = client.call(&["REPLICAOF", "NO", "ONE"]).await {
                tracing::error!(error = %e, "failed to promote the new primary; aborting failover");
                state.failover_in_progress.store(false, Ordering::SeqCst);
                return;
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "could not reach the chosen replica; aborting failover");
            state.failover_in_progress.store(false, Ordering::SeqCst);
            return;
        }
    }

    for (host, port) in state.config.known_replicas.iter().skip(1) {
        match RespClient::connect(host, *port).await {
            Ok(mut client) => {
                let _ = client.call(&["REPLICAOF", &new_host, &new_port.to_string()]).await;
            }
            Err(e) => {
                tracing::warn!(%host, port, error = %e, "could not re-point this replica during failover")
            }
        }
    }

    let old = state.current_addr.lock().unwrap().clone();
    *state.current_addr.lock().unwrap() = (new_host.clone(), new_port);
    *state.sdown_since.lock().unwrap() = None;

    // Best-effort notification so connected clients watching pub/sub can
    // re-route — the new primary should be reachable since we just
    // promoted it.
    if let Ok(mut client) = RespClient::connect(&new_host, new_port).await {
        let message = format!("{} {} {} {} {}", state.config.name, old.0, old.1, new_host, new_port);
        let _ = client.call(&["PUBLISH", "__sentinel__:switch-master", &message]).await;
    }

    tracing::warn!(
        master = %state.config.name,
        old = %format!("{}:{}", old.0, old.1),
        new = %format!("{new_host}:{new_port}"),
        "+switch-master"
    );
    state.failover_in_progress.store(false, Ordering::SeqCst);
}
