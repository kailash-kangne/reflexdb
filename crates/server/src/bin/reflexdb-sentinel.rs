//! `reflexdb-sentinel <sentinel.conf>` — automatic failover for a
//! monitored primary/replica group. See `docs/06-ha-replication-cluster.md`
//! §2, and each module's doc comment for exactly what's implemented vs.
//! simplified from real Redis Sentinel.

#[path = "reflexdb-sentinel/commands.rs"]
mod commands;
#[path = "reflexdb-sentinel/config.rs"]
mod config;
#[path = "reflexdb-sentinel/state.rs"]
mod state;

use std::sync::Arc;

use dashmap::DashMap;

use config::SentinelConfig;
use state::MasterState;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let Some(conf_path) = std::env::args().nth(1) else {
        eprintln!("usage: reflexdb-sentinel <sentinel.conf>");
        std::process::exit(1);
    };

    let cfg = match SentinelConfig::load(conf_path.as_ref()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("reflexdb-sentinel: {e}");
            std::process::exit(1);
        }
    };

    if cfg.masters.is_empty() {
        eprintln!("reflexdb-sentinel: no 'sentinel monitor' directives found in {conf_path}");
        std::process::exit(1);
    }

    let masters: Arc<DashMap<String, Arc<MasterState>>> = Arc::new(DashMap::new());
    let sentinel_port = cfg.port;
    for master_cfg in cfg.masters {
        let name = master_cfg.name.clone();
        let state = Arc::new(MasterState::new(master_cfg));
        masters.insert(name.clone(), state.clone());
        tracing::info!(master = %name, "now monitoring");
        tokio::spawn(async move { state::watch(sentinel_port, state).await });
    }

    commands::run("0.0.0.0", sentinel_port, masters).await
}
