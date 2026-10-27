//! `sentinel.conf` parsing. Same line-based, `redis.conf`-style format as
//! the main server's config, but with Sentinel's own directive set per
//! `docs/06-ha-replication-cluster.md` §2.
//!
//! Scope, deliberately: real Sentinel auto-discovers a master's replicas
//! and peer Sentinels via `INFO` and a `__sentinel__:hello` pub/sub
//! gossip channel. reflexdb's replication protocol doesn't carry a
//! replica's own listening port yet (see `crates/commands/src/
//! replication.rs`), so auto-discovery isn't possible — replicas and
//! peer Sentinels are configured explicitly instead (`sentinel
//! known-replica`/`sentinel known-sentinel`). This is a real, working
//! subset of Sentinel, not the full auto-discovering protocol.

use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct MonitoredMaster {
    pub name: String,
    pub host: String,
    pub port: u16,
    /// How many Sentinels (including this one) must agree a master is down
    /// before failover starts.
    pub quorum: usize,
    pub down_after_ms: u64,
    pub known_replicas: Vec<(String, u16)>,
    pub known_sentinels: Vec<(String, u16)>,
}

#[derive(Debug, Clone, Default)]
pub struct SentinelConfig {
    pub port: u16,
    pub masters: Vec<MonitoredMaster>,
}

impl SentinelConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
        let mut cfg = SentinelConfig { port: 26379, masters: Vec::new() };

        for raw_line in text.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut words = line.split_whitespace();
            let Some(directive) = words.next() else { continue };

            if directive == "port" {
                let value = words.next().ok_or("port requires a value")?;
                cfg.port = value.parse().map_err(|_| format!("invalid port: {value}"))?;
                continue;
            }
            if directive != "sentinel" {
                continue; // unknown directives are ignored, matching the main server's config
            }

            let sub = words.next().ok_or("expected a 'sentinel' subdirective")?;
            match sub {
                "monitor" => {
                    let (Some(name), Some(host), Some(port), Some(quorum)) =
                        (words.next(), words.next(), words.next(), words.next())
                    else {
                        return Err("sentinel monitor <name> <host> <port> <quorum>".to_string());
                    };
                    cfg.masters.push(MonitoredMaster {
                        name: name.to_string(),
                        host: host.to_string(),
                        port: port.parse().map_err(|_| format!("invalid master port: {port}"))?,
                        quorum: quorum.parse().map_err(|_| format!("invalid quorum: {quorum}"))?,
                        down_after_ms: 5_000,
                        known_replicas: Vec::new(),
                        known_sentinels: Vec::new(),
                    });
                }
                "down-after-milliseconds" => {
                    let (Some(name), Some(ms)) = (words.next(), words.next()) else {
                        return Err("sentinel down-after-milliseconds <name> <ms>".to_string());
                    };
                    let master = find_master(&mut cfg, name)?;
                    master.down_after_ms =
                        ms.parse().map_err(|_| format!("invalid down-after-milliseconds: {ms}"))?;
                }
                "known-replica" => {
                    let (Some(name), Some(host), Some(port)) = (words.next(), words.next(), words.next())
                    else {
                        return Err("sentinel known-replica <name> <host> <port>".to_string());
                    };
                    let port: u16 = port.parse().map_err(|_| format!("invalid replica port: {port}"))?;
                    find_master(&mut cfg, name)?.known_replicas.push((host.to_string(), port));
                }
                "known-sentinel" => {
                    let (Some(name), Some(host), Some(port)) = (words.next(), words.next(), words.next())
                    else {
                        return Err("sentinel known-sentinel <name> <host> <port>".to_string());
                    };
                    let port: u16 = port.parse().map_err(|_| format!("invalid sentinel port: {port}"))?;
                    find_master(&mut cfg, name)?.known_sentinels.push((host.to_string(), port));
                }
                // failover-timeout/parallel-syncs/auth-pass: accepted but not
                // acted on yet (no in-progress-failover timeout enforcement,
                // no staggered replica resync, no master auth) — see the
                // module doc's scope note.
                _ => {}
            }
        }

        Ok(cfg)
    }
}

fn find_master<'a>(cfg: &'a mut SentinelConfig, name: &str) -> Result<&'a mut MonitoredMaster, String> {
    cfg.masters.iter_mut().find(|m| m.name == name).ok_or_else(|| {
        format!("'{name}' must be declared with 'sentinel monitor' before other directives reference it")
    })
}
