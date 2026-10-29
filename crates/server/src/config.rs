//! Minimal line-based config file, plus the same directives as `--flags`,
//! which take precedence over file values.

use std::fs;
use std::path::PathBuf;

use commands::MaxMemoryPolicy;
use persistence::aof::Fsync;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub requirepass: Option<String>,
    pub maxmemory: Option<u64>,
    pub maxmemory_policy: MaxMemoryPolicy,
    /// Directory RDB/AOF files are written to.
    pub dir: PathBuf,
    /// RDB snapshot filename within `dir`.
    pub dbfilename: String,
    /// `appendonly yes/no`.
    pub appendonly: bool,
    /// AOF filename within `dir`, used when `appendonly` is enabled.
    pub appendfilename: String,
    pub appendfsync: Fsync,
    /// `tiered-storage yes/no` — evicted keys spill to an on-disk LSM tier
    /// (via the `tiered` crate / `fjall`) instead of being dropped.
    pub tiered_storage: bool,
    /// `tiered-memory-limit`: RAM budget before the tier kicks in.
    pub tiered_memory_limit: u64,
    /// `tiered-dir`: where the tier's files land, relative to `dir` if not
    /// absolute.
    pub tiered_dir: PathBuf,
    /// `singleflight-max-wait-ms`: hard cap on how long a `GET ...
    /// SINGLEFLIGHT` caller blocks before getting a timeout error.
    pub singleflight_max_wait_ms: u64,
    /// `replicaof <host> <port>` — start up already replicating from a
    /// primary, equivalent to issuing `REPLICAOF` right after boot.
    pub replicaof: Option<(String, u16)>,
    /// `tls-port`: 0 disables TLS. When set, a second listener accepts TLS
    /// connections on this port alongside the plain-text one on `port`
    /// (TLS-only operation by also disabling `port` is not supported here; we
    /// always keep the plain listener for simplicity).
    pub tls_port: u16,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    /// `notify-keyspace-events` — raw flag string (parsed into
    /// `NotifyFlags` once, in `main.rs`, when building `ServerContext`).
    pub notify_keyspace_events: String,
    /// `history-enabled yes/no` — turns on `GETAT`/`HISTORY` (see
    /// `persistence::history`'s docs). Off by default to avoid extra disk I/O.
    pub history_enabled: bool,
    /// `history-dir`: where the disk-backed history store's files land,
    /// relative to `dir` if not absolute.
    pub history_dir: PathBuf,
    /// `history-retention-ms`: how long a recorded version is kept before
    /// the periodic sweep (`main.rs`) reclaims it. Default 24h.
    pub history_retention_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 7379,
            databases: 16,
            requirepass: None,
            maxmemory: None,
            maxmemory_policy: MaxMemoryPolicy::NoEviction,
            dir: PathBuf::from("."),
            dbfilename: "dump.rdb".to_string(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: Fsync::EverySec,
            tiered_storage: false,
            tiered_memory_limit: 250 * 1024 * 1024,
            tiered_dir: PathBuf::from("tiered"),
            singleflight_max_wait_ms: 10_000,
            replicaof: None,
            tls_port: 0,
            tls_cert_file: None,
            tls_key_file: None,
            notify_keyspace_events: String::new(),
            history_enabled: false,
            history_dir: PathBuf::from("history"),
            history_retention_ms: 24 * 60 * 60 * 1000,
        }
    }
}

impl Config {
    pub fn rdb_path(&self) -> PathBuf {
        self.dir.join(&self.dbfilename)
    }

    pub fn aof_path(&self) -> PathBuf {
        self.dir.join(&self.appendfilename)
    }

    pub fn tiered_dir_path(&self) -> PathBuf {
        if self.tiered_dir.is_absolute() {
            self.tiered_dir.clone()
        } else {
            self.dir.join(&self.tiered_dir)
        }
    }

    pub fn history_dir_path(&self) -> PathBuf {
        if self.history_dir.is_absolute() {
            self.history_dir.clone()
        } else {
            self.dir.join(&self.history_dir)
        }
    }
}

impl Config {
    pub fn from_args(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut cfg = Config::default();
        let args: Vec<String> = args.collect();
        let mut i = 0;

        // Optional leading config-file path (no `--` prefix).
        if let Some(first) = args.first() {
            if !first.starts_with("--") {
                cfg.apply_file(first)?;
                i = 1;
            }
        }

        while i < args.len() {
            let flag = args[i].trim_start_matches("--");
            let value = args.get(i + 1).ok_or_else(|| format!("missing value for --{flag}"))?;
            cfg.apply_directive(flag, value)?;
            i += 2;
        }

        Ok(cfg)
    }

    fn apply_file(&mut self, path: &str) -> Result<(), String> {
        let contents =
            fs::read_to_string(path).map_err(|e| format!("cannot read config file {path}: {e}"))?;
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.splitn(2, char::is_whitespace);
            let directive = parts.next().unwrap_or_default();
            let value = parts.next().unwrap_or_default().trim().trim_matches('"');
            self.apply_directive(directive, value)?;
        }
        Ok(())
    }

    fn apply_directive(&mut self, directive: &str, value: &str) -> Result<(), String> {
        match directive.to_ascii_lowercase().as_str() {
            "bind" => self.bind = value.split_whitespace().next().unwrap_or("127.0.0.1").to_string(),
            "port" => self.port = value.parse().map_err(|_| format!("invalid port: {value}"))?,
            "databases" => {
                self.databases = value.parse().map_err(|_| format!("invalid databases: {value}"))?
            }
            "requirepass" => self.requirepass = if value.is_empty() { None } else { Some(value.to_string()) },
            "maxmemory" => self.maxmemory = Some(parse_memory(value)?),
            "maxmemory-policy" => {
                self.maxmemory_policy = MaxMemoryPolicy::parse(value)
                    .ok_or_else(|| format!("invalid maxmemory-policy: {value}"))?
            }
            "dir" => self.dir = PathBuf::from(value),
            "dbfilename" => self.dbfilename = value.to_string(),
            "appendonly" => self.appendonly = value.eq_ignore_ascii_case("yes"),
            "appendfilename" => self.appendfilename = value.to_string(),
            "appendfsync" => {
                self.appendfsync = match value.to_ascii_lowercase().as_str() {
                    "always" => Fsync::Always,
                    "everysec" => Fsync::EverySec,
                    "no" => Fsync::No,
                    other => return Err(format!("invalid appendfsync: {other}")),
                }
            }
            "tiered-storage" => self.tiered_storage = value.eq_ignore_ascii_case("yes"),
            "tiered-memory-limit" => self.tiered_memory_limit = parse_memory(value)?,
            "tiered-dir" => self.tiered_dir = PathBuf::from(value),
            "singleflight-max-wait-ms" => {
                self.singleflight_max_wait_ms =
                    value.parse().map_err(|_| format!("invalid singleflight-max-wait-ms: {value}"))?
            }
            "replicaof" | "slaveof" => {
                let mut parts = value.split_whitespace();
                let (Some(host), Some(port)) = (parts.next(), parts.next()) else {
                    return Err(format!("invalid {directive}: {value} (expected '<host> <port>')"));
                };
                self.replicaof = Some((
                    host.to_string(),
                    port.parse().map_err(|_| format!("invalid {directive} port: {port}"))?,
                ));
            }
            "tls-port" => self.tls_port = value.parse().map_err(|_| format!("invalid tls-port: {value}"))?,
            "tls-cert-file" => self.tls_cert_file = Some(PathBuf::from(value)),
            "tls-key-file" => self.tls_key_file = Some(PathBuf::from(value)),
            "notify-keyspace-events" => self.notify_keyspace_events = value.to_string(),
            "history-enabled" => self.history_enabled = value.eq_ignore_ascii_case("yes"),
            "history-dir" => self.history_dir = PathBuf::from(value),
            "history-retention-ms" => {
                self.history_retention_ms =
                    value.parse().map_err(|_| format!("invalid history-retention-ms: {value}"))?
            }
            other => {
                tracing::warn!("ignoring unsupported config directive: {other}");
            }
        }
        Ok(())
    }
}

fn parse_memory(value: &str) -> Result<u64, String> {
    let value = value.trim();
    let (num, mult) = match value.to_ascii_lowercase() {
        v if v.ends_with("gb") => (v.trim_end_matches("gb").to_string(), 1024 * 1024 * 1024),
        v if v.ends_with("mb") => (v.trim_end_matches("mb").to_string(), 1024 * 1024),
        v if v.ends_with("kb") => (v.trim_end_matches("kb").to_string(), 1024),
        v => (v, 1),
    };
    num.trim().parse::<u64>().map(|n| n * mult).map_err(|_| format!("invalid maxmemory: {value}"))
}
