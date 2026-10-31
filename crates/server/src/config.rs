//! Minimal line-based config file, plus the same directives as `--flags`,
//! which take precedence over file values.

use std::fs;
use std::path::PathBuf;

use commands::{MaxMemoryPolicy, TieredOverflowPolicy};
use persistence::aof::Fsync;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub requirepass: Option<String>,
    pub maxmemory: Option<u64>,
    pub maxmemory_policy: MaxMemoryPolicy,
    /// `maxclients`: hard ceiling on simultaneously open connections — once
    /// reached, new connections wait for a slot to free up rather than
    /// being accepted unbounded. Default `10,000`, same as real Redis.
    /// Pro of raising it: supports more concurrent clients. Con: each
    /// connection holds a tokio task plus its own read/write buffers, so a
    /// much higher value raises worst-case memory/fd usage if clients
    /// actually use all of it; too low rejects legitimate traffic under
    /// a connection spike.
    pub maxclients: usize,
    /// `proto-max-bulk-len`: largest single bulk-string element the RESP
    /// decoder accepts, and the largest value `APPEND`/`SETRANGE`/`SET`/
    /// `BITFIELD` will build or operate on. Default `64 MiB`, matching
    /// real Redis. Pro of raising it: large-value workloads (big blobs
    /// stored as strings) stop being rejected. Con: raises the worst-case
    /// memory a single client command can force the server to allocate
    /// before any application-level check runs — don't raise this past
    /// what you're willing to let one connection allocate at once.
    pub proto_max_bulk_len: usize,
    /// Directory RDB/AOF files are written to.
    pub dir: PathBuf,
    /// RDB snapshot filename within `dir`.
    pub dbfilename: String,
    /// `appendonly yes/no`.
    pub appendonly: bool,
    /// AOF filename within `dir`, used when `appendonly` is enabled.
    pub appendfilename: String,
    /// `appendfsync always/everysec/no` — see `Fsync` for the tradeoffs.
    /// Defaults to `no` (fastest, OS decides when to flush); set it
    /// explicitly in the config file for a tighter durability bound.
    pub appendfsync: Fsync,
    /// `profile cache|durable|replicated` — preset bundle that sets
    /// `appendonly`/`appendfsync`/`min-replicas-to-write` to a coherent
    /// durability posture. Directives listed *after* `profile` in the file
    /// override its individual values. `None` = no preset applied.
    pub profile: Option<String>,
    /// `min-replicas-to-write`: refuse write commands when fewer than this
    /// many connected replicas are healthy (not flagged slow). 0 = off.
    /// Same semantics as Redis's directive of the same name.
    pub min_replicas_to_write: usize,
    /// `tiered-storage yes/no` — evicted keys spill to an on-disk LSM tier
    /// (via the `tiered` crate / `fjall`) instead of being dropped.
    pub tiered_storage: bool,
    /// `tiered-memory-limit`: RAM budget before the tier kicks in.
    pub tiered_memory_limit: u64,
    /// `tiered-dir`: where the tier's files land, relative to `dir` if not
    /// absolute.
    pub tiered_dir: PathBuf,
    /// `tiered-disk-limit`: capacity the disk tier enforces once exceeded.
    /// What "exceeded" does is `tiered-overflow-policy`'s choice.
    /// `None` (default) means unbounded.
    pub tiered_disk_limit: Option<u64>,
    /// `tiered-overflow-policy`: `reject-writes` (default — preserve all
    /// acknowledged data, refuse new keys with `ERR TIERED_STORAGE_FULL`),
    /// `evict-oldest` (drop oldest tiered entries — cache semantics, real
    /// data loss), or `no-limit` (no enforcement). Only consulted when
    /// `tiered-disk-limit` is set.
    pub tiered_overflow_policy: TieredOverflowPolicy,
    /// `eviction-tick-ms`: how often the background task re-checks
    /// `maxmemory`/`tiered-memory-limit`/`tiered-disk-limit` and runs TTL
    /// active-expire. Lowering this reacts to a write burst faster (lower
    /// peak RAM/disk overshoot) for free — it's a background task, not on
    /// the request path, so it costs a bit of extra idle-tick CPU, not
    /// request latency. Keep it well above 0; an interval of a few ms
    /// would just burn CPU spinning between ticks that mostly find
    /// nothing to do.
    pub eviction_tick_ms: u64,
    /// `eviction-low-watermark`: once an eviction pass starts, how far
    /// below the triggering limit it drains to (as a fraction of that
    /// limit) instead of stopping right at it. Default `0.5`, valid range
    /// `(0.0, 1.0]`. Pro of a lower value (e.g. `0.3`): more headroom
    /// after a cleanup pass, so a write burst takes longer to re-trigger
    /// eviction. Con: each pass does more work (moves/drops more keys) and
    /// briefly holds less of the working set in the faster tier. `1.0`
    /// ("drain exactly to the limit") is cheapest per pass but can
    /// re-trigger on the very next write under sustained load.
    pub eviction_low_watermark: f64,
    /// `eviction-max-per-tick`: hard cap on how many keys one background
    /// eviction pass will move or drop, regardless of how far over budget
    /// things are. Default `10,000`. Pro of a higher value: a huge
    /// overshoot drains in fewer ticks. Con: one catastrophic overshoot
    /// can monopolize a tick's CPU, delaying that tick's TTL active-expire
    /// and scheduled-command work. Pro of a lower value: bounds worst-case
    /// per-tick latency; con: a large overshoot takes many ticks to drain.
    pub max_evictions_per_tick: usize,
    /// `singleflight-max-wait-ms`: hard cap on how long a `GET ...
    /// SINGLEFLIGHT` caller blocks before getting a timeout error.
    pub singleflight_max_wait_ms: u64,
    /// `replicaof <host> <port>` — start up already replicating from a
    /// primary, equivalent to issuing `REPLICAOF` right after boot.
    pub replicaof: Option<(String, u16)>,
    /// `tls-port`: 0 disables TLS. When set, a second listener accepts TLS
    /// connections on this port alongside the plain-text one on `port`;
    /// set `port 0` for TLS-only operation.
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
    /// the periodic sweep (`main.rs`) reclaims it. Default 24h; 0 disables
    /// age-based expiry (the per-key version cap still applies).
    pub history_retention_ms: u64,
    /// `history-max-versions-per-key`: maximum past versions retained for
    /// each key. Must be between 1 and 1,000,000. Default 200.
    pub history_max_versions_per_key: usize,
    /// `slowlog-log-slower-than`: microseconds a command must take to be
    /// logged; `-1` disables logging, `0` logs everything.
    pub slowlog_log_slower_than_us: i64,
    /// `slowlog-max-len`: oldest entries drop once the log exceeds this.
    pub slowlog_max_len: usize,
    /// `repl-backlog-size`: per-replica outbound queue depth (number of
    /// pending values), set when a replica attaches via `PSYNC`. Default
    /// `32,768`. Pro of a larger value: absorbs a longer write burst
    /// before a replica is flagged slow. Con: a slow replica can hold
    /// that many unflushed values in memory on the primary first.
    pub replica_backlog_size: usize,
    /// `repl-backlog-max-bytes`: total bytes of queued commands one
    /// replica may hold before it's treated as slow and further writes to
    /// it are dropped. Default `64 MiB`. Pro of a larger value: tolerates
    /// a slower/further-away replica. Con: that many bytes are held in
    /// memory per lagging replica — real Redis's comparable default
    /// (`client-output-buffer-limit replica`, `1 GiB`) is intentionally
    /// much looser; this default trades replica tolerance for primary
    /// memory safety.
    pub replica_backlog_max_bytes: usize,
    /// `repl-slow-disconnect-delay-ms`: how long a replica can stay
    /// flagged slow before its connection is actually dropped (forcing a
    /// full resync on reconnect). Default `5000` (5s). Pro of a longer
    /// delay: survives a brief network blip without losing the link. Con:
    /// a replica that's behind keeps counting as connected (for `WAIT`/
    /// `CLIENT LIST`) for longer before being cut loose.
    pub replica_slow_disconnect_ms: u64,
    /// `live-sweep-ms`: idle gap between LIVE fan-out sweeps — writes
    /// landing in the gap dedup into one re-evaluation. Default `10`.
    /// Pro of a larger value: less fan-out CPU under a sustained write
    /// storm. Con: subscribers see staler intermediate values. `0` runs
    /// every dirty batch immediately.
    pub live_sweep_ms: u64,
    /// `live-pace-chunk`: after this many subscriber socket writes inside
    /// one sweep, the fan-out thread yields `live-pace-ms`. Default `64`;
    /// `0` disables pacing entirely. Pro of a smaller chunk: smoother CPU,
    /// writers never queue behind a push burst. Con: a large subscriber
    /// set takes longer to fully drain.
    pub live_pace_chunk: usize,
    /// `live-pace-ms`: yield length per `live-pace-chunk` writes. Default `1`.
    pub live_pace_ms: u64,
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
            maxclients: 10_000,
            proto_max_bulk_len: 64 * 1024 * 1024,
            dir: PathBuf::from("."),
            dbfilename: "dump.rdb".to_string(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: Fsync::No,
            profile: None,
            min_replicas_to_write: 0,
            tiered_storage: false,
            tiered_memory_limit: 250 * 1024 * 1024,
            tiered_dir: PathBuf::from("tiered"),
            tiered_disk_limit: None,
            tiered_overflow_policy: TieredOverflowPolicy::RejectWrites,
            eviction_tick_ms: 100,
            eviction_low_watermark: 0.5,
            max_evictions_per_tick: 10_000,
            singleflight_max_wait_ms: 10_000,
            replicaof: None,
            tls_port: 0,
            tls_cert_file: None,
            tls_key_file: None,
            notify_keyspace_events: String::new(),
            history_enabled: false,
            history_dir: PathBuf::from("history"),
            history_retention_ms: 24 * 60 * 60 * 1000,
            history_max_versions_per_key: 200,
            slowlog_log_slower_than_us: 10_000,
            slowlog_max_len: 128,
            replica_backlog_size: 32_768,
            replica_backlog_max_bytes: 64 * 1024 * 1024,
            replica_slow_disconnect_ms: 5_000,
            live_sweep_ms: 10,
            live_pace_chunk: 64,
            live_pace_ms: 1,
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
            "maxclients" => {
                let limit: usize = value.parse().map_err(|_| format!("invalid maxclients: {value}"))?;
                if limit == 0 {
                    return Err("maxclients must be at least 1".into());
                }
                self.maxclients = limit;
            }
            "proto-max-bulk-len" => self.proto_max_bulk_len = parse_memory(value)? as usize,
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
            "profile" => match value.to_ascii_lowercase().as_str() {
                "cache" => {
                    self.profile = Some("cache".into());
                    self.appendonly = false;
                }
                "durable" => {
                    self.profile = Some("durable".into());
                    self.appendonly = true;
                    self.appendfsync = Fsync::Always;
                }
                "replicated" => {
                    self.profile = Some("replicated".into());
                    self.appendonly = true;
                    self.appendfsync = Fsync::EverySec;
                    self.min_replicas_to_write = 1;
                }
                other => return Err(format!("invalid profile: {other} (expected cache|durable|replicated)")),
            },
            "min-replicas-to-write" => {
                self.min_replicas_to_write =
                    value.parse().map_err(|_| format!("invalid min-replicas-to-write: {value}"))?
            }
            "tiered-storage" => self.tiered_storage = value.eq_ignore_ascii_case("yes"),
            "tiered-memory-limit" => self.tiered_memory_limit = parse_memory(value)?,
            "tiered-dir" => self.tiered_dir = PathBuf::from(value),
            "tiered-disk-limit" => {
                self.tiered_disk_limit =
                    (!value.is_empty() && value != "0").then(|| parse_memory(value)).transpose()?
            }
            "tiered-overflow-policy" => {
                self.tiered_overflow_policy = TieredOverflowPolicy::parse(value)
                    .ok_or_else(|| format!("invalid tiered-overflow-policy: {value} (expected reject-writes|evict-oldest|no-limit)"))?
            }
            "eviction-tick-ms" => {
                self.eviction_tick_ms =
                    value.parse().map_err(|_| format!("invalid eviction-tick-ms: {value}"))?
            }
            "eviction-low-watermark" => {
                let watermark: f64 =
                    value.parse().map_err(|_| format!("invalid eviction-low-watermark: {value}"))?;
                if !(watermark > 0.0 && watermark <= 1.0) {
                    return Err("eviction-low-watermark must be in (0.0, 1.0]".into());
                }
                self.eviction_low_watermark = watermark;
            }
            "eviction-max-per-tick" => {
                self.max_evictions_per_tick =
                    value.parse().map_err(|_| format!("invalid eviction-max-per-tick: {value}"))?
            }
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
            "history-max-versions-per-key" => {
                let limit: usize =
                    value.parse().map_err(|_| format!("invalid history-max-versions-per-key: {value}"))?;
                if !(1..=1_000_000).contains(&limit) {
                    return Err("history-max-versions-per-key must be between 1 and 1000000".into());
                }
                self.history_max_versions_per_key = limit;
            }
            "slowlog-log-slower-than" => {
                self.slowlog_log_slower_than_us =
                    value.parse().map_err(|_| format!("invalid slowlog-log-slower-than: {value}"))?
            }
            "slowlog-max-len" => {
                self.slowlog_max_len =
                    value.parse().map_err(|_| format!("invalid slowlog-max-len: {value}"))?
            }
            "repl-backlog-size" => {
                self.replica_backlog_size =
                    value.parse().map_err(|_| format!("invalid repl-backlog-size: {value}"))?
            }
            "repl-backlog-max-bytes" => self.replica_backlog_max_bytes = parse_memory(value)? as usize,
            "repl-slow-disconnect-delay-ms" => {
                self.replica_slow_disconnect_ms =
                    value.parse().map_err(|_| format!("invalid repl-slow-disconnect-delay-ms: {value}"))?
            }
            "live-sweep-ms" => {
                self.live_sweep_ms = value.parse().map_err(|_| format!("invalid live-sweep-ms: {value}"))?
            }
            "live-pace-chunk" => {
                self.live_pace_chunk =
                    value.parse().map_err(|_| format!("invalid live-pace-chunk: {value}"))?
            }
            "live-pace-ms" => {
                self.live_pace_ms = value.parse().map_err(|_| format!("invalid live-pace-ms: {value}"))?
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(pairs: &[(&str, &str)]) -> impl Iterator<Item = String> {
        pairs
            .iter()
            .flat_map(|(flag, value)| [format!("--{flag}"), value.to_string()])
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn new_knobs_parse_and_override_their_defaults() {
        let cfg = Config::from_args(args(&[
            ("maxclients", "500"),
            ("proto-max-bulk-len", "128mb"),
            ("eviction-low-watermark", "0.25"),
            ("eviction-max-per-tick", "2000"),
            ("repl-backlog-size", "1024"),
            ("repl-backlog-max-bytes", "16mb"),
            ("repl-slow-disconnect-delay-ms", "1500"),
        ]))
        .unwrap();
        assert_eq!(cfg.maxclients, 500);
        assert_eq!(cfg.proto_max_bulk_len, 128 * 1024 * 1024);
        assert_eq!(cfg.eviction_low_watermark, 0.25);
        assert_eq!(cfg.max_evictions_per_tick, 2000);
        assert_eq!(cfg.replica_backlog_size, 1024);
        assert_eq!(cfg.replica_backlog_max_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.replica_slow_disconnect_ms, 1500);
    }

    #[test]
    fn profile_presets_and_explicit_overrides() {
        let cfg = Config::from_args(args(&[("profile", "cache")])).unwrap();
        assert!(!cfg.appendonly);

        let cfg = Config::from_args(args(&[("profile", "durable")])).unwrap();
        assert!(cfg.appendonly);
        assert_eq!(cfg.appendfsync, Fsync::Always);

        let cfg = Config::from_args(args(&[("profile", "replicated")])).unwrap();
        assert!(cfg.appendonly);
        assert_eq!(cfg.appendfsync, Fsync::EverySec);
        assert_eq!(cfg.min_replicas_to_write, 1);

        // Directives after `profile` override its values.
        let cfg = Config::from_args(args(&[
            ("profile", "durable"),
            ("appendfsync", "everysec"),
            ("min-replicas-to-write", "2"),
        ]))
        .unwrap();
        assert_eq!(cfg.appendfsync, Fsync::EverySec);
        assert_eq!(cfg.min_replicas_to_write, 2);

        assert!(Config::from_args(args(&[("profile", "bogus")])).is_err());
    }

    #[test]
    fn rejects_out_of_range_values() {
        assert!(Config::from_args(args(&[("maxclients", "0")])).is_err());
        assert!(Config::from_args(args(&[("eviction-low-watermark", "0")])).is_err());
        assert!(Config::from_args(args(&[("eviction-low-watermark", "1.5")])).is_err());
    }

    #[test]
    fn defaults_match_documented_values() {
        let cfg = Config::default();
        assert_eq!(cfg.maxclients, 10_000);
        assert_eq!(cfg.proto_max_bulk_len, 64 * 1024 * 1024);
        assert_eq!(cfg.eviction_low_watermark, 0.5);
        assert_eq!(cfg.max_evictions_per_tick, 10_000);
        assert_eq!(cfg.replica_backlog_size, 32_768);
        assert_eq!(cfg.replica_backlog_max_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.replica_slow_disconnect_ms, 5_000);
    }
}
