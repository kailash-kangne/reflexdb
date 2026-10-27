use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use dashmap::DashMap;
use keyspace::{Store, Ttl, Value as KVal};
use rand::seq::SliceRandom;
use resp::{Protocol, Request, Value};
use tokio::sync::mpsc::UnboundedSender;

use crate::live::LiveSub;
use crate::replication::ReplicaState;
use crate::schedule::Scheduler;
use crate::set_opts::{self, ParseError};
use crate::singleflight::SingleFlightLease;
use crate::{
    client, hash, history, info, keys, list, live, pubsub, replication, schedule, script, set, singleflight,
    snapshot, stream, string, txn, zset,
};

/// `maxmemory-policy` — which keys are candidates for eviction once
/// `maxmemory` is exceeded. The `-lru`/`-lfu` variants rank candidates by
/// `keyspace::Db::access_stats` (a plain "last access instant"/"access
/// count" pair, not real Redis's sampled-and-decayed approximation); both
/// scan every key in the DB to find the best candidate rather than
/// sampling a handful, which is fine at this scale but wouldn't be at
/// millions of keys — same trade-off `SCAN` already makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaxMemoryPolicy {
    #[default]
    NoEviction,
    AllKeysRandom,
    VolatileRandom,
    AllKeysLru,
    VolatileLru,
    AllKeysLfu,
    VolatileLfu,
    VolatileTtl,
}

impl MaxMemoryPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "noeviction" => Some(Self::NoEviction),
            "allkeys-random" => Some(Self::AllKeysRandom),
            "volatile-random" => Some(Self::VolatileRandom),
            "allkeys-lru" => Some(Self::AllKeysLru),
            "volatile-lru" => Some(Self::VolatileLru),
            "allkeys-lfu" => Some(Self::AllKeysLfu),
            "volatile-lfu" => Some(Self::VolatileLfu),
            "volatile-ttl" => Some(Self::VolatileTtl),
            _ => None,
        }
    }

    /// Only `volatile-*` policies restrict eviction to keys that have a
    /// TTL set; the `allkeys-*` ones (and `-random`) don't care.
    fn volatile_only(self) -> bool {
        matches!(self, Self::VolatileRandom | Self::VolatileLru | Self::VolatileLfu | Self::VolatileTtl)
    }
}

/// Parsed `notify-keyspace-events` flags — see `notify_keyspace_event`'s
/// docs for how these gate `__keyspace@<db>__`/`__keyevent@<db>__` pub/sub
/// notifications. Default (empty string) means nothing is enabled, same as
/// real Redis.
#[derive(Debug, Clone, Default)]
pub struct NotifyFlags {
    keyspace: bool,
    keyevent: bool,
    /// One-character class codes that are enabled (`g$lshzxe...`, see
    /// `notify_keyspace_event`'s callers for which class each command uses).
    classes: String,
}

impl NotifyFlags {
    pub fn parse(spec: &str) -> Self {
        let mut flags = Self::default();
        for c in spec.chars() {
            match c {
                'K' => flags.keyspace = true,
                'E' => flags.keyevent = true,
                // `A` is shorthand for "all the data-mutating classes".
                'A' => flags.classes.push_str("g$lshzxet"),
                other => flags.classes.push(other),
            }
        }
        flags
    }

    fn enabled(&self, class: char) -> bool {
        self.classes.contains(class)
    }
}

/// Shared, read-mostly server state every connection dispatches against,
/// plus the pub/sub + LIVE subscription registries (Phase 3), the `WATCH`
/// registry backing transactions, the `SCRIPT` cache (Phase 4), and the
/// RDB/AOF persistence + `maxmemory` eviction state (Phase 5).
pub struct ServerContext {
    pub store: Store,
    pub requirepass: Option<String>,
    /// ACL user table; always has at least `default`, which mirrors plain
    /// `requirepass` behavior until `ACL SETUSER` touches it or other users
    /// are added. See `crate::acl`.
    pub(crate) acl_users: DashMap<String, crate::acl::AclUser>,
    pub start_time: Instant,

    next_client_id: AtomicU64,
    /// Outbound push channel per connected client, used to deliver pub/sub
    /// messages and LIVE updates asynchronously from whichever connection
    /// triggered them.
    pub(crate) clients: DashMap<u64, UnboundedSender<Value>>,
    pub(crate) channels: DashMap<Bytes, HashSet<u64>>,
    pub(crate) patterns: DashMap<Bytes, HashSet<u64>>,
    /// fingerprint -> subscription (one per distinct `LIVE <cmd> <args>`).
    pub(crate) live_subs: DashMap<u64, LiveSub>,
    /// (db, key) -> fingerprints of LIVE subscriptions that depend on that key.
    pub(crate) key_watchers: DashMap<(usize, Bytes), HashSet<u64>>,
    /// Mirrors `key_watchers.len()`, but as a single atomic instead of a
    /// `DashMap` length (which sums every shard's length under its own
    /// lock). `on_key_changed` runs on *every* write, including plain
    /// `SET`/`INCR`/etc. that have nothing to do with LIVE — checking this
    /// first lets it skip hashing the key and locking a `key_watchers` shard
    /// entirely when no one has ever subscribed. See `live::notify_key_change`.
    pub(crate) key_watchers_len: AtomicUsize,
    /// (db, key) -> clients that `WATCH`ed it (for `MULTI`/`EXEC`).
    pub(crate) watchers: DashMap<(usize, Bytes), HashSet<u64>>,
    /// Mirrors `watchers.len()` — same rationale as `key_watchers_len`,
    /// for `txn::touch_watchers`.
    pub(crate) watchers_len: AtomicUsize,
    /// Clients whose watched keys changed since their last `WATCH`/`EXEC`;
    /// their next `EXEC` aborts (Redis's "dirty CAS" flag, kept server-side
    /// instead of on the per-connection `ClientState` since another
    /// connection's write is what sets it).
    pub(crate) dirty_clients: DashMap<u64, ()>,
    /// `SCRIPT LOAD`/`EVAL` cache: sha1-hex -> source, for `EVALSHA`.
    pub(crate) scripts: DashMap<String, String>,

    /// `CLIENT SETNAME`d names, keyed by client id — kept here (not just on
    /// `ClientState`) so `CLIENT LIST` from one connection can see every
    /// other connection's name.
    pub(crate) client_names: DashMap<u64, String>,
    /// `CLIENT TRACKING ON` bookkeeping: (db, key) -> client ids that have
    /// read that key since enabling tracking and want an invalidation push
    /// the next time it changes. Only `GET` feeds this (see
    /// `dispatch_get`) — real Redis tracks every read, this is a scoped-down
    /// version that covers the common case. RESP3 only; cleared per-key
    /// after firing once, matching real `CLIENT TRACKING`'s default
    /// (non-`BCAST`) mode.
    pub(crate) tracking_table: DashMap<(usize, Bytes), HashSet<u64>>,
    /// Mirrors `tracking_table.len()` — same rationale as `key_watchers_len`.
    /// `CLIENT TRACKING` is RESP3-only and off by default, so this is
    /// usually `0` and lets `on_key_changed` skip it entirely.
    pub(crate) tracking_table_len: AtomicUsize,

    /// Where `SAVE`/`BGSAVE` write their RDB-style snapshot. `None` means
    /// those commands are unavailable (no `dir`/`dbfilename` configured).
    pub(crate) rdb_path: Option<PathBuf>,
    last_save: AtomicU64,
    /// Append-only command log, present only when `appendonly yes`.
    pub(crate) aof: Option<persistence::aof::AofWriter>,
    /// Where `aof` is writing to — needed by `BGREWRITEAOF` to know what
    /// file to atomically swap the rewritten one in over. `None` exactly
    /// when `aof` is `None`.
    pub(crate) aof_path: Option<PathBuf>,
    pub maxmemory: Option<u64>,
    pub maxmemory_policy: MaxMemoryPolicy,
    /// Updated periodically (not on every write) by a background tick —
    /// see `ServerContext::run_eviction_tick`.
    approx_memory: AtomicUsize,
    /// `tiered-memory-limit`: once `approx_memory` exceeds this, keys get
    /// moved to the disk tier instead of being deleted. Only meaningful if
    /// `store` was built with a tiering handler (`Store::new_with_tiering`);
    /// otherwise there's nowhere to move them and this is ignored.
    pub tiered_limit: Option<u64>,
    /// `SCHEDULE AT`/`SCHEDULE IN` queue, drained by `run_due_scheduled`.
    pub(crate) scheduler: Scheduler,
    /// Active `GET ... SINGLEFLIGHT` leases, keyed by (db, key).
    pub(crate) singleflight: DashMap<(usize, Bytes), std::sync::Arc<SingleFlightLease>>,
    /// Mirrors `singleflight.len()` — same rationale as `key_watchers_len`.
    /// `SINGLEFLIGHT` leases only exist while a cache-miss recompute is in
    /// flight, so this is `0` the overwhelming majority of the time.
    pub(crate) singleflight_len: AtomicUsize,
    /// `singleflight-max-wait-ms`: hard cap on how long the connection loop
    /// keeps retrying a blocked `SINGLEFLIGHT` wait before giving up.
    pub singleflight_max_wait: Duration,

    /// Connected replicas (clients that sent `PSYNC`), keyed by client id.
    pub(crate) replicas: DashMap<u64, ReplicaState>,
    /// Logical replication offset: incremented once per replicated write
    /// (a command count, not a byte offset like real Redis — see
    /// `docs/06-ha-replication-cluster.md`'s implementation notes).
    pub(crate) master_repl_offset: AtomicU64,
    /// Random id handed to replicas in `FULLRESYNC`, generated once at
    /// startup (not persisted — a restart gets a fresh identity, same as a
    /// real primary after a hard crash with `repl-id` not preserved).
    pub replid: String,
    /// `None` = acting as a primary. `Some((host, port))` = replicating
    /// from that primary (`REPLICAOF`/`SLAVEOF`).
    pub(crate) replica_of: std::sync::Mutex<Option<(String, u16)>>,
    /// Bumped on every `REPLICAOF` call; the background replication-link
    /// task (owned by the `server` crate, which has the `Arc` this crate's
    /// synchronous `dispatch()` can't hold) watches this to know when to
    /// drop its current link and reconnect elsewhere.
    pub(crate) replication_epoch: AtomicU64,
    /// Woken on every `REPLCONF ACK`, so `WAIT` doesn't have to poll on a
    /// tight timer.
    pub replica_ack_notify: tokio::sync::Notify,
    /// Woken on every `on_key_changed` (i.e. every successful write to any
    /// key), so the blocking commands (`BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/
    /// `BZPOPMAX`) don't have to poll on a tight timer. Deliberately one
    /// notifier for the whole store rather than one per key: with it being
    /// a `Notify` (not a channel), over-waking a blocked client just costs
    /// it a wasted re-check of the keys it's actually interested in.
    pub blocking_notify: tokio::sync::Notify,

    /// `notify-keyspace-events` — parsed once at startup, immutable after
    /// that (matches how `maxmemory_policy` is handled).
    pub notify_keyspace_events: NotifyFlags,

    /// `SLOWLOG` ring buffer, newest first.
    pub(crate) slowlog: std::sync::Mutex<std::collections::VecDeque<crate::info::SlowLogEntry>>,
    slowlog_next_id: AtomicU64,
    /// `slowlog-log-slower-than`: microseconds a command must take to be
    /// logged; `-1` disables logging, `0` logs everything.
    pub slowlog_threshold_us: i64,
    /// `slowlog-max-len`: oldest entries drop once the log exceeds this.
    pub slowlog_max_len: usize,

    /// Disk-backed version log backing `GETAT`/`HISTORY` (see
    /// `persistence::history`'s module docs). `None` means `history-enabled
    /// no` (the default) — every write skips `record_history` entirely in
    /// that case, same `Option`-as-feature-flag shape as `aof`/`rdb_path`.
    /// Strings only, matching `docs/11-time-travel.md`'s v1 scope note:
    /// `record_history` re-reads the key's current string value on every
    /// write rather than `on_key_changed` carrying the new value directly,
    /// since that callback is shared by every write command regardless of
    /// whether history is on — an acceptable extra lookup given it only
    /// runs at all when `history-enabled yes`. Does not decide Bet 1
    /// (`docs/15-strategic-bets.md`): this is a side-car on top of the
    /// existing mutable keyspace, not "current value is just a query over
    /// the log."
    pub(crate) history: Option<persistence::history::HistoryWriter>,
}

impl ServerContext {
    pub fn new(num_dbs: usize, requirepass: Option<String>) -> Self {
        Self::with_store(Store::new(num_dbs), requirepass)
    }

    /// Like `new`, but with a pre-built `Store` — used when the store needs
    /// a tiering handler (`Store::new_with_tiering`), since constructing
    /// that handler lives outside this crate (see the `tiered` crate).
    pub fn with_store(store: Store, requirepass: Option<String>) -> Self {
        Self {
            store,
            acl_users: ServerContext::init_acl(requirepass.as_deref()),
            requirepass,
            start_time: Instant::now(),
            next_client_id: AtomicU64::new(1),
            clients: DashMap::new(),
            channels: DashMap::new(),
            patterns: DashMap::new(),
            live_subs: DashMap::new(),
            key_watchers: DashMap::new(),
            key_watchers_len: AtomicUsize::new(0),
            watchers: DashMap::new(),
            watchers_len: AtomicUsize::new(0),
            scripts: DashMap::new(),
            dirty_clients: DashMap::new(),
            client_names: DashMap::new(),
            tracking_table: DashMap::new(),
            tracking_table_len: AtomicUsize::new(0),
            rdb_path: None,
            last_save: AtomicU64::new(0),
            aof: None,
            aof_path: None,
            maxmemory: None,
            maxmemory_policy: MaxMemoryPolicy::NoEviction,
            approx_memory: AtomicUsize::new(0),
            tiered_limit: None,
            scheduler: Scheduler::default(),
            singleflight: DashMap::new(),
            singleflight_len: AtomicUsize::new(0),
            singleflight_max_wait: Duration::from_secs(10),
            replicas: DashMap::new(),
            master_repl_offset: AtomicU64::new(0),
            replid: generate_replid(),
            replica_of: std::sync::Mutex::new(None),
            replication_epoch: AtomicU64::new(0),
            replica_ack_notify: tokio::sync::Notify::new(),
            blocking_notify: tokio::sync::Notify::new(),
            notify_keyspace_events: NotifyFlags::default(),
            slowlog: std::sync::Mutex::new(std::collections::VecDeque::new()),
            slowlog_next_id: AtomicU64::new(0),
            slowlog_threshold_us: 10_000,
            slowlog_max_len: 128,
            history: None,
        }
    }

    /// Record how long a command took, logging it to `SLOWLOG` if it was
    /// over `slowlog_threshold_us`. `req` is only cloned when actually
    /// logging, so this is cheap on the (overwhelmingly common) fast path.
    pub(crate) fn record_command_timing(&self, req: &[Bytes], duration: Duration) {
        if self.slowlog_threshold_us < 0 {
            return;
        }
        let micros = duration.as_micros() as u64;
        if micros < self.slowlog_threshold_us as u64 {
            return;
        }
        let entry = crate::info::SlowLogEntry {
            id: self.slowlog_next_id.fetch_add(1, Ordering::Relaxed),
            timestamp_secs: SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
            duration_us: micros,
            args: req.to_vec(),
        };
        let mut log = self.slowlog.lock().unwrap();
        log.push_front(entry);
        while log.len() > self.slowlog_max_len {
            log.pop_back();
        }
    }

    pub fn with_singleflight_max_wait(mut self, d: Duration) -> Self {
        self.singleflight_max_wait = d;
        self
    }

    pub fn with_notify_keyspace_events(mut self, flags: NotifyFlags) -> Self {
        self.notify_keyspace_events = flags;
        self
    }

    pub fn with_rdb_path(mut self, path: PathBuf) -> Self {
        self.rdb_path = Some(path);
        self
    }

    pub fn with_tiered_limit(mut self, limit: Option<u64>) -> Self {
        self.tiered_limit = limit;
        self
    }

    pub fn with_aof(mut self, writer: persistence::aof::AofWriter, path: PathBuf) -> Self {
        self.aof = Some(writer);
        self.aof_path = Some(path);
        self
    }

    pub fn with_maxmemory(mut self, limit: Option<u64>, policy: MaxMemoryPolicy) -> Self {
        self.maxmemory = limit;
        self.maxmemory_policy = policy;
        self
    }

    /// `history-enabled yes` — turns on `GETAT`/`HISTORY`, backed by
    /// `writer` (see `ServerContext::history`'s docs for scope).
    pub fn with_history(mut self, writer: persistence::history::HistoryWriter) -> Self {
        self.history = Some(writer);
        self
    }

    pub fn mark_saved(&self) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        self.last_save.store(now, Ordering::Relaxed);
    }

    pub fn last_save(&self) -> u64 {
        self.last_save.load(Ordering::Relaxed)
    }

    pub(crate) fn append_aof(&self, db_index: usize, req: &[Bytes]) {
        if let Some(writer) = &self.aof {
            if let Err(e) = writer.append(db_index, req) {
                tracing::warn!(error = %e, "failed to append to AOF");
            }
        }
    }

    /// Flush the AOF to disk, for the `appendfsync everysec` background tick.
    pub fn aof_fsync(&self) {
        if let Some(writer) = &self.aof {
            if let Err(e) = writer.fsync() {
                tracing::warn!(error = %e, "failed to fsync AOF");
            }
        }
    }

    /// Blocks until everything queued to the AOF so far has actually been
    /// written *and* fsync'd — for `WAITAOF`, which needs a real answer,
    /// not just "is AOF turned on" (see `persistence::aof`'s module docs
    /// for why that distinction matters now that `append`/`fsync` are
    /// fire-and-forget). `None` if AOF isn't enabled at all.
    pub(crate) fn flush_aof(&self) -> Option<std::io::Result<()>> {
        self.aof.as_ref().map(|writer| writer.flush_and_fsync())
    }

    pub fn aof_enabled(&self) -> bool {
        self.aof.is_some()
    }

    /// Blocks until every `GETAT`/`HISTORY` version recorded so far has
    /// actually landed on disk. Not used on any request path (recording is
    /// fire-and-forget by design, see `persistence::history`'s docs) —
    /// only by tests that need a deterministic "has this committed yet"
    /// instead of sleeping/polling. No-op if history isn't enabled.
    #[cfg(test)]
    pub(crate) fn history_flush(&self) {
        if let Some(writer) = &self.history {
            writer.flush();
        }
    }

    /// Deletes every recorded version older than `cutoff_ms` across every
    /// key and database. Intended to be called periodically from a
    /// background tick on `history-retention-ms` (see `server/src/main.rs`)
    /// — this is what actually bounds the disk-backed history store's
    /// size, replacing the old in-RAM design's per-key version cap. No-op
    /// (returns `0`) if history isn't enabled.
    pub fn history_sweep(&self, cutoff_ms: u64) -> usize {
        self.history.as_ref().map(|writer| writer.sweep_older_than(cutoff_ms)).unwrap_or(0)
    }

    pub fn connected_clients(&self) -> usize {
        self.clients.len()
    }

    /// Active `GET ... SINGLEFLIGHT` leases right now — a proxy for
    /// "waiters", not an exact count of blocked connections (a lease can
    /// exist with zero current waiters, between the leaseholder taking it
    /// and anyone else asking for the same key).
    pub fn singleflight_waiters(&self) -> usize {
        self.singleflight.len()
    }

    pub fn maxmemory_exceeded(&self) -> bool {
        match self.maxmemory {
            Some(limit) => self.approx_memory.load(Ordering::Relaxed) as u64 > limit,
            None => false,
        }
    }

    /// Refresh the memory estimate and run both eviction mechanisms,
    /// bounded to avoid one tick doing unbounded work on a huge overshoot:
    ///
    /// 1. If `tiered_limit` is set and the store has a tiering handler,
    ///    move keys to the disk tier first — transparent to clients (`GET`
    ///    still works, just promotes them back), so no LIVE/WATCH
    ///    notification fires for this.
    /// 2. If still (or separately) over `maxmemory`, fall back to the
    ///    harder `maxmemory_policy`: delete keys, or reject writes under
    ///    `noeviction` (see the OOM check in `dispatch`).
    pub fn run_eviction_tick(&self) -> usize {
        let mut evicted = 0;

        if let Some(limit) = self.tiered_limit {
            for _ in 0..64 {
                let used = self.refresh_memory_usage();
                if used as u64 <= limit || !self.tier_one() {
                    break;
                }
                evicted += 1;
            }
        }

        if let Some(limit) = self.maxmemory {
            for _ in 0..64 {
                let used = self.refresh_memory_usage();
                if used as u64 <= limit {
                    break;
                }
                if self.maxmemory_policy == MaxMemoryPolicy::NoEviction || !self.delete_one() {
                    break;
                }
                evicted += 1;
            }
        }

        evicted
    }

    fn refresh_memory_usage(&self) -> usize {
        let used = self.store.approx_bytes();
        self.approx_memory.store(used, Ordering::Relaxed);
        used
    }

    /// Move one random key from RAM to the disk tier. Returns `false` if
    /// there's nothing left to move.
    fn tier_one(&self) -> bool {
        let mut rng = rand::thread_rng();
        for db_index in 0..self.store.len() {
            let db = self.store.db(db_index).expect("index within store.len()");
            let keys = db.keys_snapshot();
            if let Some(victim) = keys.choose(&mut rng) {
                if db.evict_to_tier(victim) {
                    return true;
                }
            }
        }
        false
    }

    /// Delete one key chosen per `maxmemory_policy`. Returns `false` if
    /// there's no eligible key left (or the policy is `NoEviction`).
    fn delete_one(&self) -> bool {
        let mut rng = rand::thread_rng();
        for db_index in 0..self.store.len() {
            let db = self.store.db(db_index).expect("index within store.len()");
            let keys = db.keys_snapshot();
            let candidates: Vec<Bytes> = if self.maxmemory_policy == MaxMemoryPolicy::NoEviction {
                return false;
            } else if self.maxmemory_policy.volatile_only() {
                keys.into_iter().filter(|k| matches!(db.ttl(k), Ttl::Expires(_))).collect()
            } else {
                keys
            };
            if candidates.is_empty() {
                continue;
            }
            let victim = match self.maxmemory_policy {
                MaxMemoryPolicy::AllKeysRandom | MaxMemoryPolicy::VolatileRandom => {
                    candidates.choose(&mut rng).cloned()
                }
                MaxMemoryPolicy::AllKeysLru | MaxMemoryPolicy::VolatileLru => candidates
                    .into_iter()
                    .filter_map(|k| db.access_stats(&k).map(|(last, _)| (k, last)))
                    .min_by_key(|(_, last)| *last)
                    .map(|(k, _)| k),
                MaxMemoryPolicy::AllKeysLfu | MaxMemoryPolicy::VolatileLfu => candidates
                    .into_iter()
                    .filter_map(|k| db.access_stats(&k).map(|(_, count)| (k, count)))
                    .min_by_key(|(_, count)| *count)
                    .map(|(k, _)| k),
                MaxMemoryPolicy::VolatileTtl => candidates
                    .into_iter()
                    .filter_map(|k| match db.ttl(&k) {
                        Ttl::Expires(d) => Some((k, d)),
                        _ => None,
                    })
                    .min_by_key(|(_, d)| *d)
                    .map(|(k, _)| k),
                MaxMemoryPolicy::NoEviction => unreachable!("handled above"),
            };
            if let Some(victim) = victim {
                db.del(std::slice::from_ref(&victim));
                on_key_changed(self, db_index, victim);
                return true;
            }
        }
        false
    }

    /// Register a new connection's outbound push channel, returning its id
    /// and the receiving half the connection loop should poll alongside its
    /// socket reads.
    pub fn register_client(&self) -> (u64, tokio::sync::mpsc::UnboundedReceiver<Value>) {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.clients.insert(id, tx);
        (id, rx)
    }

    /// Drop every subscription (channels, patterns, LIVE) a disconnecting
    /// client held. Cheap enough at MVP scale; a production version would
    /// index subscriptions per-client to avoid the full scan.
    pub fn disconnect_client(&self, id: u64) {
        self.clients.remove(&id);
        self.replicas.remove(&id);
        self.client_names.remove(&id);
        let mut tracking_removed = 0usize;
        self.tracking_table.retain(|_, subs| {
            subs.remove(&id);
            let keep = !subs.is_empty();
            tracking_removed += (!keep) as usize;
            keep
        });
        self.tracking_table_len.fetch_sub(tracking_removed, Ordering::Relaxed);
        self.channels.retain(|_, subs| {
            subs.remove(&id);
            !subs.is_empty()
        });
        self.patterns.retain(|_, subs| {
            subs.remove(&id);
            !subs.is_empty()
        });
        let mut watchers_removed = 0usize;
        self.watchers.retain(|_, subs| {
            subs.remove(&id);
            let keep = !subs.is_empty();
            watchers_removed += (!keep) as usize;
            keep
        });
        self.watchers_len.fetch_sub(watchers_removed, Ordering::Relaxed);
        self.dirty_clients.remove(&id);
        let mut emptied = Vec::new();
        for mut entry in self.live_subs.iter_mut() {
            entry.clients.remove(&id);
            if entry.clients.is_empty() {
                emptied.push(*entry.key());
            }
        }
        for fingerprint in emptied {
            self.drop_live_sub(fingerprint);
        }
    }

    pub(crate) fn drop_live_sub(&self, fingerprint: u64) {
        if let Some((_, sub)) = self.live_subs.remove(&fingerprint) {
            let watcher_key = (sub.db_index, sub.key);
            if let Some(mut watchers) = self.key_watchers.get_mut(&watcher_key) {
                watchers.remove(&fingerprint);
                if watchers.is_empty() {
                    drop(watchers);
                    self.key_watchers.remove(&watcher_key);
                    self.key_watchers_len.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    pub(crate) fn push_to(&self, client_id: u64, value: Value) {
        if let Some(sender) = self.clients.get(&client_id) {
            let _ = sender.send(value);
        }
    }
}

/// Per-connection state.
pub struct ClientState {
    pub id: u64,
    pub db_index: usize,
    pub protocol: Protocol,
    pub authenticated: bool,
    /// Count of channels + patterns + LIVE fingerprints this client holds.
    /// RESP2 connections with at least one subscription are restricted to
    /// (P)SUBSCRIBE/(P)UNSUBSCRIBE/PING/QUIT/RESET/LIVE/UNLIVE, same as
    /// stock Redis pub/sub-mode connections.
    pub subscriptions: usize,

    /// `true` between `MULTI` and `EXEC`/`DISCARD`.
    pub in_multi: bool,
    /// Set when a queued command was rejected outright (e.g. unknown
    /// command); the next `EXEC` aborts with `EXECABORT` without running
    /// anything, matching Redis.
    pub multi_error: bool,
    pub queued: Vec<Request>,
    /// Keys this client has `WATCH`ed, for cleanup on `UNWATCH`/`EXEC`/
    /// `DISCARD`/disconnect.
    pub watched: Vec<(usize, Bytes)>,
    /// `true` only for the scratch `ClientState` the replication-link
    /// background task dispatches through — exempts it from the
    /// replica-read-only check so the replicated stream can actually apply.
    pub is_replication_link: bool,
    /// Which ACL user this connection authenticated as (`"default"` until
    /// `AUTH <user> <pass>` names someone else).
    pub acl_user: String,
    /// Set by `blocking_deadline` the first time a `BLPOP`-family command
    /// blocks, so the deadline survives across the connection loop's
    /// retries of the exact same request. Cleared by `clear_blocking_deadline`
    /// once that command resolves.
    pub block_deadline: Option<Instant>,
    /// `true` once a blocking command with `timeout 0` (block forever) has
    /// started — distinguishes "no deadline is set yet" from "no deadline
    /// *because* the client asked to wait forever", since both read as
    /// `block_deadline: None`.
    pub block_forever: bool,
    /// `CLIENT SETNAME`; mirrored into `ServerContext::client_names` for
    /// `CLIENT LIST` to see from other connections.
    pub client_name: String,
    /// `CLIENT TRACKING ON` — see `ServerContext::tracking_table`'s docs.
    pub tracking: bool,
}

impl ClientState {
    pub fn new(requires_auth: bool) -> Self {
        Self::with_id(0, requires_auth)
    }

    pub fn with_id(id: u64, requires_auth: bool) -> Self {
        Self {
            id,
            db_index: 0,
            protocol: Protocol::Resp2,
            authenticated: !requires_auth,
            subscriptions: 0,
            in_multi: false,
            multi_error: false,
            queued: Vec::new(),
            watched: Vec::new(),
            is_replication_link: false,
            acl_user: "default".to_string(),
            block_deadline: None,
            block_forever: false,
            client_name: String::new(),
            tracking: false,
        }
    }

    /// State used by the background task applying a primary's replicated
    /// command stream: authenticated (it's not a network client subject to
    /// `requirepass`) and exempt from the replica-read-only check.
    pub fn for_replication_link() -> Self {
        let mut state = Self::with_id(0, false);
        state.is_replication_link = true;
        state
    }
}

fn generate_replid() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..40).map(|_| std::char::from_digit(rng.gen_range(0..16), 16).unwrap()).collect()
}

/// What the connection loop should do after a command runs.
pub enum Outcome {
    Reply(Value),
    /// Several replies/pushes in a row (e.g. `SUBSCRIBE a b c` sends one
    /// confirmation per channel).
    Multi(Vec<Value>),
    CloseConnection(Value),
    /// `GET ... SINGLEFLIGHT` hit an active lease someone else holds. The
    /// connection loop (the only place actually able to `.await`) should
    /// wait on the lease's `Notify` (or its own bounded timeout, whichever
    /// comes first) and then re-dispatch the exact same request.
    SingleFlightWait(std::sync::Arc<SingleFlightLease>),
    /// Send nothing back. Used only by `REPLCONF ACK`: replying on a
    /// replication link would interleave an extra frame into the stream of
    /// replicated commands the other side is decoding, corrupting it.
    NoReply,
    /// `WAIT numreplicas timeout` hasn't been satisfied yet — the
    /// connection loop polls `ServerContext::count_replicas_at_least`
    /// (woken by `replica_ack_notify`) until it is, or the timeout elapses.
    /// `numlocal` is `Some(_)` only for `WAITAOF` (there's no AOF-replica
    /// concept here, so the local count is just "is `appendonly` on", not
    /// an actual wait): when set, the connection loop replies with the
    /// two-element `[local, replicas]` array `WAITAOF` expects instead of
    /// a bare integer.
    WaitForReplicas {
        numreplicas: usize,
        target_offset: u64,
        timeout: Option<Duration>,
        numlocal: Option<i64>,
    },
    /// `BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/`BZPOPMAX` found nothing to pop
    /// yet. The connection loop waits on `blocking_notify` (or its own
    /// bounded timeout) and re-dispatches the exact same request — same
    /// retry-the-original-request shape as `SingleFlightWait`. `deadline`
    /// is `None` for `timeout 0` (block forever, matching Redis). `on_timeout`
    /// is the reply to send once the deadline passes without a match.
    BlockWait {
        deadline: Option<Instant>,
        on_timeout: Value,
    },
}

/// `Instant::now() + dur` panics if the sum overflows the clock's internal
/// representation — reachable straight from client input (`EXPIRE`/`SET EX`/
/// `SET PX` with an enormous TTL, `RESTORE`'s TTL, `GET ... SINGLEFLIGHT`'s
/// lease, `SCHEDULE`, blocking command timeouts, ...). Saturates to the
/// farthest representable instant instead of panicking — indistinguishable
/// from "a very long time from now" to every caller here, none of which
/// actually need the exact value once it's this far out.
pub(crate) fn saturating_deadline(dur: Duration) -> Instant {
    let now = Instant::now();
    if let Some(t) = now.checked_add(dur) {
        return t;
    }
    let mut probe = dur;
    loop {
        probe /= 2;
        if probe.is_zero() {
            return now;
        }
        if let Some(t) = now.checked_add(probe) {
            return t;
        }
    }
}

pub(crate) fn reply(v: Value) -> Outcome {
    Outcome::Reply(v)
}

pub(crate) fn err(msg: impl Into<String>) -> Outcome {
    Outcome::Reply(Value::error(msg.into()))
}

pub(crate) fn wrong_args(cmd: &str) -> Outcome {
    err(format!("ERR wrong number of arguments for '{cmd}' command"))
}

pub(crate) fn wrong_type() -> Outcome {
    err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Fan a successful single-key mutation out to the LIVE re-evaluation path
/// (Phase 3), the `WATCH` dirty-CAS path (Phase 4), and any `SINGLEFLIGHT`
/// waiters parked on this key (Phase 6) — every command that mutates
/// exactly one key calls this once it knows the write stuck.
pub(crate) fn on_key_changed(ctx: &ServerContext, db_index: usize, key: Bytes) {
    if ctx.history.is_some() {
        record_history(ctx, db_index, &key);
    }
    live::notify_key_change(ctx, db_index, key.clone());
    txn::touch_watchers(ctx, db_index, key.clone());
    // `singleflight`/`tracking_table` are empty on the overwhelmingly common
    // path (most writes never had a SINGLEFLIGHT lease or a tracked read on
    // this exact key) — the atomic counter check skips hashing `key` and
    // locking a shard of either `DashMap` for nothing, same rationale as
    // `key_watchers_len`/`watchers_len` above.
    // `singleflight`/`tracking_table` are empty on the overwhelmingly common
    // path (most writes never had a SINGLEFLIGHT lease or a tracked read on
    // this exact key) — the atomic counter check skips hashing `key` and
    // locking a shard of either `DashMap` for nothing, same rationale as
    // `key_watchers_len`/`watchers_len` above.
    if ctx.singleflight_len.load(Ordering::Relaxed) != 0 {
        if let Some((_, lease)) = ctx.singleflight.remove(&(db_index, key.clone())) {
            ctx.singleflight_len.fetch_sub(1, Ordering::Relaxed);
            lease.notify.notify_waiters();
        }
    }
    ctx.blocking_notify.notify_waiters();
    if ctx.tracking_table_len.load(Ordering::Relaxed) != 0 {
        if let Some((_, clients)) = ctx.tracking_table.remove(&(db_index, key.clone())) {
            ctx.tracking_table_len.fetch_sub(1, Ordering::Relaxed);
            let push = Value::Push(vec![Value::bulk("invalidate"), Value::Array(vec![Value::bulk(key)])]);
            for client_id in clients {
                ctx.push_to(client_id, push.clone());
            }
        }
    }
}

/// Queues a `(now, current-string-value-or-None)` version of `key` for the
/// disk-backed history log (`persistence::history::HistoryWriter`), called
/// from `on_key_changed` whenever `ctx.history` is `Some`. See
/// `ServerContext::history`'s docs for what this simplifies away vs. the
/// real design. Re-reads the key instead of being handed the new value
/// directly, since `on_key_changed` is shared by every write command and
/// doesn't carry one — an acceptable extra lookup given this only runs at
/// all when `history-enabled yes`. `HistoryWriter::record` itself is
/// fire-and-forget (see its module docs), so this doesn't block the write
/// path on any disk I/O.
fn record_history(ctx: &ServerContext, db_index: usize, key: &Bytes) {
    let Some(writer) = &ctx.history else { return };
    let Some(db) = ctx.store.db(db_index) else { return };
    // `with_value` returns `None` for a missing key and `Some(None)` for a
    // non-string value; `.flatten()` collapses both into `None` here,
    // which is exactly the distinction `GETAT`/`HISTORY` need: "string
    // with this value" (`Some`) vs. "deleted, or never a string" (`None`).
    let snapshot = db.with_value(key, |v| match v {
        KVal::String(s) => Some(s.clone()),
        _ => None,
    });
    let snapshot = snapshot.flatten();
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
    writer.record(db_index, key.clone(), now_ms, snapshot);
}

/// Publishes a `notify-keyspace-events` notification for a single-key
/// write, same wire format as real Redis: `__keyspace@<db>__:<key>` gets
/// `event` as its message (if the `K` flag is set), and
/// `__keyevent@<db>__:<event>` gets `key` as its message (if `E` is set) —
/// gated on `class` being one of the enabled class codes either way. A
/// no-op (not even a config lookup past the first flag check) when
/// `notify-keyspace-events` is unset, which is the default.
///
/// Scoped down from real Redis: only commands that go through this call
/// (one per command module, right next to `on_key_changed`) fire a
/// notification — background TTL expiry (the `g`/`x` "expired" event)
/// isn't wired up, since that lives in the `keyspace` crate, which doesn't
/// know about `ServerContext`/pub-sub.
pub(crate) fn notify_keyspace_event(
    ctx: &ServerContext,
    db_index: usize,
    class: char,
    event: &str,
    key: &[u8],
) {
    let flags = &ctx.notify_keyspace_events;
    if !flags.enabled(class) {
        return;
    }
    if flags.keyspace {
        let channel = [b"__keyspace@", db_index.to_string().as_bytes(), b"__:", key].concat();
        ctx.publish(&channel, Bytes::copy_from_slice(event.as_bytes()));
    }
    if flags.keyevent {
        let channel = format!("__keyevent@{db_index}__:{event}");
        ctx.publish(channel.as_bytes(), Bytes::copy_from_slice(key));
    }
}

/// `BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/`BZPOPMAX` aren't in `WRITE_COMMANDS`
/// (see their module docs for why — the raw blocking command is never
/// itself logged to the AOF or replicated), so they need to run the same
/// replica-read-only / `maxmemory` guards `dispatch()` applies to ordinary
/// write commands by hand, before touching the keyspace.
pub(crate) fn check_write_allowed(ctx: &ServerContext, state: &ClientState) -> Option<Outcome> {
    if ctx.is_replica() && !state.is_replication_link {
        return Some(err("READONLY You can't write against a read only replica."));
    }
    if ctx.maxmemory_exceeded() && ctx.maxmemory_policy == MaxMemoryPolicy::NoEviction {
        return Some(err("OOM command not allowed when used memory > 'maxmemory'."));
    }
    None
}

/// Parses a blocking command's trailing `timeout` argument (seconds,
/// fractional allowed) and turns it into the deadline `Outcome::BlockWait`
/// needs — reusing `state.block_deadline` across retries of the *same*
/// blocked command so the timeout doesn't silently restart on every
/// `blocking_notify` wakeup. Returns `Err` for a malformed timeout.
pub(crate) fn blocking_deadline(
    state: &mut ClientState,
    timeout: &Bytes,
) -> Result<Option<Instant>, Outcome> {
    if let Some(deadline) = state.block_deadline {
        return Ok(Some(deadline));
    }
    if state.block_forever {
        return Ok(None);
    }
    // Clamped well under `Duration::from_secs_f64`'s own panic threshold
    // (~5.8e11s) so a client-supplied `inf`/huge timeout can't crash the
    // connection — ~31 years is already indistinguishable from "forever"
    // for a blocking command.
    const MAX_BLOCK_SECS: f64 = 1e9;
    let secs: f64 = match std::str::from_utf8(timeout).ok().and_then(|s| s.parse::<f64>().ok()) {
        Some(n) if n >= 0.0 => n.min(MAX_BLOCK_SECS),
        _ => return Err(err("ERR timeout is not a float or out of range")),
    };
    if secs == 0.0 {
        state.block_forever = true;
        Ok(None)
    } else {
        let deadline = saturating_deadline(Duration::from_secs_f64(secs));
        state.block_deadline = Some(deadline);
        Ok(Some(deadline))
    }
}

/// Clears the per-connection blocking-timeout bookkeeping `blocking_deadline`
/// sets up — call once a blocking command actually resolves (found a value
/// or hit its deadline), so the *next* blocking command starts fresh.
pub(crate) fn clear_blocking_deadline(state: &mut ClientState) {
    state.block_deadline = None;
    state.block_forever = false;
}

/// Same fan-out, for whole-database resets like `FLUSHDB`.
pub(crate) fn on_flush(ctx: &ServerContext, db_index: usize) {
    live::notify_flush(ctx, db_index);
    txn::touch_all_watchers(ctx, db_index);
}

pub fn dispatch(ctx: &ServerContext, state: &mut ClientState, req: Request) -> Outcome {
    let dispatch_start = Instant::now();
    if req.is_empty() {
        return err("ERR empty command");
    }
    let name = String::from_utf8_lossy(&req[0]).to_ascii_uppercase();
    let args = &req[1..];

    // Only a small allowlist works before AUTH succeeds, matching requirepass behavior.
    if !state.authenticated && !matches!(name.as_str(), "AUTH" | "HELLO" | "QUIT" | "RESET") {
        return err("NOAUTH Authentication required.");
    }

    // ACL: command + key-pattern checks against the authenticated user.
    // Exempt the same small set of always-available commands as the
    // pre-AUTH allowlist above, plus the replication-apply path (which
    // isn't a real network client and shouldn't be fenced by a human
    // operator's ACL).
    if state.authenticated
        && !state.is_replication_link
        && !matches!(name.as_str(), "AUTH" | "HELLO" | "QUIT" | "RESET")
    {
        if let Some(user) = ctx.acl_users.get(&state.acl_user) {
            if !user.enabled {
                return err("NOAUTH Authentication required.");
            }
            if !user.can_run(&name) {
                return err(format!(
                    "NOPERM User {} has no permissions to run the '{}' command",
                    state.acl_user,
                    name.to_ascii_lowercase()
                ));
            }
            if crate::acl::is_key_command(&name) {
                if let Some(key) = args.first() {
                    if !user.can_access_key(key) {
                        return err(format!(
                            "NOPERM No permissions to access a key for user {}",
                            state.acl_user
                        ));
                    }
                }
            }
        }
    }

    // RESP2 clients holding at least one subscription are restricted to the
    // pub/sub-mode command set, matching stock Redis.
    if state.protocol == Protocol::Resp2
        && state.subscriptions > 0
        && !matches!(
            name.as_str(),
            "SUBSCRIBE"
                | "UNSUBSCRIBE"
                | "PSUBSCRIBE"
                | "PUNSUBSCRIBE"
                | "PING"
                | "QUIT"
                | "RESET"
                | "LIVE"
                | "UNLIVE"
        )
    {
        return err(format!(
            "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET / LIVE / UNLIVE are allowed in this context",
            name.to_ascii_lowercase()
        ));
    }

    // Inside MULTI, every command except the ones that manage the
    // transaction itself gets queued instead of executed.
    if state.in_multi && !matches!(name.as_str(), "EXEC" | "DISCARD" | "MULTI" | "RESET" | "QUIT") {
        if name == "WATCH" {
            return err("ERR WATCH inside MULTI is not allowed");
        }
        if !KNOWN_COMMANDS.iter().any(|c| c.eq_ignore_ascii_case(&name)) {
            state.multi_error = true;
            return err(format!("ERR unknown command '{name}'"));
        }
        state.queued.push(req);
        return reply(Value::Simple("QUEUED".into()));
    }

    let is_write = WRITE_COMMANDS.contains(name.as_str());
    if is_write && ctx.is_replica() && !state.is_replication_link {
        return err("READONLY You can't write against a read only replica.");
    }
    if is_write && ctx.maxmemory_exceeded() && ctx.maxmemory_policy == MaxMemoryPolicy::NoEviction {
        return err("OOM command not allowed when used memory > 'maxmemory'.");
    }
    // Snapshot the request for the AOF before it's consumed by the match
    // below — cheap, since `Bytes`/`Vec` cloning here is just refcount
    // bumps and a pointer-vec copy, not a deep data copy.
    let aof_req = if is_write { Some(req.clone()) } else { None };

    let outcome = match name.as_str() {
        "PING" => match args.len() {
            0 => reply(Value::Simple("PONG".into())),
            1 => reply(Value::bulk(args[0].clone())),
            _ => wrong_args("ping"),
        },
        "ECHO" => match args {
            [msg] => reply(Value::bulk(msg.clone())),
            _ => wrong_args("echo"),
        },
        "QUIT" => Outcome::CloseConnection(Value::ok()),
        "AUTH" => cmd_auth(ctx, state, args),
        "HELLO" => cmd_hello(ctx, state, args),
        "RESET" => {
            state.db_index = 0;
            state.protocol = Protocol::Resp2;
            state.authenticated = ctx.requirepass.is_none();
            state.in_multi = false;
            state.multi_error = false;
            state.queued.clear();
            ctx.dirty_clients.remove(&state.id);
            txn::clear_watches(ctx, state);
            reply(Value::Simple("RESET".into()))
        }
        "SELECT" => cmd_select(ctx, state, args),
        "DBSIZE" => match ctx.store.db(state.db_index) {
            Some(db) => reply(Value::Integer(db.dbsize() as i64)),
            None => err("ERR DB index is out of range"),
        },
        "FLUSHDB" => match ctx.store.db(state.db_index) {
            Some(db) => {
                db.flush();
                on_flush(ctx, state.db_index);
                reply(Value::ok())
            }
            None => err("ERR DB index is out of range"),
        },
        "FLUSHALL" => {
            for idx in 0..ctx.store.len() {
                if let Some(db) = ctx.store.db(idx) {
                    db.flush();
                    on_flush(ctx, idx);
                }
            }
            reply(Value::ok())
        }

        "MULTI" => txn::cmd_multi(state),
        "EXEC" => txn::cmd_exec(ctx, state),
        "DISCARD" => txn::cmd_discard(ctx, state),
        "WATCH" => txn::cmd_watch(ctx, state, args),
        "UNWATCH" => txn::cmd_unwatch(ctx, state),

        "EVAL" => script::cmd_eval(ctx, state, args),
        "EVAL_RO" => script::cmd_eval_ro(ctx, state, args),
        "EVALSHA" => script::cmd_evalsha(ctx, state, args),
        "EVALSHA_RO" => script::cmd_evalsha_ro(ctx, state, args),
        "SCRIPT" => script::cmd_script(ctx, args),
        "SET" => cmd_set(ctx, state, args),
        "GET" => singleflight::cmd_get(ctx, state, args),
        "APPEND" => string::cmd_append(ctx, state, args),
        "INCR" => string::cmd_incr(ctx, state, args),
        "INCRBY" => string::cmd_incrby(ctx, state, args),
        "DECR" => string::cmd_decr(ctx, state, args),
        "DECRBY" => string::cmd_decrby(ctx, state, args),
        "SETNX" => string::cmd_setnx(ctx, state, args),
        "MSET" => string::cmd_mset(ctx, state, args),
        "MGET" => string::cmd_mget(ctx, state, args),
        "STRLEN" => string::cmd_strlen(ctx, state, args),
        "GETRANGE" => string::cmd_getrange(ctx, state, args),
        "KEYS" => keys::cmd_keys(ctx, state, args),
        "DEL" | "UNLINK" => cmd_del(ctx, state, args),
        "EXISTS" => cmd_exists(ctx, state, args),
        "TTL" => cmd_ttl(ctx, state, args, false),
        "PTTL" => cmd_ttl(ctx, state, args, true),
        "EXPIRE" => cmd_expire(ctx, state, args, false),
        "PEXPIRE" => cmd_expire(ctx, state, args, true),
        "EXPIREAT" => cmd_expireat(ctx, state, args, false),
        "PEXPIREAT" => cmd_expireat(ctx, state, args, true),
        "PERSIST" => cmd_persist(ctx, state, args),
        "RENAME" => cmd_rename(ctx, state, args),
        "RENAMENX" => cmd_renamenx(ctx, state, args),
        "GETDEL" => string::cmd_getdel(ctx, state, args),
        "INCRBYFLOAT" => string::cmd_incrbyfloat(ctx, state, args),
        "GETAT" => history::cmd_getat(ctx, state, args),
        "HISTORY" => history::cmd_history(ctx, state, args),
        "TYPE" => cmd_type(ctx, state, args),
        "OBJECT" => cmd_object(ctx, state, args),
        "COMMAND" => cmd_command(args),
        "CLIENT" => client::cmd_client(ctx, state, args),
        "SCAN" => keys::cmd_scan(ctx, state, args),
        "INFO" => info::cmd_info(ctx, args),
        "ACL" => crate::acl::cmd_acl(ctx, state, args),
        "SLOWLOG" => info::cmd_slowlog(ctx, args),
        "LATENCY" => info::cmd_latency(ctx, args),
        "MEMORY" => info::cmd_memory(ctx, state, args),

        "LPUSH" => list::cmd_lpush(ctx, state, args),
        "RPUSH" => list::cmd_rpush(ctx, state, args),
        "LPOP" => list::cmd_lpop(ctx, state, args),
        "RPOP" => list::cmd_rpop(ctx, state, args),
        "LLEN" => list::cmd_llen(ctx, state, args),
        "LRANGE" => list::cmd_lrange(ctx, state, args),
        "LTRIM" => list::cmd_ltrim(ctx, state, args),
        "LSET" => list::cmd_lset(ctx, state, args),
        "LINSERT" => list::cmd_linsert(ctx, state, args),
        "LREM" => list::cmd_lrem(ctx, state, args),
        "RPOPLPUSH" => list::cmd_rpoplpush(ctx, state, args),
        "LMOVE" => list::cmd_lmove(ctx, state, args),
        "LPUSHX" => list::cmd_lpushx(ctx, state, args),
        "RPUSHX" => list::cmd_rpushx(ctx, state, args),
        "BLPOP" => list::cmd_blpop(ctx, state, args),
        "BRPOP" => list::cmd_brpop(ctx, state, args),
        "BLMOVE" => list::cmd_blmove(ctx, state, args),

        "HSET" => hash::cmd_hset(ctx, state, args),
        "HGET" => hash::cmd_hget(ctx, state, args),
        "HDEL" => hash::cmd_hdel(ctx, state, args),
        "HEXISTS" => hash::cmd_hexists(ctx, state, args),
        "HLEN" => hash::cmd_hlen(ctx, state, args),
        "HGETALL" => hash::cmd_hgetall(ctx, state, args),
        "HMGET" => hash::cmd_hmget(ctx, state, args),
        "HINCRBY" => hash::cmd_hincrby(ctx, state, args),
        "HKEYS" => hash::cmd_hkeys(ctx, state, args),
        "HVALS" => hash::cmd_hvals(ctx, state, args),
        "HSCAN" => hash::cmd_hscan(ctx, state, args),

        "SADD" => set::cmd_sadd(ctx, state, args),
        "SREM" => set::cmd_srem(ctx, state, args),
        "SMEMBERS" => set::cmd_smembers(ctx, state, args),
        "SISMEMBER" => set::cmd_sismember(ctx, state, args),
        "SCARD" => set::cmd_scard(ctx, state, args),
        "SINTERCARD" => set::cmd_sintercard(ctx, state, args),
        "SPOP" => set::cmd_spop(ctx, state, args),
        "SRANDMEMBER" => set::cmd_srandmember(ctx, state, args),
        "SUNION" => set::cmd_sunion(ctx, state, args),
        "SINTER" => set::cmd_sinter(ctx, state, args),
        "SDIFF" => set::cmd_sdiff(ctx, state, args),
        "SSCAN" => set::cmd_sscan(ctx, state, args),

        "ZADD" => zset::cmd_zadd(ctx, state, args),
        "ZINCRBY" => zset::cmd_zincrby(ctx, state, args),
        "ZSCORE" => zset::cmd_zscore(ctx, state, args),
        "ZREM" => zset::cmd_zrem(ctx, state, args),
        "ZCARD" => zset::cmd_zcard(ctx, state, args),
        "ZRANGE" => zset::cmd_zrange(ctx, state, args),
        "ZRANGEBYSCORE" => zset::cmd_zrangebyscore(ctx, state, args),
        "ZREVRANGE" => zset::cmd_zrevrange(ctx, state, args),
        "ZREVRANGEBYSCORE" => zset::cmd_zrevrangebyscore(ctx, state, args),
        "ZRANK" => zset::cmd_zrank(ctx, state, args),
        "ZREVRANK" => zset::cmd_zrevrank(ctx, state, args),
        "ZCOUNT" => zset::cmd_zcount(ctx, state, args),
        "ZREMRANGEBYSCORE" => zset::cmd_zremrangebyscore(ctx, state, args),
        "ZREMRANGEBYRANK" => zset::cmd_zremrangebyrank(ctx, state, args),
        "ZSCAN" => zset::cmd_zscan(ctx, state, args),
        "ZPOPMIN" => zset::cmd_zpopmin(ctx, state, args),
        "ZPOPMAX" => zset::cmd_zpopmax(ctx, state, args),
        "BZPOPMIN" => zset::cmd_bzpopmin(ctx, state, args),
        "BZPOPMAX" => zset::cmd_bzpopmax(ctx, state, args),

        "XADD" => stream::cmd_xadd(ctx, state, args),
        "XLEN" => stream::cmd_xlen(ctx, state, args),
        "XRANGE" => stream::cmd_xrange(ctx, state, args),
        "XREVRANGE" => stream::cmd_xrevrange(ctx, state, args),
        "XDEL" => stream::cmd_xdel(ctx, state, args),
        "XTRIM" => stream::cmd_xtrim(ctx, state, args),
        "XGROUP" => stream::cmd_xgroup(ctx, state, args),
        "XREADGROUP" => stream::cmd_xreadgroup(ctx, state, args),
        "XACK" => stream::cmd_xack(ctx, state, args),
        "XPENDING" => stream::cmd_xpending(ctx, state, args),
        "XREAD" => stream::cmd_xread(ctx, state, args),

        "SUBSCRIBE" => pubsub::cmd_subscribe(ctx, state, args),
        "UNSUBSCRIBE" => pubsub::cmd_unsubscribe(ctx, state, args),
        "PSUBSCRIBE" => pubsub::cmd_psubscribe(ctx, state, args),
        "PUNSUBSCRIBE" => pubsub::cmd_punsubscribe(ctx, state, args),
        "PUBLISH" => pubsub::cmd_publish(ctx, args),
        "PUBSUB" => pubsub::cmd_pubsub(ctx, args),

        "LIVE" => live::cmd_live(ctx, state, args),
        "UNLIVE" => live::cmd_unlive(ctx, state, args),

        "SAVE" => snapshot::cmd_save(ctx),
        "BGSAVE" => snapshot::cmd_bgsave(ctx),
        "LASTSAVE" => snapshot::cmd_lastsave(ctx),
        "BGREWRITEAOF" => snapshot::cmd_bgrewriteaof(ctx),
        "DUMP" => snapshot::cmd_dump(ctx, state, args),
        "RESTORE" => snapshot::cmd_restore(ctx, state, args),

        "SCHEDULE" => schedule::cmd_schedule(ctx, state, args),

        "REPLICAOF" | "SLAVEOF" => replication::cmd_replicaof(ctx, args),
        "PSYNC" => replication::cmd_psync(ctx, state, args),
        "REPLCONF" => replication::cmd_replconf(ctx, state, args),
        "WAIT" => replication::cmd_wait(ctx, args),
        "WAITAOF" => replication::cmd_waitaof(ctx, args),

        _ => err(format!(
            "ERR unknown command '{name}', with args beginning with: {}",
            args.first().map(|a| format!("'{}'", String::from_utf8_lossy(a))).unwrap_or_default()
        )),
    };

    if is_write && !matches!(outcome, Outcome::Reply(Value::Error(_))) {
        if let Some(logged_req) = aof_req {
            ctx.append_aof(state.db_index, &logged_req);
            // "Effects replication": each underlying write (including ones
            // issued via EVAL's redis.call or MULTI/EXEC) is replicated
            // individually as it actually happens, not as the original
            // script/transaction wrapper — avoids replicating anything
            // non-deterministic. This also gives chain replication for
            // free: a replica applying this same command re-replicates it
            // to its own replicas via this exact code path.
            ctx.replicate(state.db_index, &logged_req);
        }
    }

    ctx.record_command_timing(&req, dispatch_start.elapsed());
    outcome
}

fn cmd_auth(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let (username, password) = match args {
        [pass] => ("default".to_string(), pass),
        [user, pass] => (String::from_utf8_lossy(user).into_owned(), pass),
        _ => return wrong_args("auth"),
    };
    if args.len() == 1 && ctx.acl_users.get("default").is_some_and(|u| u.nopass) {
        return err("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?");
    }
    try_authenticate(ctx, state, &username, password)
}

/// Shared by `AUTH` and `HELLO ... AUTH`: checks a username/password pair
/// against the ACL user table (which always has at least `default`, mirroring
/// plain `requirepass` when ACL hasn't been touched — see `acl::AclUser::default_user`).
fn try_authenticate(
    ctx: &ServerContext,
    state: &mut ClientState,
    username: &str,
    password: &[u8],
) -> Outcome {
    match ctx.acl_users.get(username) {
        Some(user)
            if user.enabled
                && (user.nopass || user.password_hashes.contains(&crate::acl::sha256_hex(password))) =>
        {
            state.authenticated = true;
            state.acl_user = username.to_string();
            reply(Value::ok())
        }
        _ => err("WRONGPASS invalid username-password pair or user is disabled."),
    }
}

fn cmd_hello(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let mut i = 0;
    if let Some(ver) = args.first() {
        match ver.as_ref() {
            b"2" => state.protocol = Protocol::Resp2,
            b"3" => state.protocol = Protocol::Resp3,
            _ => return err("NOPROTO unsupported protocol version"),
        }
        i = 1;
    }
    while i < args.len() {
        let word = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match word.as_str() {
            "AUTH" => {
                if i + 2 >= args.len() {
                    return err("ERR syntax error in HELLO");
                }
                let username = String::from_utf8_lossy(&args[i + 1]).into_owned();
                let pass = &args[i + 2];
                if let Outcome::Reply(Value::Error(e)) = try_authenticate(ctx, state, &username, pass) {
                    return err(e);
                }
                i += 3;
            }
            _ => return err("ERR syntax error in HELLO"),
        }
    }
    if !state.authenticated {
        return err("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
    }
    reply(Value::Map(vec![
        (Value::bulk(&b"server"[..]), Value::bulk(&b"reflexdb"[..])),
        (Value::bulk(&b"version"[..]), Value::bulk(&b"0.1.0"[..])),
        (Value::bulk(&b"proto"[..]), Value::Integer(if state.protocol == Protocol::Resp3 { 3 } else { 2 })),
        (Value::bulk(&b"mode"[..]), Value::bulk(&b"standalone"[..])),
        (Value::bulk(&b"role"[..]), Value::bulk(&b"master"[..])),
        (Value::bulk(&b"modules"[..]), Value::Array(vec![])),
    ]))
}

fn cmd_select(ctx: &ServerContext, state: &mut ClientState, args: &[Bytes]) -> Outcome {
    let [idx] = args else { return wrong_args("select") };
    let idx: i64 = match std::str::from_utf8(idx).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    if idx < 0 || idx as usize >= ctx.store.len() {
        return err("ERR DB index is out of range");
    }
    state.db_index = idx as usize;
    reply(Value::ok())
}

fn cmd_set(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.len() < 2 {
        return wrong_args("set");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let key = args[0].clone();
    let value = args[1].clone();

    let opts = match set_opts::parse(&args[2..]) {
        Ok(o) => o,
        Err(ParseError::Syntax) => return err("ERR syntax error"),
    };

    let exists = db.get(&key).is_some();
    if opts.nx && exists {
        return reply(Value::NullBulk);
    }
    if opts.xx && !exists {
        return reply(Value::NullBulk);
    }

    let expires_at = if opts.keep_ttl { db.current_expiry(&key).flatten() } else { opts.expires_at };

    db.set(key.clone(), KVal::String(value), expires_at);
    on_key_changed(ctx, state.db_index, key.clone());
    notify_keyspace_event(ctx, state.db_index, '$', "set", &key);
    reply(Value::ok())
}

/// Plain `GET key` (no `SINGLEFLIGHT` modifier) — also used by
/// `singleflight::cmd_get` as its fast path when a value is already cached.
pub(crate) fn dispatch_get(ctx: &ServerContext, state: &ClientState, key: &Bytes) -> Outcome {
    let db = ctx.store.db(state.db_index).expect("valid db index");
    if state.tracking && state.protocol == Protocol::Resp3 {
        match ctx.tracking_table.entry((state.db_index, key.clone())) {
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                ctx.tracking_table_len.fetch_add(1, Ordering::Relaxed);
                slot.insert(HashSet::new()).insert(state.id);
            }
            dashmap::mapref::entry::Entry::Occupied(mut slot) => {
                slot.get_mut().insert(state.id);
            }
        }
    }
    // `with_value` (not `get`): a key holding a large List/Hash/Set/ZSet
    // would otherwise pay for a full deep clone of the collection on every
    // `GET` just to discover it's the wrong type and throw the clone away.
    match db.with_value(key, |v| match v {
        KVal::String(s) => Ok(s.clone()),
        _ => Err(()),
    }) {
        Some(Ok(s)) => reply(Value::bulk(s)),
        Some(Err(())) => wrong_type(),
        None => reply(Value::NullBulk),
    }
}

fn cmd_del(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("del");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let removed = db.del(args);
    // `db.del` only reports an aggregate count, not which of `args` it
    // actually removed, so (like `on_key_changed` already does here) this
    // fires for every requested key, not just the ones that existed.
    for key in args {
        on_key_changed(ctx, state.db_index, key.clone());
        notify_keyspace_event(ctx, state.db_index, 'g', "del", key);
    }
    reply(Value::Integer(removed as i64))
}

fn cmd_exists(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    if args.is_empty() {
        return wrong_args("exists");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    reply(Value::Integer(db.exists(args) as i64))
}

fn cmd_ttl(ctx: &ServerContext, state: &ClientState, args: &[Bytes], millis: bool) -> Outcome {
    let [key] = args else { return wrong_args(if millis { "pttl" } else { "ttl" }) };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let n = match db.ttl(key) {
        Ttl::NoKey => -2,
        Ttl::Persistent => -1,
        Ttl::Expires(d) => {
            if millis {
                d.as_millis() as i64
            } else {
                d.as_secs_f64().round() as i64
            }
        }
    };
    reply(Value::Integer(n))
}

fn cmd_expire(ctx: &ServerContext, state: &ClientState, args: &[Bytes], millis: bool) -> Outcome {
    let (key, amount, sliding) = match args {
        [key, amount] => (key, amount, false),
        [key, amount, flag] if flag.eq_ignore_ascii_case(b"SLIDING") => (key, amount, true),
        _ => return wrong_args(if millis { "pexpire" } else { "expire" }),
    };
    let n: i64 = match std::str::from_utf8(amount).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let dur =
        if millis { Duration::from_millis(n.max(0) as u64) } else { Duration::from_secs(n.max(0) as u64) };
    if n <= 0 {
        // Non-positive TTL deletes the key immediately, matching Redis.
        let deleted = db.del(std::slice::from_ref(key));
        on_key_changed(ctx, state.db_index, key.clone());
        if deleted > 0 {
            notify_keyspace_event(ctx, state.db_index, 'g', "del", key);
        }
        return reply(Value::Integer(if deleted > 0 { 1 } else { 0 }));
    }
    let changed =
        if sliding { db.expire_sliding(key, dur) } else { db.expire(key, Some(saturating_deadline(dur))) };
    if changed {
        on_key_changed(ctx, state.db_index, key.clone());
        notify_keyspace_event(ctx, state.db_index, 'g', "expire", key);
    }
    reply(Value::Integer(if changed { 1 } else { 0 }))
}

/// `EXPIREAT`/`PEXPIREAT key timestamp` — like `EXPIRE`/`PEXPIRE`, but
/// `timestamp` is an absolute Unix time (seconds or milliseconds) instead
/// of a relative duration. A timestamp in the past deletes the key
/// immediately, matching Redis.
fn cmd_expireat(ctx: &ServerContext, state: &ClientState, args: &[Bytes], millis: bool) -> Outcome {
    let [key, timestamp] = args else { return wrong_args(if millis { "pexpireat" } else { "expireat" }) };
    let ts: i64 = match std::str::from_utf8(timestamp).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let now = SystemTime::now();
    let target = if millis {
        UNIX_EPOCH.checked_add(Duration::from_millis(ts.max(0) as u64))
    } else {
        UNIX_EPOCH.checked_add(Duration::from_secs(ts.max(0) as u64))
    };
    let in_past = ts <= 0 || target.is_none() || target.is_some_and(|t| t <= now);
    if in_past {
        let deleted = db.del(std::slice::from_ref(key));
        on_key_changed(ctx, state.db_index, key.clone());
        if deleted > 0 {
            notify_keyspace_event(ctx, state.db_index, 'g', "del", key);
        }
        return reply(Value::Integer(if deleted > 0 { 1 } else { 0 }));
    }
    let dur = target.expect("checked above").duration_since(now).unwrap_or_default();
    let changed = db.expire(key, Some(saturating_deadline(dur)));
    if changed {
        on_key_changed(ctx, state.db_index, key.clone());
        notify_keyspace_event(ctx, state.db_index, 'g', "expire", key);
    }
    reply(Value::Integer(if changed { 1 } else { 0 }))
}

/// `RENAME`/`RENAMENX key newkey` — moves a key (value + TTL) to a new
/// name. `RENAME` overwrites `newkey` if it already exists; `RENAMENX`
/// refuses (replying `0`) instead.
fn cmd_rename(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    rename(ctx, state, args, false)
}

fn cmd_renamenx(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    rename(ctx, state, args, true)
}

fn rename(ctx: &ServerContext, state: &ClientState, args: &[Bytes], nx: bool) -> Outcome {
    let [key, newkey] = args else { return wrong_args(if nx { "renamenx" } else { "rename" }) };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get`: only existence matters for the RENAMENX
    // check, not a clone of `newkey`'s potentially huge List/Hash/Set/ZSet
    // value — and doing this before touching `key` at all skips cloning
    // `key`'s value too when RENAMENX is about to fail anyway.
    if nx && key != newkey && db.with_value(newkey, |_| ()).is_some() {
        return reply(Value::Integer(0));
    }
    let Some(value) = db.get(key) else {
        return err("ERR no such key");
    };
    let expires_at = db.current_expiry(key).flatten();
    db.set(newkey.clone(), value, expires_at);
    if key != newkey {
        db.del(std::slice::from_ref(key));
    }
    on_key_changed(ctx, state.db_index, key.clone());
    on_key_changed(ctx, state.db_index, newkey.clone());
    notify_keyspace_event(ctx, state.db_index, 'g', "rename_from", key);
    notify_keyspace_event(ctx, state.db_index, 'g', "rename_to", newkey);
    reply(if nx { Value::Integer(1) } else { Value::ok() })
}

fn cmd_persist(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("persist") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let changed = matches!(db.ttl(key), Ttl::Expires(_)) && db.expire(key, None);
    if changed {
        notify_keyspace_event(ctx, state.db_index, 'g', "persist", key);
        on_key_changed(ctx, state.db_index, key.clone());
    }
    reply(Value::Integer(if changed { 1 } else { 0 }))
}

fn cmd_type(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [key] = args else { return wrong_args("type") };
    let db = ctx.store.db(state.db_index).expect("valid db index");
    // `with_value`, not `get` — `type_name()` only needs a peek, not a
    // clone of a potentially huge List/Hash/Set/ZSet.
    match db.with_value(key, |v| v.type_name()) {
        Some(name) => reply(Value::Simple(name.to_string())),
        None => reply(Value::Simple("none".to_string())),
    }
}

/// `OBJECT ENCODING key` — reports a plausible encoding name for the given
/// key's type and size, mirroring the `listpack`/`quicklist`/`hashtable`/
/// `skiplist`/`intset` family Redis clients expect, using the same
/// small-collection thresholds documented in `docs/09-architecture.md`.
const ENCODING_THRESHOLD: usize = 128;

fn cmd_object(ctx: &ServerContext, state: &ClientState, args: &[Bytes]) -> Outcome {
    let [sub, key] = args else { return wrong_args("object") };
    if !sub.eq_ignore_ascii_case(b"ENCODING") {
        return err("ERR syntax error");
    }
    let db = ctx.store.db(state.db_index).expect("valid db index");
    let encoding = db.with_value(key, |v| match v {
        KVal::String(s) => {
            if std::str::from_utf8(s).ok().and_then(|x| x.parse::<i64>().ok()).is_some() {
                "int"
            } else if s.len() <= 44 {
                "embstr"
            } else {
                "raw"
            }
        }
        KVal::List(l) => {
            if l.len() <= ENCODING_THRESHOLD {
                "listpack"
            } else {
                "quicklist"
            }
        }
        KVal::Hash(h) => {
            if h.len() <= ENCODING_THRESHOLD {
                "listpack"
            } else {
                "hashtable"
            }
        }
        KVal::Set(s) => {
            if s.len() <= ENCODING_THRESHOLD {
                "listpack"
            } else {
                "hashtable"
            }
        }
        KVal::ZSet(z) => {
            if z.len() <= ENCODING_THRESHOLD {
                "listpack"
            } else {
                "skiplist"
            }
        }
        // Real Redis always reports `stream` here — there's no small/large
        // encoding split for streams the way there is for the other types.
        KVal::Stream(_) => "stream",
    });
    match encoding {
        Some(name) => reply(Value::bulk(name)),
        None => err("ERR no such key"),
    }
}

fn cmd_command(args: &[Bytes]) -> Outcome {
    let sub = args.first().map(|a| String::from_utf8_lossy(a).to_ascii_uppercase());
    match sub.as_deref() {
        Some("COUNT") => reply(Value::Integer(KNOWN_COMMANDS.len() as i64)),
        Some("DOCS") => reply(Value::Map(vec![])),
        Some("INFO") => {
            let items = args[1..]
                .iter()
                .map(|name| {
                    let known =
                        KNOWN_COMMANDS.iter().any(|c| c.eq_ignore_ascii_case(&String::from_utf8_lossy(name)));
                    if known {
                        Value::Array(vec![
                            Value::bulk(name.clone()),
                            Value::Integer(-1),
                            Value::Array(vec![]),
                        ])
                    } else {
                        Value::NullArray
                    }
                })
                .collect();
            reply(Value::Array(items))
        }
        _ => reply(Value::Array(
            KNOWN_COMMANDS
                .iter()
                .map(|name| Value::Array(vec![Value::bulk(*name), Value::Integer(-1), Value::Array(vec![])]))
                .collect(),
        )),
    }
}

const KNOWN_COMMANDS: &[&str] = &[
    "PING",
    "ECHO",
    "QUIT",
    "AUTH",
    "HELLO",
    "RESET",
    "SELECT",
    "DBSIZE",
    "FLUSHDB",
    "FLUSHALL",
    "SET",
    "GET",
    "APPEND",
    "INCR",
    "INCRBY",
    "DECR",
    "DECRBY",
    "SETNX",
    "MSET",
    "MGET",
    "STRLEN",
    "GETRANGE",
    "KEYS",
    "DEL",
    "UNLINK",
    "EXISTS",
    "TTL",
    "PTTL",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "PERSIST",
    "RENAME",
    "RENAMENX",
    "GETDEL",
    "INCRBYFLOAT",
    "GETAT",
    "HISTORY",
    "TYPE",
    "OBJECT",
    "COMMAND",
    "CLIENT",
    "SCAN",
    "LPUSH",
    "RPUSH",
    "LPUSHX",
    "RPUSHX",
    "LPOP",
    "RPOP",
    "LLEN",
    "LRANGE",
    "LTRIM",
    "LSET",
    "LINSERT",
    "LREM",
    "RPOPLPUSH",
    "LMOVE",
    "BLPOP",
    "BRPOP",
    "BLMOVE",
    "HSET",
    "HGET",
    "HDEL",
    "HEXISTS",
    "HLEN",
    "HGETALL",
    "HMGET",
    "HINCRBY",
    "HKEYS",
    "HVALS",
    "HSCAN",
    "SADD",
    "SREM",
    "SMEMBERS",
    "SISMEMBER",
    "SCARD",
    "SINTERCARD",
    "SPOP",
    "SRANDMEMBER",
    "SUNION",
    "SINTER",
    "SDIFF",
    "SSCAN",
    "ZADD",
    "ZSCORE",
    "ZREM",
    "ZCARD",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZRANK",
    "ZREVRANK",
    "ZCOUNT",
    "ZREMRANGEBYSCORE",
    "ZREMRANGEBYRANK",
    "ZSCAN",
    "ZPOPMIN",
    "ZPOPMAX",
    "BZPOPMIN",
    "BZPOPMAX",
    "XADD",
    "XLEN",
    "XRANGE",
    "XREVRANGE",
    "XDEL",
    "XTRIM",
    "XGROUP",
    "XREADGROUP",
    "XACK",
    "XPENDING",
    "XREAD",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "PUBLISH",
    "PUBSUB",
    "LIVE",
    "UNLIVE",
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    "EVAL",
    "EVAL_RO",
    "EVALSHA",
    "EVALSHA_RO",
    "SCRIPT",
    "SAVE",
    "BGSAVE",
    "LASTSAVE",
    "BGREWRITEAOF",
    "DUMP",
    "RESTORE",
    "SCHEDULE",
    "REPLICAOF",
    "SLAVEOF",
    "PSYNC",
    "REPLCONF",
    "WAIT",
    "WAITAOF",
    "INFO",
    "SLOWLOG",
    "LATENCY",
    "MEMORY",
    "ACL",
];

/// Commands that mutate the keyspace — used both by `EVAL_RO`/`EVALSHA_RO`
/// (to reject writes from scripts that promised to be read-only) and by
/// `dispatch()` itself (to decide what gets logged to the AOF and what
/// counts against `maxmemory`'s OOM check). `RESTORE` is here since it
/// creates a key; `SAVE`/`BGSAVE`/`DUMP` don't mutate the keyspace so they
/// aren't. The blocking commands (`BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/
/// `BZPOPMAX`) are deliberately **not** here even though they mutate the
/// keyspace: logging/replicating the raw blocking request would be wrong
/// (see `list::bpop`'s docs) — they log their own already-resolved
/// `LPOP`/`RPOP`/`LMOVE`/`ZPOPMIN`/`ZPOPMAX` effect by hand instead, and
/// apply the replica-read-only/`maxmemory` guards themselves via
/// `check_write_allowed` rather than through the `is_write` check below.
const WRITE_COMMAND_NAMES: &[&str] = &[
    "SET",
    "APPEND",
    "INCR",
    "INCRBY",
    "DECR",
    "DECRBY",
    "SETNX",
    "MSET",
    "DEL",
    "UNLINK",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "PERSIST",
    "RENAME",
    "RENAMENX",
    "GETDEL",
    "INCRBYFLOAT",
    "FLUSHDB",
    "FLUSHALL",
    "LPUSH",
    "RPUSH",
    "LPUSHX",
    "RPUSHX",
    "LPOP",
    "RPOP",
    "LTRIM",
    "LSET",
    "LINSERT",
    "LREM",
    "RPOPLPUSH",
    "LMOVE",
    "HSET",
    "HDEL",
    "HINCRBY",
    "SADD",
    "SREM",
    "SPOP",
    "ZADD",
    "ZINCRBY",
    "ZREM",
    "ZPOPMIN",
    "ZPOPMAX",
    "ZREMRANGEBYSCORE",
    "ZREMRANGEBYRANK",
    "RESTORE",
    "XADD",
    "XDEL",
    "XTRIM",
    "XGROUP",
    "XREADGROUP",
    "XACK",
];

/// `WRITE_COMMAND_NAMES`, but as a `HashSet` for O(1) membership checks —
/// `dispatch()` does this check on every single call, so a linear scan over
/// ~35 strings there is wasted work on every command, not just writes.
pub(crate) static WRITE_COMMANDS: std::sync::LazyLock<HashSet<&'static str>> =
    std::sync::LazyLock::new(|| WRITE_COMMAND_NAMES.iter().copied().collect());

#[cfg(test)]
mod tests {
    use super::*;

    fn req(words: &[&str]) -> Request {
        words.iter().map(|w| Bytes::copy_from_slice(w.as_bytes())).collect()
    }

    fn run(ctx: &ServerContext, state: &mut ClientState, words: &[&str]) -> Value {
        match dispatch(ctx, state, req(words)) {
            Outcome::Reply(v) => v,
            Outcome::CloseConnection(v) => v,
            Outcome::Multi(mut vs) => vs.pop().expect("at least one reply"),
            Outcome::SingleFlightWait(_) => panic!("test helper can't await a SINGLEFLIGHT wait"),
            Outcome::NoReply => panic!("test helper doesn't support NoReply (REPLCONF ACK)"),
            Outcome::WaitForReplicas { .. } => panic!("test helper can't await WAIT"),
            Outcome::BlockWait { on_timeout, .. } => on_timeout,
        }
    }

    fn fresh() -> (ServerContext, ClientState) {
        let ctx = ServerContext::new(16, None);
        let (id, _rx) = ctx.register_client();
        let state = ClientState::with_id(id, false);
        (ctx, state)
    }

    fn fresh_with_notify(spec: &str) -> (ServerContext, ClientState) {
        let ctx = ServerContext::new(16, None).with_notify_keyspace_events(NotifyFlags::parse(spec));
        let (id, _rx) = ctx.register_client();
        let state = ClientState::with_id(id, false);
        (ctx, state)
    }

    /// Also returns the temp dir the disk-backed history store was opened
    /// in, so the test can clean it up afterward. `fjall` holds an
    /// exclusive lock on its directory, so each call needs a directory of
    /// its own — tests using this helper run concurrently in the same
    /// process/pid.
    fn fresh_with_history() -> (ServerContext, ClientState, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("reflexdb-dispatch-history-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let writer = persistence::history::HistoryWriter::open(&dir).unwrap();
        let ctx = ServerContext::new(16, None).with_history(writer);
        let (id, _rx) = ctx.register_client();
        let state = ClientState::with_id(id, false);
        (ctx, state, dir)
    }

    #[test]
    fn keyspace_notifications_fire_on_matching_classes_only() {
        let (ctx, mut st) = fresh_with_notify("KE$l");
        let (sub_id, mut sub_rx) = ctx.register_client();
        ctx.channels.entry(Bytes::from_static(b"__keyevent@0__:set")).or_default().insert(sub_id);
        ctx.channels.entry(Bytes::from_static(b"__keyspace@0__:mylist")).or_default().insert(sub_id);
        // Not subscribed to hash events, so HSET (class 'h', not in "$l")
        // shouldn't publish anything even if it were subscribed.
        run(&ctx, &mut st, &["SET", "k", "v"]);
        run(&ctx, &mut st, &["RPUSH", "mylist", "a"]);
        run(&ctx, &mut st, &["HSET", "h", "f", "v"]);

        let mut got_keyevent_set = false;
        let mut got_keyspace_lpush = false;
        while let Ok(msg) = sub_rx.try_recv() {
            if let Value::Push(parts) = msg {
                if let [_, Value::Bulk(channel), Value::Bulk(payload)] = parts.as_slice() {
                    if channel.as_ref() == b"__keyevent@0__:set" && payload.as_ref() == b"k" {
                        got_keyevent_set = true;
                    }
                    if channel.as_ref() == b"__keyspace@0__:mylist" && payload.as_ref() == b"rpush" {
                        got_keyspace_lpush = true;
                    }
                }
            }
        }
        assert!(got_keyevent_set, "expected a __keyevent@0__:set notification");
        assert!(got_keyspace_lpush, "expected a __keyspace@0__:mylist notification");
    }

    #[test]
    fn keyspace_notifications_disabled_by_default() {
        let (ctx, mut st) = fresh();
        let (sub_id, mut sub_rx) = ctx.register_client();
        ctx.channels.entry(Bytes::from_static(b"__keyevent@0__:set")).or_default().insert(sub_id);
        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert!(sub_rx.try_recv().is_err());
    }

    #[test]
    fn blpop_pops_immediately_when_a_key_has_data() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["RPUSH", "l2", "x"]);
        assert_eq!(
            run(&ctx, &mut st, &["BLPOP", "l1", "l2", "0"]),
            Value::Array(vec![Value::bulk(&b"l2"[..]), Value::bulk(&b"x"[..])])
        );
    }

    #[test]
    fn blpop_blocks_when_no_key_has_data_and_times_out() {
        let (ctx, mut st) = fresh();
        match dispatch(&ctx, &mut st, req(&["BLPOP", "missing", "0.05"])) {
            Outcome::BlockWait { deadline, on_timeout } => {
                assert!(deadline.is_some());
                assert_eq!(on_timeout, Value::NullArray);
            }
            _ => panic!("expected BlockWait"),
        }
        // A fresh BLPOP after RPUSH'ing resolves immediately, same as above.
        run(&ctx, &mut st, &["RPUSH", "missing", "v"]);
        assert_eq!(
            run(&ctx, &mut st, &["BLPOP", "missing", "0"]),
            Value::Array(vec![Value::bulk(&b"missing"[..]), Value::bulk(&b"v"[..])])
        );
    }

    #[test]
    fn blpop_timeout_zero_blocks_forever_deadline_is_none() {
        let (ctx, mut st) = fresh();
        match dispatch(&ctx, &mut st, req(&["BLPOP", "missing", "0"])) {
            Outcome::BlockWait { deadline, .. } => assert!(deadline.is_none()),
            _ => panic!("expected BlockWait"),
        }
    }

    #[test]
    fn bzpopmin_and_zpopmin_pop_the_lowest_score() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["ZADD", "z", "3", "c", "1", "a", "2", "b"]);
        assert_eq!(
            run(&ctx, &mut st, &["ZPOPMIN", "z"]),
            Value::Array(vec![Value::bulk(&b"a"[..]), Value::bulk(&b"1"[..])])
        );
        assert_eq!(
            run(&ctx, &mut st, &["BZPOPMIN", "other", "z", "0"]),
            Value::Array(vec![Value::bulk(&b"z"[..]), Value::bulk(&b"b"[..]), Value::bulk(&b"2"[..])])
        );
    }

    #[test]
    fn zpopmax_pops_the_highest_score() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["ZADD", "z", "1", "a", "3", "c", "2", "b"]);
        assert_eq!(
            run(&ctx, &mut st, &["ZPOPMAX", "z"]),
            Value::Array(vec![Value::bulk(&b"c"[..]), Value::bulk(&b"3"[..])])
        );
    }

    #[test]
    fn blmove_moves_immediately_when_source_has_data() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["RPUSH", "src", "a", "b"]);
        assert_eq!(
            run(&ctx, &mut st, &["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0"]),
            Value::bulk(&b"a"[..])
        );
        assert_eq!(
            run(&ctx, &mut st, &["LRANGE", "dst", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"a"[..])])
        );
    }

    #[test]
    fn blocking_commands_are_rejected_on_a_read_only_replica() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["REPLICAOF", "127.0.0.1", "9999"]);
        assert_eq!(
            run(&ctx, &mut st, &["BLPOP", "k", "0"]),
            Value::error("READONLY You can't write against a read only replica.")
        );
    }

    #[test]
    fn client_id_getname_setname_round_trip() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["CLIENT", "ID"]), Value::Integer(st.id as i64));
        assert_eq!(run(&ctx, &mut st, &["CLIENT", "GETNAME"]), Value::bulk(&b""[..]));
        assert_eq!(run(&ctx, &mut st, &["CLIENT", "SETNAME", "my-conn"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["CLIENT", "GETNAME"]), Value::bulk(&b"my-conn"[..]));
        assert_eq!(
            run(&ctx, &mut st, &["CLIENT", "SETNAME", "bad name"]),
            Value::error("ERR Client names cannot contain spaces, newlines or special characters.")
        );
    }

    #[test]
    fn client_tracking_requires_resp3() {
        let (ctx, mut st) = fresh();
        assert_eq!(
            run(&ctx, &mut st, &["CLIENT", "TRACKING", "ON"]),
            Value::error("ERR Client tracking is only supported in RESP3 mode (see `HELLO 3`)")
        );
        run(&ctx, &mut st, &["HELLO", "3"]);
        assert_eq!(run(&ctx, &mut st, &["CLIENT", "TRACKING", "ON"]), Value::ok());
        assert!(st.tracking);
        assert_eq!(run(&ctx, &mut st, &["CLIENT", "TRACKING", "OFF"]), Value::ok());
        assert!(!st.tracking);
    }

    #[test]
    fn client_tracking_pushes_invalidation_on_write() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["HELLO", "3"]);
        run(&ctx, &mut st, &["CLIENT", "TRACKING", "ON"]);
        run(&ctx, &mut st, &["SET", "k", "v1"]);
        run(&ctx, &mut st, &["GET", "k"]);
        assert!(ctx.tracking_table.contains_key(&(0, Bytes::from_static(b"k"))));
        run(&ctx, &mut st, &["SET", "k", "v2"]);
        assert!(!ctx.tracking_table.contains_key(&(0, Bytes::from_static(b"k"))));
    }

    #[test]
    fn list_push_pop_range() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["RPUSH", "l", "a", "b", "c"]), Value::Integer(3));
        assert_eq!(
            run(&ctx, &mut st, &["LRANGE", "l", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"a"[..]), Value::bulk(&b"b"[..]), Value::bulk(&b"c"[..])])
        );
        assert_eq!(run(&ctx, &mut st, &["LPOP", "l"]), Value::bulk(&b"a"[..]));
        assert_eq!(run(&ctx, &mut st, &["LLEN", "l"]), Value::Integer(2));
    }

    #[test]
    fn list_linsert_lrem_lset_ltrim() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["RPUSH", "l", "a", "b", "c"]);
        // LINSERT b before -> a, X, b, c
        assert_eq!(run(&ctx, &mut st, &["LINSERT", "l", "BEFORE", "b", "x"]), Value::Integer(4));
        assert_eq!(
            run(&ctx, &mut st, &["LRANGE", "l", "0", "-1"]),
            Value::Array(vec![
                Value::bulk(&b"a"[..]),
                Value::bulk(&b"x"[..]),
                Value::bulk(&b"b"[..]),
                Value::bulk(&b"c"[..]),
            ])
        );
        assert_eq!(run(&ctx, &mut st, &["LINSERT", "l", "AFTER", "missing", "y"]), Value::Integer(-1));

        // LSET index 0 -> "z"
        assert_eq!(run(&ctx, &mut st, &["LSET", "l", "0", "z"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["LSET", "l", "99", "z"]), Value::error("ERR index out of range"));

        // LREM removes "x"
        assert_eq!(run(&ctx, &mut st, &["LREM", "l", "0", "x"]), Value::Integer(1));
        assert_eq!(
            run(&ctx, &mut st, &["LRANGE", "l", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"z"[..]), Value::bulk(&b"b"[..]), Value::bulk(&b"c"[..])])
        );

        // LTRIM to just the middle element.
        assert_eq!(run(&ctx, &mut st, &["LTRIM", "l", "1", "1"]), Value::ok());
        assert_eq!(
            run(&ctx, &mut st, &["LRANGE", "l", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"b"[..])])
        );
    }

    #[test]
    fn lmove_and_rpoplpush() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["RPUSH", "src", "a", "b", "c"]);
        assert_eq!(run(&ctx, &mut st, &["RPOPLPUSH", "src", "dst"]), Value::bulk(&b"c"[..]));
        assert_eq!(
            run(&ctx, &mut st, &["LRANGE", "dst", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"c"[..])])
        );
        assert_eq!(run(&ctx, &mut st, &["LMOVE", "src", "src", "LEFT", "RIGHT"]), Value::bulk(&b"a"[..]));
        assert_eq!(
            run(&ctx, &mut st, &["LRANGE", "src", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"b"[..]), Value::bulk(&b"a"[..])])
        );
    }

    #[test]
    fn scan_filters_by_pattern_and_type() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "key:1", "v"]);
        run(&ctx, &mut st, &["SET", "key:2", "v"]);
        run(&ctx, &mut st, &["RPUSH", "other", "v"]);
        let Value::Array(reply) = run(&ctx, &mut st, &["SCAN", "0", "MATCH", "key:*"]) else {
            panic!("expected [cursor, items]")
        };
        let Value::Array(items) = &reply[1] else { panic!("expected items array") };
        assert_eq!(items.len(), 2);

        let Value::Array(reply) = run(&ctx, &mut st, &["SCAN", "0", "TYPE", "list"]) else {
            panic!("expected [cursor, items]")
        };
        assert_eq!(reply[1], Value::Array(vec![Value::bulk(&b"other"[..])]));
    }

    #[test]
    fn waitaof_reports_local_without_blocking_when_aof_is_off() {
        let (ctx, mut st) = fresh();
        assert_eq!(
            run(&ctx, &mut st, &["WAITAOF", "0", "0", "100"]),
            Value::Array(vec![Value::Integer(0), Value::Integer(0)])
        );
        assert_eq!(
            run(&ctx, &mut st, &["WAITAOF", "1", "0", "100"]),
            Value::error("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.")
        );
    }

    /// `AofWriter::append`/`fsync` are fire-and-forget (see
    /// `persistence::aof`'s module docs) — `WAITAOF numlocal>=1` has to do
    /// a real round trip to the writer thread to earn the `1` it reports,
    /// not just check whether AOF is turned on. This forces a write right
    /// before `WAITAOF` and confirms the reply reflects a real flush
    /// (rather than racing the fire-and-forget writer thread).
    #[test]
    fn waitaof_actually_flushes_the_aof_before_reporting_local_acked() {
        let path =
            std::env::temp_dir().join(format!("reflexdb-dispatch-waitaof-test-{}.aof", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let writer = persistence::aof::AofWriter::open(&path, persistence::aof::Fsync::EverySec).unwrap();
        let ctx = ServerContext::new(16, None).with_aof(writer, path.clone());
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);

        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert_eq!(
            run(&ctx, &mut st, &["WAITAOF", "1", "0", "100"]),
            Value::Array(vec![Value::Integer(1), Value::Integer(0)])
        );
        assert_eq!(persistence::aof::replay(&path).unwrap().len(), 2); // SELECT + SET

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn list_wrong_type_against_string() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "s", "v"]);
        assert_eq!(
            run(&ctx, &mut st, &["LPUSH", "s", "x"]),
            Value::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }

    #[test]
    fn hash_set_get_del() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["HSET", "h", "f1", "v1", "f2", "v2"]), Value::Integer(2));
        assert_eq!(run(&ctx, &mut st, &["HGET", "h", "f1"]), Value::bulk(&b"v1"[..]));
        assert_eq!(run(&ctx, &mut st, &["HLEN", "h"]), Value::Integer(2));
        assert_eq!(run(&ctx, &mut st, &["HDEL", "h", "f1", "missing"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["HEXISTS", "h", "f1"]), Value::Integer(0));
    }

    #[test]
    fn string_counters_append_and_ranges() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["SET", "s", "foo"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["APPEND", "s", "bar"]), Value::Integer(6));
        assert_eq!(run(&ctx, &mut st, &["GET", "s"]), Value::bulk(&b"foobar"[..]));
        assert_eq!(run(&ctx, &mut st, &["STRLEN", "s"]), Value::Integer(6));
        assert_eq!(run(&ctx, &mut st, &["GETRANGE", "s", "0", "2"]), Value::bulk(&b"foo"[..]));
        assert_eq!(run(&ctx, &mut st, &["GETRANGE", "s", "-3", "-1"]), Value::bulk(&b"bar"[..]));

        assert_eq!(run(&ctx, &mut st, &["SETNX", "c", "10"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["SETNX", "c", "99"]), Value::Integer(0));
        assert_eq!(run(&ctx, &mut st, &["INCR", "c"]), Value::Integer(11));
        assert_eq!(run(&ctx, &mut st, &["INCRBY", "c", "5"]), Value::Integer(16));
        assert_eq!(run(&ctx, &mut st, &["DECR", "c"]), Value::Integer(15));
        assert_eq!(run(&ctx, &mut st, &["DECRBY", "c", "5"]), Value::Integer(10));
        assert_eq!(
            run(&ctx, &mut st, &["INCR", "s"]),
            Value::error("ERR value is not an integer or out of range")
        );

        assert_eq!(run(&ctx, &mut st, &["MSET", "k1", "v1", "k2", "v2"]), Value::ok());
        assert_eq!(
            run(&ctx, &mut st, &["MGET", "k1", "k2", "missing"]),
            Value::Array(vec![Value::bulk(&b"v1"[..]), Value::bulk(&b"v2"[..]), Value::NullBulk])
        );

        let Value::Array(keys) = run(&ctx, &mut st, &["KEYS", "k*"]) else { panic!("expected array") };
        let mut keys: Vec<String> = keys
            .into_iter()
            .map(|v| match v {
                Value::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
                other => panic!("expected bulk string, got {other:?}"),
            })
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["k1", "k2"]);

        assert_eq!(run(&ctx, &mut st, &["FLUSHALL"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["DBSIZE"]), Value::Integer(0));
    }

    #[test]
    fn zincrby_creates_and_increments_a_member_score() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["ZINCRBY", "z", "5", "a"]), Value::bulk(&b"5"[..]));
        assert_eq!(run(&ctx, &mut st, &["ZINCRBY", "z", "2.5", "a"]), Value::bulk(&b"7.5"[..]));
        assert_eq!(run(&ctx, &mut st, &["ZSCORE", "z", "a"]), Value::bulk(&b"7.5"[..]));
    }

    #[test]
    fn set_add_members_intersect_card() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["SADD", "s1", "a", "b", "c"]), Value::Integer(3));
        assert_eq!(run(&ctx, &mut st, &["SADD", "s2", "b", "c", "d"]), Value::Integer(3));
        assert_eq!(run(&ctx, &mut st, &["SINTERCARD", "2", "s1", "s2"]), Value::Integer(2));
        assert_eq!(run(&ctx, &mut st, &["SISMEMBER", "s1", "a"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["SCARD", "s1"]), Value::Integer(3));
    }

    #[test]
    fn zset_add_score_range() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]), Value::Integer(3));
        assert_eq!(run(&ctx, &mut st, &["ZSCORE", "z", "b"]), Value::bulk(&b"2"[..]));
        assert_eq!(
            run(&ctx, &mut st, &["ZRANGE", "z", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"a"[..]), Value::bulk(&b"b"[..]), Value::bulk(&b"c"[..])])
        );
        assert_eq!(
            run(&ctx, &mut st, &["ZRANGEBYSCORE", "z", "2", "+inf"]),
            Value::Array(vec![Value::bulk(&b"b"[..]), Value::bulk(&b"c"[..])])
        );
        assert_eq!(run(&ctx, &mut st, &["ZCARD", "z"]), Value::Integer(3));
    }

    #[test]
    fn zset_rank_and_remove_ranges() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]);
        assert_eq!(run(&ctx, &mut st, &["ZRANK", "z", "b"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["ZREVRANK", "z", "b"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["ZRANK", "z", "missing"]), Value::NullBulk);
        assert_eq!(
            run(&ctx, &mut st, &["ZREVRANGE", "z", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"c"[..]), Value::bulk(&b"b"[..]), Value::bulk(&b"a"[..])])
        );
        assert_eq!(run(&ctx, &mut st, &["ZCOUNT", "z", "2", "+inf"]), Value::Integer(2));
        assert_eq!(run(&ctx, &mut st, &["ZREMRANGEBYSCORE", "z", "-inf", "1"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["ZREMRANGEBYRANK", "z", "0", "0"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["ZCARD", "z"]), Value::Integer(1));
    }

    #[test]
    fn xadd_xlen_xrange_round_trip() {
        let (ctx, mut st) = fresh();
        let id1 = match run(&ctx, &mut st, &["XADD", "s", "1-1", "field", "a"]) {
            Value::Bulk(b) => b,
            other => panic!("expected a bulk ID, got {other:?}"),
        };
        assert_eq!(id1.as_ref(), b"1-1");
        run(&ctx, &mut st, &["XADD", "s", "1-2", "field", "b"]);
        assert_eq!(run(&ctx, &mut st, &["XLEN", "s"]), Value::Integer(2));

        // An ID that doesn't advance the stream is rejected.
        match run(&ctx, &mut st, &["XADD", "s", "1-1", "field", "c"]) {
            Value::Error(_) => {}
            other => panic!("expected an error, got {other:?}"),
        }

        let range = run(&ctx, &mut st, &["XRANGE", "s", "-", "+"]);
        assert_eq!(
            range,
            Value::Array(vec![
                Value::Array(vec![
                    Value::bulk(&b"1-1"[..]),
                    Value::Array(vec![Value::bulk(&b"field"[..]), Value::bulk(&b"a"[..])])
                ]),
                Value::Array(vec![
                    Value::bulk(&b"1-2"[..]),
                    Value::Array(vec![Value::bulk(&b"field"[..]), Value::bulk(&b"b"[..])])
                ]),
            ])
        );

        assert_eq!(run(&ctx, &mut st, &["XDEL", "s", "1-1"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["XLEN", "s"]), Value::Integer(1));
    }

    #[test]
    fn xadd_nomkstream_and_maxlen() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["XADD", "nope", "NOMKSTREAM", "*", "f", "v"]), Value::NullBulk);
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "nope"]), Value::Integer(0));

        for i in 1..=5u64 {
            run(&ctx, &mut st, &["XADD", "s", "MAXLEN", "3", &format!("{i}-0"), "f", "v"]);
        }
        assert_eq!(run(&ctx, &mut st, &["XLEN", "s"]), Value::Integer(3));
    }

    #[test]
    fn xadd_against_wrong_type_key_errors() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "v"]);
        match run(&ctx, &mut st, &["XADD", "k", "*", "f", "v"]) {
            Value::Error(e) => assert!(e.starts_with("WRONGTYPE")),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
    }

    #[test]
    fn xread_sees_entries_after_the_given_id_only() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["XADD", "s", "1-1", "f", "a"]);
        run(&ctx, &mut st, &["XADD", "s", "2-1", "f", "b"]);

        let result = run(&ctx, &mut st, &["XREAD", "STREAMS", "s", "1-1"]);
        match result {
            Value::Array(streams) => {
                assert_eq!(streams.len(), 1);
                match &streams[0] {
                    Value::Array(pair) => match &pair[1] {
                        Value::Array(entries) => assert_eq!(entries.len(), 1), // only 2-1
                        other => panic!("expected entries array, got {other:?}"),
                    },
                    other => panic!("expected [name, entries], got {other:?}"),
                }
            }
            other => panic!("expected an array, got {other:?}"),
        }

        // Nothing newer than the last entry.
        assert_eq!(run(&ctx, &mut st, &["XREAD", "STREAMS", "s", "2-1"]), Value::NullArray);
        // `$` in a non-blocking read also means "nothing yet".
        assert_eq!(run(&ctx, &mut st, &["XREAD", "STREAMS", "s", "$"]), Value::NullArray);
    }

    #[test]
    fn xgroup_readgroup_ack_and_pending_round_trip() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["XADD", "s", "1-1", "f", "a"]);
        run(&ctx, &mut st, &["XADD", "s", "2-1", "f", "b"]);
        assert_eq!(run(&ctx, &mut st, &["XGROUP", "CREATE", "s", "g", "0"]), Value::ok());
        // Duplicate group name.
        match run(&ctx, &mut st, &["XGROUP", "CREATE", "s", "g", "0"]) {
            Value::Error(e) => assert!(e.starts_with("BUSYGROUP")),
            other => panic!("expected BUSYGROUP, got {other:?}"),
        }

        let delivered = run(&ctx, &mut st, &["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"]);
        match delivered {
            Value::Array(streams) => match &streams[0] {
                Value::Array(pair) => match &pair[1] {
                    Value::Array(entries) => assert_eq!(entries.len(), 2),
                    other => panic!("expected entries array, got {other:?}"),
                },
                other => panic!("expected [name, entries], got {other:?}"),
            },
            other => panic!("expected an array, got {other:?}"),
        }

        // A second consumer asking for `>` sees nothing new.
        assert_eq!(
            run(&ctx, &mut st, &["XREADGROUP", "GROUP", "g", "c2", "STREAMS", "s", ">"]),
            Value::NullArray
        );

        // Summary form: both entries still pending, owned by c1.
        match run(&ctx, &mut st, &["XPENDING", "s", "g"]) {
            Value::Array(items) => {
                assert_eq!(items[0], Value::Integer(2));
                assert_eq!(items[1], Value::bulk(&b"1-1"[..]));
                assert_eq!(items[2], Value::bulk(&b"2-1"[..]));
            }
            other => panic!("expected an array, got {other:?}"),
        }

        assert_eq!(run(&ctx, &mut st, &["XACK", "s", "g", "1-1"]), Value::Integer(1));
        match run(&ctx, &mut st, &["XPENDING", "s", "g"]) {
            Value::Array(items) => assert_eq!(items[0], Value::Integer(1)),
            other => panic!("expected an array, got {other:?}"),
        }

        assert_eq!(run(&ctx, &mut st, &["XGROUP", "DESTROY", "s", "g"]), Value::Integer(1));
        match run(&ctx, &mut st, &["XPENDING", "s", "g"]) {
            Value::Error(e) => assert!(e.starts_with("NOGROUP")),
            other => panic!("expected NOGROUP, got {other:?}"),
        }
    }

    #[test]
    fn rename_and_renamenx() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k1", "v1"]);
        assert_eq!(run(&ctx, &mut st, &["RENAME", "k1", "k2"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["GET", "k2"]), Value::bulk(&b"v1"[..]));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "k1"]), Value::Integer(0));
        assert_eq!(run(&ctx, &mut st, &["RENAME", "missing", "k3"]), Value::Error("ERR no such key".into()));

        run(&ctx, &mut st, &["SET", "k3", "v3"]);
        assert_eq!(run(&ctx, &mut st, &["RENAMENX", "k2", "k3"]), Value::Integer(0));
        assert_eq!(run(&ctx, &mut st, &["RENAMENX", "k2", "k4"]), Value::Integer(1));
    }

    #[test]
    fn expireat_in_the_past_deletes_immediately() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert_eq!(run(&ctx, &mut st, &["EXPIREAT", "k", "1"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "k"]), Value::Integer(0));
    }

    #[test]
    fn getdel_and_incrbyfloat() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert_eq!(run(&ctx, &mut st, &["GETDEL", "k"]), Value::bulk(&b"v"[..]));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "k"]), Value::Integer(0));
        assert_eq!(run(&ctx, &mut st, &["GETDEL", "k"]), Value::NullBulk);

        run(&ctx, &mut st, &["SET", "n", "10.5"]);
        assert_eq!(run(&ctx, &mut st, &["INCRBYFLOAT", "n", "0.1"]), Value::bulk(&b"10.6"[..]));
    }

    #[test]
    fn getat_and_history_are_errors_when_disabled() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "v"]);
        match run(&ctx, &mut st, &["GETAT", "k", "1"]) {
            Value::Error(_) => {}
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[test]
    fn getat_and_history_track_string_versions() {
        let (ctx, mut st, dir) = fresh_with_history();
        run(&ctx, &mut st, &["SET", "k", "v1"]);
        // `record_history` hands versions to the disk writer thread
        // fire-and-forget (see `persistence::history`'s docs); block until
        // they've actually landed instead of sleeping/polling for it.
        ctx.history_flush();
        // Millisecond-resolution timestamps can land in the same tick as a
        // write right next to them under CI/scheduling jitter — sleep
        // comfortably past that on both sides of the point we query, so
        // the test isn't flaky about which write "ts_after_v1" lands after.
        std::thread::sleep(std::time::Duration::from_millis(20));
        let ts_after_v1 = now_ms();
        std::thread::sleep(std::time::Duration::from_millis(20));
        run(&ctx, &mut st, &["SET", "k", "v2"]);
        ctx.history_flush();
        std::thread::sleep(std::time::Duration::from_millis(20));
        run(&ctx, &mut st, &["GETDEL", "k"]);
        ctx.history_flush();

        // As of right after the first write, GETAT should still see "v1".
        assert_eq!(run(&ctx, &mut st, &["GETAT", "k", &ts_after_v1.to_string()]), Value::bulk(&b"v1"[..]));
        // As of now (after the GETDEL), the key is gone.
        assert_eq!(run(&ctx, &mut st, &["GETAT", "k", &now_ms().to_string()]), Value::NullBulk);

        match run(&ctx, &mut st, &["HISTORY", "k"]) {
            Value::Array(versions) => assert_eq!(versions.len(), 3), // v1, v2, deleted
            other => panic!("expected an array, got {other:?}"),
        }
        // Newest-first, capped by LIMIT: the most recent version is the
        // GETDEL's delete, i.e. a nil value.
        match run(&ctx, &mut st, &["HISTORY", "k", "LIMIT", "1"]) {
            Value::Array(versions) => match versions.as_slice() {
                [Value::Array(entry)] => assert_eq!(entry.get(1), Some(&Value::NullBulk)),
                other => panic!("expected exactly one version, got {other:?}"),
            },
            other => panic!("expected an array, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The bug the old in-RAM-forever design had: a key's history used to
    /// survive the key's own deletion with no way to reclaim it. The
    /// disk-backed store doesn't special-case `DEL`/expiry at all — ageing
    /// out is purely time-based (`history_sweep`) — so this just confirms
    /// a swept-away version is actually gone, not that deletion triggers
    /// anything on its own.
    #[test]
    fn history_sweep_reclaims_old_versions_regardless_of_live_key_state() {
        let (ctx, mut st, dir) = fresh_with_history();
        run(&ctx, &mut st, &["SET", "k", "v1"]);
        ctx.history_flush();
        run(&ctx, &mut st, &["DEL", "k"]);
        ctx.history_flush();

        let removed = ctx.history_sweep(now_ms() as u64 + 1);
        assert!(removed >= 1, "expected at least the v1 version to be swept, removed {removed}");
        assert_eq!(run(&ctx, &mut st, &["HISTORY", "k"]), Value::Array(vec![]));

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn now_ms() -> u128 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
    }

    #[test]
    fn hash_bulk_read_and_incr() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["HSET", "h", "a", "1", "b", "2"]);
        assert_eq!(
            run(&ctx, &mut st, &["HMGET", "h", "a", "missing", "b"]),
            Value::Array(vec![Value::bulk(&b"1"[..]), Value::NullBulk, Value::bulk(&b"2"[..])])
        );
        assert_eq!(run(&ctx, &mut st, &["HINCRBY", "h", "a", "4"]), Value::Integer(5));
        // `Hash` is a plain `HashMap`, so `HKEYS`' order isn't guaranteed —
        // compare as a set, not a specific sequence.
        let keys = match run(&ctx, &mut st, &["HKEYS", "h"]) {
            Value::Array(items) => {
                let mut items: Vec<Bytes> = items
                    .into_iter()
                    .map(|v| if let Value::Bulk(b) = v { b } else { panic!("not bulk") })
                    .collect();
                items.sort();
                items
            }
            other => panic!("expected an array, got {other:?}"),
        };
        assert_eq!(keys, vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
    }

    #[test]
    fn set_algebra_and_random_pick() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SADD", "s1", "a", "b"]);
        run(&ctx, &mut st, &["SADD", "s2", "b", "c"]);
        let union = match run(&ctx, &mut st, &["SUNION", "s1", "s2"]) {
            Value::Set(v) => v.len(),
            other => panic!("expected a set, got {other:?}"),
        };
        assert_eq!(union, 3);
        assert_eq!(run(&ctx, &mut st, &["SINTER", "s1", "s2"]), Value::Set(vec![Value::bulk(&b"b"[..])]));
        assert_eq!(run(&ctx, &mut st, &["SDIFF", "s1", "s2"]), Value::Set(vec![Value::bulk(&b"a"[..])]));
        match run(&ctx, &mut st, &["SRANDMEMBER", "s1"]) {
            Value::Bulk(b) => assert!(b.as_ref() == b"a" || b.as_ref() == b"b"),
            other => panic!("expected a bulk string, got {other:?}"),
        }
        match run(&ctx, &mut st, &["SPOP", "s1", "2"]) {
            Value::Set(v) => assert_eq!(v.len(), 2),
            other => panic!("expected a set, got {other:?}"),
        }
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "s1"]), Value::Integer(0));
    }

    #[test]
    fn list_pushx_is_a_noop_on_a_missing_key() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["LPUSHX", "missing", "x"]), Value::Integer(0));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "missing"]), Value::Integer(0));

        run(&ctx, &mut st, &["RPUSH", "l", "a"]);
        assert_eq!(run(&ctx, &mut st, &["RPUSHX", "l", "b"]), Value::Integer(2));
    }

    #[test]
    fn collections_delete_key_once_emptied() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SADD", "s", "only"]);
        assert_eq!(run(&ctx, &mut st, &["SREM", "s", "only"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "s"]), Value::Integer(0));
    }

    #[test]
    fn object_encoding_reports_plausible_names() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "n", "123"]);
        assert_eq!(run(&ctx, &mut st, &["OBJECT", "ENCODING", "n"]), Value::bulk(&b"int"[..]));
        run(&ctx, &mut st, &["RPUSH", "l", "x"]);
        assert_eq!(run(&ctx, &mut st, &["OBJECT", "ENCODING", "l"]), Value::bulk(&b"listpack"[..]));
    }

    #[test]
    fn publish_delivers_to_subscriber() {
        let ctx = ServerContext::new(16, None);
        let (sub_id, mut rx) = ctx.register_client();
        let mut sub = ClientState::with_id(sub_id, false);
        let (pub_id, _pub_rx) = ctx.register_client();
        let mut publisher = ClientState::with_id(pub_id, false);

        match dispatch(&ctx, &mut sub, req(&["SUBSCRIBE", "news"])) {
            Outcome::Multi(vs) => assert_eq!(vs.len(), 1),
            _ => panic!("expected Outcome::Multi from SUBSCRIBE"),
        }
        assert_eq!(sub.subscriptions, 1);

        assert_eq!(run(&ctx, &mut publisher, &["PUBLISH", "news", "hello"]), Value::Integer(1));

        let pushed = rx.try_recv().expect("subscriber should have received a push");
        assert_eq!(
            pushed,
            Value::Push(vec![
                Value::bulk(&b"message"[..]),
                Value::bulk(&b"news"[..]),
                Value::bulk(&b"hello"[..]),
            ])
        );
    }

    #[test]
    fn psubscribe_matches_glob_pattern() {
        let ctx = ServerContext::new(16, None);
        let (sub_id, mut rx) = ctx.register_client();
        let mut sub = ClientState::with_id(sub_id, false);
        dispatch(&ctx, &mut sub, req(&["PSUBSCRIBE", "news.*"]));

        let (pub_id, _) = ctx.register_client();
        let mut publisher = ClientState::with_id(pub_id, false);
        run(&ctx, &mut publisher, &["PUBLISH", "news.sports", "goal"]);

        let pushed = rx.try_recv().expect("pattern subscriber should receive a push");
        assert_eq!(
            pushed,
            Value::Push(vec![
                Value::bulk(&b"pmessage"[..]),
                Value::bulk(&b"news.*"[..]),
                Value::bulk(&b"news.sports"[..]),
                Value::bulk(&b"goal"[..]),
            ])
        );
    }

    #[test]
    fn live_pushes_updated_result_on_write() {
        let ctx = ServerContext::new(16, None);
        let (watcher_id, mut rx) = ctx.register_client();
        let mut watcher = ClientState::with_id(watcher_id, false);

        let initial = run(&ctx, &mut watcher, &["LIVE", "GET", "user:name"]);
        let fingerprint_hex = match &initial {
            Value::Array(items) => match &items[2] {
                Value::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
                _ => panic!("expected bulk fingerprint"),
            },
            _ => panic!("expected array reply"),
        };
        assert_eq!(
            initial,
            Value::Array(vec![
                Value::bulk(&b"live"[..]),
                Value::bulk(&b"fingerprint"[..]),
                Value::bulk(fingerprint_hex.clone()),
                Value::bulk(&b"result"[..]),
                Value::NullBulk,
            ])
        );

        let (writer_id, _) = ctx.register_client();
        let mut writer = ClientState::with_id(writer_id, false);
        run(&ctx, &mut writer, &["SET", "user:name", "Alice"]);

        let pushed = rx.try_recv().expect("watcher should receive a LIVE push after the write");
        assert_eq!(
            pushed,
            Value::Array(vec![
                Value::bulk(&b"live"[..]),
                Value::bulk(&b"fingerprint"[..]),
                Value::bulk(fingerprint_hex),
                Value::bulk(&b"result"[..]),
                Value::bulk(&b"Alice"[..]),
            ])
        );
    }

    #[test]
    fn unlive_stops_further_pushes() {
        let ctx = ServerContext::new(16, None);
        let (watcher_id, mut rx) = ctx.register_client();
        let mut watcher = ClientState::with_id(watcher_id, false);

        let initial = run(&ctx, &mut watcher, &["LIVE", "GET", "k"]);
        let fp_hex = match &initial {
            Value::Array(items) => match &items[2] {
                Value::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
                _ => panic!("expected bulk fingerprint"),
            },
            _ => panic!("expected array reply"),
        };

        assert_eq!(run(&ctx, &mut watcher, &["UNLIVE", &fp_hex]), Value::Integer(1));

        let (writer_id, _) = ctx.register_client();
        let mut writer = ClientState::with_id(writer_id, false);
        run(&ctx, &mut writer, &["SET", "k", "v"]);

        assert!(rx.try_recv().is_err(), "should not receive a push after UNLIVE");
    }

    #[test]
    fn live_rejects_write_commands_as_handlers() {
        let ctx = ServerContext::new(16, None);
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        let result = run(&ctx, &mut st, &["LIVE", "SET", "k", "v"]);
        assert!(matches!(result, Value::Error(_)));
    }

    #[test]
    fn multi_exec_basic_round_trip() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["MULTI"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["SET", "a", "1"]), Value::Simple("QUEUED".into()));
        assert_eq!(run(&ctx, &mut st, &["GET", "a"]), Value::Simple("QUEUED".into()));
        assert!(st.in_multi); // still inside MULTI
        let result = run(&ctx, &mut st, &["EXEC"]);
        assert_eq!(result, Value::Array(vec![Value::ok(), Value::bulk(&b"1"[..])]));
        assert!(!st.in_multi);
    }

    #[test]
    fn discard_drops_queued_commands() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["MULTI"]);
        run(&ctx, &mut st, &["SET", "a", "1"]);
        assert_eq!(run(&ctx, &mut st, &["DISCARD"]), Value::ok());
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "a"]), Value::Integer(0));
    }

    #[test]
    fn unknown_command_in_multi_aborts_exec() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["MULTI"]);
        assert!(matches!(run(&ctx, &mut st, &["NOTACOMMAND"]), Value::Error(_)));
        assert_eq!(
            run(&ctx, &mut st, &["EXEC"]),
            Value::error("EXECABORT Transaction discarded because of previous errors.")
        );
    }

    #[test]
    fn watch_aborts_exec_when_key_changes_elsewhere() {
        let ctx = ServerContext::new(16, None);
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        run(&ctx, &mut st, &["SET", "balance", "100"]);
        run(&ctx, &mut st, &["WATCH", "balance"]);
        run(&ctx, &mut st, &["MULTI"]);
        run(&ctx, &mut st, &["SET", "balance", "200"]);

        // A different connection changes the watched key before EXEC.
        let (other_id, _) = ctx.register_client();
        let mut other = ClientState::with_id(other_id, false);
        run(&ctx, &mut other, &["SET", "balance", "999"]);

        assert_eq!(run(&ctx, &mut st, &["EXEC"]), Value::NullArray);
        assert_eq!(run(&ctx, &mut st, &["GET", "balance"]), Value::bulk(&b"999"[..]));
    }

    #[test]
    fn unwatch_prevents_a_stale_abort() {
        let ctx = ServerContext::new(16, None);
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        run(&ctx, &mut st, &["WATCH", "k"]);

        let (other_id, _) = ctx.register_client();
        let mut other = ClientState::with_id(other_id, false);
        run(&ctx, &mut other, &["SET", "k", "v"]);

        assert_eq!(run(&ctx, &mut st, &["UNWATCH"]), Value::ok());
        run(&ctx, &mut st, &["MULTI"]);
        run(&ctx, &mut st, &["SET", "k", "v2"]);
        assert_eq!(run(&ctx, &mut st, &["EXEC"]), Value::Array(vec![Value::ok()]));
    }

    #[test]
    fn eval_returns_a_plain_lua_value() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["EVAL", "return 1 + 1", "0"]), Value::Integer(2));
        assert_eq!(run(&ctx, &mut st, &["EVAL", "return 'hello'", "0"]), Value::bulk(&b"hello"[..]));
        assert_eq!(
            run(&ctx, &mut st, &["EVAL", "return {1, 2, 3}", "0"]),
            Value::Array(vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)])
        );
        assert_eq!(run(&ctx, &mut st, &["EVAL", "return nil", "0"]), Value::NullBulk);
    }

    #[test]
    fn eval_math_random_is_deterministic_per_script_source() {
        let (ctx, mut st) = fresh();
        let script = "return math.random(1, 1000000)";
        let first = run(&ctx, &mut st, &["EVAL", script, "0"]);
        let second = run(&ctx, &mut st, &["EVAL", script, "0"]);
        // Same script text run twice -> same math.random sequence, since
        // the RNG is seeded from the script's own source each time.
        assert_eq!(first, second);
        // A different (even trivially different) script gets an
        // independent seed, so there's no reason to expect the same draw.
        let other = run(&ctx, &mut st, &["EVAL", "return math.random(1, 1000000) + 0", "0"]);
        assert_ne!(first, other);
    }

    #[test]
    fn eval_sandboxes_os_and_io() {
        let (ctx, mut st) = fresh();
        let os_result = run(&ctx, &mut st, &["EVAL", "return os.time()", "0"]);
        let Value::Error(msg) = os_result else { panic!("expected an error reply") };
        assert!(msg.contains("'os'"), "unexpected error message: {msg}");

        let io_result = run(&ctx, &mut st, &["EVAL", "return io.open('/etc/passwd')", "0"]);
        let Value::Error(msg) = io_result else { panic!("expected an error reply") };
        assert!(msg.contains("'io'"), "unexpected error message: {msg}");
    }

    #[test]
    fn eval_keys_and_argv_are_bound() {
        let (ctx, mut st) = fresh();
        let result = run(&ctx, &mut st, &["EVAL", "return {KEYS[1], ARGV[1]}", "1", "mykey", "myarg"]);
        assert_eq!(result, Value::Array(vec![Value::bulk(&b"mykey"[..]), Value::bulk(&b"myarg"[..])]));
    }

    #[test]
    fn eval_redis_call_bridges_into_the_real_keyspace() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["EVAL", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "k", "v"]);
        assert_eq!(run(&ctx, &mut st, &["GET", "k"]), Value::bulk(&b"v"[..]));
        assert_eq!(
            run(&ctx, &mut st, &["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]),
            Value::bulk(&b"v"[..])
        );
    }

    #[test]
    fn eval_pcall_turns_errors_into_a_table_instead_of_raising() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["EVAL", "return redis.call('SET', KEYS[1], 'v')", "1", "s"]);
        let result = run(
            &ctx,
            &mut st,
            &[
                "EVAL",
                "local ok, e = pcall(function() redis.call('LPUSH', KEYS[1], 'x') end) return 'caught'",
                "1",
                "s",
            ],
        );
        assert_eq!(result, Value::bulk(&b"caught"[..]));

        let pcall_result = run(
            &ctx,
            &mut st,
            &["EVAL", "local r = redis.pcall('LPUSH', KEYS[1], 'x') return r.err", "1", "s"],
        );
        assert!(matches!(pcall_result, Value::Bulk(_)));
    }

    #[test]
    fn eval_ro_rejects_write_commands() {
        let (ctx, mut st) = fresh();
        let result = run(&ctx, &mut st, &["EVAL_RO", "return redis.call('SET', KEYS[1], 'v')", "1", "k"]);
        assert!(matches!(result, Value::Error(_)));
    }

    #[test]
    fn script_load_and_evalsha_round_trip() {
        let (ctx, mut st) = fresh();
        let sha = run(&ctx, &mut st, &["SCRIPT", "LOAD", "return 'loaded'"]);
        let sha_hex = match &sha {
            Value::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
            _ => panic!("expected bulk sha"),
        };
        assert_eq!(
            run(&ctx, &mut st, &["SCRIPT", "EXISTS", &sha_hex]),
            Value::Array(vec![Value::Integer(1)])
        );
        assert_eq!(run(&ctx, &mut st, &["EVALSHA", &sha_hex, "0"]), Value::bulk(&b"loaded"[..]));
        assert_eq!(run(&ctx, &mut st, &["SCRIPT", "FLUSH"]), Value::ok());
        assert_eq!(
            run(&ctx, &mut st, &["SCRIPT", "EXISTS", &sha_hex]),
            Value::Array(vec![Value::Integer(0)])
        );
        assert!(matches!(run(&ctx, &mut st, &["EVALSHA", &sha_hex, "0"]), Value::Error(_)));
    }

    #[test]
    fn eval_disallows_nested_transaction_commands() {
        let (ctx, mut st) = fresh();
        let result = run(&ctx, &mut st, &["EVAL", "return redis.call('MULTI')", "0"]);
        assert!(matches!(result, Value::Error(_)));
    }

    #[test]
    fn expire_sliding_keeps_a_hot_key_alive_across_reads() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "session", "data"]);
        assert_eq!(run(&ctx, &mut st, &["EXPIRE", "session", "100", "SLIDING"]), Value::Integer(1));
        assert!(matches!(run(&ctx, &mut st, &["TTL", "session"]), Value::Integer(n) if n > 90));

        // A plain (non-sliding) EXPIRE turns sliding mode back off.
        run(&ctx, &mut st, &["EXPIRE", "session", "100"]);
        run(&ctx, &mut st, &["GET", "session"]);
        assert!(matches!(run(&ctx, &mut st, &["TTL", "session"]), Value::Integer(n) if n <= 100));
    }

    /// `Instant::now() + Duration` panics on overflow. `EXPIRE`/`PEXPIRE`,
    /// `SET EX/PX`, `SET ... SLIDING`, `RESTORE`'s TTL, `GET ...
    /// SINGLEFLIGHT`, and blocking-command timeouts all turn a plain
    /// client-supplied integer into exactly that addition — none of them
    /// should be able to crash the connection just by supplying a huge
    /// value instead of returning a (large-but-finite) TTL or timing out.
    #[test]
    fn huge_client_supplied_durations_saturate_instead_of_panicking() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert_eq!(run(&ctx, &mut st, &["EXPIRE", "k", &i64::MAX.to_string()]), Value::Integer(1));
        assert!(matches!(run(&ctx, &mut st, &["TTL", "k"]), Value::Integer(n) if n > 0));

        assert_eq!(run(&ctx, &mut st, &["PEXPIRE", "k", &i64::MAX.to_string()]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["EXPIRE", "k", &i64::MAX.to_string(), "SLIDING"]), Value::Integer(1));

        assert_eq!(
            run(&ctx, &mut st, &["SET", "k2", "v", "EX", &i64::MAX.to_string()]),
            Value::Simple("OK".into())
        );
        assert_eq!(
            run(&ctx, &mut st, &["SET", "k3", "v", "PX", &i64::MAX.to_string()]),
            Value::Simple("OK".into())
        );

        let payload = bincode::serialize(&KVal::String(Bytes::from("x"))).unwrap();
        let restore_req: Request = vec![
            Bytes::from_static(b"RESTORE"),
            Bytes::from_static(b"k4"),
            Bytes::from(u64::MAX.to_string()),
            Bytes::from(payload),
        ];
        match dispatch(&ctx, &mut st, restore_req) {
            Outcome::Reply(v) => assert_eq!(v, Value::Simple("OK".into())),
            _ => panic!("expected a plain Reply from RESTORE"),
        }

        assert_eq!(
            run(&ctx, &mut st, &["GET", "k5", "SINGLEFLIGHT", &u64::MAX.to_string()]),
            Value::NullBulk
        );

        // BLPOP's timeout is a float in seconds, parsed independently of
        // the integer paths above — `inf` used to panic in
        // `Duration::from_secs_f64` before even reaching `Instant` math.
        assert_eq!(run(&ctx, &mut st, &["BLPOP", "nosuchlist", "inf"]), Value::NullArray);
    }

    #[test]
    fn singleflight_first_caller_becomes_leaseholder() {
        let (ctx, mut st) = fresh();
        // Cache miss, first caller: becomes leaseholder, gets nil.
        assert_eq!(run(&ctx, &mut st, &["GET", "hot", "SINGLEFLIGHT", "5000"]), Value::NullBulk);
    }

    #[test]
    fn singleflight_second_caller_is_told_to_wait() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["GET", "hot", "SINGLEFLIGHT", "5000"]);
        match dispatch(&ctx, &mut st, req(&["GET", "hot", "SINGLEFLIGHT", "5000"])) {
            Outcome::SingleFlightWait(_) => {}
            _ => panic!("expected SingleFlightWait, got a reply instead"),
        }
    }

    #[test]
    fn singleflight_set_wakes_waiters_and_clears_the_lease() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["GET", "hot", "SINGLEFLIGHT", "5000"]);
        assert!(ctx.singleflight.contains_key(&(0, Bytes::from("hot"))));
        run(&ctx, &mut st, &["SET", "hot", "computed-value"]);
        assert!(!ctx.singleflight.contains_key(&(0, Bytes::from("hot"))));
        // Lease cleared: a later miss on a *different* key starts fresh.
        assert_eq!(run(&ctx, &mut st, &["GET", "hot"]), Value::bulk(&b"computed-value"[..]));
    }

    #[test]
    fn singleflight_existing_value_bypasses_the_lease_entirely() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert_eq!(run(&ctx, &mut st, &["GET", "k", "SINGLEFLIGHT", "5000"]), Value::bulk(&b"v"[..]));
        assert!(!ctx.singleflight.contains_key(&(0, Bytes::from("k"))));
    }

    #[test]
    fn psync_returns_fullresync_and_snapshot_and_registers_replica() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "v"]);
        match dispatch(&ctx, &mut st, req(&["PSYNC", "?", "-1"])) {
            Outcome::Multi(values) => {
                assert_eq!(values.len(), 2);
                match &values[0] {
                    Value::Simple(s) => assert!(s.starts_with("FULLRESYNC ")),
                    other => panic!("expected simple string, got {other:?}"),
                }
                assert!(matches!(&values[1], Value::Bulk(_)));
            }
            _ => panic!("expected Outcome::Multi from PSYNC"),
        }
        assert_eq!(ctx.connected_replicas(), 1);
    }

    #[test]
    fn write_commands_replicate_to_connected_replicas() {
        let (ctx, mut st) = fresh();
        let (replica_id, mut rx) = ctx.register_client();
        ctx.register_replica(replica_id);

        run(&ctx, &mut st, &["SET", "k", "v"]);

        // First push should be a SELECT for this replica's first command...
        let first = rx.try_recv().expect("expected a SELECT push");
        assert_eq!(first, Value::Array(vec![Value::bulk(&b"SELECT"[..]), Value::bulk(&b"0"[..])]));
        // ...then the replicated command itself.
        let second = rx.try_recv().expect("expected the replicated SET");
        assert_eq!(
            second,
            Value::Array(vec![Value::bulk(&b"SET"[..]), Value::bulk(&b"k"[..]), Value::bulk(&b"v"[..])])
        );
        assert_eq!(ctx.master_repl_offset(), 1);
    }

    #[test]
    fn replica_rejects_client_writes_but_accepts_replication_link_writes() {
        let ctx = ServerContext::new(16, None);
        ctx.set_replica_of(Some(("1.2.3.4".to_string(), 6379)));
        let (id, _rx) = ctx.register_client();
        let mut client = ClientState::with_id(id, false);
        assert_eq!(
            run(&ctx, &mut client, &["SET", "k", "v"]),
            Value::error("READONLY You can't write against a read only replica.")
        );

        let mut link = ClientState::for_replication_link();
        assert_eq!(run(&ctx, &mut link, &["SET", "k", "v"]), Value::ok());
        assert_eq!(run(&ctx, &mut client, &["GET", "k"]), Value::bulk(&b"v"[..]));
    }

    #[test]
    fn wait_returns_immediately_when_no_replicas_are_required() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert_eq!(run(&ctx, &mut st, &["WAIT", "0", "1000"]), Value::Integer(0));
    }

    #[test]
    fn wait_blocks_until_a_replica_acks_the_offset() {
        let (ctx, mut st) = fresh();
        let (replica_id, _rx) = ctx.register_client();
        ctx.register_replica(replica_id);
        run(&ctx, &mut st, &["SET", "k", "v"]);

        match dispatch(&ctx, &mut st, req(&["WAIT", "1", "1000"])) {
            Outcome::WaitForReplicas { numreplicas, target_offset, .. } => {
                assert_eq!(numreplicas, 1);
                assert_eq!(target_offset, ctx.master_repl_offset());
            }
            _ => panic!("expected WaitForReplicas, got a reply instead"),
        }

        // Once the replica ACKs, a fresh WAIT resolves immediately.
        ctx.ack_replica(replica_id, ctx.master_repl_offset());
        assert_eq!(run(&ctx, &mut st, &["WAIT", "1", "1000"]), Value::Integer(1));
    }

    #[test]
    fn info_reply_is_a_bulk_string_with_expected_sections() {
        let (ctx, mut st) = fresh();
        let info = run(&ctx, &mut st, &["INFO"]);
        let text = match info {
            Value::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
            other => panic!("expected bulk string, got {other:?}"),
        };
        assert!(text.contains("# Server"));
        assert!(text.contains("# Replication"));
        assert!(text.contains("role:master"));
        assert!(text.contains("# Memory"));
    }

    #[test]
    fn slowlog_logs_commands_over_threshold_and_can_be_reset() {
        let mut ctx = ServerContext::new(16, None);
        ctx.slowlog_threshold_us = 0; // log everything, for a deterministic test
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);

        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert!(matches!(run(&ctx, &mut st, &["SLOWLOG", "LEN"]), Value::Integer(n) if n > 0));

        match run(&ctx, &mut st, &["SLOWLOG", "GET", "1"]) {
            Value::Array(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    Value::Array(fields) => assert_eq!(fields.len(), 4),
                    other => panic!("expected an array entry, got {other:?}"),
                }
            }
            other => panic!("expected an array, got {other:?}"),
        }

        assert_eq!(run(&ctx, &mut st, &["SLOWLOG", "RESET"]), Value::ok());
        // With threshold 0 ("log everything"), SLOWLOG RESET logs itself as
        // the very next command — so LEN is 1 here (just that entry), not 0.
        assert_eq!(run(&ctx, &mut st, &["SLOWLOG", "LEN"]), Value::Integer(1));
    }

    #[test]
    fn memory_usage_reports_a_key_size_and_nil_for_missing() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["SET", "k", "hello"]);
        assert!(matches!(run(&ctx, &mut st, &["MEMORY", "USAGE", "k"]), Value::Integer(n) if n > 0));
        assert_eq!(run(&ctx, &mut st, &["MEMORY", "USAGE", "missing"]), Value::NullBulk);
    }

    #[test]
    fn acl_default_user_matches_plain_requirepass_behavior() {
        let (ctx, mut st) = fresh();
        assert_eq!(run(&ctx, &mut st, &["ACL", "WHOAMI"]), Value::bulk(&b"default"[..]));
        // No requirepass configured: default user has no restrictions.
        assert_eq!(run(&ctx, &mut st, &["SET", "k", "v"]), Value::ok());
    }

    #[test]
    fn acl_setuser_restricts_commands_and_keys() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["ACL", "SETUSER", "alice", "on", ">pw", "~cache:*", "+GET", "+SET"]);

        let (id, _rx) = ctx.register_client();
        let mut alice = ClientState::with_id(id, true);
        assert_eq!(run(&ctx, &mut alice, &["AUTH", "alice", "pw"]), Value::ok());
        assert_eq!(alice.acl_user, "alice");

        // Allowed command on an allowed key.
        assert_eq!(run(&ctx, &mut alice, &["SET", "cache:1", "v"]), Value::ok());
        // Allowed command, disallowed key pattern.
        assert!(matches!(run(&ctx, &mut alice, &["SET", "other:1", "v"]), Value::Error(_)));
        // Disallowed command entirely.
        assert!(matches!(run(&ctx, &mut alice, &["DEL", "cache:1"]), Value::Error(_)));

        // Wrong password is rejected.
        let (id2, _rx2) = ctx.register_client();
        let mut alice2 = ClientState::with_id(id2, true);
        assert!(matches!(run(&ctx, &mut alice2, &["AUTH", "alice", "wrong"]), Value::Error(_)));
    }

    #[test]
    fn acl_setuser_accepts_rw_scoped_key_patterns_and_wipe_all_passwords() {
        let (ctx, mut st) = fresh();
        // `%R~`/`%W~` aren't enforced as read/write-separated, but must not
        // error out `SETUSER` — they're folded into normal key access.
        assert_eq!(
            run(&ctx, &mut st, &["ACL", "SETUSER", "dana", "on", ">pw", "%R~read:*", "%W~write:*", "+GET"]),
            Value::ok()
        );
        let (id, _rx) = ctx.register_client();
        let mut dana = ClientState::with_id(id, true);
        run(&ctx, &mut dana, &["AUTH", "dana", "pw"]);
        assert_eq!(run(&ctx, &mut dana, &["GET", "read:1"]), Value::NullBulk);
        assert!(matches!(run(&ctx, &mut dana, &["GET", "other:1"]), Value::Error(_)));

        // `!` wipes all password hashes (distinct from `nopass`, which also
        // flips the "no password required" flag).
        run(&ctx, &mut st, &["ACL", "SETUSER", "dana", ">anotherpw", "!"]);
        let (id2, _rx2) = ctx.register_client();
        let mut dana2 = ClientState::with_id(id2, true);
        assert!(matches!(run(&ctx, &mut dana2, &["AUTH", "dana", "anotherpw"]), Value::Error(_)));
    }

    #[test]
    fn acl_deluser_removes_a_user_but_not_default() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["ACL", "SETUSER", "bob", "on", "nopass", "allkeys", "allcommands"]);
        assert_eq!(run(&ctx, &mut st, &["ACL", "DELUSER", "bob"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["ACL", "DELUSER", "default"]), Value::Integer(0));
    }

    #[test]
    fn acl_users_and_list_report_known_users() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["ACL", "SETUSER", "carol", "on", "nopass", "allkeys", "allcommands"]);
        match run(&ctx, &mut st, &["ACL", "USERS"]) {
            Value::Array(items) => {
                let names: Vec<String> = items
                    .into_iter()
                    .map(|v| match v {
                        Value::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
                        _ => panic!(),
                    })
                    .collect();
                assert!(names.contains(&"default".to_string()));
                assert!(names.contains(&"carol".to_string()));
            }
            other => panic!("expected an array, got {other:?}"),
        }
    }

    #[test]
    fn schedule_in_fires_after_the_delay_elapses() {
        let (ctx, mut st) = fresh();
        let id = run(&ctx, &mut st, &["SCHEDULE", "IN", "0", "SET", "k", "v"]);
        assert!(matches!(id, Value::Integer(_)));

        // 0ms delay: due immediately on the next drain.
        std::thread::sleep(std::time::Duration::from_millis(5));
        let fired = ctx.run_due_scheduled();
        assert_eq!(fired, 1);
        assert_eq!(run(&ctx, &mut st, &["GET", "k"]), Value::bulk(&b"v"[..]));
    }

    #[test]
    fn schedule_list_and_cancel() {
        let (ctx, mut st) = fresh();
        let id = match run(&ctx, &mut st, &["SCHEDULE", "IN", "60000", "SET", "k", "v"]) {
            Value::Integer(n) => n,
            other => panic!("expected integer id, got {other:?}"),
        };

        let listed = run(&ctx, &mut st, &["SCHEDULE", "LIST"]);
        assert_eq!(
            listed,
            Value::Array(vec![Value::Array(vec![
                Value::Integer(id),
                listed_epoch(&listed),
                Value::bulk(&b"SET"[..]),
                Value::bulk(&b"k"[..]),
                Value::bulk(&b"v"[..]),
            ])])
        );

        assert_eq!(run(&ctx, &mut st, &["SCHEDULE", "CANCEL", &id.to_string()]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["SCHEDULE", "LIST"]), Value::Array(vec![]));
        // A cancelled task should not fire even once its time comes.
        assert_eq!(ctx.run_due_scheduled(), 0);
        assert_eq!(run(&ctx, &mut st, &["GET", "k"]), Value::NullBulk);
    }

    /// Helper so the cancel/list test doesn't have to hardcode a wall-clock
    /// timestamp: pulls the epoch-ms element back out of a `SCHEDULE LIST`
    /// reply to build the exact expected value.
    fn listed_epoch(listed: &Value) -> Value {
        match listed {
            Value::Array(items) => match &items[0] {
                Value::Array(fields) => fields[1].clone(),
                _ => panic!("expected nested array"),
            },
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn save_without_rdb_path_errors() {
        let (ctx, mut st) = fresh();
        assert!(matches!(run(&ctx, &mut st, &["SAVE"]), Value::Error(_)));
    }

    #[test]
    fn save_load_round_trip_via_rdb_path() {
        let dir = std::env::temp_dir().join(format!("reflexdb-dispatch-rdb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dump.rdb");

        let ctx = ServerContext::new(16, None).with_rdb_path(path.clone());
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        run(&ctx, &mut st, &["SET", "k", "v"]);
        assert_eq!(run(&ctx, &mut st, &["SAVE"]), Value::ok());
        assert!(run(&ctx, &mut st, &["LASTSAVE"]) != Value::Integer(0));

        let restored_ctx = ServerContext::new(16, None);
        persistence::rdb::load(&path, &restored_ctx.store).unwrap();
        assert_eq!(
            restored_ctx.store.db(0).unwrap().get(b"k"),
            Some(keyspace::Value::String(Bytes::from("v")))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dump_and_restore_round_trip_a_value() {
        let (ctx, mut st) = fresh();
        run(&ctx, &mut st, &["RPUSH", "l", "a", "b"]);
        let dumped = run(&ctx, &mut st, &["DUMP", "l"]);
        let payload = match dumped {
            Value::Bulk(b) => b,
            other => panic!("expected bulk dump payload, got {other:?}"),
        };

        // Build the RESTORE request from raw bytes directly rather than via
        // the `run()`/`req()` string helpers, since the bincode payload
        // isn't necessarily valid UTF-8.
        let restore_req = |key: &str, payload: Bytes| -> Request {
            vec![Bytes::from("RESTORE"), Bytes::from(key.to_string()), Bytes::from("0"), payload]
        };

        match dispatch(&ctx, &mut st, restore_req("l", payload.clone())) {
            Outcome::Reply(v) => {
                assert_eq!(v, Value::error("BUSYKEY Target key name already exists."))
            }
            _ => panic!("expected a reply"),
        }

        run(&ctx, &mut st, &["DEL", "l"]);
        match dispatch(&ctx, &mut st, restore_req("l", payload)) {
            Outcome::Reply(v) => assert_eq!(v, Value::ok()),
            _ => panic!("expected a reply"),
        }
        assert_eq!(
            run(&ctx, &mut st, &["LRANGE", "l", "0", "-1"]),
            Value::Array(vec![Value::bulk(&b"a"[..]), Value::bulk(&b"b"[..])])
        );
    }

    #[test]
    fn maxmemory_noeviction_rejects_writes_once_over_budget() {
        let ctx = ServerContext::new(16, None).with_maxmemory(Some(1), MaxMemoryPolicy::NoEviction);
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        run(&ctx, &mut st, &["SET", "k", "a-fairly-long-value-to-exceed-one-byte"]);
        ctx.run_eviction_tick(); // refreshes the memory estimate (NoEviction means nothing gets evicted)
        let result = run(&ctx, &mut st, &["SET", "k2", "v"]);
        assert!(matches!(result, Value::Error(_)));
    }

    #[test]
    fn maxmemory_allkeys_random_evicts_until_under_budget() {
        let ctx = ServerContext::new(16, None).with_maxmemory(Some(80), MaxMemoryPolicy::AllKeysRandom);
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        for i in 0..20 {
            run(&ctx, &mut st, &["SET", &format!("k{i}"), "some reasonably sized value here"]);
        }
        let evicted = ctx.run_eviction_tick();
        assert!(evicted > 0, "expected at least one key to be evicted");
        assert!(ctx.store.db(0).unwrap().dbsize() < 20);
    }

    #[test]
    fn maxmemory_allkeys_lru_evicts_the_least_recently_used_key_first() {
        // `delete_one` is exercised directly (it's private, but this test
        // module is in the same file) rather than through
        // `run_eviction_tick`'s budget loop, which would keep evicting
        // past the first key since any single remaining key still exceeds
        // a 1-byte budget.
        let ctx = ServerContext::new(16, None).with_maxmemory(None, MaxMemoryPolicy::AllKeysLru);
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        run(&ctx, &mut st, &["SET", "old", "v"]);
        run(&ctx, &mut st, &["SET", "new", "v"]);
        // Touch "new" so it's more recently accessed than "old".
        run(&ctx, &mut st, &["GET", "new"]);
        assert!(ctx.delete_one());
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "old"]), Value::Integer(0));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "new"]), Value::Integer(1));
    }

    #[test]
    fn maxmemory_allkeys_lfu_evicts_the_least_frequently_used_key_first() {
        let ctx = ServerContext::new(16, None).with_maxmemory(None, MaxMemoryPolicy::AllKeysLfu);
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        run(&ctx, &mut st, &["SET", "rare", "v"]);
        run(&ctx, &mut st, &["SET", "hot", "v"]);
        for _ in 0..5 {
            run(&ctx, &mut st, &["GET", "hot"]);
        }
        assert!(ctx.delete_one());
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "rare"]), Value::Integer(0));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "hot"]), Value::Integer(1));
    }

    #[test]
    fn maxmemory_volatile_ttl_only_evicts_keys_with_an_expiry_and_picks_the_soonest() {
        let ctx = ServerContext::new(16, None).with_maxmemory(None, MaxMemoryPolicy::VolatileTtl);
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);
        run(&ctx, &mut st, &["SET", "persistent", "v"]);
        run(&ctx, &mut st, &["SET", "soon", "v"]);
        run(&ctx, &mut st, &["EXPIRE", "soon", "1000"]);
        run(&ctx, &mut st, &["SET", "later", "v"]);
        run(&ctx, &mut st, &["EXPIRE", "later", "9999"]);
        assert!(ctx.delete_one());
        // Only a volatile (has-a-TTL) key is ever a candidate, and the one
        // closest to expiring is picked over the one with more time left.
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "persistent"]), Value::Integer(1));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "soon"]), Value::Integer(0));
        assert_eq!(run(&ctx, &mut st, &["EXISTS", "later"]), Value::Integer(1));
    }

    /// Trivial in-memory stand-in for the real `tiered::FjallTier`, just to
    /// exercise the full `ServerContext`/`dispatch()` integration without
    /// pulling the `tiered` crate (and fjall) into this one.
    #[derive(Default)]
    struct MockTier {
        disk: DashMap<Bytes, (keyspace::Value, Option<Duration>)>,
    }

    impl keyspace::TieringHandler for MockTier {
        fn evict(&self, key: &Bytes, value: &keyspace::Value, ttl: Option<Duration>) {
            self.disk.insert(key.clone(), (value.clone(), ttl));
        }
        fn promote(&self, key: &[u8]) -> Option<(keyspace::Value, Option<Duration>)> {
            self.disk.remove(key).map(|(_, v)| v)
        }
        fn contains(&self, key: &[u8]) -> bool {
            self.disk.contains_key(key)
        }
        fn remove(&self, key: &[u8]) {
            self.disk.remove(key);
        }
    }

    #[test]
    fn tiered_storage_moves_keys_to_disk_and_promotes_on_read() {
        let tier = std::sync::Arc::new(MockTier::default());
        let store = Store::new_with_tiering(16, tier.clone());
        let ctx = ServerContext::with_store(store, None).with_tiered_limit(Some(50));
        let (id, _rx) = ctx.register_client();
        let mut st = ClientState::with_id(id, false);

        for i in 0..20 {
            run(&ctx, &mut st, &["SET", &format!("k{i}"), "some reasonably sized value here"]);
        }
        let moved = ctx.run_eviction_tick();
        assert!(moved > 0, "expected at least one key to be tiered to disk");
        assert!(!tier.disk.is_empty());
        let ram_count = ctx.store.db(0).unwrap().dbsize();
        assert!(ram_count < 20);

        // EXISTS must stay truthful for a tiered key without reading it off
        // disk, and GET must transparently promote it back into RAM.
        let tiered_key = tier.disk.iter().next().unwrap().key().clone();
        let tiered_key_str = String::from_utf8(tiered_key.to_vec()).unwrap();
        assert_eq!(run(&ctx, &mut st, &["EXISTS", &tiered_key_str]), Value::Integer(1));
        assert_eq!(
            run(&ctx, &mut st, &["GET", &tiered_key_str]),
            Value::bulk(&b"some reasonably sized value here"[..])
        );
        assert_eq!(ctx.store.db(0).unwrap().dbsize(), ram_count + 1);
    }
}
