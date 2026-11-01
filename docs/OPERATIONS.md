# Operations

Running reflexdb in production: config, durability, backups, replication, and monitoring.
For the security posture behind these choices see `docs/SECURITY.md`; for the data layout
see `docs/ARCHITECTURE.md`.

## Running

```sh
reflexdb-server reflexdb.conf            # config file
reflexdb-server --port 7379 --dir /var/lib/reflexdb --appendonly yes   # flags only
reflexdb-server reflexdb.conf --port 7380   # file first, then --flag overrides
```

Every config-file directive doubles as a `--flag` (see `reflexdb.conf` for the annotated
list). `CONFIG GET`/`CONFIG SET` expose the runtime-tunable subset; `CONFIG REWRITE` is
not implemented — edit the file and restart for structural directives.

Only one server may use a `dir` — a second boot on the same directory refuses to start
(lockfile). Always give each instance its own `dir`.

## Durability

Pick a `profile` and override individual directives after it if needed:

| profile        | appendonly | appendfsync | meaning                                             |
|----------------|------------|-------------|-----------------------------------------------------|
| `cache`        | no         | —           | lose everything on restart (default)                |
| `durable`      | yes        | always      | every acked write survives `kill -9`                |
| `replicated`   | yes        | everysec    | ~1s window, plus `min-replicas-to-write` for writes |

- `SAVE` — synchronous RDB snapshot (blocks the server; fine for small datasets).
- `BGSAVE` — background RDB snapshot.
- `BGREWRITEAOF` — background AOF compaction: serializes the live dataset as the minimal
  command stream and swaps it in for the grown AOF.
- `auto-aof-rewrite-percentage` (default 100) + `auto-aof-rewrite-min-size` (default 64mb):
  the server rewrites automatically once the AOF exceeds the min size *and* has grown by
  the percentage since the last rewrite. `0` disables. Only one rewrite runs at a time.

## Backup & restore

Artifacts are self-contained: copy `dump.rdb` (RDB mode) or `appendonly.aof` (AOF mode)
out of `dir` to back up; drop the file into a fresh `dir` and boot to restore.

```sh
redis-cli SAVE                          # or BGSAVE; wait for INFO persistence to settle
cp "$dir/dump.rdb" /backups/reflexdb-$(date +%F).rdb

# restore: copy to the new instance's dir, then start it
cp /backups/reflexdb-2025-01-01.rdb "$dir/dump.rdb"
reflexdb-server --dir "$dir" ...
```

Notes:
- AOF mode: copy `appendonly.aof`. For a consistent copy either take it right after
  `BGREWRITEAOF`, or shut down the server first (`SHUTDOWN` flushes on graceful exit) —
  a live `appendfsync everysec/no` AOF can be mid-write when copied (the torn tail is
  dropped on replay, so it still boots, but the newest writes may be missing).
- Cross-check restores before you rely on them: `backup_artifacts_restore_in_a_fresh_dir`
  in `crates/server/tests/crash_recovery.rs` is the e2e that exercises exactly this path.
- Restore also works for migration: the artifact has no machine identity in it.

## Replication

```conf
replicaof 127.0.0.1 7379        # replica side — or REPLICAOF at runtime
masterauth s3cret               # when the primary sets requirepass
tls-replication yes             # encrypt the replica→primary link
```

- Replicas are read-only for clients; replication writes bypass the ACL.
- A replica that reconnects with a still-covered offset resyncs incrementally
  (`PSYNC` → `+CONTINUE`, sized by `partial-resync-backlog`, default 1mb); otherwise it
  takes a full `SYNC` snapshot. `INFO replication` shows `sync_full`/`sync_partial_ok`.
- `min-replicas-to-write N` refuses writes (NOREPLICAS) while fewer than N healthy
  replicas are connected — combine with `profile replicated` for a write-availability
  floor.
- `WAIT <n> <ms>` blocks until N replicas have acked the current replication offset.

### Automatic failover (`reflexdb-sentinel`)

```conf
port 26379
sentinel monitor mymaster 127.0.0.1 7379 2
sentinel down-after-milliseconds mymaster 5000
sentinel known-replica mymaster 127.0.0.1 7380
sentinel known-sentinel mymaster 127.0.0.1 26380   # peer sentinels, for quorum > 1
```

`reflexdb-sentinel sentinel.conf` pings the master every second; after
`down-after-milliseconds` of silence it asks peer sentinels whether they agree
(`SENTINEL IS-MASTER-DOWN-BY-ADDR`), and once `quorum` votes say down it promotes the
first `known-replica` (`REPLICAOF NO ONE`), re-points the rest at it, and publishes
`+switch-master` on `__sentinel__:switch-master`. Simplified vs. real Sentinel: no
auto-discovery (replicas/peers are configured explicitly), replica choice is
first-listed not highest-offset, and a revived old primary is not walked back to
replica duty — stop it yourself before it diverges.

## Monitoring

Two surfaces:

**`INFO`** — server, clients, memory (`used_memory` payload estimate + `used_memory_peak`
real RSS), persistence, replication, keyspace sections. Redis-compatible field names.

**`metrics-port`** — a second listener on `bind:metrics-port` answers any GET with
Prometheus text exposition (HTTP/1.0, connection-close):

```conf
metrics-port 9121
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: reflexdb
    static_configs: [{ targets: ["127.0.0.1:9121"] }]
```

Exported: `reflexdb_up`, `uptime_seconds`, `connected_clients`, `blocked_clients`,
`used_memory_bytes`/`used_memory_peak_bytes`/`maxmemory_bytes`,
`commands_processed_total`, `role_master`, `connected_replicas`, `master_repl_offset`,
`resync_full_total`/`resync_partial_total`, `aof_enabled`, `aof_size_bytes`,
`aof_rewrite_in_progress`, `reflexdb_keys{db="N"}`.

The endpoint is unauthenticated and read-only — keep it on a loopback bind or behind
your network boundary. Scrapes are cheap (they reuse the maintenance tick's cached
memory estimate unless no memory limits are configured).

## Security checklist

1. `requirepass` (or an `aclfile`) — without it, `protected-mode yes` (default) still
   refuses non-loopback clients on a non-loopback bind, but loopback is fully open.
2. `bind 127.0.0.1` unless replicas/clients really come from off-host.
3. TLS for anything crossing a network boundary (`tls-port`, `tls-replication`).
4. `min-replicas-to-write` if silent replica loss must stop writes.
5. Keep `metrics-port` and `dir` off shared mounts/adversary-reachable paths.
6. 16 failed AUTHs per connection drop the socket; `CONFIG GET` masks `requirepass`.
   Full details: `docs/SECURITY.md`.

## Upgrades & restarts

- Graceful: `SHUTDOWN` (or SIGTERM) flushes and exits. Rolling restarts should go through
  a replica first when write availability matters.
- Binary upgrades: stop, replace the binary, start on the same `dir` — RDB/AOF formats
  are stable within a release line. Replica failover is handled by promoting with
  `REPLICAOF NO ONE` on the replica, then pointing clients over.

## Benchmarking

`benchmarks/run.py` appends one JSONL row per run — keep them in a file for history:

```sh
python3 benchmarks/run.py --output bench_history.jsonl --label "$(git rev-parse --short HEAD)"
python3 benchmarks/compare.py bench_history.jsonl            # newest label vs previous
python3 benchmarks/compare.py bench_history.jsonl --base v1 --head v2
```

`compare.py` exits non-zero when any workload regresses past `--threshold` (default ±10%),
so it drops into CI or a pre-merge check as-is.
