# Tiered Storage — Operations Guide

The disk tier is **durable overflow storage with explicit capacity guarantees**: keys that no
longer fit in `tiered-memory-limit` are written to a local LSM store (fjall) and promoted back
on first read. Every tiered write is fsync'd before `evict` returns, so an acknowledged key
survives a process crash.

It is **not** marketed as general persistence yet: crash-consistency is tested per-key
(acknowledged writes survive abrupt exit), but full-dataset backup/restore and replica
consistency guarantees are not established. Treat it as capacity relief, not a database file.

## Overflow policies

`tiered-disk-limit` sets the capacity; `tiered-overflow-policy` decides what "full" means:

| Policy | Behavior on full disk tier | Data loss? |
|---|---|---|
| `reject-writes` (default) | New-key writes fail `ERR TIERED_STORAGE_FULL` once RAM *and* disk are both over their limits. Deletes, TTL ops, and single-key overwrites still work — overwriting a tiered key pulls it back to RAM and frees disk. | No |
| `evict-oldest` | Oldest tiered entries are dropped down to ~50% of the limit (`eviction-low-watermark`). | Yes — cache semantics |
| `no-limit` | Limit ignored entirely. | No enforcement |

`reject-writes` never evicts a tiered entry to admit a new write — acknowledged data is never
sacrificed to accept more. Writes that would spill RAM keys into a full tier stay in RAM and
count `tiered_spill_failures` instead of being dropped.

## Capacity planning

Each tiered entry is charged `key.len() + serialized_payload + ~64B` of LSM record/index
overhead against `tiered-disk-limit`. `INFO`'s `tiered_disk_used_bytes` is the tracked estimate
the limit enforces against — not a `du` of the directory (fjall pre-allocates journal segments,
so the directory is often larger).

- **Size the limit below the filesystem**, not equal to it. Major compaction rewrites segments
  and can transiently need up to ~the tier's own size in headroom before old files are removed.
  Keep at least 1.5× `tiered-disk-limit` free on the filesystem.
- `reject-writes` gate needs *both* limits: RAM over `tiered-memory-limit` **and** disk at
  `tiered-disk-limit`. A full disk tier alone doesn't reject — new keys still land in RAM until
  RAM is also exhausted.

## Monitoring (`INFO`)

```
tiered_memory_limit / tiered_disk_limit / tiered_overflow_policy
tiered_disk_used_bytes        tracked usage the limit checks against
tiered_evictions              keys spilled RAM → disk
tiered_disk_drops             keys lost to evict-oldest capacity drops
tiered_rejected_writes        client writes refused under reject-writes
tiered_spill_failures         spills that hit a full tier (key stayed in RAM)
tiered_expired_dropped        GC sweep removals of expired tiered entries
tiered_entries_removed        total tombstones (deletes + promotes + GC)
tiered_compactions / tiered_compacting   compaction history / in-flight
```

Alert on: `tiered_disk_used_bytes` approaching `tiered_disk_limit`, any nonzero
`tiered_rejected_writes` under `reject-writes`, or sustained `tiered_spill_failures` — each
means capacity is exhausted and intervention (bigger limit, deletes, or operator cleanup) is
needed.

## Garbage collection & compaction

Deletes and TTL expiries write LSM tombstones; they don't reclaim file space until compaction.
A ~60s background task:

1. Sweeps tiered entries whose TTL expired while untouched (lazy expiry only fires on access).
2. Runs `major_compact` once ≥10K removals accumulated or ≥1 hour passed.

Compaction is off the latency path but I/O-heavy — watch `tiered_compacting` before scheduling
maintenance windows on small disks.

## Failure modes

- **Filesystem full (not the configured limit)**: writes return an I/O error, not
  `TIERED_STORAGE_FULL`. Tiered keys remain readable; RAM operation is unaffected. Free space,
  then let GC/compaction reclaim tombstones. Keep compaction headroom (above) to prevent this.
- **Corrupt entry**: a payload that fails deserialization is logged, dropped, and counted —
  the key reads as absent rather than poisoning reads. `KEYS`-style scans skip it.
- **Disk unavailable at open**: `FjallTier::open` fails and the server logs it at startup —
  tiering is simply off (RAM-only) if the directory can't be opened... verify at deploy time;
  there is no automatic retry.
- **Crash**: acknowledged evictions are on the fsync'd journal before `evict` returns —
  reopening the directory replays them. Tested via live-directory snapshot + reopen.

## Backups

The tier directory is a self-contained fjall keyspace. To snapshot: `cp -r` the `tiered-dir`
contents while the server is *stopped*, or take a filesystem-level snapshot (the live files
are only safe to copy verbatim once the journal is idle — an in-flight compaction or heavy
write stream can produce a torn copy). Restoring = placing the directory back and restarting.
A verified online backup path doesn't exist yet — that's part of why this isn't called
persistence.

## Positioning

- It is **overflow, not a cache** by default: `reject-writes` + fsync-per-write means what the
  server acknowledges stays retrievable.
- `evict-oldest` deliberately trades durability for a bounded footprint — use it when tiered
  data is reconstructible.
- TTL'd keys are honored on disk exactly as in RAM (checked on access, swept by GC).
