# Roadmap

This roadmap is intentionally ordered around proving a product before expanding the database.
The proposed product direction and customer validation plan are in
[PRODUCT_DIRECTION.md](PRODUCT_DIRECTION.md). No future feature below should be treated as a
commitment until a real user need or a measured systems bottleneck supports it.

## P0 — Prove the customer problem

1. Talk to developers shipping long-running workflows, including agent workflows, and document
   their current state storage, failure recovery, and debugging process.
2. Show one end-to-end recovery prototype: observe a state change, identify the workflow step,
   inspect an earlier state, and resume from a known-good point.
3. Seek at least three design partners willing to try it on a real workload. Record setup time,
   adoption blockers, and whether recovery is valuable enough to switch or add a dependency.
4. Decide the initial product from that evidence. If the painful problem is ordinary cache cost
   or speed instead, change direction rather than forcing the workflow hypothesis.

## P1 — Make the prototype trustworthy and measurable

- Define exactly what `LIVE`, history, persistence, and recovery guarantee on a crash or restart.
- Establish reproducible baselines on the same hardware, build, dataset, durability settings, and
  client setup. Track throughput, p50/p99 latency,
  CPU, RSS, RAM per key, disk bytes, write amplification, and recovery time.
- Measure representative small-object, large-object, hot-key, high-cardinality, and mixed
  read/write workloads before changing data structures or runtime architecture.
- Fix measured correctness, resource-bound, and performance problems before adding more
  compatibility surface.

## P2 — Build only the validated workflow

If design partners validate recovery and inspection, the next product work is likely to include
workflow/run identifiers, mutation provenance, consistent checkpoints, bounded history retention,
and a supported way to resume. Design these around a clear atomic commit boundary. Do not assume
that a global append-only log, MVCC, or a separate history database is the right architecture
until durability, latency, and storage costs are measured.

If users instead validate a cache workload, prioritize its required command behavior, operational
reliability, and measured cost or performance. Do not bundle that path with an unvalidated
agent-memory product.

## Paused until evidence supports them

These ideas remain possible, but are not active roadmap commitments:

- Broad command parity, vector search, similarity caching, and framework-specific SDKs.
- CRDTs, active-active replication, partial resynchronization, and expanded Sentinel behavior.
- Dependency-tracked invalidation, expiration hooks, general MVCC snapshots, and arbitrary
  time-travel reads.
- HTTP/WebSocket gateway, proxy feature expansion, and additional binaries or dashboards.
- Full rewrite in another language. Rust is retained unless a measured constraint demonstrates
  that it prevents the validated product from meeting its requirements.

## Decision rule

Every new feature needs a named user, a concrete workflow, and an observable success criterion.
Every performance claim needs a reproducible benchmark. When those are absent, prefer simplifying
or measuring the current system over adding another subsystem.
