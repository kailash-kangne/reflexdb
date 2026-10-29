# Product direction: recoverable live state

## The bet

Test whether developers of long-running workflows need one fast shared store that helps them
answer three questions after a failure:

1. What changed?
2. Which step changed it, and what did the state look like before?
3. How can the workflow resume from a known-good point?

AI agents are one possible starting audience, not the definition of the product. The hypothesis is
that recovery and inspection can be more valuable than another generic cache or memory service.
That has not been validated. Do not claim the category is new or that ReflexDB already provides
safe workflow rollback: the current prototype's `GETAT`/`HISTORY` is opt-in and string-only, and
`LIVE` is not a workflow checkpoint protocol.

## What success means

Choose one workload and measure it with equivalent correctness and durability requirements. A
defensible product win could be faster recovery or simpler debugging for the validated workflow,
while staying within an acceptable latency and resource budget. It could also be a measurable
throughput, tail-latency, RAM, or disk-cost advantage on a workload customers actually run.

The win must be reproducible and useful to the buyer. Measurements that omit persistence,
replication, realistic key sizes, or operational costs cannot establish it. See
[BENCHMARKS.md](BENCHMARKS.md) for the current limitations and measurement plan.

## Customer discovery

Interview developers who operate long-running jobs, agent workflows, or multi-step pipelines.
Ask about their most recent state recovery incident, how they found the bad state, how they
recovered, what it cost, and which systems they already use. Ask them to walk through the existing
workflow. Avoid leading with ReflexDB's feature list.

Signals to continue:

- The same failure/recovery pain recurs across unrelated teams.
- Existing workarounds are expensive or unreliable, and the developer can name a concrete
  reason they'd try a replacement or add a new component.
- At least three teams agree to run a prototype against a real workflow.
- They can integrate the prototype quickly and can explain what guarantees they need before
  production use.

If interviews instead show that buyers mainly need lower cache cost or latency, pivot the initial
scope to that job and measure it directly. Do not assume both products need to be built together.

## Prototype boundary

The first prototype should demonstrate one workflow end to end: write state with a run/step
identity, observe changes, inspect prior state, recover to a defined checkpoint, and resume. Keep
the interface small and make failure semantics explicit. Measure integration effort and recovery
time with design partners.

Do not build vector search, automatic memory extraction, broad framework integrations, active-
active replication, or complete command parity for this experiment. Add only what a design partner
needs to run the prototype safely.

## Engineering principles

- Keep Rust for now. Consider another language only after profiling or hiring/deployment evidence
  shows Rust is blocking the chosen product.
- Treat RESP compatibility as an adoption aid; publish a tested compatibility matrix instead of
  claiming complete command or behavioral compatibility.
- Preserve a single clear atomic commit boundary across current values, history, persistence,
  replication, and change notifications before promising recovery semantics.
- Measure memory with process RSS and workload-level bytes per key, not only hand-counted value
  sizes. Measure disk usage and write amplification as well as RAM saved by tiering.
- Bound queues, history retention, and background work so resource use remains predictable.
- Prefer measured fixes over broad rewrites or speculative architecture.

## What to show customers

Lead with a short failure-and-recovery demo, then show reproducible measurements for the exact
workload and compatibility details. Publish the test machine, versions, dataset, concurrency,
durability settings, and raw results. Avoid unqualified performance or cost claims.
