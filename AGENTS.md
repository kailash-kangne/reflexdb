# Project Rules

Final Rust Codebase Audit, Fix & Optimization
Act as a senior Rust systems engineer and performance specialist. Audit the entire existing codebase and make it production-ready.
1. Find & Fix Issues
- Identify bugs, logic errors, race conditions, deadlocks, and edge cases.
- Fix all compiler errors, warnings, and Clippy issues where practical.
- Find panics, unnecessary `unwrap()` / `expect()`, and unsafe code risks.
- Fix incorrect error handling and resource leaks.

2. Optimize Performance
- Identify unnecessary allocations, cloning, copying, and heap usage.
- Optimize CPU, memory, I/O, locking, and async execution where relevant.
- Avoid unnecessary `Arc`, `Mutex`, `RwLock`, channels, and task spawning.
- Use efficient data structures and algorithms.
- Optimize only where measurable; avoid overengineering.

3. Improve Rust Quality
- Follow idiomatic, modern Rust practices.
- Use appropriate ownership, borrowing, lifetimes, and zero-copy techniques.
- Remove dead code, duplicated logic, unused dependencies, and unnecessary abstractions.
- Improve module boundaries, API design, and maintainability.
- Preserve existing functionality and backward compatibility unless a change is necessary.

4. Verify Everything
- Run `cargo fmt --check`.
- Run `cargo check --all-targets`.
- Run `cargo clippy --all-targets --all-features -- -D warnings` where supported.
- Run all existing tests and add tests for bugs fixed.
- Run `cargo test --all-targets` and relevant integration tests.
- Check dependency versions, feature flags, and security advisories where tooling is available.
- If applicable, run benchmarks before and after performance changes.

5. Rules
- First understand the architecture and trace critical execution paths.
- Make actual code changes; do not just provide recommendations.
- Do not rewrite the entire project or change working behavior unnecessarily.
- Do not silence warnings, weaken tests, or remove functionality just to make checks pass.
- Do not claim tests or benchmarks passed unless you actually ran them.
- If a command fails because of the environment or missing dependencies, report it clearly.
- Avoid adding dependencies unless there is a clear benefit.
- Prioritize correctness and safety over micro-optimizations.

6. Analyze and optimize this multithreaded Rust database for maximum performance and scalability, identify bottlenecks in CPU usage, memory allocation, locking, contention, and thread scheduling, eliminate unnecessary work without changing behavior, and benchmark every change against the baseline to verify measurable improvements.






## Learned so far (edit/remove freely)

See `docs/ARCHITECTURE.md` for the full design (concurrency model, persistence, tiering,
`LIVE`, single-flight, dependency choices) and `docs/FEATURES.md`/`docs/COMMANDS.md` for what's
shipped vs. planned. Facts not covered there, or worth re-stating for quick reference:

- Workspace: `cargo build` / `cargo test` / `cargo clippy --all-targets` from the repo root
  build/test all crates.
- Verify changes against a real running server with `redis-cli`, not just unit tests, before
  calling a feature done.
- Multi-line git commit messages: write to a temp file (`/tmp/commit_msg_*.txt`) and use
  `git commit -F <file>` — inline heredocs with quotes/apostrophes break the shell here.
- `Db::get(key)` deep-clones the stored `Value` — O(1) for `String`, O(n) for collections. Use
  `Db::with_value`/`with_value_mut` instead when you only need existence/type checking.
- Keyspace entries are `parking_lot::RwLock<Entry>` (reads of the same key run concurrently,
  writes are exclusive). LRU/LFU/sliding-TTL touch-ups go through `Db::touch_entry`, called after
  the read lock is dropped.
- `keyspace::Value::Stream` is `Box<Stream>`.
- `RESTORE`/RDB deserialization must use
  `bincode::DefaultOptions::new().with_fixint_encoding().with_limit(payload.len())` —
  `with_fixint_encoding()` is mandatory alongside `with_limit()` or it silently misreads
  length-prefixed fields (see `crates/persistence/src/rdb.rs`).
- RESP protocol limits (`crates/resp/src/codec.rs`): `MAX_MULTIBULK_COUNT`/`MAX_BULK_LEN` cap
  declared lengths before allocation. Don't remove without replacing — see
  `crates/resp/src/tests.rs` and `fuzz/fuzz_targets/resp_decode.rs`.
- Running the fuzz suite needs `rustup toolchain install nightly` +
  `cargo +nightly install cargo-fuzz`; from `fuzz/`: `cargo +nightly fuzz run <target> --
  -max_total_time=<secs>`.
- `Hash`/`Set` iteration order is randomized per-process — never assert ordering from
  `HKEYS`/`HVALS`/`HGETALL`/`SMEMBERS` in tests; sort first.
- `ZSet` keeps a `BTreeSet<(score, member)>` index alongside its `HashMap` so `ZPOPMIN`/
  `ZPOPMAX` are O(log n) and range scans are O(n) — see `docs/ARCHITECTURE.md` and
  `docs/BENCHMARKS.md` for the numbers behind this.
- `keyspace::Hash`/`Set`/`List` are packed small representations (`Vec`/`IndexMap`-backed)
  under 128 elements that promote one-way to `VecDeque`/`IndexMap`/`IndexSet` on growth —
  see `keyspace/src/collections.rs`. Serde is byte-identical to the old types (RDB/AOF safe).
- `docs/BENCHMARKS.md` is one measurement pass on one dev machine — re-measure before quoting
  numbers externally.
- Benchmarking two builds: use `benchmarks/ab.sh` (interleaved A/B rounds) + `compare.py`.
  Sequential "all of A then all of B" runs drift ±10–25% on a loaded dev machine and produce
  fake regressions; interleaving cancels it. Never trust a single run pair.
- `parse_memory` config values accept only `gb`/`mb`/`kb` suffixes — bare `1k`/`1m` are invalid.
- This machine runs corporate network extensions (Cisco AnyConnect socket filter, SentinelOne,
  FireEye, Zscaler) that RST non-loopback connections to unsigned dev binaries — e2e tests must
  assert "refused" broadly (DENIED *or* reset/timeout), never a specific reply.
