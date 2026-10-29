//! RDB-style point-in-time snapshots and an append-only command log (AOF)
//! for reflexdb. See `docs/05-persistence-tiering.md`.
//!
//! Neither format is byte-compatible with external RDB/AOF files —
//! that's an internal, version-coupled binary format we don't try to
//! replicate. reflexdb just needs to survive its own restarts.

pub mod aof;
pub mod history;
pub mod rdb;
