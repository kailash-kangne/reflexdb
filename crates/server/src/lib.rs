//! Shared between the `reflexdb-server` and `reflexdb-sentinel` binaries —
//! per `docs/06-ha-replication-cluster.md` §2's decision, Sentinel ships as
//! a second binary sharing most of its code with the main server, not a
//! separate codebase. Today that shared part is config parsing and the
//! tiny RESP client Sentinel uses to talk to the nodes it monitors.

pub mod config;
pub mod resp_client;
