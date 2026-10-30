//! Command dispatch: connection/string/generic commands (Phase 1), the
//! List/Hash/Set/ZSet collection types (Phase 2), Pub/Sub + `LIVE`/`UNLIVE`
//! reactivity (Phase 3), `MULTI`/`EXEC`/`WATCH` transactions plus Lua
//! scripting (Phase 4), and RDB/AOF persistence plus `maxmemory` eviction
//! (Phase 5).

mod acl;
mod client;
mod dispatch;
mod extended;
mod hash;
mod history;
mod info;
mod keys;
mod list;
mod live;
mod pattern;
mod pubsub;
mod range;
mod replication;
mod scan;
mod schedule;
mod script;
mod set;
mod set_opts;
mod singleflight;
mod snapshot;
mod stream;
mod string;
mod txn;
mod zset;

pub use dispatch::{
    dispatch, request_may_write, ClientState, MaxMemoryPolicy, NotifyFlags, Outcome, ServerContext,
};
pub use replication::ReplicaMessage;
pub use singleflight::SingleFlightLease;
