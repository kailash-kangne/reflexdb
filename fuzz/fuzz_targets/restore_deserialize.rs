//! Fuzz target for `RESTORE`'s deserialization path
//! (`bincode::DefaultOptions::new().with_limit(...).deserialize::<keyspace::Value>`,
//! see `commands::snapshot::cmd_restore`'s doc comment) — any Redis client
//! can send `RESTORE key ttl <arbitrary bytes>`, so this payload is fully
//! attacker-controlled and must never panic or hang, only ever return
//! `Ok`/`Err`. Also covers `persistence::rdb::deserialize` (`SAVE`/`BGSAVE`
//! file loading and `PSYNC` full-resync), which uses the same
//! size-bounded config for the same reason.
#![no_main]

use bincode::Options;
use keyspace::Value;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Must match `commands::snapshot::cmd_restore`'s exact config
    // (`with_fixint_encoding` + `with_limit`) — see its doc comment for
    // why both are required together.
    let _ = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(data.len() as u64)
        .deserialize::<Value>(data);
});
