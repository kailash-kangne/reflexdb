//! Fuzz target for `resp::RequestDecoder` — the first thing untrusted bytes
//! off the wire hit, so it's the highest-value target to fuzz: it should
//! never panic no matter what garbage a client sends, only ever return
//! `Ok`/`Err` from `decode`.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let mut decoder = resp::RequestDecoder::default();
    // Keep decoding until the buffer can't yield another frame or errors
    // out — a single fuzz input may contain several pipelined requests.
    loop {
        match decoder.decode(&mut buf) {
            Ok(Some(_request)) => continue,
            Ok(None) | Err(_) => break,
        }
    }
});
