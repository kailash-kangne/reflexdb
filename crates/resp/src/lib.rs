//! Hand-rolled RESP2/RESP3 codec for reflexdb.

mod codec;
mod value;

#[cfg(test)]
mod tests;

pub use codec::{encode_reply, encode_rfx_reply, Request, RequestDecoder, RespError, Wire};
pub use value::{Protocol, Value};
