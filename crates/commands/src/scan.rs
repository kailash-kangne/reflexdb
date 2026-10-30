//! Shared option-parsing and reply-shaping for the `SCAN`-family commands
//! (`HSCAN`/`SSCAN`/`ZSCAN`). Each one replies `[cursor, [elements...]]`.
//!
//! This is a simplified, single-pass "scan": since we don't yet maintain a
//! real incremental cursor over a resizing hash table, every call walks the
//! whole collection and always returns cursor `"0"` (i.e. "done"). `COUNT`
//! is parsed for client compatibility but doesn't limit the batch size yet.

use bytes::Bytes;

use crate::pattern;
use resp::Value;

#[derive(Debug, Default)]
pub struct ScanOptions {
    pattern: Option<Vec<u8>>,
    count: Option<usize>,
    /// `TYPE <name>` — only meaningful to the keyspace-wide `SCAN`, ignored
    /// (but still parsed, for client compatibility) by `HSCAN`/`SSCAN`/`ZSCAN`.
    type_filter: Option<String>,
}

pub fn parse_opts(args: &[Bytes]) -> Result<ScanOptions, &'static str> {
    let mut opts = ScanOptions::default();
    let mut i = 0;
    while i < args.len() {
        let word = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match word.as_str() {
            "MATCH" => {
                let pat = args.get(i + 1).ok_or("ERR syntax error")?;
                opts.pattern = Some(pat.to_vec());
                i += 2;
            }
            "COUNT" => {
                let n = args.get(i + 1).ok_or("ERR syntax error")?;
                opts.count = std::str::from_utf8(n).ok().and_then(|s| s.parse().ok());
                if opts.count.is_none() {
                    return Err("ERR value is not an integer or out of range");
                }
                i += 2;
            }
            "TYPE" => {
                let t = args.get(i + 1).ok_or("ERR syntax error")?;
                opts.type_filter = Some(String::from_utf8_lossy(t).to_ascii_lowercase());
                i += 2;
            }
            _ => return Err("ERR syntax error"),
        }
    }
    Ok(opts)
}

pub fn matches_pattern(opts: &ScanOptions, text: &[u8]) -> bool {
    match &opts.pattern {
        Some(pat) => pattern::matches(pat, text),
        None => true,
    }
}

pub fn matches_type(opts: &ScanOptions, type_name: &str) -> bool {
    match &opts.type_filter {
        Some(t) => t == type_name,
        None => true,
    }
}

pub struct ScanReply {
    items: Vec<Value>,
}

impl ScanReply {
    pub fn done(items: Vec<Value>) -> Self {
        Self { items }
    }

    pub fn into_value(self) -> Value {
        Value::Array(vec![Value::bulk(&b"0"[..]), Value::Array(self.items)])
    }
}
