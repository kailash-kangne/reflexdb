//! Shared option-parsing and reply-shaping for the `SCAN`-family commands
//! (`HSCAN`/`SSCAN`/`ZSCAN`). Each one replies `[cursor, [elements...]]`.
//!
//! `SCAN` paginates for real: the cursor is an index into the sorted list of
//! matching keys (sorted because key iteration order isn't stable across
//! calls), so `COUNT` bounds each page. `HSCAN`/`SSCAN`/`ZSCAN` still walk
//! their whole collection and always return `"0"` — `COUNT` is parsed for
//! client compatibility but doesn't limit them yet.

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

impl ScanOptions {
    pub fn count(&self) -> Option<usize> {
        self.count
    }

    pub fn has_type_filter(&self) -> bool {
        self.type_filter.is_some()
    }
}

pub struct ScanReply {
    cursor: u64,
    items: Vec<Value>,
}

impl ScanReply {
    pub fn done(items: Vec<Value>) -> Self {
        Self::page(0, items)
    }

    pub fn page(next_cursor: u64, items: Vec<Value>) -> Self {
        Self { cursor: next_cursor, items }
    }

    pub fn into_value(self) -> Value {
        Value::Array(vec![Value::bulk(self.cursor.to_string()), Value::Array(self.items)])
    }
}
