use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::dispatch::saturating_deadline;

/// Parsed `SET key value [EX s|PX ms|KEEPTTL] [NX|XX]` options.
#[derive(Debug, Default)]
pub struct SetOptions {
    pub expires_at: Option<Instant>,
    pub keep_ttl: bool,
    pub nx: bool,
    pub xx: bool,
}

pub enum ParseError {
    Syntax,
}

pub fn parse(args: &[Bytes]) -> Result<SetOptions, ParseError> {
    let mut opts = SetOptions::default();
    let mut i = 0;
    while i < args.len() {
        let word = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match word.as_str() {
            "EX" | "PX" => {
                let raw = args.get(i + 1).ok_or(ParseError::Syntax)?;
                let n: i64 =
                    std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok()).ok_or(ParseError::Syntax)?;
                if n <= 0 {
                    return Err(ParseError::Syntax);
                }
                let dur = if word == "EX" {
                    Duration::from_secs(n as u64)
                } else {
                    Duration::from_millis(n as u64)
                };
                opts.expires_at = Some(saturating_deadline(dur));
                i += 2;
            }
            "KEEPTTL" => {
                opts.keep_ttl = true;
                i += 1;
            }
            "NX" => {
                opts.nx = true;
                i += 1;
            }
            "XX" => {
                opts.xx = true;
                i += 1;
            }
            _ => return Err(ParseError::Syntax),
        }
    }
    if opts.nx && opts.xx {
        return Err(ParseError::Syntax);
    }
    if opts.keep_ttl && opts.expires_at.is_some() {
        return Err(ParseError::Syntax);
    }
    Ok(opts)
}
