use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::dispatch::saturating_deadline;

/// Parsed `SET key value [EX s|PX ms|EXAT s|PXAT ms|KEEPTTL] [NX|XX] [GET]`
/// options.
#[derive(Debug, Default)]
pub struct SetOptions {
    pub expires_at: Option<Instant>,
    pub keep_ttl: bool,
    pub nx: bool,
    pub xx: bool,
    /// `GET`: return the old value (or nil) instead of `OK`. A non-string
    /// old value is a WRONGTYPE error and aborts the write, matching Redis.
    pub get: bool,
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
            "EXAT" | "PXAT" => {
                let raw = args.get(i + 1).ok_or(ParseError::Syntax)?;
                let ts: i64 =
                    std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok()).ok_or(ParseError::Syntax)?;
                if ts <= 0 {
                    return Err(ParseError::Syntax);
                }
                let target_ms = if word == "EXAT" { ts.saturating_mul(1000) } else { ts };
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
                    .unwrap_or(0);
                let delta = target_ms - now_ms;
                // A timestamp in the past means the key expires at once —
                // clamp to "now" rather than rejecting (same observable
                // result as Redis's "set then instantly expire").
                opts.expires_at = Some(if delta <= 0 {
                    Instant::now()
                } else {
                    saturating_deadline(Duration::from_millis(delta as u64))
                });
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
            "GET" => {
                opts.get = true;
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
