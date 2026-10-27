//! Shared index-range handling for `LRANGE`/`ZRANGE`-style commands.

use bytes::Bytes;

pub fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Convert Redis-style (possibly negative, possibly out-of-range) start/stop
/// indices into a clamped `[lo, hi]` inclusive range over a sequence of `len`.
pub fn normalize(start: i64, stop: i64, len: i64) -> (i64, i64) {
    let norm = |i: i64| -> i64 {
        if i < 0 {
            (len + i).max(0)
        } else {
            i
        }
    };
    let lo = norm(start).min(len);
    let hi = norm(stop).min(len - 1);
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::normalize;

    #[test]
    fn negative_indices_wrap_from_the_end() {
        assert_eq!(normalize(0, -1, 5), (0, 4));
        assert_eq!(normalize(-3, -1, 5), (2, 4));
        assert_eq!(normalize(0, 100, 5), (0, 4));
        assert_eq!(normalize(3, 1, 5), (3, 1));
    }
}
