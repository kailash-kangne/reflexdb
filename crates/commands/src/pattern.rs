//! Minimal glob matcher for `SCAN ... MATCH pattern`, supporting the subset
//! of glob syntax: `*`, `?`, and
//! `[...]` character classes. No backslash-escaping support yet.

/// Returns true if `text` matches `pattern`.
pub fn matches(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut last_star: Option<usize> = None;
    let mut star_text: usize = 0;

    while t < text.len() {
        if p < pattern.len() {
            if pattern[p] == b'*' {
                last_star = Some(p);
                star_text = t;
                p += 1;
                continue;
            }
            match match_one(pattern, p, text[t]) {
                MatchStep::Matched(np) => {
                    p = np;
                    t += 1;
                    continue;
                }
                MatchStep::Mismatched => {}
                MatchStep::LiteralTail => return pattern[p..] == text[t..],
            }
        }
        if let Some(star) = last_star {
            p = star + 1;
            star_text += 1;
            t = star_text;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

enum MatchStep {
    Matched(usize),
    Mismatched,
    LiteralTail,
}

fn match_one(pattern: &[u8], p: usize, ch: u8) -> MatchStep {
    if pattern[p] == b'?' {
        return MatchStep::Matched(p + 1);
    }
    if pattern[p] == b'[' {
        return match class_matches(pattern, p, ch) {
            Some((true, np)) => MatchStep::Matched(np),
            Some((false, _)) => MatchStep::Mismatched,
            None => MatchStep::LiteralTail,
        };
    }
    if pattern[p] == ch {
        MatchStep::Matched(p + 1)
    } else {
        MatchStep::Mismatched
    }
}

fn class_matches(pattern: &[u8], p: usize, ch: u8) -> Option<(bool, usize)> {
    // pattern[p] == b'['
    let rest = &pattern[p + 1..];
    let close = rest.iter().position(|&b| b == b']')?;
    let class = &rest[..close];
    let np = p + 1 + close + 1;

    let (negate, class) = match class.first() {
        Some(b'^') => (true, &class[1..]),
        _ => (false, class),
    };

    let mut hit = false;
    let mut i = 0;
    while i < class.len() {
        if i + 2 < class.len() && class[i + 1] == b'-' {
            if class[i] <= ch && ch <= class[i + 2] {
                hit = true;
            }
            i += 3;
        } else {
            if class[i] == ch {
                hit = true;
            }
            i += 1;
        }
    }
    Some((hit != negate, np))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn star_and_question_mark() {
        assert!(matches(b"foo*", b"foobar"));
        assert!(matches(b"f?o", b"foo"));
        assert!(!matches(b"f?o", b"fooo"));
        assert!(matches(b"*", b"anything"));
    }

    #[test]
    fn character_class() {
        assert!(matches(b"[a-c]at", b"bat"));
        assert!(!matches(b"[a-c]at", b"zat"));
        assert!(matches(b"[^a-c]at", b"zat"));
    }

    #[test]
    fn wildcard_backtracking_handles_adversarial_pattern() {
        let pattern = b"*a*a*a*a*a*a*a*a*b";
        let text = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac";
        assert!(!matches(pattern, text));
    }

    #[test]
    fn malformed_open_class_matches_literal_bracket() {
        assert!(matches(b"[abc", b"[abc"));
        assert!(!matches(b"[abc", b"xabc"));
    }
}
