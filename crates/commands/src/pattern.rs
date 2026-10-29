//! Minimal glob matcher for `SCAN ... MATCH pattern`, supporting the subset
//! of glob syntax: `*`, `?`, and
//! `[...]` character classes. No backslash-escaping support yet.

pub fn matches(pattern: &[u8], text: &[u8]) -> bool {
    matches_from(pattern, text)
}

fn matches_from(pattern: &[u8], text: &[u8]) -> bool {
    match pattern.first() {
        None => text.is_empty(),
        Some(b'*') => {
            // Collapse consecutive '*' and try every possible split point.
            let rest = &pattern[1..];
            if rest.first() == Some(&b'*') {
                return matches_from(rest, text);
            }
            (0..=text.len()).any(|i| matches_from(rest, &text[i..]))
        }
        Some(b'?') => !text.is_empty() && matches_from(&pattern[1..], &text[1..]),
        Some(b'[') => {
            let Some(close) = pattern.iter().position(|&b| b == b']') else {
                return pattern == text;
            };
            if text.is_empty() {
                return false;
            }
            let class = &pattern[1..close];
            if class_matches(class, text[0]) {
                matches_from(&pattern[close + 1..], &text[1..])
            } else {
                false
            }
        }
        Some(&c) => !text.is_empty() && text[0] == c && matches_from(&pattern[1..], &text[1..]),
    }
}

fn class_matches(class: &[u8], ch: u8) -> bool {
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
    hit != negate
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
}
