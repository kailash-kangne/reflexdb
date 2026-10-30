//! Minimal glob matcher for `SCAN ... MATCH pattern`, supporting the subset
//! of glob syntax: `*`, `?`, `[...]` character classes, and `\`-escaping
//! (matches Redis's `stringmatchlen`: `\x` matches the literal byte `x`,
//! including escaping `*`/`?`/`[`/`\` themselves; a trailing, unpaired `\`
//! matches itself literally).

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
    if pattern[p] == b'\\' {
        return match_escaped(pattern, p, ch);
    }
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

/// `pattern[p] == b'\\'`: the next byte (if any) is matched literally,
/// bypassing any special meaning it would otherwise have (`\*`, `\?`,
/// `\[`, `\\`). A trailing, unpaired `\` (nothing after it) matches
/// itself literally instead of being dropped.
fn match_escaped(pattern: &[u8], p: usize, ch: u8) -> MatchStep {
    match pattern.get(p + 1) {
        Some(&literal) => {
            if literal == ch {
                MatchStep::Matched(p + 2)
            } else {
                MatchStep::Mismatched
            }
        }
        None => {
            if ch == b'\\' {
                MatchStep::Matched(p + 1)
            } else {
                MatchStep::Mismatched
            }
        }
    }
}

/// Scans a `[...]` character class starting at `pattern[p] == b'['`.
/// Returns `Some((hit, next_p))` for a well-formed class (one with a
/// closing `]`), where `next_p` is the pattern index just past it, or
/// `None` for an unterminated class — the caller then falls back to
/// treating `pattern[p..]` as a literal tail (`MatchStep::LiteralTail`).
/// A `\]` inside the class matches a literal `]` rather than closing it
/// (and never starts an `a-z` range), same as `\` anywhere else.
fn class_matches(pattern: &[u8], p: usize, ch: u8) -> Option<(bool, usize)> {
    let mut i = p + 1;
    let negate = pattern.get(i) == Some(&b'^');
    if negate {
        i += 1;
    }
    let mut hit = false;
    loop {
        let b = *pattern.get(i)?;
        if b == b']' {
            i += 1;
            break;
        }
        if b == b'\\' {
            let literal = match pattern.get(i + 1) {
                Some(&next) => {
                    i += 2;
                    next
                }
                None => {
                    i += 1;
                    b'\\'
                }
            };
            if literal == ch {
                hit = true;
            }
            continue;
        }
        if pattern.get(i + 1) == Some(&b'-') && pattern.get(i + 2).is_some_and(|&c| c != b']') {
            let (lo, hi) = (b, pattern[i + 2]);
            if lo <= ch && ch <= hi {
                hit = true;
            }
            i += 3;
            continue;
        }
        if b == ch {
            hit = true;
        }
        i += 1;
    }
    Some((hit != negate, i))
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

    #[test]
    fn backslash_escapes_special_characters() {
        assert!(matches(b"a\\*b", b"a*b"));
        assert!(!matches(b"a\\*b", b"axb"));
        assert!(matches(b"a\\?b", b"a?b"));
        assert!(matches(b"a\\[b", b"a[b"));
        assert!(matches(b"a\\\\b", b"a\\b"));
    }

    #[test]
    fn trailing_unpaired_backslash_matches_itself() {
        assert!(matches(b"end\\", b"end\\"));
        assert!(!matches(b"end\\", b"end"));
    }

    #[test]
    fn backslash_escapes_inside_character_class() {
        assert!(matches(b"[\\]a]", b"]"));
        assert!(matches(b"[\\]a]", b"a"));
        assert!(!matches(b"[\\]a]", b"b"));
        // A lone trailing `\` inside an (unterminated) class still falls
        // back to the literal-tail path, same as any other malformed class.
        assert!(matches(b"[abc\\", b"[abc\\"));
    }
}
