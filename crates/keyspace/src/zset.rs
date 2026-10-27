//! Sorted set storage. Keeps two views in sync: `scores` (a `HashMap`, for
//! O(1) `ZSCORE`/membership checks) and `by_score` (a `BTreeSet`, kept
//! ordered by `(score, member)` at all times). Range queries and extreme-
//! element pops read off `by_score` directly instead of sorting on every
//! call — see `pop_extreme`'s docs for why that used to be a real
//! performance bug, not just a style choice.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Wraps an `f64` score with a total ordering (`f64::total_cmp`, stable
/// since Rust 1.62) so `(score, member)` pairs can live in a `BTreeSet` —
/// plain `f64` isn't `Ord` (NaN has no defined comparison). The command
/// layer rejects non-finite scores before they ever reach `ZSet`, but this
/// keeps the type itself panic-free regardless of that.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct ScoreKey(f64);

impl Eq for ScoreKey {}

impl PartialOrd for ScoreKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoreKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// `(score, member)`, in the order a `BTreeSet<Entry>` sorts them —
/// ascending by score, ties broken by member bytes, matching Redis.
type Entry = (ScoreKey, Bytes);

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ZSet {
    scores: HashMap<Bytes, f64>,
    by_score: BTreeSet<Entry>,
}

impl ZSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a member's score. Returns `true` if the member is
    /// new. Keeps `by_score` in lockstep: an update has to remove the old
    /// `(old_score, member)` entry before inserting the new one, since
    /// `BTreeSet` orders by the whole tuple — just overwriting wouldn't
    /// move it.
    pub fn insert(&mut self, member: Bytes, score: f64) -> bool {
        let is_new = match self.scores.insert(member.clone(), score) {
            Some(old_score) => {
                self.by_score.remove(&(ScoreKey(old_score), member.clone()));
                false
            }
            None => true,
        };
        self.by_score.insert((ScoreKey(score), member));
        is_new
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self.scores.remove(member) {
            Some(score) => {
                self.by_score.remove(&(ScoreKey(score), Bytes::copy_from_slice(member)));
                true
            }
            None => false,
        }
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(member).copied()
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Rough memory estimate for `maxmemory` accounting: member bytes plus
    /// a small constant per entry for the score + both indexes' overhead.
    pub fn approx_bytes(&self) -> usize {
        self.scores.keys().map(|m| m.len() + 24).sum::<usize>() + self.by_score.len() * 32
    }

    /// Members sorted ascending by `(score, member)`, matching Redis's
    /// tie-break-by-lexicographic-member-order rule. `O(n)` — `by_score`
    /// is already in this order, this just clones it out.
    pub fn sorted(&self) -> Vec<(Bytes, f64)> {
        self.by_score.iter().map(|(ScoreKey(s), m)| (m.clone(), *s)).collect()
    }

    /// Removes and returns the `count` lowest-scoring (`min`) or
    /// highest-scoring (`!min`) members, same tie-break rule as `sorted()`.
    /// `ZPOPMIN`/`ZPOPMAX`/`BZPOPMIN`/`BZPOPMAX`'s implementation.
    ///
    /// `O(count log n)` — `BTreeSet::pop_first`/`pop_last` are `O(log n)`
    /// each. This used to be a real bug: an earlier version sorted every
    /// member (`O(n log n)`) just to throw away all but `count` of them,
    /// then a later fix cut that to `O(n)` average via partial selection —
    /// still `O(n)` *per pop*, so draining a large set one element at a
    /// time stayed `O(n²)` overall. Maintaining `by_score` as an always-
    /// sorted `BTreeSet` (this version) removes that cost entirely: a pop
    /// no longer has to look at any member it isn't actually returning.
    pub fn pop_extreme(&mut self, count: usize, min: bool) -> Vec<(Bytes, f64)> {
        let count = count.min(self.scores.len());
        let mut popped = Vec::with_capacity(count);
        for _ in 0..count {
            let entry = if min { self.by_score.pop_first() } else { self.by_score.pop_last() };
            let Some((ScoreKey(score), member)) = entry else { break };
            self.scores.remove(&member);
            popped.push((member, score));
        }
        popped
    }
}

/// Keeps the on-disk/wire shape as a plain `(member, score)` list — the
/// `by_score` index is derived, not stored, so RDB/tiered-storage payloads
/// don't pay to persist the same data twice. Also means this type's
/// internal representation (adding `by_score`) didn't change what a
/// serialized `ZSet` looks like on the wire.
impl Serialize for ZSet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.scores.len()))?;
        for (member, score) in &self.scores {
            seq.serialize_element(&(member, score))?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for ZSet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let members = Vec::<(Bytes, f64)>::deserialize(deserializer)?;
        let mut z = ZSet::default();
        for (member, score) in members {
            z.insert(member, score);
        }
        Ok(z)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_by_score_then_member() {
        let mut z = ZSet::new();
        z.insert(Bytes::from("b"), 1.0);
        z.insert(Bytes::from("a"), 1.0);
        z.insert(Bytes::from("c"), 0.5);
        let members = z.sorted();
        let sorted: Vec<&[u8]> = members.iter().map(|(m, _)| m.as_ref()).collect();
        assert_eq!(sorted, vec![b"c".as_ref(), b"a".as_ref(), b"b".as_ref()]);
    }

    #[test]
    fn insert_reports_new_vs_update() {
        let mut z = ZSet::new();
        assert!(z.insert(Bytes::from("a"), 1.0));
        assert!(!z.insert(Bytes::from("a"), 2.0));
        assert_eq!(z.score(b"a"), Some(2.0));
    }

    /// Specifically exercises the lockstep-update bug this implementation
    /// has to avoid: updating a member's score must move its `by_score`
    /// entry, not leave a stale one sitting at the old score.
    #[test]
    fn updating_a_score_moves_it_in_sorted_order() {
        let mut z = ZSet::new();
        z.insert(Bytes::from("a"), 10.0);
        z.insert(Bytes::from("b"), 20.0);
        assert_eq!(z.sorted(), vec![(Bytes::from("a"), 10.0), (Bytes::from("b"), 20.0)]);

        z.insert(Bytes::from("a"), 30.0); // now higher than "b"
        assert_eq!(z.sorted(), vec![(Bytes::from("b"), 20.0), (Bytes::from("a"), 30.0)]);
        assert_eq!(z.len(), 2); // not 3 — no leftover stale entry
    }

    #[test]
    fn remove_drops_the_sorted_entry_too() {
        let mut z = ZSet::new();
        z.insert(Bytes::from("a"), 1.0);
        z.insert(Bytes::from("b"), 2.0);
        assert!(z.remove(b"a"));
        assert!(!z.remove(b"a")); // already gone
        assert_eq!(z.sorted(), vec![(Bytes::from("b"), 2.0)]);
    }

    #[test]
    fn pop_extreme_matches_sorted_for_min_and_max() {
        let mut z = ZSet::new();
        z.insert(Bytes::from("b"), 1.0);
        z.insert(Bytes::from("a"), 1.0); // ties with "b", breaks by member
        z.insert(Bytes::from("c"), 0.5);
        z.insert(Bytes::from("d"), 2.0);

        let mut for_min = z.clone();
        let popped_min = for_min.pop_extreme(2, true);
        assert_eq!(popped_min, vec![(Bytes::from("c"), 0.5), (Bytes::from("a"), 1.0)]);
        assert_eq!(for_min.len(), 2);

        let mut for_max = z.clone();
        let popped_max = for_max.pop_extreme(2, false);
        assert_eq!(popped_max, vec![(Bytes::from("d"), 2.0), (Bytes::from("b"), 1.0)]);
        assert_eq!(for_max.len(), 2);
    }

    #[test]
    fn pop_extreme_count_beyond_len_pops_everything_in_order() {
        let mut z = ZSet::new();
        z.insert(Bytes::from("x"), 3.0);
        z.insert(Bytes::from("y"), 1.0);
        let popped = z.pop_extreme(10, true);
        assert_eq!(popped, vec![(Bytes::from("y"), 1.0), (Bytes::from("x"), 3.0)]);
        assert!(z.is_empty());
    }

    #[test]
    fn pop_extreme_zero_count_is_a_noop() {
        let mut z = ZSet::new();
        z.insert(Bytes::from("a"), 1.0);
        assert_eq!(z.pop_extreme(0, true), Vec::new());
        assert_eq!(z.len(), 1);
    }

    #[test]
    fn serde_round_trip_preserves_members_and_scores() {
        let mut z = ZSet::new();
        z.insert(Bytes::from("a"), 1.5);
        z.insert(Bytes::from("b"), -2.0);
        let bytes = bincode::serialize(&z).unwrap();
        let restored: ZSet = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.sorted(), z.sorted());
    }
}
