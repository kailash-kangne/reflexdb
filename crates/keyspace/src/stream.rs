//! Stream storage (`XADD`/`XRANGE`/`XREAD`/`XGROUP`/`XREADGROUP`/`XACK`/
//! `XPENDING`). Scope note: this covers the command surface the project
//! actually implements — `XCLAIM`/`XAUTOCLAIM`/`XINFO`/`XSETID`/`BLOCK`ing
//! reads/`MINID` trimming/exclusive range bounds (`(id`) are **not**
//! implemented. Entries are kept in a plain `BTreeMap` ordered by ID, which
//! is what gives `XRANGE`/`XREAD`-since-id their ordering for free (no
//! separate index needed) at the cost of being an in-memory structure, not
//! Redis's own radix-tree-of-listpacks (`stream.c`) — fine at the scale
//! this project targets, same trade-off as `ZSet` re-sorting on every
//! range query.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A stream entry ID: `<ms>-<seq>`, ordered first by `ms` then `seq` —
/// `derive(Ord)` on a two-field tuple struct gives exactly that ordering
/// for free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };

    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// The next representable ID after this one — `seq + 1`, or `ms + 1,
    /// seq 0` if `seq` is already saturated. Used for "exclusive start"
    /// range scans (`XRANGE`'s bounds are inclusive, so an exclusive-of-id
    /// lower bound for `XREAD`'s `id` argument is implemented as
    /// `range(id.next()..)`).
    pub fn next(self) -> Self {
        match self.seq.checked_add(1) {
            Some(seq) => Self { ms: self.ms, seq },
            None => Self { ms: self.ms.saturating_add(1), seq: 0 },
        }
    }

    pub fn to_display_string(self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    /// Parses a complete `<ms>-<seq>` or bare `<ms>` (seq defaults to
    /// `default_seq`, so callers can pick `0` for a range start and
    /// `u64::MAX` for a range end — matches real Redis's own bare-number
    /// range-bound behavior) ID. Does **not** accept `*`/`<ms>-*` (see
    /// `parse_for_add`) or `-`/`+` (handled by the caller, since those are
    /// only meaningful as `XRANGE` bounds, not as an ID in general).
    pub fn parse(s: &str, default_seq: u64) -> Option<Self> {
        match s.split_once('-') {
            Some((ms, seq)) => Some(Self { ms: ms.parse().ok()?, seq: seq.parse().ok()? }),
            None => Some(Self { ms: s.parse().ok()?, seq: default_seq }),
        }
    }
}

/// What the caller asked `XADD` to use as the new entry's ID.
pub enum IdSpec {
    /// `*` — fully automatic: `now_ms`, with `seq` bumped if another entry
    /// already exists at that same millisecond.
    Auto,
    /// `<ms>-*` — automatic sequence number within a caller-chosen `ms`.
    AutoSeq(u64),
    /// `<ms>-<seq>` — fully explicit; must be greater than `last_id`.
    Explicit(StreamId),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum XaddError {
    /// The given (or, for `AutoSeq`, the implied) ID is not greater than
    /// the stream's current last ID — matches Redis's
    /// `ERR The ID specified in XADD is equal or smaller than the target
    /// stream top item`.
    NotMonotonic,
    /// Not produced by `Stream::add` itself — a convenience variant so
    /// callers (`commands::stream::cmd_xadd`) can report "this key holds
    /// something other than a stream" through the same `Result` as the ID
    /// validation error, instead of a separate enum.
    WrongType,
}

/// A still-unacknowledged delivery of one entry to one consumer in one
/// group — Redis calls this the PEL (pending entries list).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingEntry {
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConsumerGroup {
    pub last_delivered_id: StreamId,
    pub pending: BTreeMap<StreamId, PendingEntry>,
    /// Consumers known to this group (via `XREADGROUP` or `XGROUP
    /// CREATECONSUMER`), even ones with an empty PEL right now.
    pub consumers: HashSet<Bytes>,
}

/// `(id, fields)` pairs as returned by `Stream::range`/`range_rev`/`after`/
/// `read_group` — named mainly to keep clippy's `type_complexity` lint
/// quiet, not because it's reused for anything beyond that.
pub type Entries = Vec<(StreamId, Vec<(Bytes, Bytes)>)>;

/// `XPENDING key group`'s summary-form result: `(count, min_id, max_id,
/// per_consumer_counts)`.
pub type PendingSummary = (usize, Option<StreamId>, Option<StreamId>, Vec<(Bytes, usize)>);

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Stream {
    entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    last_id: StreamId,
    /// Highest ID ever removed by `XDEL` — reported by real Redis's
    /// `XINFO STREAM` (not implemented here) but kept anyway since it's
    /// essentially free to maintain and documents stream state correctly.
    max_deleted_id: StreamId,
    /// Total entries ever added, including ones since trimmed/deleted —
    /// distinct from `entries.len()`, same distinction real Redis makes.
    entries_added: u64,
    groups: HashMap<Bytes, ConsumerGroup>,
}

impl Stream {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn last_id(&self) -> StreamId {
        self.last_id
    }

    pub fn max_deleted_id(&self) -> StreamId {
        self.max_deleted_id
    }

    pub fn entries_added(&self) -> u64 {
        self.entries_added
    }

    pub fn approx_bytes(&self) -> usize {
        self.entries
            .values()
            .map(|fields| fields.iter().map(|(f, v)| f.len() + v.len() + 16).sum::<usize>() + 24)
            .sum::<usize>()
            + self.groups.values().map(|g| g.pending.len() * 48 + g.consumers.len() * 24).sum::<usize>()
    }

    /// Resolves `spec` against the stream's current `last_id` and, if
    /// valid, inserts the entry and returns its final ID.
    pub fn add(
        &mut self,
        spec: IdSpec,
        now_ms: u64,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Result<StreamId, XaddError> {
        let id = match spec {
            IdSpec::Auto => {
                if now_ms > self.last_id.ms {
                    StreamId { ms: now_ms, seq: 0 }
                } else {
                    // Clock didn't advance (or went backward) since the
                    // last entry — stay monotonic by bumping `seq` at the
                    // same `ms` instead, same as real Redis.
                    self.last_id.next()
                }
            }
            IdSpec::AutoSeq(ms) => {
                if ms == self.last_id.ms {
                    self.last_id.next()
                } else {
                    StreamId { ms, seq: 0 }
                }
            }
            IdSpec::Explicit(id) => id,
        };
        // `0-0` is never valid (even as the very first entry) — matches
        // real Redis's "ID must be greater than 0-0" — which this single
        // `<=` check already covers, since a brand-new stream's `last_id`
        // starts at `StreamId::ZERO` (`0-0`).
        if id <= self.last_id {
            return Err(XaddError::NotMonotonic);
        }
        self.entries.insert(id, fields);
        self.last_id = id;
        self.entries_added += 1;
        Ok(id)
    }

    /// Inclusive `[start, end]` range, oldest-first, capped at `count`.
    pub fn range(&self, start: StreamId, end: StreamId, count: Option<usize>) -> Entries {
        if start > end {
            return Vec::new();
        }
        let iter = self.entries.range(start..=end).map(|(id, fields)| (*id, fields.clone()));
        match count {
            Some(n) => iter.take(n).collect(),
            None => iter.collect(),
        }
    }

    /// Same as `range`, but newest-first (`XREVRANGE`).
    pub fn range_rev(&self, start: StreamId, end: StreamId, count: Option<usize>) -> Entries {
        if start > end {
            return Vec::new();
        }
        let iter = self.entries.range(start..=end).rev().map(|(id, fields)| (*id, fields.clone()));
        match count {
            Some(n) => iter.take(n).collect(),
            None => iter.collect(),
        }
    }

    /// Entries strictly after `after`, oldest-first, capped at `count` —
    /// what plain `XREAD` (no consumer group) scans; `XREADGROUP ... >`
    /// has its own copy of this logic in `read_group` since it also needs
    /// to record deliveries in the PEL as it goes.
    pub fn after(&self, after: StreamId, count: Option<usize>) -> Entries {
        let iter = self.entries.range(after.next()..).map(|(id, fields)| (*id, fields.clone()));
        match count {
            Some(n) => iter.take(n).collect(),
            None => iter.collect(),
        }
    }

    /// `XDEL` — removes the given IDs if present. Deliberately does
    /// **not** delete the key even if this empties the stream: unlike
    /// every other collection type here, a stream with zero entries is
    /// still a valid stream (it may still have consumer groups attached),
    /// matching real Redis's documented behavior.
    pub fn del(&mut self, ids: &[StreamId]) -> usize {
        let mut removed = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                removed += 1;
                if *id > self.max_deleted_id {
                    self.max_deleted_id = *id;
                }
            }
        }
        removed
    }

    /// `XTRIM`/`XADD ... MAXLEN` — drops the oldest entries until at most
    /// `maxlen` remain. Always exact (`~` approximate trimming isn't
    /// distinguished from `=` exact trimming — see `docs/02-data-
    /// structures.md`'s small-collection-threshold precedent for the same
    /// kind of simplification). Returns the number of entries removed.
    pub fn trim_maxlen(&mut self, maxlen: usize) -> usize {
        let excess = self.entries.len().saturating_sub(maxlen);
        let to_remove: Vec<StreamId> = self.entries.keys().take(excess).copied().collect();
        for id in &to_remove {
            self.entries.remove(id);
            if *id > self.max_deleted_id {
                self.max_deleted_id = *id;
            }
        }
        to_remove.len()
    }

    pub fn group(&self, name: &[u8]) -> Option<&ConsumerGroup> {
        self.groups.get(name)
    }

    /// `(group name, last_delivered_id)` for every group — used by
    /// `BGREWRITEAOF` (`persistence::aof::reconstruct_commands`) to
    /// restore each group's read position via `XGROUP CREATE ... SETID`
    /// after rewriting a stream's entries.
    pub fn group_starts(&self) -> Vec<(Bytes, StreamId)> {
        self.groups.iter().map(|(name, g)| (name.clone(), g.last_delivered_id)).collect()
    }

    /// `XGROUP CREATE key group id`. `start` is normally resolved from
    /// `$` (current `last_id`) or an explicit ID by the caller before
    /// this is called. Returns `false` if the group already exists.
    pub fn create_group(&mut self, name: Bytes, start: StreamId) -> bool {
        if self.groups.contains_key(&name) {
            return false;
        }
        self.groups.insert(
            name,
            ConsumerGroup { last_delivered_id: start, pending: BTreeMap::new(), consumers: HashSet::new() },
        );
        true
    }

    pub fn destroy_group(&mut self, name: &[u8]) -> bool {
        self.groups.remove(name).is_some()
    }

    /// `XGROUP SETID key group id`. Returns `false` if the group doesn't
    /// exist.
    pub fn set_group_id(&mut self, name: &[u8], id: StreamId) -> bool {
        match self.groups.get_mut(name) {
            Some(g) => {
                g.last_delivered_id = id;
                true
            }
            None => false,
        }
    }

    /// `XGROUP CREATECONSUMER`. Returns `None` if the group doesn't
    /// exist, `Some(true)` if the consumer is new, `Some(false)` if it
    /// already existed.
    pub fn create_consumer(&mut self, group: &[u8], consumer: Bytes) -> Option<bool> {
        let g = self.groups.get_mut(group)?;
        Some(g.consumers.insert(consumer))
    }

    /// `XGROUP DELCONSUMER` — also drops every pending entry owned by
    /// that consumer, returning how many were dropped (the count real
    /// Redis's `DELCONSUMER` reports). `None` if the group doesn't exist.
    pub fn delete_consumer(&mut self, group: &[u8], consumer: &[u8]) -> Option<usize> {
        let g = self.groups.get_mut(group)?;
        g.consumers.remove(consumer);
        let before = g.pending.len();
        g.pending.retain(|_, p| p.consumer != consumer);
        Some(before - g.pending.len())
    }

    /// `XREADGROUP GROUP group consumer ... STREAMS key id`. `id = None`
    /// means `>` (new, never-delivered entries); `Some(after)` means
    /// "this consumer's own already-pending entries with ID > `after`"
    /// (`0` re-reads its whole PEL). Returns `None` if the group doesn't
    /// exist. `ack_immediately` is `XREADGROUP ... NOACK`: the entries
    /// are delivered but never recorded in the PEL at all.
    pub fn read_group(
        &mut self,
        group: &[u8],
        consumer: Bytes,
        after: Option<StreamId>,
        count: Option<usize>,
        now_ms: u64,
        ack_immediately: bool,
    ) -> Option<Entries> {
        let entries_snapshot = self.entries.clone();
        let g = self.groups.get_mut(group)?;
        g.consumers.insert(consumer.clone());

        let results = match after {
            None => {
                // `>`: everything after what this group has delivered so
                // far, advancing `last_delivered_id` as we go.
                let iter = entries_snapshot.range(g.last_delivered_id.next()..);
                let picked: Entries = match count {
                    Some(n) => iter.take(n).map(|(id, f)| (*id, f.clone())).collect(),
                    None => iter.map(|(id, f)| (*id, f.clone())).collect(),
                };
                if let Some((last, _)) = picked.last() {
                    g.last_delivered_id = *last;
                }
                if !ack_immediately {
                    for (id, _) in &picked {
                        g.pending.insert(
                            *id,
                            PendingEntry {
                                consumer: consumer.clone(),
                                delivery_time_ms: now_ms,
                                delivery_count: 1,
                            },
                        );
                    }
                }
                picked
            }
            Some(after) => {
                // Re-delivering this consumer's own PEL: ids already
                // assigned to `consumer`, greater than `after`. Bumps
                // delivery_count/delivery_time on each one re-sent, same
                // as real Redis.
                let ids: Vec<StreamId> = g
                    .pending
                    .range(after.next()..)
                    .filter(|(_, p)| p.consumer == consumer)
                    .map(|(id, _)| *id)
                    .take(count.unwrap_or(usize::MAX))
                    .collect();
                let mut picked = Vec::with_capacity(ids.len());
                for id in ids {
                    // The entry may have since been `XDEL`ed/trimmed —
                    // real Redis reports those as a nil-fields entry
                    // rather than silently dropping them; same here.
                    let fields = entries_snapshot.get(&id).cloned().unwrap_or_default();
                    if let Some(p) = g.pending.get_mut(&id) {
                        p.delivery_time_ms = now_ms;
                        p.delivery_count += 1;
                    }
                    picked.push((id, fields));
                }
                picked
            }
        };
        Some(results)
    }

    /// `XACK key group id...` — removes the given IDs from the group's
    /// PEL, regardless of which consumer owned them. Returns the number
    /// actually removed (ids with no pending entry don't count, matching
    /// real Redis).
    pub fn ack(&mut self, group: &[u8], ids: &[StreamId]) -> usize {
        let Some(g) = self.groups.get_mut(group) else { return 0 };
        ids.iter().filter(|id| g.pending.remove(id).is_some()).count()
    }

    /// `XPENDING key group` (summary form): `(count, min_id, max_id,
    /// per_consumer_counts)`. `None` if the group doesn't exist.
    pub fn pending_summary(&self, group: &[u8]) -> Option<PendingSummary> {
        let g = self.groups.get(group)?;
        if g.pending.is_empty() {
            return Some((0, None, None, Vec::new()));
        }
        let min = *g.pending.keys().next().unwrap();
        let max = *g.pending.keys().next_back().unwrap();
        let mut per_consumer: HashMap<Bytes, usize> = HashMap::new();
        for p in g.pending.values() {
            *per_consumer.entry(p.consumer.clone()).or_default() += 1;
        }
        let mut counts: Vec<(Bytes, usize)> = per_consumer.into_iter().collect();
        counts.sort_by(|a, b| a.0.cmp(&b.0));
        Some((g.pending.len(), Some(min), Some(max), counts))
    }

    /// `XPENDING key group start end count [consumer]` (extended form):
    /// `(id, consumer, idle_ms, delivery_count)` for each matching
    /// pending entry, oldest-first. `None` if the group doesn't exist.
    pub fn pending_detail(
        &self,
        group: &[u8],
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer_filter: Option<&[u8]>,
        now_ms: u64,
    ) -> Option<Vec<(StreamId, Bytes, u64, u64)>> {
        let g = self.groups.get(group)?;
        if start > end {
            return Some(Vec::new());
        }
        Some(
            g.pending
                .range(start..=end)
                .filter(|(_, p)| consumer_filter.is_none_or(|c| p.consumer == c))
                .take(count)
                .map(|(id, p)| {
                    (*id, p.consumer.clone(), now_ms.saturating_sub(p.delivery_time_ms), p.delivery_count)
                })
                .collect(),
        )
    }

    /// Everything still pending for `consumer` across the whole group —
    /// what `XREADGROUP`'s "re-read my own PEL from 0" shares its
    /// implementation with, exposed here for completeness/tests.
    #[cfg(test)]
    fn pending_for(&self, group: &[u8], consumer: &[u8]) -> Vec<StreamId> {
        self.groups
            .get(group)
            .map(|g| g.pending.iter().filter(|(_, p)| p.consumer == consumer).map(|(id, _)| *id).collect())
            .unwrap_or_default()
    }
}

/// Also used directly: `*` isn't representable as a plain `StreamId`, so
/// `XADD`'s own argument parsing (not `StreamId::parse`) handles `*`/
/// `<ms>-*`/explicit IDs via this.
pub fn parse_for_add(s: &[u8]) -> Option<IdSpec> {
    if s == b"*" {
        return Some(IdSpec::Auto);
    }
    let s = std::str::from_utf8(s).ok()?;
    if let Some(ms_part) = s.strip_suffix("-*") {
        return Some(IdSpec::AutoSeq(ms_part.parse().ok()?));
    }
    StreamId::parse(s, 0).map(IdSpec::Explicit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(k: &str, v: &str) -> Vec<(Bytes, Bytes)> {
        vec![(Bytes::from(k.to_string()), Bytes::from(v.to_string()))]
    }

    #[test]
    fn auto_ids_are_monotonic_even_with_a_stuck_clock() {
        let mut s = Stream::new();
        let id1 = s.add(IdSpec::Auto, 100, f("a", "1")).unwrap();
        assert_eq!(id1, StreamId::new(100, 0));
        // Clock didn't move — must still bump past id1.
        let id2 = s.add(IdSpec::Auto, 100, f("a", "2")).unwrap();
        assert_eq!(id2, StreamId::new(100, 1));
        let id3 = s.add(IdSpec::Auto, 50, f("a", "3")).unwrap(); // clock went backward
        assert_eq!(id3, StreamId::new(100, 2));
    }

    #[test]
    fn explicit_id_must_be_strictly_greater_than_last() {
        let mut s = Stream::new();
        s.add(IdSpec::Explicit(StreamId::new(5, 0)), 0, f("a", "1")).unwrap();
        assert_eq!(
            s.add(IdSpec::Explicit(StreamId::new(5, 0)), 0, f("a", "2")),
            Err(XaddError::NotMonotonic)
        );
        assert_eq!(
            s.add(IdSpec::Explicit(StreamId::new(4, 9)), 0, f("a", "2")),
            Err(XaddError::NotMonotonic)
        );
        assert!(s.add(IdSpec::Explicit(StreamId::new(5, 1)), 0, f("a", "2")).is_ok());
    }

    #[test]
    fn range_and_del_and_trim() {
        let mut s = Stream::new();
        for i in 1..=5u64 {
            s.add(IdSpec::Explicit(StreamId::new(i, 0)), 0, f("n", &i.to_string())).unwrap();
        }
        assert_eq!(s.len(), 5);
        let all = s.range(StreamId::MIN, StreamId::MAX, None);
        assert_eq!(all.len(), 5);
        assert_eq!(all[0].0, StreamId::new(1, 0));

        let rev = s.range_rev(StreamId::MIN, StreamId::MAX, Some(2));
        assert_eq!(
            rev.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![StreamId::new(5, 0), StreamId::new(4, 0)]
        );

        assert_eq!(s.del(&[StreamId::new(3, 0)]), 1);
        assert_eq!(s.len(), 4);
        assert_eq!(s.max_deleted_id(), StreamId::new(3, 0));

        let removed = s.trim_maxlen(2);
        assert_eq!(removed, 2);
        assert_eq!(s.len(), 2);
        let remaining: Vec<StreamId> =
            s.range(StreamId::MIN, StreamId::MAX, None).into_iter().map(|(id, _)| id).collect();
        assert_eq!(remaining, vec![StreamId::new(4, 0), StreamId::new(5, 0)]);
    }

    #[test]
    fn consumer_group_delivery_and_ack() {
        let mut s = Stream::new();
        for i in 1..=3u64 {
            s.add(IdSpec::Explicit(StreamId::new(i, 0)), 0, f("n", &i.to_string())).unwrap();
        }
        assert!(s.create_group(Bytes::from_static(b"g"), StreamId::ZERO));
        assert!(!s.create_group(Bytes::from_static(b"g"), StreamId::ZERO)); // duplicate

        let delivered = s.read_group(b"g", Bytes::from_static(b"c1"), None, None, 1000, false).unwrap();
        assert_eq!(delivered.len(), 3);
        assert_eq!(s.pending_for(b"g", b"c1").len(), 3);

        // A second consumer asking for `>` sees nothing new.
        let more = s.read_group(b"g", Bytes::from_static(b"c2"), None, None, 1000, false).unwrap();
        assert!(more.is_empty());

        // c1 re-reads its own PEL from the start.
        let replay =
            s.read_group(b"g", Bytes::from_static(b"c1"), Some(StreamId::ZERO), None, 2000, false).unwrap();
        assert_eq!(replay.len(), 3);

        assert_eq!(s.ack("g".as_bytes(), &[StreamId::new(1, 0), StreamId::new(2, 0)]), 2);
        assert_eq!(s.pending_for(b"g", b"c1").len(), 1);

        let (count, min, max, _) = s.pending_summary(b"g").unwrap();
        assert_eq!(count, 1);
        assert_eq!(min, Some(StreamId::new(3, 0)));
        assert_eq!(max, Some(StreamId::new(3, 0)));

        assert_eq!(s.delete_consumer(b"g", b"c1"), Some(1));
        assert!(s.pending_for(b"g", b"c1").is_empty());
    }

    #[test]
    fn parse_for_add_handles_star_forms() {
        assert!(matches!(parse_for_add(b"*"), Some(IdSpec::Auto)));
        assert!(matches!(parse_for_add(b"5-*"), Some(IdSpec::AutoSeq(5))));
        assert!(matches!(parse_for_add(b"5-3"), Some(IdSpec::Explicit(id)) if id == StreamId::new(5, 3)));
        assert!(parse_for_add(b"not-a-number-*").is_none());
    }
}
