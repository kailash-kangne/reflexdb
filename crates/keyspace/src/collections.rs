//! Packed small representations for `Hash`/`Set`/`List`. Below
//! [`SMALL_MAX`] elements a collection is a linear-scanned `Vec` — no
//! per-element index/hash overhead, and lookups beat hashing at this
//! size anyway. The first insert past the threshold promotes to the
//! hashed representation; promotion is one-way (like Redis's
//! listpack→table conversion) so a collection hovering at the boundary
//! doesn't thrash between layouts.
//!
//! Serde emits the same map/seq wire shape the plain
//! `HashMap`/`IndexSet`/`VecDeque` types produced, so RDB/AOF bytes are
//! unchanged. `PartialEq` compares contents, not layout — a Small and a
//! Big holding the same elements are equal.

use std::collections::VecDeque;

use bytes::Bytes;
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Element-count threshold for the packed representation — mirrors
/// Redis's `*-max-listpack-entries` default.
const SMALL_MAX: usize = 128;

#[derive(Debug, Clone)]
pub enum Hash {
    Small(Vec<(Bytes, Bytes)>),
    Big(IndexMap<Bytes, Bytes>),
}

impl Hash {
    pub fn new() -> Self {
        Self::Small(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Small(v) => v.len(),
            Self::Big(m) => m.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn find(small: &[(Bytes, Bytes)], f: &[u8]) -> Option<usize> {
        small.iter().position(|(k, _)| k.as_ref() == f)
    }

    pub fn get(&self, f: &[u8]) -> Option<&Bytes> {
        match self {
            Self::Small(v) => Self::find(v, f).map(|i| &v[i].1),
            Self::Big(m) => m.get(f),
        }
    }

    pub fn get_mut(&mut self, f: &[u8]) -> Option<&mut Bytes> {
        match self {
            Self::Small(v) => Self::find(v, f).map(|i| &mut v[i].1),
            Self::Big(m) => m.get_mut(f),
        }
    }

    pub fn contains_key(&self, f: &[u8]) -> bool {
        match self {
            Self::Small(v) => Self::find(v, f).is_some(),
            Self::Big(m) => m.contains_key(f),
        }
    }

    pub fn insert(&mut self, f: Bytes, v: Bytes) -> Option<Bytes> {
        if let Self::Small(vec) = self {
            if let Some(i) = Self::find(vec, &f) {
                return Some(std::mem::replace(&mut vec[i].1, v));
            }
            if vec.len() >= SMALL_MAX {
                let m: IndexMap<Bytes, Bytes> = vec.drain(..).collect();
                *self = Self::Big(m);
            } else {
                vec.push((f, v));
                return None;
            }
        }
        match self {
            Self::Big(m) => m.insert(f, v),
            Self::Small(_) => unreachable!(),
        }
    }

    pub fn remove(&mut self, f: &[u8]) -> Option<Bytes> {
        match self {
            Self::Small(v) => Self::find(v, f).map(|i| v.swap_remove(i).1),
            Self::Big(m) => m.shift_remove(f),
        }
    }

    pub fn get_index(&self, i: usize) -> Option<(&Bytes, &Bytes)> {
        match self {
            Self::Small(v) => v.get(i).map(|(k, v)| (k, v)),
            Self::Big(m) => m.get_index(i),
        }
    }

    pub fn iter(&self) -> HashIter<'_> {
        match self {
            Self::Small(v) => HashIter::Small(v.iter()),
            Self::Big(m) => HashIter::Big(m.iter()),
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &Bytes> + '_ {
        self.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &Bytes> + '_ {
        self.iter().map(|(_, v)| v)
    }

    /// Packed repr only carries the `Vec`'s own header plus payload
    /// bytes; the map repr pays hashing overhead per field.
    pub fn approx_bytes(&self) -> usize {
        match self {
            Self::Small(v) => 24 + v.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>(),
            Self::Big(m) => m.iter().map(|(k, v)| k.len() + v.len() + 32).sum(),
        }
    }
}

impl Default for Hash {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for Hash {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().all(|(k, v)| other.get(k) == Some(v))
    }
}

pub enum HashIter<'a> {
    Small(std::slice::Iter<'a, (Bytes, Bytes)>),
    Big(indexmap::map::Iter<'a, Bytes, Bytes>),
}

impl<'a> Iterator for HashIter<'a> {
    type Item = (&'a Bytes, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(it) => it.next().map(|(k, v)| (k, v)),
            Self::Big(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Small(it) => it.size_hint(),
            Self::Big(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for HashIter<'_> {}

impl<'a> IntoIterator for &'a Hash {
    type Item = (&'a Bytes, &'a Bytes);
    type IntoIter = HashIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Serialize for Hash {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_map(self.iter())
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Vec::<(Bytes, Bytes)>::deserialize(d)?;
        Ok(if v.len() <= SMALL_MAX { Self::Small(v) } else { Self::Big(v.into_iter().collect()) })
    }
}

#[derive(Debug, Clone)]
pub enum Set {
    Small(Vec<Bytes>),
    Big(IndexSet<Bytes>),
}

impl Set {
    pub fn new() -> Self {
        Self::Small(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Small(v) => v.len(),
            Self::Big(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn find(small: &[Bytes], m: &[u8]) -> Option<usize> {
        small.iter().position(|e| e.as_ref() == m)
    }

    pub fn contains(&self, m: &[u8]) -> bool {
        match self {
            Self::Small(v) => Self::find(v, m).is_some(),
            Self::Big(s) => s.contains(m),
        }
    }

    /// Same semantics as `IndexSet::insert`: false if already present.
    pub fn insert(&mut self, m: Bytes) -> bool {
        if let Self::Small(v) = self {
            if Self::find(v, &m).is_some() {
                return false;
            }
            if v.len() >= SMALL_MAX {
                let s: IndexSet<Bytes> = v.drain(..).collect();
                *self = Self::Big(s);
            } else {
                v.push(m);
                return true;
            }
        }
        match self {
            Self::Big(s) => s.insert(m),
            Self::Small(_) => unreachable!(),
        }
    }

    /// `IndexSet::swap_remove` semantics — O(1), doesn't preserve order.
    pub fn swap_remove(&mut self, m: &[u8]) -> bool {
        match self {
            Self::Small(v) => Self::find(v, m).map(|i| v.swap_remove(i)).is_some(),
            Self::Big(s) => s.swap_remove(m),
        }
    }

    pub fn swap_remove_index(&mut self, i: usize) -> Option<Bytes> {
        match self {
            Self::Small(v) => (i < v.len()).then(|| v.swap_remove(i)),
            Self::Big(s) => s.swap_remove_index(i),
        }
    }

    pub fn get_index(&self, i: usize) -> Option<&Bytes> {
        match self {
            Self::Small(v) => v.get(i),
            Self::Big(s) => s.get_index(i),
        }
    }

    pub fn iter(&self) -> SetIter<'_> {
        match self {
            Self::Small(v) => SetIter::Small(v.iter()),
            Self::Big(s) => SetIter::Big(s.iter()),
        }
    }

    pub fn extend<I: IntoIterator<Item = Bytes>>(&mut self, it: I) {
        for m in it {
            self.insert(m);
        }
    }

    pub fn approx_bytes(&self) -> usize {
        match self {
            Self::Small(v) => 24 + v.iter().map(|b| b.len()).sum::<usize>(),
            Self::Big(s) => s.iter().map(|b| b.len() + 16).sum(),
        }
    }
}

impl Default for Set {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for Set {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().all(|m| other.contains(m))
    }
}

impl FromIterator<Bytes> for Set {
    fn from_iter<I: IntoIterator<Item = Bytes>>(it: I) -> Self {
        let mut s = Self::new();
        s.extend(it);
        s
    }
}

pub enum SetIter<'a> {
    Small(std::slice::Iter<'a, Bytes>),
    Big(indexmap::set::Iter<'a, Bytes>),
}

impl<'a> Iterator for SetIter<'a> {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(it) => it.next(),
            Self::Big(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Small(it) => it.size_hint(),
            Self::Big(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for SetIter<'_> {}

impl<'a> IntoIterator for &'a Set {
    type Item = &'a Bytes;
    type IntoIter = SetIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Serialize for Set {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for Set {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Vec::<Bytes>::deserialize(d)?;
        Ok(if v.len() <= SMALL_MAX { Self::Small(v) } else { Self::Big(v.into_iter().collect()) })
    }
}

#[derive(Debug, Clone)]
pub enum List {
    Small(Vec<Bytes>),
    Big(VecDeque<Bytes>),
}

impl List {
    pub fn new() -> Self {
        Self::Small(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Small(v) => v.len(),
            Self::Big(d) => d.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// vec.len() >= SMALL_MAX → promote before the next push.
    fn maybe_promote(&mut self) {
        if let Self::Small(v) = self {
            if v.len() >= SMALL_MAX {
                *self = Self::Big(v.drain(..).collect());
            }
        }
    }

    pub fn push_front(&mut self, v: Bytes) {
        match self {
            Self::Small(vec) => vec.insert(0, v),
            Self::Big(d) => d.push_front(v),
        }
        self.maybe_promote();
    }

    pub fn push_back(&mut self, v: Bytes) {
        match self {
            Self::Small(vec) => vec.push(v),
            Self::Big(d) => d.push_back(v),
        }
        self.maybe_promote();
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        match self {
            Self::Small(v) => (!v.is_empty()).then(|| v.remove(0)),
            Self::Big(d) => d.pop_front(),
        }
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        match self {
            Self::Small(v) => v.pop(),
            Self::Big(d) => d.pop_back(),
        }
    }

    pub fn get(&self, i: usize) -> Option<&Bytes> {
        match self {
            Self::Small(v) => v.get(i),
            Self::Big(d) => d.get(i),
        }
    }

    pub fn get_mut(&mut self, i: usize) -> Option<&mut Bytes> {
        match self {
            Self::Small(v) => v.get_mut(i),
            Self::Big(d) => d.get_mut(i),
        }
    }

    pub fn insert(&mut self, i: usize, v: Bytes) {
        match self {
            Self::Small(vec) => vec.insert(i.min(vec.len()), v),
            Self::Big(d) => d.insert(i.min(d.len()), v),
        }
        self.maybe_promote();
    }

    pub fn remove(&mut self, i: usize) -> Option<Bytes> {
        match self {
            Self::Small(v) => (i < v.len()).then(|| v.remove(i)),
            Self::Big(d) => d.remove(i),
        }
    }

    pub fn truncate(&mut self, len: usize) {
        match self {
            Self::Small(v) => v.truncate(len),
            Self::Big(d) => d.truncate(len),
        }
    }

    pub fn retain(&mut self, mut f: impl FnMut(&Bytes) -> bool) {
        match self {
            Self::Small(v) => v.retain(|b| f(b)),
            Self::Big(d) => d.retain(|b| f(b)),
        }
    }

    pub fn clear(&mut self) {
        match self {
            Self::Small(v) => v.clear(),
            Self::Big(d) => d.clear(),
        }
    }

    pub fn iter(&self) -> ListIter<'_> {
        match self {
            Self::Small(v) => ListIter::Small(v.iter()),
            Self::Big(d) => ListIter::Big(d.iter()),
        }
    }

    pub fn approx_bytes(&self) -> usize {
        match self {
            Self::Small(v) => 24 + v.iter().map(|b| b.len()).sum::<usize>(),
            Self::Big(d) => d.iter().map(|b| b.len() + 16).sum(),
        }
    }
}

impl Default for List {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for List {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl From<Vec<Bytes>> for List {
    fn from(v: Vec<Bytes>) -> Self {
        if v.len() <= SMALL_MAX {
            Self::Small(v)
        } else {
            Self::Big(v.into())
        }
    }
}

impl FromIterator<Bytes> for List {
    fn from_iter<I: IntoIterator<Item = Bytes>>(it: I) -> Self {
        let v: Vec<Bytes> = it.into_iter().collect();
        if v.len() <= SMALL_MAX {
            Self::Small(v)
        } else {
            Self::Big(v.into())
        }
    }
}

impl std::ops::Index<usize> for List {
    type Output = Bytes;
    fn index(&self, i: usize) -> &Bytes {
        match self {
            Self::Small(v) => &v[i],
            Self::Big(d) => &d[i],
        }
    }
}

impl std::ops::IndexMut<usize> for List {
    fn index_mut(&mut self, i: usize) -> &mut Bytes {
        match self {
            Self::Small(v) => &mut v[i],
            Self::Big(d) => &mut d[i],
        }
    }
}

pub enum ListIter<'a> {
    Small(std::slice::Iter<'a, Bytes>),
    Big(std::collections::vec_deque::Iter<'a, Bytes>),
}

impl<'a> Iterator for ListIter<'a> {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(it) => it.next(),
            Self::Big(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Small(it) => it.size_hint(),
            Self::Big(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for ListIter<'_> {}
impl DoubleEndedIterator for ListIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(it) => it.next_back(),
            Self::Big(it) => it.next_back(),
        }
    }
}

impl<'a> IntoIterator for &'a List {
    type Item = &'a Bytes;
    type IntoIter = ListIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Serialize for List {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for List {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Vec::<Bytes>::deserialize(d)?;
        Ok(if v.len() <= SMALL_MAX { Self::Small(v) } else { Self::Big(v.into_iter().collect()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn promotion_keeps_semantics_across_the_boundary() {
        // Cross the threshold both ways — one-way promote, ops still right.
        let mut h = Hash::new();
        for i in 0..SMALL_MAX + 5 {
            assert!(h.insert(Bytes::from(format!("f{i}")), Bytes::from(format!("v{i}"))).is_none());
            assert_eq!(matches!(h, Hash::Big(_)), i >= SMALL_MAX);
        }
        assert_eq!(h.len(), SMALL_MAX + 5);
        assert_eq!(h.get(b"f0").unwrap().as_ref(), b"v0");
        for i in 0..SMALL_MAX {
            h.remove(format!("f{i}").as_bytes());
        }
        assert_eq!(h.len(), 5);
        assert_eq!(h.get(b"f130").unwrap().as_ref(), b"v130");

        let mut l = List::new();
        for i in 0..SMALL_MAX + 3 {
            l.push_back(Bytes::from(format!("e{i}")));
        }
        assert!(matches!(l, List::Big(_)));
        assert_eq!(l.len(), SMALL_MAX + 3);
        assert_eq!(l.pop_front().unwrap().as_ref(), b"e0");
        assert_eq!(l[SMALL_MAX + 1].as_ref(), format!("e{}", SMALL_MAX + 2).as_bytes());

        let mut s = Set::new();
        for i in 0..SMALL_MAX + 2 {
            assert!(s.insert(Bytes::from(format!("m{i}"))));
        }
        assert!(!s.insert(Bytes::from("m0")));
        assert!(matches!(s, Set::Big(_)));
        assert!(s.swap_remove(b"m1"));
        assert_eq!(s.len(), SMALL_MAX + 1);
    }

    #[test]
    fn serde_is_layout_independent() {
        // Same contents, both layouts → identical bincode (RDB/AOF compat).
        let h_small = {
            let mut h = Hash::new();
            h.insert(Bytes::from("a"), Bytes::from("1"));
            h
        };
        let h_big = Hash::Big([(Bytes::from("a"), Bytes::from("1"))].into_iter().collect());
        assert_eq!(bincode::serialize(&h_small).unwrap(), bincode::serialize(&h_big).unwrap());
        let back: Hash = bincode::deserialize(&bincode::serialize(&h_big).unwrap()).unwrap();
        assert_eq!(back, h_small);

        let s_big: Set = (0..SMALL_MAX + 1).map(|i| Bytes::from(format!("m{i}"))).collect();
        assert!(matches!(s_big, Set::Big(_)));
        let s_back: Set = bincode::deserialize(&bincode::serialize(&s_big).unwrap()).unwrap();
        assert_eq!(s_back, s_big);
        assert!(matches!(s_back, Set::Big(_)));
    }
}
