//! Implementation of a Vec where the elements are always sorted

use std::{cmp::Ordering, collections::HashSet, hash::Hash};

use serde::{Deserialize, Serialize};

/// Trait used for sorting elements of a vec.
///
/// Compared to Ord, this trait allows the sorting to be done not directly on the element
/// themselves, but on keys that can be extracted.
pub trait Sortable {
    type Key: Ord + ?Sized;

    fn key(&self) -> &Self::Key;
}

impl<T: Sortable> Sortable for &T {
    type Key = T::Key;

    fn key(&self) -> &Self::Key {
        (*self).key()
    }
}

/// A vec of elements, sorted and without duplicates. This struct is a wrapper over a `Vec<T>`
/// that keeps the sorting invariant.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SortedVec<T>(Vec<T>);

impl<T: Sortable> SortedVec<T> {
    /// Creates a new empty vec
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Creates a sorted vec from a vec of elements
    pub fn from_vec(mut vec: Vec<T>) -> Self {
        // Sort and remove duplicates
        vec.sort_by(|a, b| a.key().cmp(b.key()));
        vec.dedup_by(|a, b| a.key() == b.key());

        Self(vec)
    }

    /// Merge 2 vecs and return a third one that is already sorted
    pub fn merge(self, other: Self) -> Self {
        let mut self_iter = self.0.into_iter();
        let mut other_iter = other.0.into_iter();
        let mut res = Vec::new();

        let mut self_next = self_iter.next();
        let mut other_next = other_iter.next();

        loop {
            match (self_next, other_next) {
                (Some(self_item), Some(other_item)) => {
                    match self_item.key().cmp(other_item.key()) {
                        Ordering::Less => {
                            res.push(self_item);
                            self_next = self_iter.next();
                            other_next = Some(other_item);
                        }
                        Ordering::Equal => {
                            res.push(self_item);
                            res.push(other_item);
                            self_next = self_iter.next();
                            other_next = other_iter.next();
                        }
                        Ordering::Greater => {
                            res.push(other_item);
                            self_next = Some(self_item);
                            other_next = other_iter.next();
                        }
                    }
                }
                (Some(self_item), None) => {
                    res.push(self_item);
                    self_next = self_iter.next();
                    other_next = other_iter.next();
                }
                (None, Some(other_item)) => {
                    res.push(other_item);
                    other_next = other_iter.next();
                    self_next = self_iter.next();
                }
                (None, None) => {
                    break;
                }
            }
        }

        Self::unchecked_from_vec(res)
    }

    /// Inserts a new element inside an existing vec, without overwriting existing ones.
    ///
    /// Returns false if there is already an element with this key and the provided element was not
    /// inserted. Return true otherwise.
    pub fn insert(&mut self, value: T) -> bool {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(value.key()))
        {
            Ok(_) => false,
            Err(idx) => {
                self.0.insert(idx, value);
                true
            }
        }
    }

    /// Removes the element with the given key from the vec.
    ///
    /// Returns false if the element was not present in the vec, or true otherwise.
    pub fn remove(&mut self, key: &T::Key) -> bool {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(key))
        {
            Ok(idx) => {
                self.0.remove(idx);
                true
            }
            Err(_) => false,
        }
    }

    /// Removes the element with the given name if the condition returns true.
    ///
    /// Returns None if the element was not present in the vec, Some(false) is the element was found
    /// but the condition returned false, or Some(true) if the condition returned true and the
    /// element was deleted.
    pub fn remove_if<F: FnOnce(&T) -> bool>(&mut self, key: &T::Key, condition: F) -> Option<bool> {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(key))
        {
            Ok(idx) => {
                if condition(&self.0[idx]) {
                    self.0.remove(idx);
                    Some(true)
                } else {
                    Some(false)
                }
            }
            Err(_) => None,
        }
    }

    /// Finds the element with the provided key in the vec.
    pub fn find(&self, key: &T::Key) -> Option<&T> {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(key))
        {
            Ok(idx) => Some(&self.0[idx]),
            Err(_) => None,
        }
    }

    /// Finds the element with the provided key in the vec, return a mutable reference.
    pub fn find_mut(&mut self, key: &T::Key) -> Option<&mut T> {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(key))
        {
            Ok(idx) => Some(&mut self.0[idx]),
            Err(_) => None,
        }
    }

    /// Returns the length of the vec
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the vec contains no element
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over T, by reference.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    /// Returns an iterator over T, by mutable reference.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, T> {
        self.0.iter_mut()
    }

    /// Removes the last element in the vec and returns it
    pub fn pop(&mut self) -> Option<T> {
        self.0.pop()
    }

    /// Iterates on two sorted vecs with the same key types and apply functions on their items:
    ///
    /// - `fself` is applied to items that are only present in "self"
    /// - `fboth` is applied to items that present in both vecs
    /// - `fother` is applied to items that are only present in "other"
    ///
    /// The function returns an iterator that yields the output of the closures.
    pub fn iter_zip_map<'a, U, FSelf, FBoth, FOther, Item>(
        &'a self,
        other: &'a SortedVec<U>,
        fself: FSelf,
        fboth: FBoth,
        fother: FOther,
    ) -> ZipMapIter<'a, T, U, FSelf, FBoth, FOther>
    where
        U: Sortable<Key = T::Key>,
        FSelf: FnMut(&T) -> Item,
        FBoth: FnMut(&T, &U) -> Item,
        FOther: FnMut(&U) -> Item,
    {
        let mut self_iter = self.iter();
        let mut other_iter = other.iter();

        ZipMapIter {
            self_item: self_iter.next(),
            other_item: other_iter.next(),
            self_iter,
            other_iter,
            fself,
            fboth,
            fother,
        }
    }

    /// Remove duplicates in the sorted vec, using another key than the one from the [`Sortable`]
    /// trait.
    ///
    /// # Example
    /// ```
    /// use brume::sorted_vec::SortedVec;
    /// use brume::update::*;
    /// use brume::vfs::VirtualPathBuf;
    ///
    /// let mut list = SortedVec::new();
    ///
    /// let update_a = VfsDiff::file_modified(VirtualPathBuf::new("/dir/a").unwrap());
    /// let applicable_a = ApplicableUpdate::new(UpdateTarget::Local, &update_a);
    /// list.insert(applicable_a);
    ///
    /// let update_b = VfsDiff::file_created(VirtualPathBuf::new("/dir/a").unwrap());
    /// let applicable_b = ApplicableUpdate::new(UpdateTarget::Local, &update_b);
    /// list.insert(applicable_b.clone());
    ///
    /// let update_c = VfsDiff::file_created(VirtualPathBuf::new("/dir/a").unwrap());
    /// let applicable_c = ApplicableUpdate::new(UpdateTarget::Remote, &update_c);
    /// list.insert(applicable_c.clone());
    ///
    /// let update_d = VfsDiff::file_created(VirtualPathBuf::new("/dir/d").unwrap());
    /// let applicable_d = ApplicableUpdate::new(UpdateTarget::Remote, &update_d);
    /// list.insert(applicable_d.clone());
    ///
    /// let mut simplified = list.dedup_by(|update| (update.path().to_owned(), update.target()));
    /// assert_eq!(simplified.len(), 3);
    /// let first = simplified.pop().unwrap();
    /// assert_eq!(first, applicable_d);
    /// let second = simplified.pop().unwrap();
    /// assert_eq!(second, applicable_c);
    /// let third = simplified.pop().unwrap();
    /// assert_eq!(third, applicable_b);
    /// ```
    pub fn dedup_by<K: Hash + Eq, F: Fn(&T) -> K>(self, f: F) -> Self {
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for elem in self.into_iter() {
            if seen.insert(f(&elem)) {
                result.push(elem);
            }
        }

        // Ok because if the input is sorted, the output will be too
        SortedVec::unchecked_from_vec(result)
    }

    /// Creates a new [`SortedVec`] from a vector that is already sorted
    ///
    /// If the vec is not sorted, this may result in undefined behavior
    pub fn unchecked_from_vec(vec: Vec<T>) -> Self {
        Self(vec)
    }

    /// Extends a vec with the elements of another one, the vecs are both relatively sorted.
    ///
    /// This means that the last element of self is smaller that the first element of other.
    pub fn unchecked_extend<I: IntoIterator<Item = T>>(&mut self, other: I) {
        self.0.extend(other);
    }

    /// Converts a `Vec<SortedVec>` into a `SortedVec`, assuming that the vecs inside the vec are
    /// already relatively sorted.
    ///
    /// This means that the last element of the vec at index n is always smaller than the
    /// first element of the vec at index n + 1.
    pub fn unchecked_flatten<I: IntoIterator<Item = Self>>(iter: I) -> Self {
        let flattened = iter.into_iter().flat_map(|sorted| sorted.0).collect();

        Self::from_vec(flattened)
    }
}

impl<T> IntoIterator for SortedVec<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// An iterator that zips two [`SortedVec`], Created by the [`iter_zip_map`] method.
///
/// [`iter_zip_map`]: SortedVec::iter_zip_map
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct ZipMapIter<'a, T, U, FSelf, FBoth, FOther> {
    self_iter: std::slice::Iter<'a, T>,
    other_iter: std::slice::Iter<'a, U>,
    fself: FSelf,
    fboth: FBoth,
    fother: FOther,

    self_item: Option<&'a T>,
    other_item: Option<&'a U>,
}

impl<'a, T, U, FSelf, FBoth, FOther, Item> Iterator for ZipMapIter<'a, T, U, FSelf, FBoth, FOther>
where
    T: Sortable,
    U: Sortable<Key = T::Key>,
    FSelf: FnMut(&T) -> Item,
    FBoth: FnMut(&T, &U) -> Item,
    FOther: FnMut(&U) -> Item,
{
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.self_item, self.other_item) {
            (Some(self_item), Some(other_item)) => match self_item.key().cmp(other_item.key()) {
                Ordering::Less => {
                    self.self_item = self.self_iter.next();
                    Some((self.fself)(self_item))
                }
                Ordering::Equal => {
                    self.self_item = self.self_iter.next();
                    self.other_item = self.other_iter.next();
                    Some((self.fboth)(self_item, other_item))
                }
                Ordering::Greater => {
                    self.other_item = self.other_iter.next();
                    Some((self.fother)(other_item))
                }
            },

            // Handle the case where one vec is longer that the other
            (Some(self_item), None) => {
                self.self_item = self.self_iter.next();
                Some((self.fself)(self_item))
            }
            (None, Some(other_item)) => {
                self.other_item = self.other_iter.next();
                Some((self.fother)(other_item))
            }
            (None, None) => None,
        }
    }
}
impl<T: Sortable> From<Vec<T>> for SortedVec<T> {
    fn from(value: Vec<T>) -> Self {
        Self::from_vec(value)
    }
}

impl<T: Sortable> From<SortedVec<T>> for Vec<T> {
    fn from(value: SortedVec<T>) -> Self {
        value.0
    }
}

impl<T: Sortable + Clone> From<&[T]> for SortedVec<T> {
    fn from(value: &[T]) -> Self {
        Self::from_vec(value.to_vec())
    }
}

impl<const N: usize, T: Sortable + Clone> From<[T; N]> for SortedVec<T> {
    fn from(value: [T; N]) -> Self {
        Self::from_vec(value.to_vec())
    }
}

impl<T: Sortable> Default for SortedVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Sortable> SortedVec<T> {
    /// Insert a new element inside an existing vec, and eventually overwrite exisisting elements.
    ///
    /// Return the replaced element if present, or None if there was no element with this key.
    pub fn replace(&mut self, value: T) -> Option<T> {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(value.key()))
        {
            Ok(idx) => {
                let prev = self.0[idx].clone();
                self.0[idx] = value;
                Some(prev)
            }
            Err(idx) => {
                self.0.insert(idx, value);
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test_utils::TestNode::{D, F},
        vfs::dir_tree::NodeKind,
    };

    use super::SortedVec;

    #[test]
    fn test_ordering_creation() {
        let test_nodes = vec![
            F("f1"),
            D("a", vec![]),
            D("f1", vec![]),
            F("f2"),
            F("a"),
            D("b", vec![]),
        ]
        .into_iter()
        .map(|val| val.into_node())
        .collect();

        let vec = SortedVec::from_vec(test_nodes);

        let reference: Vec<_> = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        assert!(
            vec.0
                .iter()
                .zip(reference.iter())
                .all(|(a, b)| a.structural_eq(b))
        )
    }

    #[test]
    fn test_insertion() {
        let test_nodes = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        let mut vec = SortedVec::from_vec(test_nodes);

        assert!(!vec.insert(F("b").into_node()));
        assert!(vec.insert(D("e", vec![]).into_node()));
        assert!(!vec.insert(D("a", vec![]).into_node()));
        assert!(vec.insert(F("f3").into_node()));

        let reference: Vec<_> = vec![
            D("a", vec![]),
            D("b", vec![]),
            D("e", vec![]),
            F("f1"),
            F("f2"),
            F("f3"),
        ]
        .into_iter()
        .map(|val| val.into_node())
        .collect();

        assert!(
            vec.0
                .iter()
                .zip(reference.iter())
                .all(|(a, b)| a.structural_eq(b))
        )
    }

    #[test]
    fn test_replacement() {
        let test_nodes = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        let mut vec = SortedVec::from_vec(test_nodes);

        assert!(vec.replace(F("b").into_node()).is_some());
        assert!(vec.replace(D("e", vec![]).into_node()).is_none());
        assert!(vec.replace(D("a", vec![]).into_node()).is_some());
        assert!(vec.replace(F("f3").into_node()).is_none());

        let reference: Vec<_> = vec![
            D("a", vec![]),
            F("b"),
            D("e", vec![]),
            F("f1"),
            F("f2"),
            F("f3"),
        ]
        .into_iter()
        .map(|val| val.into_node())
        .collect();

        assert!(
            vec.0
                .iter()
                .zip(reference.iter())
                .all(|(a, b)| a.structural_eq(b))
        )
    }

    #[test]
    fn test_removal() {
        let test_nodes = vec![
            D("a", vec![]),
            D("b", vec![]),
            D("e", vec![]),
            F("f1"),
            F("f2"),
            F("f3"),
        ]
        .into_iter()
        .map(|val| val.into_node())
        .collect();

        let mut vec = SortedVec::from_vec(test_nodes);

        assert!(vec.remove("e"));
        assert!(!vec.remove("j"));
        assert!(!vec.remove("e"));
        assert!(
            vec.remove_if("f3", |node| node.kind() == NodeKind::File)
                .unwrap()
        );
        assert_eq!(
            vec.remove_if("a", |node| node.kind() == NodeKind::File),
            Some(false)
        );

        let reference: Vec<_> = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        assert!(
            vec.0
                .iter()
                .zip(reference.iter())
                .all(|(a, b)| a.structural_eq(b))
        )
    }

    #[test]
    fn test_pop() {
        let test_nodes = vec![D("a", vec![]), D("b", vec![]), D("c", vec![])]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        let mut vec = SortedVec::from_vec(test_nodes);

        assert_eq!(vec.pop().unwrap().name(), "c");
        assert_eq!(vec.pop().unwrap().name(), "b");
        assert_eq!(vec.pop().unwrap().name(), "a");
        assert!(vec.pop().is_none());
    }

    #[test]
    fn test_merge() {
        let nodes1 = vec![F("ab"), F("ad"), F("be"), F("bf")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();
        let vec1 = SortedVec::from_vec(nodes1);

        let nodes2 = vec![F("ac"), F("ae")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();
        let vec2 = SortedVec::from_vec(nodes2);

        let reference: Vec<_> = vec![F("ab"), F("ac"), F("ad"), F("ae"), F("be"), F("bf")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        let vec = vec1.merge(vec2);

        assert_eq!(vec.len(), reference.len());
        assert!(
            vec.0
                .iter()
                .zip(reference.iter())
                .all(|(a, b)| a.structural_eq(b))
        )
    }
}
