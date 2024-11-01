//! Implementation of a list where the elements are always sorted

use std::cmp::Ordering;

/// Trait used for sorting elements of a list.
///
/// Compared to Ord, this trait allows the sorting to be done not directly on the element
/// themselves, but on keys that can be extracted.
pub trait Sortable {
    type Key: Ord + ?Sized;

    fn key(&self) -> &Self::Key;
}

/// A list of nodes, sorted and without duplicates. This struct is a wrapper over a `Vec<TreeNode>`
/// that keeps the sorting invariant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SortedList<T>(Vec<T>);

impl<T: Sortable> SortedList<T> {
    /// Creates a new empty list
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Creates a sorted list from a vec of elements
    pub fn from_vec(mut vec: Vec<T>) -> Self {
        // Sort and remove duplicates
        vec.sort_by(|a, b| a.key().cmp(b.key()));
        vec.dedup_by(|a, b| a.key() == b.key());

        Self(vec)
    }

    /// Insert a new element inside an existing list, without overwriting existing ones.
    ///
    /// Return false if there is already an element with this key and the provided elment was not
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

    /// Remove the element with the given key from the list.
    ///
    /// Return false if the element was not present in the list, or true otherwise.
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

    /// Remove the element with the given name if the condition returns true.
    ///
    /// Return false if the element was not present in the list or if the condition returned false.
    /// Return true otherwise.
    pub fn remove_if<F: FnOnce(&T) -> bool>(&mut self, key: &T::Key, condition: F) -> bool {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(key))
        {
            Ok(idx) => {
                if condition(&self.0[idx]) {
                    self.0.remove(idx);
                    true
                } else {
                    false
                }
            }
            Err(_) => false,
        }
    }

    /// Find the element with the provided key in the list.
    pub fn find(&self, key: &T::Key) -> Option<&T> {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(key))
        {
            Ok(idx) => Some(&self.0[idx]),
            Err(_) => None,
        }
    }

    /// Find the element with the provided key in the list, return a mutable reference.
    pub fn find_mut(&mut self, key: &T::Key) -> Option<&mut T> {
        match self
            .0
            .binary_search_by(|candidate| candidate.key().cmp(key))
        {
            Ok(idx) => Some(&mut self.0[idx]),
            Err(_) => None,
        }
    }

    /// Return the length of the list
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Return true if the list contains no element
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator over [`TreeNode`], by reference.
    pub fn iter(&self) -> std::slice::Iter<T> {
        self.0.iter()
    }

    /// Iterate on two sorted lists with the same key types and apply functions on their items:
    ///
    /// - `fself` is applied to items that are only present in "self"
    /// - `fboth` is applied to items that present in both lists
    /// - `fother` is applied to items that are only present in "other"
    ///
    /// The function calls will be done in order, and the resulting vec will be sorted by the order
    /// of the input lists.
    pub fn iter_zip_map<U, FSelf, FBoth, FOther, Ret, Err>(
        &self,
        other: &SortedList<U>,
        mut fself: FSelf,
        mut fboth: FBoth,
        mut fother: FOther,
    ) -> Result<Vec<Ret>, Err>
    where
        U: Sortable<Key = T::Key>,
        FSelf: FnMut(&T) -> Result<Ret, Err>,
        FBoth: FnMut(&T, &U) -> Result<Ret, Err>,
        FOther: FnMut(&U) -> Result<Ret, Err>,
    {
        let mut ret = Vec::new();
        let mut self_iter = self.iter();
        let mut other_iter = other.iter();

        let mut self_item_opt = self_iter.next();
        let mut other_item_opt = other_iter.next();

        while let (Some(self_item), Some(other_item)) = (self_item_opt, other_item_opt) {
            match self_item.key().cmp(other_item.key()) {
                Ordering::Less => {
                    ret.push(fself(self_item)?);
                    self_item_opt = self_iter.next()
                }
                Ordering::Equal => {
                    ret.push(fboth(self_item, other_item)?);
                    self_item_opt = self_iter.next();
                    other_item_opt = other_iter.next();
                }
                Ordering::Greater => {
                    ret.push(fother(other_item)?);
                    other_item_opt = other_iter.next();
                }
            }
        }

        // Handle the remaining nodes that are present in an iterator and not the
        // other one
        while let Some(self_item) = self_item_opt {
            ret.push(fself(self_item)?);
            self_item_opt = self_iter.next();
        }

        while let Some(other_item) = other_item_opt {
            ret.push(fother(other_item)?);
            other_item_opt = other_iter.next();
        }

        Ok(ret)
    }

    /// Create a new [`SortedList`] from a vector that is already sorted
    ///
    /// If the vec is not sorted, this may result in undefined behavior
    pub fn unchecked_from_vec(vec: Vec<T>) -> Self {
        Self(vec)
    }

    /// Extend a list with the elements of another one, the lists are both relatively sorted.
    ///
    /// This means that the last element of self is smaller that the first element of other.
    pub fn unchecked_extend(&mut self, other: Self) {
        self.0.extend(other);
    }

    /// Convert a `Vec<SortedList>` into a `SortedList`, assuming that the lists inside the vec are
    /// already relatively sorted.
    ///
    /// This means that the last element of the list at index n is always smaller than the
    /// first element of the list at index n + 1.
    pub fn unchecked_flatten<I: IntoIterator<Item = Self>>(iter: I) -> Self {
        let flattened = iter.into_iter().flat_map(|sorted| sorted.0).collect();

        Self::from_vec(flattened)
    }
}

impl<T> IntoIterator for SortedList<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: Sortable> From<Vec<T>> for SortedList<T> {
    fn from(value: Vec<T>) -> Self {
        Self::from_vec(value)
    }
}

impl<T: Sortable> From<SortedList<T>> for Vec<T> {
    fn from(value: SortedList<T>) -> Self {
        value.0
    }
}

impl<T: Sortable + Clone> From<&[T]> for SortedList<T> {
    fn from(value: &[T]) -> Self {
        Self::from_vec(value.to_vec())
    }
}

impl<const N: usize, T: Sortable + Clone> From<[T; N]> for SortedList<T> {
    fn from(value: [T; N]) -> Self {
        Self::from_vec(value.to_vec())
    }
}

impl<T: Sortable> Default for SortedList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Sortable> SortedList<T> {
    /// Insert a new [`TreeNode`] inside an existing list, and eventually overwrite exisisting node.
    ///
    /// Return the replaced node if present, or None if there was no node with this name.
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

    use super::SortedList;

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

        let list = SortedList::from_vec(test_nodes);

        let reference: Vec<_> = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        assert!(list
            .0
            .iter()
            .zip(reference.iter())
            .all(|(a, b)| a.structural_eq(b)))
    }

    #[test]
    fn test_insertion() {
        let test_nodes = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        let mut list = SortedList::from_vec(test_nodes);

        assert!(!list.insert(F("b").into_node()));
        assert!(list.insert(D("e", vec![]).into_node()));
        assert!(!list.insert(D("a", vec![]).into_node()));
        assert!(list.insert(F("f3").into_node()));

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

        assert!(list
            .0
            .iter()
            .zip(reference.iter())
            .all(|(a, b)| a.structural_eq(b)))
    }

    #[test]
    fn test_replacement() {
        let test_nodes = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        let mut list = SortedList::from_vec(test_nodes);

        assert!(list.replace(F("b").into_node()).is_some());
        assert!(list.replace(D("e", vec![]).into_node()).is_none());
        assert!(list.replace(D("a", vec![]).into_node()).is_some());
        assert!(list.replace(F("f3").into_node()).is_none());

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

        assert!(list
            .0
            .iter()
            .zip(reference.iter())
            .all(|(a, b)| a.structural_eq(b)))
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

        let mut list = SortedList::from_vec(test_nodes);

        assert!(list.remove("e"));
        assert!(!list.remove("j"));
        assert!(!list.remove("e"));
        assert!(list.remove_if("f3", |node| node.kind() == NodeKind::File));
        assert!(!list.remove_if("a", |node| node.kind() == NodeKind::File));

        let reference: Vec<_> = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .into_iter()
            .map(|val| val.into_node())
            .collect();

        assert!(list
            .0
            .iter()
            .zip(reference.iter())
            .all(|(a, b)| a.structural_eq(b)))
    }
}
