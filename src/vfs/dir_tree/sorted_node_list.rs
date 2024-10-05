use super::TreeNode;

/// A list of nodes, sorted. This struct is a wrapper over a `Vec<TreeNode>` that keeps the sorting
/// invariant.
#[derive(Debug, Clone)]
pub(super) struct SortedNodeList(Vec<TreeNode>);

impl SortedNodeList {
    /// Creates a new empty list
    pub(super) fn new() -> Self {
        Self(Vec::new())
    }

    /// Creates a list from a vec of [`TreeNode`]
    pub(super) fn from_vec(mut vec: Vec<TreeNode>) -> Self {
        // Sort and remove duplicates
        vec.sort_by(TreeNode::name_cmp);
        vec.dedup_by(|a, b| a.name_eq(b));

        Self(vec)
    }

    /// Insert a new [`TreeNode`] inside an existing list, without overwriting existing nodes.
    ///
    /// Return false if there is already a node with this name and the provided node was not
    /// inserted. Returns true otherwise.
    pub(super) fn insert(&mut self, value: TreeNode) -> bool {
        match self
            .0
            .binary_search_by(|candidate| candidate.name_cmp(&value))
        {
            Ok(_) => false,
            Err(idx) => {
                self.0.insert(idx, value);
                true
            }
        }
    }

    /// Insert a new [`TreeNode`] inside an existing list, and eventually overwrite exisisting node.
    ///
    /// Return the replaced node if present, or None if there was no node with this name.
    pub(super) fn replace(&mut self, value: TreeNode) -> Option<TreeNode> {
        match self
            .0
            .binary_search_by(|candidate| candidate.name_cmp(&value))
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

    /// Returns the length of the list
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns an iterator over [`TreeNode`], by reference.
    pub(super) fn iter(&self) -> std::slice::Iter<TreeNode> {
        self.0.iter()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test_utils::TestNode::{D, F},
        vfs::TreeNode,
    };

    use super::SortedNodeList;

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
        .iter()
        .map(TreeNode::from)
        .collect();

        let list = SortedNodeList::from_vec(test_nodes);

        let reference: Vec<_> = [D("a", vec![]), D("b", vec![]), F("f1"), F("f2")]
            .iter()
            .map(TreeNode::from)
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
            .iter()
            .map(TreeNode::from)
            .collect();

        let mut list = SortedNodeList::from_vec(test_nodes);

        assert!(!list.insert(F("b").into()));
        assert!(list.insert(D("e", vec![]).into()));
        assert!(!list.insert(D("a", vec![]).into()));
        assert!(list.insert(F("f3").into()));

        let reference: Vec<_> = vec![
            D("a", vec![]),
            D("b", vec![]),
            D("e", vec![]),
            F("f1"),
            F("f2"),
            F("f3"),
        ]
        .iter()
        .map(TreeNode::from)
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
            .iter()
            .map(TreeNode::from)
            .collect();

        let mut list = SortedNodeList::from_vec(test_nodes);

        assert!(list.replace(F("b").into()).is_some());
        assert!(list.replace(D("e", vec![]).into()).is_none());
        assert!(list.replace(D("a", vec![]).into()).is_some());
        assert!(list.replace(F("f3").into()).is_none());

        let reference: Vec<_> = vec![
            D("a", vec![]),
            F("b"),
            D("e", vec![]),
            F("f1"),
            F("f2"),
            F("f3"),
        ]
        .iter()
        .map(TreeNode::from)
        .collect();

        assert!(list
            .0
            .iter()
            .zip(reference.iter())
            .all(|(a, b)| a.structural_eq(b)))
    }
}
