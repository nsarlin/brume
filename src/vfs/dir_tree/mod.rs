mod dir;
mod file;
mod sorted_node_list;

pub use dir::*;
pub use file::*;

use sorted_node_list::*;

use std::cmp::Ordering;

/// A directory, seen as a tree.
///
/// It is composed of metadata for the directory itself and a list of children.
#[derive(Debug, Clone)]
pub struct DirTree {
    info: DirInfo,
    children: SortedNodeList,
}

impl DirTree {
    /// Create a new directory with no child and the provided name
    pub fn new(name: &str) -> Self {
        Self {
            info: DirInfo::new(name),
            children: SortedNodeList::new(),
        }
    }

    /// Create a new directory with the provided name and child nodes
    pub fn new_with_children(name: &str, children: Vec<TreeNode>) -> Self {
        Self {
            info: DirInfo::new(name),
            children: SortedNodeList::from_vec(children),
        }
    }

    /// Insert a new child for this directory. If there is already a child with the same name,
    /// returns false.
    pub fn insert_child(&mut self, child: TreeNode) -> bool {
        self.children.insert(child)
    }

    /// Replace an existing existing child based on its name, or insert a new one.
    ///
    /// Return the replaced child if any, or None if there was no child with this name.
    pub fn replace_child(&mut self, child: TreeNode) -> Option<TreeNode> {
        self.children.replace(child)
    }

    pub fn name(&self) -> &str {
        self.info.name()
    }
}

/// A node in a File System tree. Can represent a directory or a file.
#[derive(Debug, Clone)]
pub enum TreeNode {
    Dir(DirTree),
    File(FileInfo),
}

impl TreeNode {
    /// Return the name of the file or directory represented by this node
    pub fn name(&self) -> &str {
        match self {
            TreeNode::File(file) => file.name(),
            TreeNode::Dir(dir) => dir.name(),
        }
    }

    /// Order two Nodes based on their name
    fn name_cmp(&self, other: &Self) -> Ordering {
        self.name().cmp(other.name())
    }

    /// Equality relation based uniquely on the name of the nodes
    fn name_eq(&self, other: &Self) -> bool {
        self.name().eq(other.name())
    }

    /// Compare the structure of trees. Two trees are structurally equals if they are composed of
    /// nodes with the same names and with similare relationships.
    pub fn structural_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TreeNode::Dir(dself), TreeNode::Dir(dother)) => {
                dself.name() == dother.name()
                    && dself.children.len() == dother.children.len()
                    && dself
                        .children
                        .iter()
                        .zip(dother.children.iter())
                        .all(|(child_self, child_other)| child_self.structural_eq(child_other))
            }
            (TreeNode::Dir(_), TreeNode::File(_)) | (TreeNode::File(_), TreeNode::Dir(_)) => false,
            (TreeNode::File(fself), TreeNode::File(fother)) => fself.name() == fother.name(),
        }
    }
}
