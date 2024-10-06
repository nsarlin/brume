//! Tree structure used for recursive directory walking

mod dir;
mod file;
mod sorted_node_list;

pub use dir::*;
pub use file::*;

use sorted_node_list::*;

use std::cmp::Ordering;

use super::{
    DiffError, DirCreatedDiff, IsModified, ModificationState, VfsNodeDiff, VirtualPath,
    VirtualPathBuf,
};

/// A directory, seen as a tree.
///
/// It is composed of metadata for the directory itself and a list of children.
#[derive(Debug, Clone)]
pub struct DirTree<SyncInfo> {
    info: DirInfo<SyncInfo>,
    children: SortedNodeList<SyncInfo>,
}

impl<SyncInfo> DirTree<SyncInfo> {
    /// Create a new directory with no child and the provided name
    pub fn new(name: &str, sync: SyncInfo) -> Self {
        Self {
            info: DirInfo::new(name, sync),
            children: SortedNodeList::new(),
        }
    }

    /// Create a new directory with the provided name and child nodes
    pub fn new_with_children(
        name: &str,
        sync: SyncInfo,
        children: Vec<TreeNode<SyncInfo>>,
    ) -> Self {
        Self {
            info: DirInfo::new(name, sync),
            children: SortedNodeList::from_vec(children),
        }
    }

    /// Insert a new child for this directory. If there is already a child with the same name,
    /// returns false.
    pub fn insert_child(&mut self, child: TreeNode<SyncInfo>) -> bool {
        self.children.insert(child)
    }

    pub fn name(&self) -> &str {
        self.info.name()
    }

    pub fn sync_info(&self) -> &SyncInfo {
        self.info.sync_info()
    }
}

impl<SyncInfo: Clone> DirTree<SyncInfo> {
    /// Replace an existing existing child based on its name, or insert a new one.
    ///
    /// Return the replaced child if any, or None if there was no child with this name.
    pub fn replace_child(&mut self, child: TreeNode<SyncInfo>) -> Option<TreeNode<SyncInfo>> {
        self.children.replace(child)
    }
}

impl<SyncInfo> DirTree<SyncInfo> {
    pub fn structural_eq<OtherSyncInfo>(&self, other: &DirTree<OtherSyncInfo>) -> bool {
        self.name() == other.name()
            && self.children.len() == other.children.len()
            && self
                .children
                .iter()
                .zip(other.children.iter())
                .all(|(child_self, child_other)| child_self.structural_eq(child_other))
    }
}

/// A node in a File System tree. Can represent a directory or a file.
#[derive(Debug, Clone)]
pub enum TreeNode<SyncInfo> {
    Dir(DirTree<SyncInfo>),
    File(FileInfo<SyncInfo>),
}

impl<SyncInfo> TreeNode<SyncInfo> {
    /// Return the name of the file or directory represented by this node
    pub fn name(&self) -> &str {
        match self {
            TreeNode::File(file) => file.name(),
            TreeNode::Dir(dir) => dir.name(),
        }
    }

    pub fn is_dir(&self) -> bool {
        match self {
            TreeNode::Dir(_) => true,
            TreeNode::File(_) => false,
        }
    }

    pub fn is_file(&self) -> bool {
        match self {
            TreeNode::Dir(_) => false,
            TreeNode::File(_) => true,
        }
    }

    /// Order two Nodes based on their name
    fn name_cmp<OtherSyncInfo>(&self, other: &TreeNode<OtherSyncInfo>) -> Ordering {
        self.name().cmp(other.name())
    }

    /// Equality relation based uniquely on the name of the nodes
    fn name_eq<OtherSyncInfo>(&self, other: &TreeNode<OtherSyncInfo>) -> bool {
        self.name().eq(other.name())
    }

    /// Compare the structure of trees. Two trees are structurally equals if they have the same
    /// shape and are composed of nodes with the same names.
    pub fn structural_eq<OtherSyncInfo>(&self, other: &TreeNode<OtherSyncInfo>) -> bool {
        match (self, other) {
            (TreeNode::Dir(dself), TreeNode::Dir(dother)) => dself.structural_eq(dother),
            (TreeNode::Dir(_), TreeNode::File(_)) | (TreeNode::File(_), TreeNode::Dir(_)) => false,
            (TreeNode::File(fself), TreeNode::File(fother)) => fself.name() == fother.name(),
        }
    }

    fn path(&self, parent_path: &VirtualPath) -> VirtualPathBuf {
        let mut path = parent_path.to_owned();
        path.push(self.name());
        path
    }

    /// Create a diff where this node has been removed from the VFS
    fn to_removed_diff<OtherSyncInfo>(
        &self,
        parent_path: &VirtualPath,
    ) -> VfsNodeDiff<OtherSyncInfo> {
        match self {
            TreeNode::Dir(_) => VfsNodeDiff::DirRemoved(self.path(parent_path)),
            TreeNode::File(_) => VfsNodeDiff::FileRemoved(self.path(parent_path)),
        }
    }
}

impl<SyncInfo: Clone> TreeNode<SyncInfo> {
    /// Create a diff where this node has been inserted into the VFS
    fn to_created_diff(&self, parent_path: &VirtualPath) -> VfsNodeDiff<SyncInfo> {
        match self {
            TreeNode::Dir(dir) => {
                VfsNodeDiff::DirCreated(DirCreatedDiff::new(self.path(parent_path), dir.clone()))
            }
            TreeNode::File(_) => VfsNodeDiff::FileCreated(self.path(parent_path)),
        }
    }

    /// Diff two directories based on their content.
    pub fn diff<OtherSyncInfo: Clone>(
        &self,
        other: &TreeNode<OtherSyncInfo>,
        path: &VirtualPath,
    ) -> Result<Vec<VfsNodeDiff<OtherSyncInfo>>, DiffError>
    where
        SyncInfo: IsModified<OtherSyncInfo>,
    {
        if self.name() != other.name() {
            return Err(DiffError::InvalidSyncInfo(path.to_owned()));
        }

        match (self, other) {
            (TreeNode::Dir(dself), TreeNode::Dir(dother)) => {
                let mut dir_path = path.to_owned();
                dir_path.push(dself.name());

                match dself.sync_info().modification_state(dother.sync_info()) {
                    // The SyncInfo tells us that nothing has been modified for this dir, but can't
                    // speak about its children. So we need to walk them.
                    ModificationState::ShallowUnmodified => {
                        if dself.children.len() != dother.children.len() {
                            return Err(DiffError::InvalidSyncInfo(dir_path));
                        }
                        let mut self_dirs = dself.children.iter();
                        let mut other_dirs = dother.children.iter();

                        let diffs = std::iter::zip(self_dirs.by_ref(), other_dirs.by_ref())
                            .map(|(self_child, other_child)| {
                                self_child.diff(other_child, &dir_path)
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .flatten()
                            .collect();

                        Ok(diffs)
                    }
                    // The SyncInfo tells us that nothing has been modified recursively, so we can
                    // stop there
                    ModificationState::RecursiveUnmodified => Ok(Vec::new()),
                    // The directory has been modified, so we have to walk it recursively to find
                    // the modified nodes
                    ModificationState::Modified => {
                        let mut diffs = Vec::new();
                        // Children lists are sorted, so we iterate using this invariant
                        let mut self_iter = dself.children.iter();
                        let mut other_iter = dother.children.iter();

                        let mut self_child_opt = self_iter.next();
                        let mut other_child_opt = other_iter.next();

                        while let (Some(self_child), Some(other_child)) =
                            (self_child_opt, other_child_opt)
                        {
                            match self_child.name().cmp(other_child.name()) {
                                Ordering::Less => {
                                    // "self" iterator is behind the other one, meaning that the
                                    // current node has been removed
                                    diffs.push(self_child.to_removed_diff(&dir_path));
                                    self_child_opt = self_iter.next();
                                }
                                Ordering::Equal => {
                                    // "self" and "other" are looking at the same node, so we need
                                    // to explore its content
                                    diffs.extend(self_child.diff(other_child, &dir_path)?);
                                    self_child_opt = self_iter.next();
                                    other_child_opt = other_iter.next();
                                }
                                Ordering::Greater => {
                                    // "self" iterator is ahead of the other one, meaning that the
                                    // current node of the other iterator has been created
                                    diffs.push(other_child.to_created_diff(&dir_path));
                                    other_child_opt = other_iter.next();
                                }
                            }
                        }

                        // Handle the remaining nodes that are present in an iterator and not the
                        // other one
                        while let Some(self_child) = self_child_opt {
                            diffs.push(self_child.to_removed_diff(&dir_path));
                            self_child_opt = self_iter.next();
                        }

                        while let Some(other_child) = other_child_opt {
                            diffs.push(other_child.to_created_diff(&dir_path));
                            other_child_opt = other_iter.next();
                        }

                        Ok(diffs)
                    }
                }
            }
            (TreeNode::File(fself), TreeNode::File(fother)) => {
                if fself.sync_info().is_modified(fother.sync_info()) {
                    let mut file_path = path.to_owned();
                    file_path.push(fself.name());

                    let diff = VfsNodeDiff::FileModified(file_path);
                    Ok(vec![diff])
                } else {
                    Ok(Vec::new())
                }
            }
            (nself, nother) => Ok(vec![
                nself.to_removed_diff(path),
                nother.to_created_diff(path),
            ]),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::TestNode::{DH, FH};

    #[test]
    fn test_diff_recursive() {
        let reference = DH(
            "",
            0,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 2), FH("f2.pdf", 3)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_recursive_diff();

        let modified = DH(
            "",
            10,
            vec![
                DH("Doc", 11, vec![FH("f1.md", 12), FH("f2.pdf", 3)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_recursive_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![VfsNodeDiff::FileModified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
        );

        let modified = DH(
            "",
            10,
            vec![
                DH("Doc", 11, vec![FH("f2.pdf", 3)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_recursive_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![VfsNodeDiff::FileRemoved(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
        );

        let modified = DH(
            "",
            10,
            vec![
                DH("Doc", 11, vec![FH("f2.pdf", 3), FH("f3.pdf", 14)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_recursive_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![
                VfsNodeDiff::FileRemoved(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
                VfsNodeDiff::FileCreated(VirtualPathBuf::new("/Doc/f3.pdf").unwrap())
            ]
        );

        let modified = DH(
            "",
            10,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 2), FH("f2.pdf", 3)]),
                DH("a", 14, vec![DH("ba", 15, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_recursive_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![
                VfsNodeDiff::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
                VfsNodeDiff::DirCreated(DirCreatedDiff::new(
                    VirtualPathBuf::new("/a/ba").unwrap(),
                    DH("ba", 15, vec![DH("c", 6, vec![])]).into_dir_recursive_diff(),
                ))
            ]
        );
    }

    #[test]
    fn test_diff_shallow() {
        let reference = DH(
            "",
            0,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 2), FH("f2.pdf", 3)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_shallow_diff();

        let modified = DH(
            "",
            0,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 12), FH("f2.pdf", 3)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_shallow_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![VfsNodeDiff::FileModified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
        );

        let modified = DH(
            "",
            0,
            vec![
                DH("Doc", 11, vec![FH("f2.pdf", 3)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_shallow_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![VfsNodeDiff::FileRemoved(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
        );

        let modified = DH(
            "",
            0,
            vec![
                DH("Doc", 11, vec![FH("f2.pdf", 3), FH("f3.pdf", 14)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_shallow_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![
                VfsNodeDiff::FileRemoved(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
                VfsNodeDiff::FileCreated(VirtualPathBuf::new("/Doc/f3.pdf").unwrap())
            ]
        );

        let modified = DH(
            "",
            0,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 2), FH("f2.pdf", 3)]),
                DH("a", 14, vec![DH("ba", 15, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_shallow_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![
                VfsNodeDiff::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
                VfsNodeDiff::DirCreated(DirCreatedDiff::new(
                    VirtualPathBuf::new("/a/ba").unwrap(),
                    DH("ba", 15, vec![DH("c", 6, vec![])]).into_dir_shallow_diff(),
                ))
            ]
        );
    }
}
