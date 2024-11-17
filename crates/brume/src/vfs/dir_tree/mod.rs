//! Tree structure used for recursive directory walking

mod dir;
mod file;

pub use dir::*;
pub use file::*;

use crate::{
    sorted_vec::{Sortable, SortedVec},
    update::ModificationState,
    Error, NameMismatchError,
};

use super::{
    DiffError, InvalidPathError, IsModified, VfsNodeUpdate, VfsUpdateList, VirtualPath,
    VirtualPathBuf,
};

#[derive(Error, Debug)]
pub enum DeleteNodeError {
    #[error("the path to be deleted is invalid")]
    InvalidPath(#[from] InvalidPathError),
    #[error("cannot delete the root dir itself")]
    PathIsRoot,
}

type SortedNodeList<SyncInfo> = SortedVec<VfsNode<SyncInfo>>;

/// A directory, seen as a tree.
///
/// It is composed of metadata for the directory itself and a list of children.
#[derive(Debug, Clone)]
pub struct DirTree<SyncInfo> {
    metadata: DirMeta<SyncInfo>,
    // TODO: handle having a dir and file with the same name
    children: SortedNodeList<SyncInfo>,
}

impl<SyncInfo> DirTree<SyncInfo> {
    /// Create a new directory with no child and the provided name
    pub fn new(name: &str, sync: SyncInfo) -> Self {
        Self {
            metadata: DirMeta::new(name, sync),
            children: SortedNodeList::new(),
        }
    }

    /// Create a new directory without syncinfo.
    ///
    /// This mean that the synchronization process will need to access the concrete fs at least
    /// once.
    pub fn new_without_syncinfo(name: &str) -> Self {
        Self {
            metadata: DirMeta::new_without_syncinfo(name),
            children: SortedNodeList::new(),
        }
    }

    /// Create a new directory with the provided name and child nodes
    pub fn new_with_children(name: &str, sync: SyncInfo, children: Vec<VfsNode<SyncInfo>>) -> Self {
        Self {
            metadata: DirMeta::new(name, sync),
            children: SortedNodeList::from_vec(children),
        }
    }

    /// Insert a new child for this directory. If there is already a child with the same name,
    /// returns false.
    pub fn insert_child(&mut self, child: VfsNode<SyncInfo>) -> bool {
        self.children.insert(child)
    }

    pub fn name(&self) -> &str {
        self.metadata.name()
    }

    pub fn sync_info(&self) -> &Option<SyncInfo> {
        self.metadata.sync_info()
    }

    /// Invalidate the sync info to make them trigger a ConcreteFS sync on next run
    pub fn invalidate_sync_info(&mut self) {
        self.metadata.invalidate_sync_info()
    }

    pub fn children(&self) -> &SortedNodeList<SyncInfo> {
        &self.children
    }

    /// Return a reference to the dir at the given path. Return an error if the path does
    /// not point to a valid directory node.
    pub fn find_dir(&self, path: &VirtualPath) -> Result<&DirTree<SyncInfo>, InvalidPathError> {
        if path.is_root() {
            Ok(self)
        } else {
            self.find_node(path)
                .ok_or(InvalidPathError::NotFound(path.to_owned()))
                .and_then(|node| node.as_dir())
        }
    }

    /// Return a reference to the file at the given path. Return an error if the path does
    /// not point to a valid file.
    pub fn find_file(&self, path: &VirtualPath) -> Result<&FileMeta<SyncInfo>, InvalidPathError> {
        self.find_node(path)
            .ok_or(InvalidPathError::NotFound(path.to_owned()))
            .and_then(|node| node.as_file())
    }

    /// Return a reference to the node at the given path. Return an error if the path does
    /// not point to a valid node.
    pub fn find_node(&self, path: &VirtualPath) -> Option<&VfsNode<SyncInfo>> {
        if let Some((top_level, remainder)) = path.top_level_split() {
            if remainder.is_root() {
                self.children.find(top_level)
            } else {
                let child = self.children.find(top_level)?;

                match child {
                    VfsNode::Dir(dir) => dir.find_node(remainder),
                    VfsNode::File(_) => None,
                }
            }
        } else {
            None
        }
    }

    /// Return a mutable reference to the dir at the given path. Return an error if the path does
    /// not point to a valid directory node.
    pub fn find_dir_mut(
        &mut self,
        path: &VirtualPath,
    ) -> Result<&mut DirTree<SyncInfo>, InvalidPathError> {
        if path.is_root() {
            Ok(self)
        } else {
            self.find_node_mut(path)
                .ok_or(InvalidPathError::NotFound(path.to_owned()))
                .and_then(|node| node.as_dir_mut())
        }
    }

    /// Return a mutable reference to the file at the given path. Return an error if the path does
    /// not point to a valid file.
    pub fn find_file_mut(
        &mut self,
        path: &VirtualPath,
    ) -> Result<&mut FileMeta<SyncInfo>, InvalidPathError> {
        self.find_node_mut(path)
            .ok_or(InvalidPathError::NotFound(path.to_owned()))
            .and_then(|node| node.as_file_mut())
    }

    /// Return a mutable reference to the node at the given path. Return None if the path does
    /// not point to a valid node.
    fn find_node_mut(&mut self, path: &VirtualPath) -> Option<&mut VfsNode<SyncInfo>> {
        if let Some((top_level, remainder)) = path.top_level_split() {
            if remainder.is_root() {
                self.children.find_mut(top_level)
            } else {
                let child = self.children.find_mut(top_level)?;

                match child {
                    VfsNode::Dir(dir) => dir.find_node_mut(remainder),
                    VfsNode::File(_) => None,
                }
            }
        } else {
            None
        }
    }

    /// Remove a child from this directory. If there were no child with this name, return false.
    pub fn remove_child(&mut self, child_name: &str) -> bool {
        self.children.remove(child_name)
    }

    /// Remove a child with the given kind from this directory.
    ///
    /// If there were no child with this name return None. If a child exists but is of the wrong
    /// kind, return Some(false).
    fn remove_child_kind(&mut self, child_name: &str, node_kind: NodeKind) -> Option<bool> {
        self.children
            .remove_if(child_name, |child| child.kind() == node_kind)
    }

    /// Remove a child dir from this directory.
    ///
    /// If there were no child node with this name, return None. If the node was not a directory,
    /// return Some(false).
    pub fn remove_child_dir(&mut self, child_name: &str) -> Option<bool> {
        self.remove_child_kind(child_name, NodeKind::Dir)
    }

    /// Remove a child file from this directory.
    ///
    /// If there were no child node with this name, return None. If the node was not a file,
    /// return Some(false).
    pub fn remove_child_file(&mut self, child_name: &str) -> Option<bool> {
        self.remove_child_kind(child_name, NodeKind::File)
    }

    /// Delete the node with the current path in the tree. Return an error if the path is not a
    /// valid node.
    pub fn delete_node(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        self.delete_node_kind(path, None)
    }

    /// Delete the dir with the current path in the tree. Return an error if the path is not a
    /// valid directory.
    pub fn delete_dir(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        self.delete_node_kind(path, Some(NodeKind::Dir))
    }

    /// Delete the file with the current path in the tree. Return an error if the path is not a
    /// valid file.
    pub fn delete_file(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        self.delete_node_kind(path, Some(NodeKind::File))
    }

    fn delete_node_kind(
        &mut self,
        path: &VirtualPath,
        kind: Option<NodeKind>,
    ) -> Result<(), DeleteNodeError> {
        if let Some(parent) = path.parent() {
            self.find_dir_mut(parent)
                .map_err(|e| e.into())
                .and_then(|dir| {
                    let removed = if let Some(kind) = kind {
                        dir.remove_child_kind(path.name(), kind)
                            .ok_or_else(|| InvalidPathError::for_kind(kind, path))?
                    } else {
                        dir.remove_child(path.name())
                    };
                    if removed {
                        Ok(())
                    } else {
                        Err(InvalidPathError::NotFound(path.to_owned()).into())
                    }
                })
        } else if let Some(NodeKind::File) = kind {
            // If the path is the root but we requested a file removal, it is an error
            Err(DeleteNodeError::PathIsRoot)
        } else {
            // Else remove all the content of the current dir
            self.children = SortedNodeList::new();
            Ok(())
        }
    }

    /// Check if the two directories are structurally equals (their trees are composed of nodes of
    /// the same kind and the same name).
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

impl<SyncInfo: Clone> DirTree<SyncInfo> {
    /// Replace an existing existing child based on its name, or insert a new one.
    ///
    /// Return the replaced child if any, or None if there was no child with this name.
    pub fn replace_child(&mut self, child: VfsNode<SyncInfo>) -> Option<VfsNode<SyncInfo>> {
        self.children.replace(child)
    }
}

impl<SyncInfo: IsModified<SyncInfo>> DirTree<SyncInfo> {
    /// Diff two directories based on their content.
    pub fn diff(
        &self,
        other: &DirTree<SyncInfo>,
        parent_path: &VirtualPath,
    ) -> Result<VfsUpdateList, DiffError> {
        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.name());

        match self.sync_info().modification_state(other.sync_info()) {
            // The SyncInfo tells us that nothing has been modified for this dir, but can't
            // speak about its children. So we need to walk them.
            ModificationState::ShallowUnmodified => {
                // Since the current dir is unmodifed, both self and other should have the same
                // number of children with the same names.
                if self.children.len() != other.children.len() {
                    return Err(DiffError::InvalidSyncInfo(dir_path));
                }
                let mut self_dirs = self.children.iter();
                let mut other_dirs = other.children.iter();

                let diffs = std::iter::zip(self_dirs.by_ref(), other_dirs.by_ref())
                    .map(|(self_child, other_child)| self_child.diff(other_child, &dir_path))
                    .collect::<Result<Vec<_>, _>>()?;

                // Since the children list is sorted, we know that the resulting updates will be
                // also sorted, so we can call `unchecked_flatten`
                Ok(SortedVec::unchecked_flatten(diffs))
            }
            // The SyncInfo tells us that nothing has been modified recursively, so we can
            // stop there
            ModificationState::RecursiveUnmodified => Ok(VfsUpdateList::new()),
            // The directory has been modified, so we have to walk it recursively to find
            // the modified nodes
            ModificationState::Modified => {
                let diff_list = self.children.iter_zip_map(
                    &other.children,
                    |self_child| -> Result<_, DiffError> {
                        let mut res = SortedVec::new();
                        res.insert(self_child.to_removed_diff(&dir_path));
                        Ok(res)
                    },
                    |self_child, other_child| self_child.diff(other_child, &dir_path),
                    |other_child| {
                        let mut res = SortedVec::new();
                        res.insert(other_child.to_created_diff(&dir_path));
                        Ok(res)
                    },
                )?;

                // Since the children lists are sorted, we know that the produced updates will be
                // too, so we can directly create the sorted list from the result
                let diffs = SortedVec::unchecked_flatten(diff_list);

                Ok(diffs)
            }
        }
    }
}

/// The kind of node represented by the root of this tree
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Dir,
    File,
}

/// A node in a File System tree. Can represent a directory or a file.
#[derive(Debug, Clone)]
pub enum VfsNode<SyncInfo> {
    Dir(DirTree<SyncInfo>),
    File(FileMeta<SyncInfo>),
}

impl<SyncInfo> Sortable for VfsNode<SyncInfo> {
    type Key = str;

    fn key(&self) -> &Self::Key {
        self.name()
    }
}

impl<SyncInfo> VfsNode<SyncInfo> {
    /// Return the name of the file or directory represented by this node
    pub fn name(&self) -> &str {
        match self {
            VfsNode::File(file) => file.name(),
            VfsNode::Dir(dir) => dir.name(),
        }
    }

    pub fn is_dir(&self) -> bool {
        match self {
            VfsNode::Dir(_) => true,
            VfsNode::File(_) => false,
        }
    }

    pub fn is_file(&self) -> bool {
        match self {
            VfsNode::Dir(_) => false,
            VfsNode::File(_) => true,
        }
    }

    pub fn kind(&self) -> NodeKind {
        match self {
            VfsNode::Dir(_) => NodeKind::Dir,
            VfsNode::File(_) => NodeKind::File,
        }
    }

    pub fn as_dir(&self) -> Result<&DirTree<SyncInfo>, InvalidPathError> {
        self.find_dir(VirtualPath::root())
    }

    pub fn as_dir_mut(&mut self) -> Result<&mut DirTree<SyncInfo>, InvalidPathError> {
        self.find_dir_mut(VirtualPath::root())
    }

    pub fn as_file(&self) -> Result<&FileMeta<SyncInfo>, InvalidPathError> {
        self.find_file(VirtualPath::root())
    }

    pub fn as_file_mut(&mut self) -> Result<&mut FileMeta<SyncInfo>, InvalidPathError> {
        self.find_file_mut(VirtualPath::root())
    }

    pub fn find_dir(&self, path: &VirtualPath) -> Result<&DirTree<SyncInfo>, InvalidPathError> {
        self.find_node(path)
            .ok_or(InvalidPathError::NotFound(path.to_owned()))
            .and_then(|node| match node {
                VfsNode::Dir(dir) => Ok(dir),
                VfsNode::File(_) => Err(InvalidPathError::NotADir(path.to_owned())),
            })
    }

    pub fn find_file(&self, path: &VirtualPath) -> Result<&FileMeta<SyncInfo>, InvalidPathError> {
        self.find_node(path)
            .ok_or(InvalidPathError::NotFound(path.to_owned()))
            .and_then(|node| match node {
                VfsNode::Dir(_) => Err(InvalidPathError::NotAFile(path.to_owned())),
                VfsNode::File(file) => Ok(file),
            })
    }

    pub fn find_node(&self, path: &VirtualPath) -> Option<&Self> {
        if path.is_root() {
            Some(self)
        } else {
            match self {
                VfsNode::Dir(dir) => dir.find_node(path),
                VfsNode::File(file) => {
                    if path.len() == 1 && file.name() == path.name() {
                        Some(self)
                    } else {
                        None
                    }
                }
            }
        }
    }

    pub fn find_dir_mut(
        &mut self,
        path: &VirtualPath,
    ) -> Result<&mut DirTree<SyncInfo>, InvalidPathError> {
        self.find_node_mut(path)
            .ok_or(InvalidPathError::NotFound(path.to_owned()))
            .and_then(|node| match node {
                VfsNode::Dir(dir) => Ok(dir),
                VfsNode::File(_) => Err(InvalidPathError::NotADir(path.to_owned())),
            })
    }

    pub fn find_file_mut(
        &mut self,
        path: &VirtualPath,
    ) -> Result<&mut FileMeta<SyncInfo>, InvalidPathError> {
        self.find_node_mut(path)
            .ok_or(InvalidPathError::NotFound(path.to_owned()))
            .and_then(|node| match node {
                VfsNode::Dir(_) => Err(InvalidPathError::NotAFile(path.to_owned())),
                VfsNode::File(file) => Ok(file),
            })
    }

    pub fn find_node_mut(&mut self, path: &VirtualPath) -> Option<&mut Self> {
        if path.is_root() {
            Some(self)
        } else {
            match self {
                VfsNode::Dir(dir) => dir.find_node_mut(path),
                VfsNode::File(file) => {
                    if path.len() == 1 && file.name() == path.name() {
                        Some(self)
                    } else {
                        None
                    }
                }
            }
        }
    }

    /// Compare the structure of trees. Two trees are structurally equals if they have the same
    /// shape and are composed of nodes with the same names.
    pub fn structural_eq<OtherSyncInfo>(&self, other: &VfsNode<OtherSyncInfo>) -> bool {
        match (self, other) {
            (VfsNode::Dir(dself), VfsNode::Dir(dother)) => dself.structural_eq(dother),
            (VfsNode::Dir(_), VfsNode::File(_)) | (VfsNode::File(_), VfsNode::Dir(_)) => false,
            (VfsNode::File(fself), VfsNode::File(fother)) => fself.name() == fother.name(),
        }
    }

    fn path(&self, parent_path: &VirtualPath) -> VirtualPathBuf {
        let mut path = parent_path.to_owned();
        path.push(self.name());
        path
    }

    /// Create a diff where this node has been removed from the VFS
    pub fn to_removed_diff(&self, parent_path: &VirtualPath) -> VfsNodeUpdate {
        match self {
            VfsNode::Dir(_) => VfsNodeUpdate::DirRemoved(self.path(parent_path)),
            VfsNode::File(_) => VfsNodeUpdate::FileRemoved(self.path(parent_path)),
        }
    }

    /// Create a diff where this node has been inserted into the VFS
    pub fn to_created_diff(&self, parent_path: &VirtualPath) -> VfsNodeUpdate {
        match self {
            VfsNode::Dir(_) => VfsNodeUpdate::DirCreated(self.path(parent_path)),
            VfsNode::File(_) => VfsNodeUpdate::FileCreated(self.path(parent_path)),
        }
    }
}

impl<SyncInfo: IsModified<SyncInfo>> VfsNode<SyncInfo> {
    /// Diff two nodes based on their content.
    ///
    /// This uses the `SyncInfo` metadata and does not need to query the concrete filesystem. To
    /// diff using the true content of files, use [`FileSystemNode::diff`].
    ///
    /// [`FileSystemNode::diff`]: crate::filesystem::FileSystemNode
    pub fn diff(
        &self,
        other: &VfsNode<SyncInfo>,
        parent_path: &VirtualPath,
    ) -> Result<VfsUpdateList, DiffError> {
        if self.name() != other.name() {
            return Err(NameMismatchError {
                found: self.name().to_string(),
                expected: other.name().to_string(),
            }
            .into());
        }

        match (self, other) {
            (VfsNode::Dir(dself), VfsNode::Dir(dother)) => dself.diff(dother, parent_path),
            (VfsNode::File(fself), VfsNode::File(fother)) => {
                // Diff the file based on their sync info
                if fself.sync_info().is_modified(fother.sync_info()) {
                    let mut file_path = parent_path.to_owned();
                    file_path.push(fself.name());

                    let diff = VfsNodeUpdate::FileModified(file_path);
                    Ok(VfsUpdateList::from_vec(vec![diff]))
                } else {
                    Ok(VfsUpdateList::new())
                }
            }
            (nself, nother) => Ok(VfsUpdateList::from_vec(vec![
                nself.to_removed_diff(parent_path),
                nother.to_created_diff(parent_path),
            ])),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::TestNode::{D, DH, F, FH};

    #[test]
    fn test_find() {
        let mut base = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_dir();

        let node_ref = F("f1.md").into_node();

        assert!(base
            .find_node(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
            .unwrap()
            .structural_eq(&node_ref));
        assert!(base
            .find_node_mut(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
            .unwrap()
            .structural_eq(&node_ref));
        assert_eq!(
            base.find_file(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
                .unwrap()
                .name(),
            "f1.md"
        );
        assert_eq!(
            base.find_file_mut(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
                .unwrap()
                .name(),
            "f1.md"
        );
        assert!(base
            .find_dir(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
            .is_err());
        assert!(base
            .find_dir_mut(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
            .is_err());

        let node_ref = D("b", vec![D("c", vec![])]).into_node();
        let dir_ref = D("b", vec![D("c", vec![])]).into_dir();

        assert!(base
            .find_node(&VirtualPathBuf::new("/a/b").unwrap())
            .unwrap()
            .structural_eq(&node_ref));
        assert!(base
            .find_node_mut(&VirtualPathBuf::new("/a/b").unwrap())
            .unwrap()
            .structural_eq(&node_ref));
        assert!(base
            .find_file(&VirtualPathBuf::new("/a/b").unwrap())
            .is_err());
        assert!(base
            .find_file_mut(&VirtualPathBuf::new("/a/b").unwrap())
            .is_err());
        assert!(base
            .find_dir(&VirtualPathBuf::new("/a/b").unwrap())
            .unwrap()
            .structural_eq(&dir_ref));
        assert!(base
            .find_dir_mut(&VirtualPathBuf::new("/a/b").unwrap())
            .unwrap()
            .structural_eq(&dir_ref));

        assert!(base
            .find_node(&VirtualPathBuf::new("/e/h").unwrap())
            .is_none());
        assert!(base
            .find_node_mut(&VirtualPathBuf::new("/e/h").unwrap())
            .is_none());
        assert!(base
            .find_file(&VirtualPathBuf::new("/e/h").unwrap())
            .is_err());
        assert!(base
            .find_file_mut(&VirtualPathBuf::new("/e/h").unwrap())
            .is_err());
        assert!(base
            .find_dir(&VirtualPathBuf::new("/e/h").unwrap())
            .is_err());
        assert!(base
            .find_dir_mut(&VirtualPathBuf::new("/e/h").unwrap())
            .is_err());
    }

    #[test]
    fn test_delete() {
        let base = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_dir();

        let without_f1_ref = D(
            "",
            vec![
                D("Doc", vec![F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_dir();

        let without_b_ref = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_dir();

        let empty_ref = D("", vec![]).into_dir();

        let mut without_f1 = base.clone();
        without_f1
            .delete_node(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
            .unwrap();

        assert!(without_f1.structural_eq(&without_f1_ref));

        let mut without_f1 = base.clone();
        without_f1
            .delete_file(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
            .unwrap();

        assert!(without_f1.structural_eq(&without_f1_ref));

        let mut without_f1 = base.clone();
        assert!(without_f1
            .delete_dir(&VirtualPathBuf::new("/Doc/f1.md").unwrap())
            .is_err());
        assert!(without_f1.structural_eq(&base));

        let mut without_b = base.clone();
        without_b
            .delete_node(&VirtualPathBuf::new("/a/b").unwrap())
            .unwrap();

        assert!(without_b.structural_eq(&without_b_ref));

        let mut without_b = base.clone();
        without_b
            .delete_dir(&VirtualPathBuf::new("/a/b").unwrap())
            .unwrap();

        assert!(without_b.structural_eq(&without_b_ref));

        let mut without_b = base.clone();
        assert!(without_b
            .delete_file(&VirtualPathBuf::new("/a/b").unwrap())
            .is_err());
        assert!(without_b.structural_eq(&base));

        let mut identical = base.clone();
        assert!(identical
            .delete_node(&VirtualPathBuf::new("/e/h").unwrap())
            .is_err());
        assert!(identical.structural_eq(&base));

        let mut identical = base.clone();
        assert!(identical
            .delete_file(&VirtualPathBuf::new("/e/h").unwrap())
            .is_err());
        assert!(identical.structural_eq(&base));

        let mut identical = base.clone();
        assert!(identical
            .delete_file(&VirtualPathBuf::new("/e/h").unwrap())
            .is_err());
        assert!(identical.structural_eq(&base));

        let mut empty = base.clone();
        empty
            .delete_node(&VirtualPathBuf::new("/").unwrap())
            .unwrap();

        assert!(empty.structural_eq(&empty_ref));

        let mut empty = base.clone();
        empty
            .delete_dir(&VirtualPathBuf::new("/").unwrap())
            .unwrap();

        assert!(empty.structural_eq(&empty_ref));

        let mut empty = base.clone();
        assert!(empty
            .delete_file(&VirtualPathBuf::new("/").unwrap())
            .is_err());
        assert!(empty.structural_eq(&base));
    }

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
            vec![VfsNodeUpdate::FileModified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
            .into()
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
            vec![VfsNodeUpdate::FileRemoved(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
            .into()
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
                VfsNodeUpdate::FileRemoved(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
                VfsNodeUpdate::FileCreated(VirtualPathBuf::new("/Doc/f3.pdf").unwrap())
            ]
            .into()
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
                VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
                VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/a/ba").unwrap(),)
            ]
            .into()
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
            vec![VfsNodeUpdate::FileModified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
            .into()
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
            vec![VfsNodeUpdate::FileRemoved(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
            .into()
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
                VfsNodeUpdate::FileRemoved(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
                VfsNodeUpdate::FileCreated(VirtualPathBuf::new("/Doc/f3.pdf").unwrap())
            ]
            .into()
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
                VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
                VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/a/ba").unwrap(),)
            ]
            .into()
        );
    }
}
