//! Tree structure used for recursive directory walking

mod dir;
mod file;

pub use dir::*;
pub use file::*;
use serde::{Deserialize, Serialize};

use crate::{
    sorted_vec::{Sortable, SortedVec},
    update::{FailedUpdateApplication, ModificationState, VirtualReconciledUpdate},
    Error, NameMismatchError,
};

use super::{
    DiffError, InvalidPathError, IsModified, VfsDiff, VfsDiffList, VirtualPath, VirtualPathBuf,
};

#[derive(Error, Debug)]
pub enum DeleteNodeError {
    #[error("the path to be deleted is invalid")]
    InvalidPath(#[from] InvalidPathError),
    #[error("cannot delete the root dir itself")]
    PathIsRoot,
}

type SortedNodeList<SyncInfo> = SortedVec<VfsNode<SyncInfo>>;

/// The synchronization state of a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeState<SyncInfo> {
    /// The node has been correctly synchronized
    Ok(SyncInfo),
    /// The node should be re-synchronized at the next synchro, whatever its state
    NeedResync,
    /// The previous synchronization of this node returned an error, it will be re-synchronized
    /// next time
    Error(FailedUpdateApplication),
    /// There is a conflict on this node that needs to be manually resolved
    Conflict(VfsDiff),
}

impl<SyncInfo> NodeState<SyncInfo> {
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict(_))
    }
}

impl<SyncInfo> From<&NodeState<SyncInfo>> for NodeState<()> {
    fn from(value: &NodeState<SyncInfo>) -> Self {
        match value {
            NodeState::Ok(_) => NodeState::Ok(()),
            NodeState::NeedResync => NodeState::NeedResync,
            NodeState::Error(failed_update) => NodeState::Error(failed_update.clone()),
            NodeState::Conflict(update) => NodeState::Conflict(update.clone()),
        }
    }
}

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
    /// Creates a new directory with no child and the provided name
    pub fn new(name: &str, sync: SyncInfo) -> Self {
        Self {
            metadata: DirMeta::new(name, sync),
            children: SortedNodeList::new(),
        }
    }

    /// Creates a new directory that will always trigger a resync on the first synchro
    pub fn new_force_resync(name: &str) -> Self {
        Self {
            metadata: DirMeta::new_force_resync(name),
            children: SortedNodeList::new(),
        }
    }

    /// Creates a new directory with the provided [`state`]
    ///
    /// [`state`]: NodeState
    pub fn new_with_state(name: &str, state: NodeState<SyncInfo>) -> Self {
        Self {
            metadata: DirMeta::new_with_state(name, state),
            children: SortedNodeList::new(),
        }
    }

    /// Creates a new directory in the error [`state`]
    ///
    /// [`state`]: NodeState
    pub fn new_error(name: &str, error: FailedUpdateApplication) -> Self {
        Self {
            metadata: DirMeta::new_error(name, error),
            children: SortedNodeList::new(),
        }
    }

    /// Creates a new directory with the provided name and child nodes
    pub fn new_with_children(name: &str, sync: SyncInfo, children: Vec<VfsNode<SyncInfo>>) -> Self {
        Self {
            metadata: DirMeta::new(name, sync),
            children: SortedNodeList::from_vec(children),
        }
    }

    /// Inserts a new child for this directory. If there is already a child with the same name,
    /// returns false.
    pub fn insert_child(&mut self, child: VfsNode<SyncInfo>) -> bool {
        self.children.insert(child)
    }

    pub fn name(&self) -> &str {
        self.metadata.name()
    }

    pub fn state(&self) -> &NodeState<SyncInfo> {
        self.metadata.state()
    }

    pub fn state_mut(&mut self) -> &mut NodeState<SyncInfo> {
        self.metadata.state_mut()
    }

    /// Invalidates the sync info to make them trigger a FSBackend sync on next run
    pub fn force_resync(&mut self) {
        self.metadata.force_resync()
    }

    pub fn children(&self) -> &SortedNodeList<SyncInfo> {
        &self.children
    }

    /// Returns a reference to the dir at the given path.
    ///
    /// Returns an error if the path does not point to a valid directory node.
    pub fn find_dir(&self, path: &VirtualPath) -> Result<&DirTree<SyncInfo>, InvalidPathError> {
        if path.is_root() {
            Ok(self)
        } else {
            self.find_node(path)
                .ok_or(InvalidPathError::NotFound(path.to_owned()))
                .and_then(|node| node.as_dir())
        }
    }

    /// Returns a reference to the file at the given path.
    ///
    /// Returns an error if the path does not point to a valid file.
    pub fn find_file(&self, path: &VirtualPath) -> Result<&FileMeta<SyncInfo>, InvalidPathError> {
        self.find_node(path)
            .ok_or(InvalidPathError::NotFound(path.to_owned()))
            .and_then(|node| node.as_file())
    }

    /// Returns a reference to the node at the given path.
    ///
    /// Returns an error if the path does not point to a valid node.
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

    /// Returns a mutable reference to the dir at the given path.
    ///
    /// Returns an error if the path does not point to a valid directory node.
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

    /// Returns a mutable reference to the file at the given path.
    ///
    /// Returns an error if the path does not point to a valid file.
    pub fn find_file_mut(
        &mut self,
        path: &VirtualPath,
    ) -> Result<&mut FileMeta<SyncInfo>, InvalidPathError> {
        self.find_node_mut(path)
            .ok_or(InvalidPathError::NotFound(path.to_owned()))
            .and_then(|node| node.as_file_mut())
    }

    /// Returns a mutable reference to the node at the given path.
    ///
    /// Returns None if the path does not point to a valid node.
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

    /// Removes a child from this directory. If there were no child with this name, returns false.
    pub fn remove_child(&mut self, child_name: &str) -> bool {
        self.children.remove(child_name)
    }

    /// Removes a child with the given kind from this directory.
    ///
    /// If there were no child with this name returns None. If a child exists but is of the wrong
    /// kind, returns Some(false).
    fn remove_child_kind(&mut self, child_name: &str, node_kind: NodeKind) -> Option<bool> {
        self.remove_child_if(child_name, |child| child.kind() == node_kind)
    }

    /// Removes a child if its node matches the provided predicate.
    ///
    /// If there were no child with this name returns None. If a child exists but the predicates is
    /// not verified, returns Some(false).
    pub fn remove_child_if<F: FnOnce(&VfsNode<SyncInfo>) -> bool>(
        &mut self,
        child_name: &str,
        condition: F,
    ) -> Option<bool> {
        self.children.remove_if(child_name, condition)
    }

    /// Removes a child dir from this directory.
    ///
    /// If there were no child node with this name, returns None. If the node was not a directory,
    /// returns Some(false).
    pub fn remove_child_dir(&mut self, child_name: &str) -> Option<bool> {
        self.remove_child_kind(child_name, NodeKind::Dir)
    }

    /// Removes a child file from this directory.
    ///
    /// If there were no child node with this name, returns None. If the node was not a file,
    /// returns Some(false).
    pub fn remove_child_file(&mut self, child_name: &str) -> Option<bool> {
        self.remove_child_kind(child_name, NodeKind::File)
    }

    /// Deletes the node with the current path in the tree.
    ///
    /// Return an error if the path is not a valid node.
    pub fn delete_node(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        self.delete_node_kind(path, None)
    }

    /// Deletes the dir with the current path in the tree.
    ///
    /// Returns an error if the path is not a valid directory.
    pub fn delete_dir(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        self.delete_node_kind(path, Some(NodeKind::Dir))
    }

    /// Deletes the file with the current path in the tree.
    ///
    /// Returns an error if the path is not a valid file.
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

    /// Checks if the two directories are structurally equals.
    ///
    /// This means that their trees are composed of nodes with the same kind and the same name.
    pub fn structural_eq<OtherSyncInfo>(&self, other: &DirTree<OtherSyncInfo>) -> bool {
        self.name() == other.name()
            && self.children.len() == other.children.len()
            && self
                .children
                .iter()
                .zip(other.children.iter())
                .all(|(child_self, child_other)| child_self.structural_eq(child_other))
    }

    /// Gets the differences between [`DirTree`], and eventually requests file content checks with
    /// their concrete backends.
    ///
    /// This will perform a structural diff between both trees, and return a
    /// [`VirtualReconciledUpdate::NeedBackendCheck`] when two files with the same name are found.
    pub(crate) fn reconciliation_diff<OtherSyncInfo>(
        &self,
        other: &DirTree<OtherSyncInfo>,
        parent_path: &VirtualPath,
    ) -> Result<SortedVec<VirtualReconciledUpdate>, DiffError> {
        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.name());

        let diff_list = self.children.iter_zip_map(
            &other.children,
            |self_child| -> Result<_, DiffError> {
                let mut res = SortedVec::new();
                res.insert(VirtualReconciledUpdate::applicable_remote(
                    &self_child.to_created_diff(&dir_path),
                ));
                Ok(res)
            },
            |self_child, other_child| self_child.reconciliation_diff(other_child, &dir_path),
            |other_child| {
                let mut res = SortedVec::new();
                res.insert(VirtualReconciledUpdate::applicable_local(
                    &other_child.to_created_diff(&dir_path),
                ));
                Ok(res)
            },
        )?;

        // Since the children lists are sorted, we know that the produced updates will be
        // too, so we can directly create the sorted list from the result
        let diffs = SortedVec::unchecked_flatten(diff_list);

        Ok(diffs)
    }

    /// Returns the list of errors for this dir and its children
    pub fn get_errors(
        &self,
        parent_path: &VirtualPath,
    ) -> Vec<(VirtualPathBuf, FailedUpdateApplication)> {
        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.name());

        let mut ret = if let NodeState::Error(err) = self.state() {
            vec![(dir_path.clone(), err.to_owned())]
        } else {
            Vec::new()
        };

        for child in self.children().iter() {
            ret.extend(child.get_errors_list(&dir_path));
        }

        ret
    }

    /// Returns the list of conflicts for this dir and its children
    pub fn get_conflicts(&self, parent_path: &VirtualPath) -> Vec<VirtualPathBuf> {
        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.name());

        let mut ret = if self.state().is_conflict() {
            vec![dir_path.clone()]
        } else {
            Vec::new()
        };

        for child in self.children().iter() {
            ret.extend(child.get_conflicts_list(&dir_path));
        }

        ret
    }
}

impl<SyncInfo: Clone> DirTree<SyncInfo> {
    /// Replaces an existing existing child based on its name, or insert a new one.
    ///
    /// Returns the replaced child if any, or None if there was no child with this name.
    pub fn replace_child(&mut self, child: VfsNode<SyncInfo>) -> Option<VfsNode<SyncInfo>> {
        self.children.replace(child)
    }
}

impl<SyncInfo: IsModified> DirTree<SyncInfo> {
    /// Diff two directories based on their content.
    pub fn diff(
        &self,
        other: &DirTree<SyncInfo>,
        parent_path: &VirtualPath,
    ) -> Result<VfsDiffList, DiffError> {
        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.name());

        match self.state().modification_state(other.state()) {
            // The SyncInfo tells us that nothing has been modified for this dir, but can't
            // speak about its children. So we need to walk them.
            ModificationState::ShallowUnmodified => {
                // Since the current dir is unmodified, both self and other should have the same
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
            ModificationState::RecursiveUnmodified => Ok(VfsDiffList::new()),
            ModificationState::Modified => {
                // If one of the dir is in error state, resync it completely
                if self.state().is_err() {
                    let mut res = SortedVec::new();
                    res.insert(VfsDiff::dir_created(dir_path));
                    Ok(res)
                } else if other.state().is_err() {
                    let mut res = SortedVec::new();
                    res.insert(VfsDiff::dir_removed(dir_path));
                    Ok(res)
                } else {
                    // The directory has been modified, so we have to walk it recursively to find
                    // the modified nodes
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

                    // Since the children lists are sorted, we know that the produced updates will
                    // be too, so we can directly create the sorted list from
                    // the result
                    let diffs = SortedVec::unchecked_flatten(diff_list);

                    Ok(diffs)
                }
            }
        }
    }
}

impl<SyncInfo> From<&DirTree<SyncInfo>> for DirTree<()> {
    fn from(value: &DirTree<SyncInfo>) -> Self {
        Self {
            metadata: (&value.metadata).into(),
            // Ok to use unchecked because the input list is sorted
            children: SortedVec::unchecked_from_vec(
                value.children.iter().map(|child| child.into()).collect(),
            ),
        }
    }
}

/// The kind of node represented by the root of this tree
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Dir,
    File,
}

impl NodeKind {
    pub fn as_str(&self) -> &str {
        match self {
            NodeKind::Dir => "Dir",
            NodeKind::File => "File",
        }
    }
}

impl TryFrom<&str> for NodeKind {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Dir" => Ok(NodeKind::Dir),
            "File" => Ok(NodeKind::File),
            _ => Err(()),
        }
    }
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
    /// Returns the name of the file or directory represented by this node
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

    pub fn set_state(&mut self, state: NodeState<SyncInfo>) {
        match self {
            VfsNode::Dir(dir_tree) => *dir_tree.state_mut() = state,
            VfsNode::File(file_meta) => *file_meta.state_mut() = state,
        }
    }

    pub fn state(&self) -> &NodeState<SyncInfo> {
        match self {
            VfsNode::Dir(dir_tree) => dir_tree.state(),
            VfsNode::File(file_meta) => file_meta.state(),
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

    pub fn delete_node(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        match self {
            VfsNode::Dir(dir) => dir.delete_node(path),
            VfsNode::File(file) => {
                if path.len() == 1 && file.name() == path.name() {
                    Err(DeleteNodeError::PathIsRoot)
                } else {
                    Err(DeleteNodeError::InvalidPath(InvalidPathError::NotFound(
                        path.to_owned(),
                    )))
                }
            }
        }
    }

    /// Compares the structure of trees.
    ///
    /// Two trees are structurally equals if they have the same shape and are composed of nodes with
    /// the same names.
    pub fn structural_eq<OtherSyncInfo>(&self, other: &VfsNode<OtherSyncInfo>) -> bool {
        match (self, other) {
            (VfsNode::Dir(dself), VfsNode::Dir(dother)) => dself.structural_eq(dother),
            (VfsNode::Dir(_), VfsNode::File(_)) | (VfsNode::File(_), VfsNode::Dir(_)) => false,
            (VfsNode::File(fself), VfsNode::File(fother)) => fself.name() == fother.name(),
        }
    }

    /// Virtual diff performed during the reconciliation.
    ///
    /// Structurally compare both FS, and return a `NeedBackendCheck` when two files have the same
    /// name.
    fn reconciliation_diff<OtherSyncInfo>(
        &self,
        other: &VfsNode<OtherSyncInfo>,
        parent_path: &VirtualPath,
    ) -> Result<SortedVec<VirtualReconciledUpdate>, DiffError> {
        match (self, other) {
            (VfsNode::Dir(dself), VfsNode::Dir(dother)) => {
                dself.reconciliation_diff(dother, parent_path)
            }
            (VfsNode::File(fself), VfsNode::File(fother)) => {
                let mut file_path = parent_path.to_owned();
                file_path.push(fself.name());
                let update = VfsDiff::file_created(file_path);

                let update = if fself.size() == fother.size() {
                    VirtualReconciledUpdate::backend_check_both(&update)
                } else {
                    VirtualReconciledUpdate::conflict_both(&update)
                };

                Ok(SortedVec::from_vec(vec![update]))
            }
            (nself, _) => {
                let mut file_path = parent_path.to_owned();
                file_path.push(nself.name());
                let update = VfsDiff::file_modified(file_path);

                let update = VirtualReconciledUpdate::conflict_both(&update);

                Ok(SortedVec::from_vec(vec![update]))
            }
        }
    }

    fn path(&self, parent_path: &VirtualPath) -> VirtualPathBuf {
        let mut path = parent_path.to_owned();
        path.push(self.name());
        path
    }

    /// Returns the list of errors for this node and its children
    pub fn get_errors_list(
        &self,
        parent_path: &VirtualPath,
    ) -> Vec<(VirtualPathBuf, FailedUpdateApplication)> {
        let mut path = self.path(parent_path);

        match self {
            VfsNode::Dir(dir) => dir.get_errors(&path),
            VfsNode::File(file) => {
                if let NodeState::Error(err) = file.state() {
                    path.push(file.name());
                    vec![(path, err.to_owned())]
                } else {
                    Vec::new()
                }
            }
        }
    }

    /// Returns the list of conflicts for this node and its children
    pub fn get_conflicts_list(&self, parent_path: &VirtualPath) -> Vec<VirtualPathBuf> {
        let mut path = self.path(parent_path);

        match self {
            VfsNode::Dir(dir) => dir.get_conflicts(&path),
            VfsNode::File(file) => {
                if file.state().is_conflict() {
                    path.push(file.name());
                    vec![path]
                } else {
                    Vec::new()
                }
            }
        }
    }

    /// Returns true if a node removal can be skipped because the node creation has not been applied
    /// on the other FS
    pub fn can_skip_removal(&self) -> bool {
        if let NodeState::Error(failed_update) = self.state() {
            return failed_update.update().is_creation();
        }
        false
    }

    /// Creates a diff where this node has been removed from the VFS
    pub fn to_removed_diff(&self, parent_path: &VirtualPath) -> VfsDiff {
        match self {
            VfsNode::Dir(_) => VfsDiff::dir_removed(self.path(parent_path)),
            VfsNode::File(_) => VfsDiff::file_removed(self.path(parent_path)),
        }
    }

    /// Creates a diff where this node has been inserted into the VFS
    pub fn to_created_diff(&self, parent_path: &VirtualPath) -> VfsDiff {
        match self {
            VfsNode::Dir(_) => VfsDiff::dir_created(self.path(parent_path)),
            VfsNode::File(_) => VfsDiff::file_created(self.path(parent_path)),
        }
    }
}

impl<SyncInfo: IsModified> VfsNode<SyncInfo> {
    /// Diff two nodes based on their content.
    ///
    /// This uses the `SyncInfo` metadata and does not need to query the concrete filesystem.g
    pub fn diff(
        &self,
        other: &VfsNode<SyncInfo>,
        parent_path: &VirtualPath,
    ) -> Result<VfsDiffList, DiffError> {
        if self.name() != other.name() {
            return Err(NameMismatchError {
                found: self.name().to_string(),
                expected: other.name().to_string(),
            }
            .into());
        }

        // If the node is in error state, we retry the same update
        if let NodeState::Error(failed_update) = self.state() {
            return Ok(VfsDiffList::from_vec(vec![failed_update.update().clone()]));
        }

        match (self, other) {
            (VfsNode::Dir(dself), VfsNode::Dir(dother)) => dself.diff(dother, parent_path),
            (VfsNode::File(fself), VfsNode::File(fother)) => {
                // Diff the file based on their sync info
                if fself.state().is_modified(fother.state()) {
                    let mut file_path = parent_path.to_owned();
                    file_path.push(fself.name());

                    let diff = VfsDiff::file_modified(file_path);
                    Ok(VfsDiffList::from_vec(vec![diff]))
                } else {
                    Ok(VfsDiffList::new())
                }
            }
            (nself, nother) => Ok(VfsDiffList::from_vec(vec![
                nself.to_removed_diff(parent_path),
                nother.to_created_diff(parent_path),
            ])),
        }
    }
}

// Converts into a generic node by dropping the backend specific sync info
impl<SyncInfo> From<&VfsNode<SyncInfo>> for VfsNode<()> {
    fn from(value: &VfsNode<SyncInfo>) -> Self {
        match value {
            VfsNode::Dir(dir_tree) => VfsNode::Dir(dir_tree.into()),
            VfsNode::File(file_meta) => VfsNode::File(file_meta.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::TestNode::{D, DE, DH, F, FE, FH};

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
            vec![VfsDiff::file_modified(
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
            vec![VfsDiff::file_removed(
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
                VfsDiff::file_removed(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
                VfsDiff::file_created(VirtualPathBuf::new("/Doc/f3.pdf").unwrap())
            ]
            .into()
        );

        let modified = DH(
            "",
            10,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 2), FH("f2.pdf", 3)]),
                DH("a", 14, vec![DH("bc", 15, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_recursive_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![
                VfsDiff::dir_removed(VirtualPathBuf::new("/a/b").unwrap()),
                VfsDiff::dir_created(VirtualPathBuf::new("/a/bc").unwrap(),)
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
            vec![VfsDiff::file_modified(
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
            vec![VfsDiff::file_removed(
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
                VfsDiff::file_removed(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
                VfsDiff::file_created(VirtualPathBuf::new("/Doc/f3.pdf").unwrap())
            ]
            .into()
        );

        let modified = DH(
            "",
            0,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 2), FH("f2.pdf", 3)]),
                DH("a", 14, vec![DH("bc", 15, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node_shallow_diff();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![
                VfsDiff::dir_removed(VirtualPathBuf::new("/a/b").unwrap()),
                VfsDiff::dir_created(VirtualPathBuf::new("/a/bc").unwrap(),)
            ]
            .into()
        );
    }

    /// Test a Vfs diff where some nodes are in error state
    #[test]
    fn test_diff_error() {
        let reference = DH(
            "",
            0,
            vec![
                DH("Doc", 1, vec![FE("f1.md", ""), FH("f2.pdf", 3)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node();

        let modified = DH(
            "",
            0,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 2), FH("f2.pdf", 3)]),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![VfsDiff::file_created(
                VirtualPathBuf::new("/Doc/f1.md").unwrap()
            )]
            .into()
        );

        let reference = DH(
            "",
            0,
            vec![
                DE("Doc", ""),
                DH("a", 4, vec![DH("b", 5, vec![DH("c", 6, vec![])])]),
            ],
        )
        .into_node();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![VfsDiff::dir_created(VirtualPathBuf::new("/Doc").unwrap())].into()
        );

        let reference = DH(
            "",
            0,
            vec![
                DH("Doc", 1, vec![FH("f1.md", 2), FH("f2.pdf", 3)]),
                DH("a", 4, vec![DE("b", "")]),
            ],
        )
        .into_node();

        let diff = reference.diff(&modified, VirtualPath::root()).unwrap();

        assert_eq!(
            diff,
            vec![VfsDiff::dir_created(VirtualPathBuf::new("/a/b").unwrap())].into()
        );
    }
}
