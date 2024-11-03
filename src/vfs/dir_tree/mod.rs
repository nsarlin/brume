//! Tree structure used for recursive directory walking

mod dir;
mod file;

use std::cmp::Ordering;

pub use dir::*;
pub use file::*;

use crate::{
    concrete::{concrete_eq_file, ConcreteFS},
    sorted_list::{Sortable, SortedList},
    Error,
};

use super::{
    DiffError, IsModified, ModificationState, SortedPatchList, VfsNodePatch, VirtualPath,
    VirtualPathBuf,
};

type SortedNodeList<SyncInfo> = SortedList<TreeNode<SyncInfo>>;

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

    /// Create a new directory with syncinfo in the [`SyncInfoState::Invalid`] state
    pub fn new_invalid(name: &str) -> Self {
        Self {
            info: DirInfo::new_invalid(name),
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

    pub fn sync_info(&self) -> &SyncInfoState<SyncInfo> {
        self.info.sync_info()
    }

    /// Return a reference to the dir at the given path. Return an error if the path does
    /// not point to a valid directory node.
    pub fn find_dir(&self, path: &VirtualPath) -> Result<&DirTree<SyncInfo>, Error> {
        self.find_node(path)
            .ok_or(Error::InvalidPath(path.to_owned()))
            .and_then(|node| match node {
                TreeNode::Dir(dir) => Ok(dir),
                TreeNode::File(_) => Err(Error::InvalidPath(path.to_owned())),
            })
    }

    /// Return a reference to the file at the given path. Return an error if the path does
    /// not point to a valid file.
    pub fn find_file(&self, path: &VirtualPath) -> Result<&FileInfo<SyncInfo>, Error> {
        self.find_node(path)
            .ok_or(Error::InvalidPath(path.to_owned()))
            .and_then(|node| match node {
                TreeNode::Dir(_) => Err(Error::InvalidPath(path.to_owned())),
                TreeNode::File(file) => Ok(file),
            })
    }

    /// Return a reference to the node at the given path. Return an error if the path does
    /// not point to a valid node.
    pub fn find_node(&self, path: &VirtualPath) -> Option<&TreeNode<SyncInfo>> {
        if let Some((top_level, remainder)) = path.top_level_split() {
            if remainder.is_root() {
                self.children.find(top_level)
            } else {
                let child = self.children.find(top_level)?;

                match child {
                    TreeNode::Dir(dir) => dir.find_node(remainder),
                    TreeNode::File(_) => None,
                }
            }
        } else {
            None
        }
    }

    /// Return a mutable reference to the dir at the given path. Return an error if the path does
    /// not point to a valid directory node.
    pub fn find_dir_mut(&mut self, path: &VirtualPath) -> Result<&mut DirTree<SyncInfo>, Error> {
        self.find_node_mut(path)
            .ok_or(Error::InvalidPath(path.to_owned()))
            .and_then(|node| match node {
                TreeNode::Dir(dir) => Ok(dir),
                TreeNode::File(_) => Err(Error::InvalidPath(path.to_owned())),
            })
    }

    /// Return a mutable reference to the file at the given path. Return an error if the path does
    /// not point to a valid file.
    pub fn find_file_mut(&mut self, path: &VirtualPath) -> Result<&mut FileInfo<SyncInfo>, Error> {
        self.find_node_mut(path)
            .ok_or(Error::InvalidPath(path.to_owned()))
            .and_then(|node| match node {
                TreeNode::Dir(_) => Err(Error::InvalidPath(path.to_owned())),
                TreeNode::File(file) => Ok(file),
            })
    }

    /// Return a mutable reference to the node at the given path. Return an error if the path does
    /// not point to a valid node.
    fn find_node_mut(&mut self, path: &VirtualPath) -> Option<&mut TreeNode<SyncInfo>> {
        if let Some((top_level, remainder)) = path.top_level_split() {
            if remainder.is_root() {
                self.children.find_mut(top_level)
            } else {
                let child = self.children.find_mut(top_level)?;

                match child {
                    TreeNode::Dir(dir) => dir.find_node_mut(remainder),
                    TreeNode::File(_) => None,
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

    /// Remove a child with the given kind from this directory. If there were no child with this
    /// name and kind, return false.
    fn remove_child_kind(&mut self, child_name: &str, node_kind: NodeKind) -> bool {
        self.children
            .remove_if(child_name, |child| child.kind() == node_kind)
    }

    /// Remove a child dir from this directory. If there were no child directory with this name,
    /// return false.
    pub fn remove_child_dir(&mut self, child_name: &str) -> bool {
        self.remove_child_kind(child_name, NodeKind::Dir)
    }

    /// Remove a child file from this directory. If there were no child file with this name,
    /// return false.
    pub fn remove_child_file(&mut self, child_name: &str) -> bool {
        self.remove_child_kind(child_name, NodeKind::File)
    }

    /// Delete the node with the current path in the tree. Return an error if the path is not a
    /// valid node.
    pub fn delete_node(&mut self, path: &VirtualPath) -> Result<(), Error> {
        self.delete_node_kind(path, None)
    }

    /// Delete the dir with the current path in the tree. Return an error if the path is not a
    /// valid directory.
    pub fn delete_dir(&mut self, path: &VirtualPath) -> Result<(), Error> {
        self.delete_node_kind(path, Some(NodeKind::Dir))
    }

    /// Delete the file with the current path in the tree. Return an error if the path is not a
    /// valid file.
    pub fn delete_file(&mut self, path: &VirtualPath) -> Result<(), Error> {
        self.delete_node_kind(path, Some(NodeKind::File))
    }

    fn delete_node_kind(
        &mut self,
        path: &VirtualPath,
        kind: Option<NodeKind>,
    ) -> Result<(), Error> {
        if let Some(parent) = path.parent() {
            self.find_dir_mut(parent).and_then(|dir| {
                let removed = if let Some(kind) = kind {
                    dir.remove_child_kind(path.name(), kind)
                } else {
                    dir.remove_child(path.name())
                };
                if removed {
                    Ok(())
                } else {
                    Err(Error::InvalidPath(path.to_owned()))
                }
            })
        } else if let Some(NodeKind::File) = kind {
            // If the path is the root but we requested a file removal, it is an error
            Err(Error::InvalidPath(path.to_owned()))
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

    pub async fn concrete_diff<Concrete: ConcreteFS, OtherSyncInfo, OtherConcrete: ConcreteFS>(
        &self,
        other: &DirTree<OtherSyncInfo>,
        concrete_self: &Concrete,
        concrete_other: &OtherConcrete,
        parent_path: &VirtualPath,
    ) -> Result<SortedPatchList, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        // Here we cannot use `iter_zip_map` or an async variant of it because it does not seem
        // possible to express the lifetimes required by the async closures

        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.name());

        let mut ret = SortedList::new();
        let mut self_iter = self.children.iter();
        let mut other_iter = other.children.iter();

        let mut self_item_opt = self_iter.next();
        let mut other_item_opt = other_iter.next();

        while let (Some(self_item), Some(other_item)) = (self_item_opt, other_item_opt) {
            match self_item.key().cmp(other_item.key()) {
                Ordering::Less => {
                    ret.insert(self_item.to_removed_diff(&dir_path));
                    self_item_opt = self_iter.next()
                }
                // We can use `unchecked_extend` because we know that the patches will be produced
                // in order
                Ordering::Equal => {
                    ret.unchecked_extend(
                        Box::pin(self_item.concrete_diff(
                            other_item,
                            concrete_self,
                            concrete_other,
                            &dir_path,
                        ))
                        .await?,
                    );
                    self_item_opt = self_iter.next();
                    other_item_opt = other_iter.next();
                }
                Ordering::Greater => {
                    ret.insert(other_item.to_created_diff(&dir_path));
                    other_item_opt = other_iter.next();
                }
            }
        }

        // Handle the remaining nodes that are present in an iterator and not the
        // other one
        while let Some(self_item) = self_item_opt {
            ret.insert(self_item.to_removed_diff(&dir_path));
            self_item_opt = self_iter.next();
        }

        while let Some(other_item) = other_item_opt {
            ret.insert(other_item.to_created_diff(&dir_path));
            other_item_opt = other_iter.next();
        }

        Ok(ret)
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

impl<SyncInfo: IsModified<SyncInfo>> DirTree<SyncInfo> {
    /// Diff two directories based on their content.
    pub fn diff(
        &self,
        other: &DirTree<SyncInfo>,
        parent_path: &VirtualPath,
    ) -> Result<SortedPatchList, DiffError> {
        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.name());

        match self.sync_info().modification_state(other.sync_info()) {
            // The SyncInfo tells us that nothing has been modified for this dir, but can't
            // speak about its children. So we need to walk them.
            ModificationState::ShallowUnmodified => {
                if self.children.len() != other.children.len() {
                    return Err(DiffError::InvalidSyncInfo(dir_path));
                }
                let mut self_dirs = self.children.iter();
                let mut other_dirs = other.children.iter();

                let diffs = std::iter::zip(self_dirs.by_ref(), other_dirs.by_ref())
                    .map(|(self_child, other_child)| self_child.diff(other_child, &dir_path))
                    .collect::<Result<Vec<_>, _>>()?;

                // Since the children list is sorted, we know that the resulting diffs, will be also
                // sorted, so we can call `unchecked_flatten`
                Ok(SortedList::unchecked_flatten(diffs))
            }
            // The SyncInfo tells us that nothing has been modified recursively, so we can
            // stop there
            ModificationState::RecursiveUnmodified => Ok(SortedPatchList::new()),
            // The directory has been modified, so we have to walk it recursively to find
            // the modified nodes
            ModificationState::Modified => {
                let diff_list = self.children.iter_zip_map(
                    &other.children,
                    |self_child| -> Result<_, DiffError> {
                        let mut res = SortedList::new();
                        res.insert(self_child.to_removed_diff(&dir_path));
                        Ok(res)
                    },
                    |self_child, other_child| self_child.diff(other_child, &dir_path),
                    |other_child| {
                        let mut res = SortedList::new();
                        res.insert(other_child.to_created_diff(&dir_path));
                        Ok(res)
                    },
                )?;

                // Since the children lists are sorted, we know that the produced patches will be
                // too, so we can directly create the sorted list from the result
                let diffs = SortedList::unchecked_flatten(diff_list);

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
pub enum TreeNode<SyncInfo> {
    Dir(DirTree<SyncInfo>),
    File(FileInfo<SyncInfo>),
}

impl<SyncInfo> Sortable for TreeNode<SyncInfo> {
    type Key = str;

    fn key(&self) -> &Self::Key {
        self.name()
    }
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

    pub fn kind(&self) -> NodeKind {
        match self {
            TreeNode::Dir(_) => NodeKind::Dir,
            TreeNode::File(_) => NodeKind::File,
        }
    }

    pub fn find_dir(&self, path: &VirtualPath) -> Result<&DirTree<SyncInfo>, Error> {
        self.find_node(path).and_then(|node| match node {
            TreeNode::Dir(dir) => Ok(dir),
            TreeNode::File(_) => Err(Error::InvalidPath(path.to_owned())),
        })
    }

    pub fn find_file(&self, path: &VirtualPath) -> Result<&FileInfo<SyncInfo>, Error> {
        self.find_node(path).and_then(|node| match node {
            TreeNode::Dir(_) => Err(Error::InvalidPath(path.to_owned())),
            TreeNode::File(file) => Ok(file),
        })
    }

    pub fn find_node(&self, path: &VirtualPath) -> Result<&Self, Error> {
        if path.is_root() {
            Ok(self)
        } else {
            match self {
                TreeNode::Dir(dir) => dir
                    .find_node(path)
                    .ok_or(Error::InvalidPath(path.to_owned())),
                TreeNode::File(file) => {
                    if path.len() == 1 && file.name() == path.name() {
                        Ok(self)
                    } else {
                        Err(Error::InvalidPath(path.to_owned()))
                    }
                }
            }
        }
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
    pub fn to_removed_diff(&self, parent_path: &VirtualPath) -> VfsNodePatch {
        match self {
            TreeNode::Dir(_) => VfsNodePatch::DirRemoved(self.path(parent_path)),
            TreeNode::File(_) => VfsNodePatch::FileRemoved(self.path(parent_path)),
        }
    }

    pub async fn concrete_diff<Concrete: ConcreteFS, OtherSyncInfo, OtherConcrete: ConcreteFS>(
        &self,
        other: &TreeNode<OtherSyncInfo>,
        concrete_self: &Concrete,
        concrete_other: &OtherConcrete,
        parent_path: &VirtualPath,
    ) -> Result<SortedPatchList, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        // TODO: handle error ?
        assert_eq!(self.name(), other.name());

        match (self, other) {
            (TreeNode::Dir(dself), TreeNode::Dir(dother)) => {
                dself
                    .concrete_diff(dother, concrete_self, concrete_other, parent_path)
                    .await
            }
            (TreeNode::File(fself), TreeNode::File(_fother)) => {
                // Diff the file based on their hash
                let mut file_path = parent_path.to_owned();
                file_path.push(fself.name());

                if !concrete_eq_file(concrete_self, concrete_other, &file_path).await? {
                    let diff = VfsNodePatch::FileModified(file_path);
                    Ok(SortedPatchList::from([diff]))
                } else {
                    Ok(SortedPatchList::new())
                }
            }
            (nself, nother) => Ok(SortedPatchList::from_vec(vec![
                nself.to_removed_diff(parent_path),
                nother.to_created_diff(parent_path),
            ])),
        }
    }
}

impl<SyncInfo> TreeNode<SyncInfo> {
    /// Create a diff where this node has been inserted into the VFS
    pub fn to_created_diff(&self, parent_path: &VirtualPath) -> VfsNodePatch {
        match self {
            TreeNode::Dir(_) => VfsNodePatch::DirCreated(self.path(parent_path)),
            TreeNode::File(_) => VfsNodePatch::FileCreated(self.path(parent_path)),
        }
    }
}

impl<SyncInfo: IsModified<SyncInfo>> TreeNode<SyncInfo> {
    /// Diff two nodes based on their content.
    ///
    /// This uses the `SyncInfo` metadata and does not need to query the concrete filesystem.
    pub fn diff(
        &self,
        other: &TreeNode<SyncInfo>,
        parent_path: &VirtualPath,
    ) -> Result<SortedPatchList, DiffError> {
        if self.name() != other.name() {
            return Err(DiffError::InvalidSyncInfo(parent_path.to_owned()));
        }

        match (self, other) {
            (TreeNode::Dir(dself), TreeNode::Dir(dother)) => dself.diff(dother, parent_path),
            (TreeNode::File(fself), TreeNode::File(fother)) => {
                // Diff the file based on their sync info
                if fself.sync_info().is_modified(fother.sync_info()) {
                    let mut file_path = parent_path.to_owned();
                    file_path.push(fself.name());

                    let diff = VfsNodePatch::FileModified(file_path);
                    Ok(SortedPatchList::from_vec(vec![diff]))
                } else {
                    Ok(SortedPatchList::new())
                }
            }
            (nself, nother) => Ok(SortedPatchList::from_vec(vec![
                nself.to_removed_diff(parent_path),
                nother.to_created_diff(parent_path),
            ])),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        test_utils::TestNode::{D, DH, F, FF, FH},
        vfs::Vfs,
    };

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
            vec![VfsNodePatch::FileModified(
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
            vec![VfsNodePatch::FileRemoved(
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
                VfsNodePatch::FileRemoved(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
                VfsNodePatch::FileCreated(VirtualPathBuf::new("/Doc/f3.pdf").unwrap())
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
                VfsNodePatch::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
                VfsNodePatch::DirCreated(VirtualPathBuf::new("/a/ba").unwrap(),)
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
            vec![VfsNodePatch::FileModified(
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
            vec![VfsNodePatch::FileRemoved(
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
                VfsNodePatch::FileRemoved(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
                VfsNodePatch::FileCreated(VirtualPathBuf::new("/Doc/f3.pdf").unwrap())
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
                VfsNodePatch::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
                VfsNodePatch::DirCreated(VirtualPathBuf::new("/a/ba").unwrap(),)
            ]
            .into()
        );
    }

    #[tokio::test]
    async fn test_concrete_diff() {
        let local_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let local_vfs = Vfs::new(local_base.clone().into_node());

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hell"), FF("f2.pdf", b"world")]),
                D(
                    "a",
                    vec![D("b", vec![D("c", vec![]), FF("test.log", b"value")])],
                ),
                D("e", vec![]),
            ],
        );
        let remote_vfs = Vfs::new(remote_base.clone().into_node());

        let diff = local_vfs
            .root()
            .concrete_diff(
                remote_vfs.root(),
                &local_base,
                &remote_base,
                VirtualPath::root(),
            )
            .await
            .unwrap();

        let reference_diff = [
            VfsNodePatch::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodePatch::FileCreated(VirtualPathBuf::new("/a/b/test.log").unwrap()),
            VfsNodePatch::DirRemoved(VirtualPathBuf::new("/e/g").unwrap()),
        ]
        .into();

        assert_eq!(diff, reference_diff);
    }
}
