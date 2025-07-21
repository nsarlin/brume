//! An in-memory representation of a FileSystem using a tree structure.
//!
//! The Vfs is used to be able to apply filesystems diff algorithms that are independent of the
//! concrete filesystem behind it.

pub mod dir_tree;

pub mod virtual_path;

use std::fmt::Debug;

use chrono::{DateTime, Utc};
pub use dir_tree::*;
use serde::{Deserialize, Serialize};
pub use virtual_path::*;

use crate::{
    NameMismatchError,
    concrete::{InvalidBytesSyncInfo, ToBytes, TryFromBytes},
    update::{
        DiffError, FailedUpdateApplication, IsModified, VfsConflict, VfsDiff, VfsDiffList,
        VfsUpdate, VfsUpdateApplicationError,
    },
};

/// The virtual in-memory representation of a file system.
///
/// The VFS serves 2 purposes
/// - Detecting [`updates`] by comparing its structure and metadata at different points in time
/// - Storing status information such as errors or conflicts
///
/// `Data` is a custom metadata type that will be stored with FS nodes.
///
/// [`updates`]: crate::update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vfs<Data> {
    root: VfsNode<Data>,
}

/// A [`Vfs`] storing a [`NodeState`] inside each nodes
pub type StatefulVfs<Data> = Vfs<NodeState<Data>>;

impl<Data> Vfs<Data> {
    pub fn new(root: VfsNode<Data>) -> Self {
        Self { root }
    }

    /// Return the root node of the VFS
    pub fn root(&self) -> &VfsNode<Data> {
        &self.root
    }

    /// Returns a mutable access to the root node of the VFS
    pub fn root_mut(&mut self) -> &mut VfsNode<Data> {
        &mut self.root
    }

    /// Returns the dir at `path` in the VFS
    pub fn find_dir(&self, path: &VirtualPath) -> Result<&DirTree<Data>, InvalidPathError> {
        self.root.find_dir(path)
    }

    /// Returns the file at `path` in the VFS
    pub fn find_file(&self, path: &VirtualPath) -> Result<&FileInfo<Data>, InvalidPathError> {
        self.root.find_file(path)
    }

    /// Returns the node at `path` in the VFS
    pub fn find_node(&self, path: &VirtualPath) -> Option<&VfsNode<Data>> {
        self.root.find_node(path)
    }

    /// Returns the dir at `path` in the VFS, as mutable
    pub fn find_dir_mut(
        &mut self,
        path: &VirtualPath,
    ) -> Result<&mut DirTree<Data>, InvalidPathError> {
        self.root.find_dir_mut(path)
    }

    /// Returns the file at `path` in the VFS, as mutable
    pub fn find_file_mut(
        &mut self,
        path: &VirtualPath,
    ) -> Result<&mut FileInfo<Data>, InvalidPathError> {
        self.root.find_file_mut(path)
    }

    /// Returns the node at `path` in the VFS, as mutable
    pub fn find_node_mut(&mut self, path: &VirtualPath) -> Option<&mut VfsNode<Data>> {
        self.root.find_node_mut(path)
    }

    /// Deletes the node with the current path in the Vfs.
    ///
    /// Return an error if the path is not a valid node.
    pub fn delete_node(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        self.root_mut().as_dir_mut()?.delete_node(path)
    }

    /// Deletes the dir with the current path in the Vfs.
    ///
    /// Returns an error if the path is not a valid directory.
    pub fn delete_dir(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        self.root_mut().as_dir_mut()?.delete_dir(path)
    }

    /// Deletes the file with the current path in the Vfs.
    ///
    /// Returns an error if the path is not a valid file.
    pub fn delete_file(&mut self, path: &VirtualPath) -> Result<(), DeleteNodeError> {
        self.root_mut().as_dir_mut()?.delete_file(path)
    }
}

impl<Data> StatefulVfs<Data> {
    /// Returns the update inside a node that is in conflict state
    pub fn find_conflict(&self, path: &VirtualPath) -> Option<&VfsConflict> {
        let node = self.find_node(path)?;

        match node.state() {
            NodeState::Conflict(update) => Some(update),
            _ => None,
        }
    }

    /// Structural comparison of two VFS, looking at the names of files and directories, but
    /// ignoring the content of files.
    pub fn structural_eq<OtherSyncInfo>(&self, other: &StatefulVfs<OtherSyncInfo>) -> bool {
        self.root.structural_eq(other.root())
    }

    pub fn empty() -> Self {
        Self {
            root: VfsNode::Dir(DirTree::new_force_resync("", DateTime::<Utc>::MIN_UTC)),
        }
    }

    /// Returns the list of nodes with error from the concrete FS
    pub fn get_errors(&self) -> Vec<FailedUpdateApplication> {
        self.root().get_errors_list()
    }

    /// Returns the list of nodes with a conflict that should be manually resolved
    pub fn get_conflicts(&self) -> Vec<VirtualPathBuf> {
        self.root().get_conflicts_list(VirtualPath::root())
    }
}

impl<Data: Clone> StatefulVfs<Data> {
    /// Applies an update to the Vfs, by adding or removing nodes.
    ///
    /// The created or modified nodes uses the SyncInfo from the [`VfsUpdate`].
    pub fn apply_update(
        &mut self,
        update: &VfsUpdate<Data>,
        loaded_vfs: &Vfs<Data>,
    ) -> Result<(), VfsUpdateApplicationError> {
        let path = update.path().to_owned();
        let parent_path = path.parent();

        // Only supported update on root node is to modify its SyncInfo
        if parent_path.is_none() && !matches!(update, VfsUpdate::DirModified(_)) {
            return Err(VfsUpdateApplicationError::PathIsRoot);
        }

        let mut parent = parent_path
            .map(|path| self.root_mut().find_dir_mut(path))
            .transpose()?;

        if let Some(parent) = parent.as_mut() {
            // Invalidate parent sync info because its content has been changed
            // parent.force_resync();

            // Remove the child if in error. It will be restored as Ok or as an Error based on the
            // result of the last concrete application attempt
            parent.remove_child_if(update.path().name(), |child| {
                child.state().is_err() && update.is_creation()
            });
        }

        match update {
            VfsUpdate::DirCreated(update) => {
                let child = VfsNode::Dir(update.clone().into());

                if path.name() != child.name() {
                    return Err(NameMismatchError {
                        expected: path.name().to_string(),
                        found: child.name().to_string(),
                    }
                    .into());
                }
                parent.unwrap().insert_child(child);
                Ok(())
            }
            VfsUpdate::DirRemoved(path) => self.delete_dir(path).map_err(|e| e.into()),
            VfsUpdate::DirModified(update) => {
                let dir = self.root_mut().find_dir_mut(update.path())?;
                let state = dir.state_mut();
                *state = update.state().clone().into_metadata();
                Ok(())
            }
            VfsUpdate::FileCreated(update) => {
                let child = VfsNode::File(FileInfo::new(
                    path.name(),
                    update.file_size(),
                    update.last_modified(),
                    NodeState::Ok(update.sync_info().clone()),
                ));
                parent.unwrap().insert_child(child);
                Ok(())
            }
            VfsUpdate::FileModified(update) => {
                let file = self.root_mut().find_file_mut(update.path())?;
                file.set_size(update.file_size());
                let state = file.state_mut();
                *state = NodeState::Ok(update.sync_info().clone());
                Ok(())
            }
            VfsUpdate::FileRemoved(path) => self.delete_file(path).map_err(|e| e.into()),
            VfsUpdate::FailedApplication(failure) => {
                let mut node = loaded_vfs
                    .find_node(failure.path())
                    .map(|node| node.clone().as_ok())
                    // If not found on the loaded vfs, it might have been removed so we try to get
                    // the node on the status one
                    .or_else(|| self.find_node(failure.path()).cloned())
                    .ok_or_else(|| {
                        VfsUpdateApplicationError::InvalidPath(InvalidPathError::NotFound(
                            failure.path().to_owned(),
                        ))
                    })?;

                let state = NodeState::Error(failure.clone());
                node.set_state(state);

                // Ok to unwrap because we checked earlier that "parent" exists
                let parent = self
                    .root_mut()
                    .find_dir_mut(path.parent().unwrap())
                    .unwrap();
                parent.replace_child(node);

                Ok(())
            }
            VfsUpdate::Conflict(conflict) => {
                let mut node = loaded_vfs
                    .find_node(conflict.path())
                    .map(|node| node.clone().as_ok())
                    // If not found on the loaded vfs, it might have been removed so we try to get
                    // the node on the status one
                    .or_else(|| self.find_node(conflict.path()).cloned())
                    .ok_or_else(|| {
                        VfsUpdateApplicationError::InvalidPath(InvalidPathError::NotFound(
                            conflict.path().to_owned(),
                        ))
                    })?;

                let state = NodeState::Conflict(conflict.clone());
                node.set_state(state);

                // Ok to unwrap because we checked earlier that "parent" exists
                let parent = self
                    .root_mut()
                    .find_dir_mut(path.parent().unwrap())
                    .unwrap();

                parent.replace_child(node);

                Ok(())
            }
        }
    }

    /// Updates the state of a node
    pub fn update_node_state(
        &mut self,
        path: &VirtualPath,
        state: NodeState<Data>,
    ) -> Result<(), VfsUpdateApplicationError> {
        let parent = self
            .root_mut()
            .find_dir_mut(path.parent().ok_or(VfsUpdateApplicationError::PathIsRoot)?)?;

        // Invalidate parent sync info because its content has been changed
        parent.force_resync();

        let node = self.root_mut().find_node_mut(path).ok_or_else(|| {
            VfsUpdateApplicationError::InvalidPath(InvalidPathError::NotFound(path.to_owned()))
        })?;
        node.set_state(state);

        Ok(())
    }
}

impl<Data: IsModified + Clone + Debug> StatefulVfs<Data> {
    /// Diff two VFS by comparing their nodes.
    ///
    /// This function returns a sorted list of [`VfsDiff`].
    /// The node comparison is based on the `SyncInfo` and might be recursive based on the result of
    /// [`modification_state`]. The result of the SyncInfo comparison on node is trusted.
    ///
    /// [`modification_state`]: IsModified::modification_state
    pub fn diff(&self, other: &Vfs<Data>) -> Result<VfsDiffList, DiffError> {
        self.root.diff(other.root(), VirtualPath::root())
    }
}

// Converts into a generic vfs by dropping the backend specific sync info
impl<Data> From<&StatefulVfs<Data>> for StatefulVfs<()> {
    fn from(value: &StatefulVfs<Data>) -> Self {
        Self {
            root: (&value.root).into(),
        }
    }
}

impl<Data: ToBytes> From<&StatefulVfs<Data>> for StatefulVfs<Vec<u8>> {
    fn from(value: &StatefulVfs<Data>) -> Self {
        Vfs::new((&value.root).into())
    }
}

impl<Data: TryFromBytes> TryFrom<StatefulVfs<Vec<u8>>> for StatefulVfs<Data> {
    type Error = InvalidBytesSyncInfo;

    fn try_from(value: StatefulVfs<Vec<u8>>) -> Result<Self, Self::Error> {
        value.root.try_into().map(Vfs::new)
    }
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use crate::{
        test_utils::ShallowTestSyncInfo,
        update::{VfsDirCreation, VfsFileUpdate},
    };

    use super::*;
    use crate::test_utils::TestNode::{D, F};

    #[test]
    fn test_apply_update() {
        let base = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_node();

        // Test dir creation
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D(
                    "e",
                    vec![D(
                        "g",
                        vec![F("tmp.txt"), D("h", vec![F("file.bin"), D("i", vec![])])],
                    )],
                ),
            ],
        )
        .into_node()
        .unwrap();

        let new_dir = D("h", vec![F("file.bin"), D("i", vec![])]).into_dir();
        let update = VfsUpdate::DirCreated(VfsDirCreation::new(
            &VirtualPathBuf::new("/e/g").unwrap(),
            new_dir,
        ));
        let ref_vfs = Vfs::new(updated);

        vfs.apply_update(&update, &ref_vfs).unwrap();
        // In real life the update will come with an update of its parent path
        assert_eq!(
            vfs.diff(&ref_vfs).unwrap(),
            vec![VfsDiff::dir_modified(VirtualPathBuf::new("/e/g").unwrap())].into()
        );

        // Test dir removal
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![]),
            ],
        )
        .into_node()
        .unwrap();

        let update = VfsUpdate::DirRemoved(VirtualPathBuf::new("/e/g").unwrap());
        let ref_vfs = Vfs::new(updated);

        vfs.apply_update(&update, &ref_vfs).unwrap();
        // In real life the update will come with an update of its parent path
        assert_eq!(
            vfs.diff(&ref_vfs).unwrap(),
            vec![VfsDiff::dir_modified(VirtualPathBuf::new("/e").unwrap())].into()
        );

        // Test file creation
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt"), F("file.bin")])]),
            ],
        )
        .into_node()
        .unwrap();

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = VfsUpdate::FileCreated(VfsFileUpdate::new(
            &VirtualPathBuf::new("/e/g/file.bin").unwrap(),
            0,
            Utc::now(),
            new_file_info,
        ));
        let ref_vfs = Vfs::new(updated);

        vfs.apply_update(&update, &ref_vfs).unwrap();
        // In real life the update will come with an update of its parent path
        assert_eq!(
            vfs.diff(&ref_vfs).unwrap(),
            vec![VfsDiff::dir_modified(VirtualPathBuf::new("/e/g").unwrap())].into()
        );

        // Test file modification
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_node()
        .unwrap();

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = VfsUpdate::FileModified(VfsFileUpdate::new(
            &VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            0,
            Utc::now(),
            new_file_info,
        ));
        let ref_vfs = Vfs::new(updated);

        vfs.apply_update(&update, &ref_vfs).unwrap();

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());

        // Test file removal
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_node()
        .unwrap();

        let update = VfsUpdate::FileRemoved(VirtualPathBuf::new("/Doc/f2.pdf").unwrap());
        let ref_vfs = Vfs::new(updated);

        vfs.apply_update(&update, &ref_vfs).unwrap();
        // In real life the update will come with an update of its parent path
        assert_eq!(
            vfs.diff(&ref_vfs).unwrap(),
            vec![VfsDiff::dir_modified(VirtualPathBuf::new("/Doc").unwrap())].into()
        );
    }

    // Specific tests for when the update is applied to the root since it is sometimes handled
    // differently
    #[test]
    fn test_apply_update_root() {
        let base = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_node();

        // Test dir creation
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
                D("h", vec![F("file.bin"), D("i", vec![])]),
            ],
        )
        .into_node()
        .unwrap();

        let new_dir = D("h", vec![F("file.bin"), D("i", vec![])]).into_dir();
        let update = VfsUpdate::DirCreated(VfsDirCreation::new(
            &VirtualPathBuf::new("/").unwrap(),
            new_dir,
        ));
        let ref_vfs = Vfs::new(updated);

        vfs.apply_update(&update, &ref_vfs).unwrap();

        assert_eq!(
            vfs.diff(&ref_vfs).unwrap(),
            vec![VfsDiff::dir_modified(VirtualPathBuf::root())].into()
        );

        // Test dir removal
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
            ],
        )
        .into_node()
        .unwrap();

        let update = VfsUpdate::DirRemoved(VirtualPathBuf::new("/e").unwrap());
        let ref_vfs = Vfs::new(updated);

        vfs.apply_update(&update, &ref_vfs).unwrap();

        assert_eq!(
            vfs.diff(&ref_vfs).unwrap(),
            vec![VfsDiff::dir_modified(VirtualPathBuf::root())].into()
        );

        // Test file creation
        let mut vfs = Vfs::new(base.clone());

        let base = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
                F("file.bin"),
            ],
        )
        .into_node();
        let updated = base.clone().unwrap();

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = VfsUpdate::FileCreated(VfsFileUpdate::new(
            &VirtualPathBuf::new("/file.bin").unwrap(),
            0,
            Utc::now(),
            new_file_info,
        ));
        let ref_vfs = Vfs::new(updated.clone());

        vfs.apply_update(&update, &ref_vfs).unwrap();

        assert_eq!(
            vfs.diff(&ref_vfs).unwrap(),
            vec![VfsDiff::dir_modified(VirtualPathBuf::root())].into()
        );

        // Test file modification
        let mut vfs = Vfs::new(base.clone());

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = VfsUpdate::FileModified(VfsFileUpdate::new(
            &VirtualPathBuf::new("/file.bin").unwrap(),
            0,
            Utc::now(),
            new_file_info,
        ));
        let ref_vfs = Vfs::new(updated.clone());

        vfs.apply_update(&update, &ref_vfs).unwrap();

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty(),);

        // Test file removal
        let mut vfs = Vfs::new(base);
        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_node()
        .unwrap();

        let ref_vfs = Vfs::new(updated);

        let update = VfsUpdate::FileRemoved(VirtualPathBuf::new("/file.bin").unwrap());

        vfs.apply_update(&update, &ref_vfs).unwrap();

        assert_eq!(
            vfs.diff(&ref_vfs).unwrap(),
            vec![VfsDiff::dir_modified(VirtualPathBuf::root())].into()
        );
    }

    #[test]
    fn test_invalid_update() {
        let base = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_node();

        // Test invalid path
        let mut vfs = Vfs::new(base.clone());
        let ref_vfs = Vfs::new(base.clone().unwrap());

        let update = VfsUpdate::FileRemoved(VirtualPathBuf::new("/e/f/h").unwrap());

        assert!(vfs.apply_update(&update, &ref_vfs).is_err());

        // Test double remove
        let mut vfs = Vfs::new(base.clone());

        let update = VfsUpdate::FileRemoved(VirtualPathBuf::new("/Doc/f3.doc").unwrap());

        assert!(vfs.apply_update(&update, &ref_vfs).is_err());
    }
}
