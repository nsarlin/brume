//! An in-memory representation of a FileSystem using a tree structure.
//!
//! The Vfs is used to be able to apply filesystems diff algorithms that are independant of the
//! concrete filesystem behind it.

pub mod dir_tree;

pub mod virtual_path;

use std::fmt::Debug;

pub use dir_tree::*;
pub use virtual_path::*;

use crate::{
    update::{
        AppliedUpdate, DiffError, IsModified, VfsNodeUpdate, VfsUpdateApplicationError,
        VfsUpdateList,
    },
    NameMismatchError,
};

/// The virtual representation of a file system, local or remote.
///
/// `SyncInfo` is a metadata type that will be stored with FS nodes and used for more efficient VFS
/// comparison, using their implementation of the [`IsModified`] trait.
#[derive(Debug)]
pub struct Vfs<SyncInfo> {
    root: VfsNode<SyncInfo>,
}

impl<SyncInfo> Vfs<SyncInfo> {
    /// Return the root node of the VFS
    pub fn root(&self) -> &VfsNode<SyncInfo> {
        &self.root
    }

    /// Return a mutable access to the root node of the VFS
    pub fn root_mut(&mut self) -> &mut VfsNode<SyncInfo> {
        &mut self.root
    }

    /// Structural comparison of two VFS, looking at the names of files and directories, but
    /// ignoring the content of files.
    pub fn structural_eq<OtherSyncInfo>(&self, other: &Vfs<OtherSyncInfo>) -> bool {
        self.root.structural_eq(other.root())
    }

    /// Apply a list of updates to the VFS, by calling [`Self::apply_update`] on each of them.
    pub fn apply_updates_list(
        &mut self,
        updates: Vec<AppliedUpdate<SyncInfo>>,
    ) -> Result<(), VfsUpdateApplicationError> {
        for update in updates {
            self.apply_update(update)?;
        }
        Ok(())
    }

    /// Apply an update to the Vfs, by adding or removing nodes.
    ///
    /// The created or modified nodes use the SyncInfo from the [`AppliedUpdate`].
    pub fn apply_update(
        &mut self,
        update: AppliedUpdate<SyncInfo>,
    ) -> Result<(), VfsUpdateApplicationError> {
        let path = update.path().to_owned();

        let parent = self
            .root_mut()
            .find_dir_mut(path.parent().ok_or(VfsUpdateApplicationError::PathIsRoot)?)?;

        // Invalidate parent sync info because its content has been changed
        parent.invalidate_sync_info();

        match update {
            AppliedUpdate::DirCreated(update) => {
                let child = VfsNode::Dir(update.into());

                if path.name() != child.name() {
                    return Err(NameMismatchError {
                        expected: path.name().to_string(),
                        found: child.name().to_string(),
                    }
                    .into());
                }
                if parent.insert_child(child) {
                    Ok(())
                } else {
                    Err(VfsUpdateApplicationError::DirExists(path.to_owned()))
                }
            }
            AppliedUpdate::DirRemoved(path) => self
                .root_mut()
                .as_dir_mut()?
                .delete_dir(&path)
                .map_err(|e| e.into()),
            AppliedUpdate::FileCreated(update) => {
                let child = VfsNode::File(FileMeta::new(
                    path.name(),
                    update.file_size(),
                    update.into_sync_info(),
                ));
                if parent.insert_child(child) {
                    Ok(())
                } else {
                    Err(VfsUpdateApplicationError::FileExists(path.to_owned()))
                }
            }
            AppliedUpdate::FileModified(update) => {
                let file = self.root_mut().find_file_mut(update.path())?;
                file.set_size(update.file_size());
                let info = file.sync_info_mut();
                *info = Some(update.into_sync_info());
                Ok(())
            }
            AppliedUpdate::FileRemoved(path) => self
                .root_mut()
                .as_dir_mut()?
                .delete_file(&path)
                .map_err(|e| e.into()),
        }
    }
}

impl<SyncInfo: IsModified<SyncInfo> + Clone> Vfs<SyncInfo> {
    /// Diff two VFS by comparing their nodes.
    ///
    /// This function returns a sorted list of [`VfsNodeUpdate`].
    /// The node comparison is based on the `SyncInfo` and might be recursive based on the result of
    /// [`modification_state`]. The result of the SyncInfo comparison on node is trusted.
    ///
    /// [`modification_state`]: IsModified::modification_state
    pub fn diff(&self, other: &Vfs<SyncInfo>) -> Result<VfsUpdateList, DiffError> {
        self.root.diff(other.root(), VirtualPath::root())
    }
}

impl<SyncInfo> Vfs<SyncInfo> {
    pub fn new(root: VfsNode<SyncInfo>) -> Self {
        Self { root }
    }

    pub fn empty() -> Self {
        Self {
            root: VfsNode::Dir(DirTree::new_without_syncinfo("")),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test_utils::ShallowTestSyncInfo,
        update::{AppliedDirCreation, AppliedFileUpdate},
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
        .into_node();

        let new_dir = D("h", vec![F("file.bin"), D("i", vec![])]).into_dir();
        let update = AppliedUpdate::DirCreated(
            AppliedDirCreation::new(&VirtualPathBuf::new("/e/g/h").unwrap(), new_dir).unwrap(),
        );

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated);

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());

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
        .into_node();

        let update = AppliedUpdate::DirRemoved(VirtualPathBuf::new("/e/g").unwrap());

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated);

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());

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
        .into_node();

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = AppliedUpdate::FileCreated(AppliedFileUpdate::new(
            &VirtualPathBuf::new("/e/g/file.bin").unwrap(),
            0,
            new_file_info,
        ));

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated);

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());

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
        .into_node();

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = AppliedUpdate::FileModified(AppliedFileUpdate::new(
            &VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            0,
            new_file_info,
        ));

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated);

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
        .into_node();

        let update = AppliedUpdate::FileRemoved(VirtualPathBuf::new("/Doc/f2.pdf").unwrap());

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated);

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());
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
        .into_node();

        let new_dir = D("h", vec![F("file.bin"), D("i", vec![])]).into_dir();
        let update = AppliedUpdate::DirCreated(
            AppliedDirCreation::new(&VirtualPathBuf::new("/h").unwrap(), new_dir).unwrap(),
        );

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated);

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());

        // Test dir removal
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
            ],
        )
        .into_node();

        let update = AppliedUpdate::DirRemoved(VirtualPathBuf::new("/e").unwrap());

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated);

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());

        // Test file creation
        let mut vfs = Vfs::new(base.clone());

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
                F("file.bin"),
            ],
        )
        .into_node();

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = AppliedUpdate::FileCreated(AppliedFileUpdate::new(
            &VirtualPathBuf::new("/file.bin").unwrap(),
            0,
            new_file_info,
        ));

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated.clone());

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());

        // Test file modification
        let mut vfs = ref_vfs;

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = AppliedUpdate::FileModified(AppliedFileUpdate::new(
            &VirtualPathBuf::new("/file.bin").unwrap(),
            0,
            new_file_info,
        ));

        vfs.apply_update(update).unwrap();

        let ref_vfs = Vfs::new(updated.clone());

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());

        // Test file removal
        let mut vfs = ref_vfs;

        let update = AppliedUpdate::FileRemoved(VirtualPathBuf::new("/file.bin").unwrap());

        vfs.apply_update(update).unwrap();

        let updated = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![F("tmp.txt")])]),
            ],
        )
        .into_node();

        let ref_vfs = Vfs::new(updated);

        assert!(vfs.diff(&ref_vfs).unwrap().is_empty());
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

        let update = AppliedUpdate::FileRemoved(VirtualPathBuf::new("/e/f/h").unwrap());

        assert!(vfs.apply_update(update).is_err());

        // Test double create
        let mut vfs = Vfs::new(base.clone());

        let new_file_info = ShallowTestSyncInfo::new(0);
        let update = AppliedUpdate::FileCreated(AppliedFileUpdate::new(
            &VirtualPathBuf::new("/e/g/tmp.txt").unwrap(),
            0,
            new_file_info,
        ));

        assert!(vfs.apply_update(update).is_err());

        // Test double remove
        let mut vfs = Vfs::new(base.clone());

        let update = AppliedUpdate::FileRemoved(VirtualPathBuf::new("/Doc/f3.doc").unwrap());

        assert!(vfs.apply_update(update).is_err());
    }
}
