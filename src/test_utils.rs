use std::{ffi::OsStr, io, time::SystemTime};

use crate::{
    concrete::local::path::LocalPath,
    vfs::{DirTree, FileInfo, IsModified, ModificationState, TreeNode},
};

#[derive(Clone)]
pub(crate) enum TestNode {
    F(&'static str),
    D(&'static str, Vec<TestNode>),
    FH(&'static str, u64),
    DH(&'static str, u64, Vec<TestNode>),
}

impl TestNode {
    pub(crate) fn into_node_recursive_diff(&self) -> TreeNode<RecursiveTestSyncInfo> {
        match self {
            TestNode::F(name) => {
                let sync = RecursiveTestSyncInfo::new(0);
                TreeNode::File(FileInfo::new(name, sync))
            }
            TestNode::D(name, children) => {
                let sync = RecursiveTestSyncInfo::new(0);
                TreeNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .iter()
                        .map(|child| child.into_node_recursive_diff())
                        .collect(),
                ))
            }
            TestNode::FH(name, hash) => {
                let sync = RecursiveTestSyncInfo::new(*hash);
                TreeNode::File(FileInfo::new(name, sync))
            }
            TestNode::DH(name, hash, children) => {
                let sync = RecursiveTestSyncInfo::new(*hash);
                TreeNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .iter()
                        .map(|child| child.into_node_recursive_diff())
                        .collect(),
                ))
            }
        }
    }

    pub(crate) fn into_node(&self) -> TreeNode<ShallowTestSyncInfo> {
        self.into_node_shallow_diff()
    }

    pub(crate) fn into_node_shallow_diff(&self) -> TreeNode<ShallowTestSyncInfo> {
        match self {
            TestNode::F(name) => {
                let sync = ShallowTestSyncInfo::new(0);
                TreeNode::File(FileInfo::new(name, sync))
            }
            TestNode::D(name, children) => {
                let sync = ShallowTestSyncInfo::new(0);
                TreeNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children.iter().map(|child| child.into_node()).collect(),
                ))
            }
            TestNode::FH(name, hash) => {
                let sync = ShallowTestSyncInfo::new(*hash);
                TreeNode::File(FileInfo::new(name, sync))
            }
            TestNode::DH(name, hash, children) => {
                let sync = ShallowTestSyncInfo::new(*hash);
                TreeNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children.iter().map(|child| child.into_node()).collect(),
                ))
            }
        }
    }

    pub(crate) fn into_dir_recursive_diff(&self) -> DirTree<RecursiveTestSyncInfo> {
        match self {
            TestNode::F(_) => {
                panic!()
            }
            TestNode::D(name, children) => {
                let sync = RecursiveTestSyncInfo::new(0);

                DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .iter()
                        .map(|child| child.into_node_recursive_diff())
                        .collect(),
                )
            }
            TestNode::FH(_, _) => {
                panic!()
            }
            TestNode::DH(name, hash, children) => {
                let sync = RecursiveTestSyncInfo::new(*hash);
                DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .iter()
                        .map(|child| child.into_node_recursive_diff())
                        .collect(),
                )
            }
        }
    }

    pub(crate) fn _into_dir(&self) -> DirTree<ShallowTestSyncInfo> {
        self.into_dir_shallow_diff()
    }

    pub(crate) fn into_dir_shallow_diff(&self) -> DirTree<ShallowTestSyncInfo> {
        match self {
            TestNode::F(_) => {
                panic!()
            }
            TestNode::D(name, children) => {
                let sync = ShallowTestSyncInfo::new(0);
                DirTree::new_with_children(
                    name,
                    sync,
                    children.iter().map(|child| child.into_node()).collect(),
                )
            }
            TestNode::FH(_, _) => {
                panic!()
            }
            TestNode::DH(name, hash, children) => {
                let sync = ShallowTestSyncInfo::new(*hash);
                DirTree::new_with_children(
                    name,
                    sync,
                    children.iter().map(|child| child.into_node()).collect(),
                )
            }
        }
    }
}

impl LocalPath for TestNode {
    type DirEntry = Self;
    fn is_file(&self) -> bool {
        match self {
            TestNode::F(_) => true,
            TestNode::D(_, _) => false,
            TestNode::FH(_, _) => true,
            TestNode::DH(_, _, _) => false,
        }
    }

    fn is_dir(&self) -> bool {
        match self {
            TestNode::F(_) => false,
            TestNode::D(_, _) => true,
            TestNode::FH(_, _) => false,
            TestNode::DH(_, _, _) => true,
        }
    }

    fn file_name(&self) -> Option<&OsStr> {
        match self {
            TestNode::F(name)
            | TestNode::D(name, _)
            | TestNode::FH(name, _)
            | TestNode::DH(name, _, _) => Some(OsStr::new(name)),
        }
    }

    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self>>> {
        match self {
            TestNode::F(_) | TestNode::FH(_, _) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected a dir",
            )),
            TestNode::D(_, children) | TestNode::DH(_, _, children) => {
                Ok(children.iter().cloned().map(|val| Ok(val)))
            }
        }
    }

    fn modification_time(&self) -> io::Result<SystemTime> {
        Ok(SystemTime::now())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecursiveTestSyncInfo {
    hash: u64,
}

impl RecursiveTestSyncInfo {
    pub(crate) fn new(hash: u64) -> Self {
        Self { hash }
    }
}

impl IsModified<Self> for RecursiveTestSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.hash == reference.hash {
            ModificationState::RecursiveUnmodified
        } else {
            ModificationState::Modified
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShallowTestSyncInfo {
    hash: u64,
}

impl ShallowTestSyncInfo {
    pub(crate) fn new(hash: u64) -> Self {
        Self { hash }
    }
}

impl IsModified<Self> for ShallowTestSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.hash == reference.hash {
            ModificationState::ShallowUnmodified
        } else {
            ModificationState::Modified
        }
    }
}
