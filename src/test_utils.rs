use std::{
    ffi::OsStr,
    io::{self, Cursor},
    time::SystemTime,
};

use xxhash_rust::xxh3::xxh3_64;

use crate::{
    concrete::{local::path::LocalPath, ConcreteFS},
    vfs::{DirTree, FileInfo, IsModified, ModificationState, TreeNode, Vfs, VirtualPath},
    Error,
};

#[derive(Clone, Debug)]
pub(crate) enum TestNode {
    F(&'static str),
    D(&'static str, Vec<TestNode>),
    FH(&'static str, u64),
    DH(&'static str, u64, Vec<TestNode>),
    FF(&'static str, &'static [u8]),
}

impl TestNode {
    pub(crate) fn name(&self) -> &str {
        match self {
            TestNode::F(name)
            | TestNode::D(name, _)
            | TestNode::FH(name, _)
            | TestNode::DH(name, _, _)
            | TestNode::FF(name, _) => name,
        }
    }

    pub(crate) fn into_node_recursive_diff(self) -> TreeNode<RecursiveTestSyncInfo> {
        match self {
            TestNode::F(name) | TestNode::FF(name, _) => {
                let sync = RecursiveTestSyncInfo::new(0);
                TreeNode::File(FileInfo::new(name, sync))
            }
            TestNode::D(name, children) => {
                let sync = RecursiveTestSyncInfo::new(0);
                TreeNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node_recursive_diff())
                        .collect(),
                ))
            }
            TestNode::FH(name, hash) => {
                let sync = RecursiveTestSyncInfo::new(hash);
                TreeNode::File(FileInfo::new(name, sync))
            }
            TestNode::DH(name, hash, children) => {
                let sync = RecursiveTestSyncInfo::new(hash);
                TreeNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node_recursive_diff())
                        .collect(),
                ))
            }
        }
    }

    pub(crate) fn into_node(self) -> TreeNode<ShallowTestSyncInfo> {
        self.into_node_shallow_diff()
    }

    pub(crate) fn into_node_shallow_diff(self) -> TreeNode<ShallowTestSyncInfo> {
        match self {
            TestNode::F(name) | TestNode::FF(name, _) => {
                let sync = ShallowTestSyncInfo::new(0);
                TreeNode::File(FileInfo::new(name, sync))
            }
            TestNode::D(name, children) => {
                let sync = ShallowTestSyncInfo::new(0);
                TreeNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node())
                        .collect(),
                ))
            }
            TestNode::FH(name, hash) => {
                let sync = ShallowTestSyncInfo::new(hash);
                TreeNode::File(FileInfo::new(name, sync))
            }
            TestNode::DH(name, hash, children) => {
                let sync = ShallowTestSyncInfo::new(hash);
                TreeNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node())
                        .collect(),
                ))
            }
        }
    }

    pub(crate) fn into_dir(self) -> DirTree<ShallowTestSyncInfo> {
        self.into_dir_shallow_diff()
    }

    pub(crate) fn into_dir_shallow_diff(self) -> DirTree<ShallowTestSyncInfo> {
        match self {
            TestNode::F(_) => {
                panic!()
            }
            TestNode::D(name, children) => {
                let sync = ShallowTestSyncInfo::new(0);
                DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node())
                        .collect(),
                )
            }
            TestNode::FH(_, _) => {
                panic!()
            }
            TestNode::DH(name, hash, children) => {
                let sync = ShallowTestSyncInfo::new(hash);
                DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node())
                        .collect(),
                )
            }
            TestNode::FF(_, _) => panic!(),
        }
    }

    fn get_node(&self, path: &VirtualPath) -> &Self {
        if path.is_root() {
            self
        } else {
            let (top_level, remainder) = path.top_level_split().unwrap();

            match self {
                TestNode::F(_) | TestNode::FF(_, _) | TestNode::FH(_, _) => panic!(),
                TestNode::D(_, children) | TestNode::DH(_, _, children) => {
                    for child in children {
                        if child.name() == top_level {
                            if remainder.is_root() {
                                return child;
                            } else {
                                return child.get_node(remainder);
                            }
                        }
                    }
                    panic!("{path:?}")
                }
            }
        }
    }
}

impl LocalPath for TestNode {
    type DirEntry = Self;
    fn is_file(&self) -> bool {
        match self {
            TestNode::F(_) | TestNode::FH(_, _) | TestNode::FF(_, _) => true,
            TestNode::D(_, _) | TestNode::DH(_, _, _) => false,
        }
    }

    fn is_dir(&self) -> bool {
        match self {
            TestNode::F(_) | TestNode::FH(_, _) | TestNode::FF(_, _) => false,
            TestNode::D(_, _) | TestNode::DH(_, _, _) => true,
        }
    }

    fn file_name(&self) -> Option<&OsStr> {
        Some(OsStr::new(self.name()))
    }

    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self>>> {
        match self {
            TestNode::F(_) | TestNode::FH(_, _) | TestNode::FF(_, _) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected a dir",
            )),
            TestNode::D(_, children) | TestNode::DH(_, _, children) => {
                Ok(children.iter().cloned().map(Ok))
            }
        }
    }

    fn modification_time(&self) -> io::Result<SystemTime> {
        Ok(SystemTime::now())
    }
}

impl ConcreteFS for TestNode {
    type SyncInfo = ShallowTestSyncInfo;

    type Error = Error;

    async fn load_virtual(&self) -> Result<Vfs<Self::SyncInfo>, Self::Error> {
        let root = self.clone().into_node();
        Ok(Vfs::new(root))
    }

    async fn open(&self, path: &VirtualPath) -> Result<impl io::Read, Self::Error> {
        match self.get_node(path) {
            TestNode::F(_) => panic!(),
            TestNode::D(_, _) => panic!(),
            TestNode::FH(_, _) => panic!(),
            TestNode::DH(_, _, _) => panic!(),
            TestNode::FF(_, content) => Ok(Cursor::new(content)),
        }
    }

    async fn write<Stream: io::Read>(
        &self,
        _path: &VirtualPath,
        _data: &mut Stream,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn mkdir(&self, _path: &VirtualPath) -> Result<(), Self::Error> {
        todo!()
    }

    async fn hash(&self, path: &VirtualPath) -> Result<u64, Self::Error> {
        match self.get_node(path) {
            TestNode::F(_) => panic!("{path:?}"),
            TestNode::D(_, _) => panic!("{path:?}"),
            TestNode::FH(_, hash) => Ok(*hash),
            TestNode::DH(_, hash, _) => Ok(*hash),
            TestNode::FF(_, content) => Ok(xxh3_64(content)),
        }
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

impl<'a> From<&'a RecursiveTestSyncInfo> for RecursiveTestSyncInfo {
    fn from(value: &'a RecursiveTestSyncInfo) -> Self {
        value.to_owned()
    }
}

impl<'a> From<&'a RecursiveTestSyncInfo> for () {
    fn from(_value: &'a RecursiveTestSyncInfo) -> Self {}
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

impl<'a> From<&'a ShallowTestSyncInfo> for ShallowTestSyncInfo {
    fn from(value: &'a ShallowTestSyncInfo) -> Self {
        value.to_owned()
    }
}

impl<'a> From<&'a ShallowTestSyncInfo> for () {
    fn from(_value: &'a ShallowTestSyncInfo) -> Self {}
}
