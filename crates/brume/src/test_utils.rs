use std::{cell::RefCell, ffi::OsStr, io::ErrorKind, ops::Deref, time::SystemTime};

use bytes::Bytes;
use futures::{stream, Stream, TryStream, TryStreamExt};
use tokio::io::{self, AsyncReadExt};
use tokio_util::io::StreamReader;
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    concrete::{local::path::LocalPath, ConcreteFS, ConcreteFsError, Named},
    filesystem::FileSystem,
    update::{IsModified, ModificationState},
    vfs::{DirTree, FileMeta, Vfs, VfsNode, VirtualPath},
};

/// Can be used to easily create Vfs for tests
#[derive(Clone, Debug)]
pub(crate) enum TestNode<'a> {
    F(&'a str),
    D(&'a str, Vec<TestNode<'a>>),
    FH(&'a str, u64),
    DH(&'a str, u64, Vec<TestNode<'a>>),
    FF(&'a str, &'a [u8]),
    L(&'a str, Option<&'a TestNode<'a>>),
}

impl TestNode<'_> {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::F(name)
            | Self::D(name, _)
            | Self::FH(name, _)
            | Self::DH(name, _, _)
            | Self::FF(name, _)
            | Self::L(name, _) => name,
        }
    }

    pub(crate) fn content(&self) -> Option<&[u8]> {
        match self {
            TestNode::F(_) => None,
            TestNode::D(_, _) => None,
            TestNode::FH(_, _) => None,
            TestNode::DH(_, _, _) => None,
            TestNode::FF(_, content) => Some(content),
            TestNode::L(_, target) => {
                if let Some(node) = target.as_ref() {
                    node.content()
                } else {
                    None
                }
            }
        }
    }

    pub(crate) fn into_node_recursive_diff(self) -> VfsNode<RecursiveTestSyncInfo> {
        match self {
            Self::F(name) => {
                let sync = RecursiveTestSyncInfo::new(0);
                VfsNode::File(FileMeta::new(name, 0, sync))
            }
            Self::FF(name, content) => {
                let sync = RecursiveTestSyncInfo::new(0);
                VfsNode::File(FileMeta::new(name, content.len() as u64, sync))
            }
            Self::D(name, children) => {
                let sync = RecursiveTestSyncInfo::new(0);
                VfsNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node_recursive_diff())
                        .collect(),
                ))
            }
            Self::FH(name, hash) => {
                let sync = RecursiveTestSyncInfo::new(hash);
                VfsNode::File(FileMeta::new(name, 0, sync))
            }
            Self::DH(name, hash, children) => {
                let sync = RecursiveTestSyncInfo::new(hash);
                VfsNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node_recursive_diff())
                        .collect(),
                ))
            }
            Self::L(_, target) => {
                if let Some(node) = target {
                    node.clone().into_node_recursive_diff()
                } else {
                    panic!("Invalid symlink")
                }
            }
        }
    }

    pub(crate) fn into_node(self) -> VfsNode<ShallowTestSyncInfo> {
        self.into_node_shallow_diff()
    }

    pub(crate) fn into_node_shallow_diff(self) -> VfsNode<ShallowTestSyncInfo> {
        match self {
            Self::F(name) => {
                let sync = ShallowTestSyncInfo::new(0);
                VfsNode::File(FileMeta::new(name, 0, sync))
            }
            Self::FF(name, content) => {
                let sync = ShallowTestSyncInfo::new(0);
                VfsNode::File(FileMeta::new(name, content.len() as u64, sync))
            }
            Self::D(name, children) => {
                let sync = ShallowTestSyncInfo::new(0);
                VfsNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node())
                        .collect(),
                ))
            }
            Self::FH(name, hash) => {
                let sync = ShallowTestSyncInfo::new(hash);
                VfsNode::File(FileMeta::new(name, 0, sync))
            }
            Self::DH(name, hash, children) => {
                let sync = ShallowTestSyncInfo::new(hash);
                VfsNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node())
                        .collect(),
                ))
            }
            Self::L(_, target) => {
                if let Some(node) = target {
                    node.clone().into_node_shallow_diff()
                } else {
                    panic!("Invalid symlink")
                }
            }
        }
    }

    pub(crate) fn into_dir(self) -> DirTree<ShallowTestSyncInfo> {
        self.into_dir_shallow_diff()
    }

    pub(crate) fn into_dir_shallow_diff(self) -> DirTree<ShallowTestSyncInfo> {
        match self {
            Self::F(_) => {
                panic!()
            }
            Self::D(name, children) => {
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
            Self::FH(_, _) => {
                panic!()
            }
            Self::DH(name, hash, children) => {
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
            Self::FF(_, _) => panic!(),
            Self::L(_, target) => {
                if let Some(node) = target {
                    node.clone().into_dir_shallow_diff()
                } else {
                    panic!("Invalid symlink")
                }
            }
        }
    }

    fn get_node(&self, path: &VirtualPath) -> &Self {
        if path.is_root() {
            self
        } else {
            let (top_level, remainder) = path.top_level_split().unwrap();

            match self {
                Self::F(_) | Self::FF(_, _) | Self::FH(_, _) => panic!(),
                Self::D(_, children) | Self::DH(_, _, children) => {
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
                Self::L(_, target) => {
                    if let Some(node) = target {
                        node.get_node(remainder)
                    } else {
                        panic!("Invalid symlink")
                    }
                }
            }
        }
    }

    fn _get_node_mut(&mut self, path: &VirtualPath) -> &mut Self {
        if path.is_root() {
            self
        } else {
            let (top_level, remainder) = path.top_level_split().unwrap();

            match self {
                Self::F(_) | Self::FF(_, _) | Self::FH(_, _) => panic!(),
                Self::D(_, children) | Self::DH(_, _, children) => {
                    for child in children {
                        if child.name() == top_level {
                            if remainder.is_root() {
                                return child;
                            } else {
                                return child._get_node_mut(remainder);
                            }
                        }
                    }
                    panic!("{path:?}")
                }
                Self::L(_, _) => todo!(),
            }
        }
    }
}

impl<'a> LocalPath for TestNode<'a> {
    type DirEntry = Self;
    fn is_file(&self) -> bool {
        match self {
            TestNode::F(_) | TestNode::FH(_, _) | TestNode::FF(_, _) => true,
            TestNode::D(_, _) | TestNode::DH(_, _, _) => false,
            TestNode::L(_, target) => {
                if let Some(node) = target {
                    node.is_file()
                } else {
                    panic!("Invalid symlink")
                }
            }
        }
    }

    fn is_dir(&self) -> bool {
        match self {
            TestNode::F(_) | TestNode::FH(_, _) | TestNode::FF(_, _) => false,
            TestNode::D(_, _) | TestNode::DH(_, _, _) => true,
            TestNode::L(_, target) => {
                if let Some(node) = target {
                    node.is_dir()
                } else {
                    panic!("Invalid symlink")
                }
            }
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
            TestNode::L(_, target) => {
                if let Some(node) = target {
                    node.read_dir()
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid symlink target",
                    ))
                }
            }
        }
    }

    fn modification_time(&self) -> io::Result<SystemTime> {
        match self {
            TestNode::F(_)
            | TestNode::D(_, _)
            | TestNode::FH(_, _)
            | TestNode::DH(_, _, _)
            | TestNode::FF(_, _) => Ok(SystemTime::now()),
            TestNode::L(_, target) => {
                if let Some(node) = target {
                    node.modification_time()
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid symlink target",
                    ))
                }
            }
        }
    }

    fn file_size(&self) -> std::io::Result<u64> {
        match self {
            TestNode::F(_) | TestNode::FH(_, _) => Ok(0),
            TestNode::D(_, _) | TestNode::DH(_, _, _) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected a file",
            )),
            TestNode::FF(_, content) => Ok(content.len() as u64),
            TestNode::L(_, target) => {
                if let Some(node) = target {
                    node.file_size()
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid symlink target",
                    ))
                }
            }
        }
    }
}

/// Like a TestNode, but own its content, wich allows modifications.
///
/// Can be used to define a test concrete fs
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum InnerConcreteTestNode {
    D(String, Vec<InnerConcreteTestNode>),
    DH(String, u64, Vec<InnerConcreteTestNode>),
    FF(String, Vec<u8>),
}

impl<'a> From<TestNode<'a>> for InnerConcreteTestNode {
    fn from(value: TestNode<'a>) -> Self {
        match value {
            TestNode::F(_) | TestNode::FH(_, _) => panic!(),
            TestNode::D(name, children) => Self::D(
                name.to_string(),
                children.into_iter().map(|child| child.into()).collect(),
            ),
            TestNode::DH(name, hash, children) => Self::DH(
                name.to_string(),
                hash,
                children.into_iter().map(|child| child.into()).collect(),
            ),
            TestNode::FF(name, content) => Self::FF(name.to_string(), content.to_vec()),
            TestNode::L(_, target) => {
                if let Some(node) = target {
                    node.clone().into()
                } else {
                    panic!("Invalid symlink")
                }
            }
        }
    }
}

impl<'a> From<&'a InnerConcreteTestNode> for TestNode<'a> {
    fn from(value: &'a InnerConcreteTestNode) -> Self {
        match value {
            InnerConcreteTestNode::D(name, children) => {
                Self::D(name, children.iter().map(|child| child.into()).collect())
            }
            InnerConcreteTestNode::DH(name, hash, children) => Self::DH(
                name,
                *hash,
                children.iter().map(|child| child.into()).collect(),
            ),
            InnerConcreteTestNode::FF(name, content) => Self::FF(name, content),
        }
    }
}

impl InnerConcreteTestNode {
    fn name(&self) -> &str {
        match self {
            InnerConcreteTestNode::D(name, _)
            | InnerConcreteTestNode::DH(name, _, _)
            | InnerConcreteTestNode::FF(name, _) => name,
        }
    }

    fn is_file(&self) -> bool {
        match self {
            InnerConcreteTestNode::D(_, _) | InnerConcreteTestNode::DH(_, _, _) => false,
            InnerConcreteTestNode::FF(_, _) => true,
        }
    }

    fn is_dir(&self) -> bool {
        match self {
            InnerConcreteTestNode::D(_, _) | InnerConcreteTestNode::DH(_, _, _) => true,
            InnerConcreteTestNode::FF(_, _) => false,
        }
    }

    fn get_node(&self, path: &VirtualPath) -> &Self {
        if path.is_root() {
            self
        } else {
            let (top_level, remainder) = path.top_level_split().unwrap();

            match self {
                Self::FF(_, _) => panic!(),
                Self::D(_, children) | Self::DH(_, _, children) => {
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

    fn get_node_mut(&mut self, path: &VirtualPath) -> &mut Self {
        if path.is_root() {
            self
        } else {
            let (top_level, remainder) = path.top_level_split().unwrap();

            match self {
                Self::FF(_, _) => panic!(),
                Self::D(_, children) | Self::DH(_, _, children) => {
                    for child in children {
                        if child.name() == top_level {
                            if remainder.is_root() {
                                return child;
                            } else {
                                return child.get_node_mut(remainder);
                            }
                        }
                    }
                    panic!("{path:?}")
                }
            }
        }
    }
}

/// Mock FS that can be used to test concrete operations.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ConcreteTestNode {
    inner: RefCell<InnerConcreteTestNode>,
}

pub(crate) type TestFileSystem = FileSystem<ConcreteTestNode>;

impl From<TestNode<'_>> for ConcreteTestNode {
    fn from(value: TestNode) -> Self {
        Self {
            inner: RefCell::new(value.into()),
        }
    }
}

impl ConcreteFS for ConcreteTestNode {
    type SyncInfo = ShallowTestSyncInfo;

    type Error = TestError;

    async fn load_virtual(&self) -> Result<Vfs<Self::SyncInfo>, Self::Error> {
        let inner = self.inner.borrow();
        let root = TestNode::from(inner.deref()).into_node();

        Ok(Vfs::new(root))
    }

    async fn open(
        &self,
        path: &VirtualPath,
    ) -> Result<impl Stream<Item = Result<Bytes, Self::Error>> + 'static, Self::Error> {
        let inner = self.inner.borrow();
        let root = TestNode::from(inner.deref());
        let node = root.get_node(path);

        if let Some(content) = node.content() {
            let owned = content.to_vec();
            let stream = stream::iter(owned.into_iter().map(|b| Ok(Bytes::from(vec![b]))));
            Ok(stream)
        } else {
            panic!("can't open node {path:?}")
        }
    }

    async fn write<Data: TryStream + Send + Unpin>(
        &self,
        path: &VirtualPath,
        data: Data,
    ) -> Result<Self::SyncInfo, Self::Error>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>,
    {
        // Read data
        let mut reader = StreamReader::new(
            data.map_ok(Bytes::from)
                .map_err(|e| io::Error::new(ErrorKind::Other, e)),
        );

        let mut content = Vec::new();
        reader.read_to_end(&mut content).await?;

        // Write into the node
        let mut inner = self.inner.borrow_mut();
        let parent = inner.get_node_mut(path.parent().unwrap());

        match parent {
            InnerConcreteTestNode::D(_, children) | InnerConcreteTestNode::DH(_, _, children) => {
                // Overwrite file
                for child in children.iter_mut() {
                    if child.name() == path.name() {
                        *child = InnerConcreteTestNode::FF(path.name().to_owned(), content);
                        return Ok(ShallowTestSyncInfo::new(0));
                    }
                }

                // Create file
                children.push(InnerConcreteTestNode::FF(path.name().to_owned(), content));
                Ok(ShallowTestSyncInfo::new(0))
            }
            InnerConcreteTestNode::FF(_, _) => panic!("{path:?}"),
        }
    }

    async fn rm(&self, path: &VirtualPath) -> Result<(), Self::Error> {
        let mut inner = self.inner.borrow_mut();
        let parent = inner.get_node_mut(path.parent().unwrap());

        match parent {
            InnerConcreteTestNode::D(_, children) | InnerConcreteTestNode::DH(_, _, children) => {
                let init_len = children.len();
                *children = children
                    .iter()
                    .filter(|child| child.name() != path.name() || child.is_dir())
                    .cloned()
                    .collect();

                if children.len() != init_len - 1 {
                    panic!("{path:?}")
                }
                Ok(())
            }
            InnerConcreteTestNode::FF(_, _) => panic!("{path:?}"),
        }
    }

    async fn mkdir(&self, path: &VirtualPath) -> Result<Self::SyncInfo, Self::Error> {
        let mut inner = self.inner.borrow_mut();
        let parent = inner.get_node_mut(path.parent().unwrap());

        match parent {
            InnerConcreteTestNode::D(_, children) | InnerConcreteTestNode::DH(_, _, children) => {
                children.push(InnerConcreteTestNode::D(
                    path.name().to_string(),
                    Vec::new(),
                ));

                Ok(ShallowTestSyncInfo::new(0))
            }
            InnerConcreteTestNode::FF(_, _) => panic!("{path:?}"),
        }
    }

    async fn rmdir(&self, path: &VirtualPath) -> Result<(), Self::Error> {
        let mut inner = self.inner.borrow_mut();
        let parent = inner.get_node_mut(path.parent().unwrap());

        match parent {
            InnerConcreteTestNode::D(_, children) | InnerConcreteTestNode::DH(_, _, children) => {
                let init_len = children.len();
                *children = children
                    .iter()
                    .filter(|child| child.name() != path.name() || child.is_file())
                    .cloned()
                    .collect();

                if children.len() != init_len - 1 {
                    panic!("{path:?} ({} - {})", children.len(), init_len)
                }
                Ok(())
            }
            InnerConcreteTestNode::FF(_, _) => panic!("{path:?}"),
        }
    }

    async fn hash(&self, path: &VirtualPath) -> Result<u64, Self::Error> {
        let inner = self.inner.borrow_mut();

        match inner.get_node(path) {
            InnerConcreteTestNode::D(_, _) => panic!("{path:?}"),
            InnerConcreteTestNode::DH(_, hash, _) => Ok(*hash),
            InnerConcreteTestNode::FF(_, content) => Ok(xxh3_64(content)),
        }
    }
}

pub type TestError = ConcreteFsError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecursiveTestSyncInfo {
    hash: u64,
}

impl RecursiveTestSyncInfo {
    pub(crate) fn new(hash: u64) -> Self {
        Self { hash }
    }
}

impl IsModified for RecursiveTestSyncInfo {
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

impl Named for ShallowTestSyncInfo {
    const NAME: &'static str = "Test FileSystem";
}

impl IsModified for ShallowTestSyncInfo {
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
