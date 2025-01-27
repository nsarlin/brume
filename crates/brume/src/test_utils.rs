use std::{cell::RefCell, ffi::OsStr, fmt::Display, io::ErrorKind, ops::Deref, time::SystemTime};

use bytes::Bytes;
use futures::{stream, Stream, TryStream, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncReadExt};
use tokio_util::io::StreamReader;
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    concrete::{local::path::LocalPath, FSBackend, FsBackendError, FsInstanceDescription, Named},
    filesystem::FileSystem,
    update::{FailedUpdateApplication, IsModified, ModificationState, VfsDiff},
    vfs::{DirTree, FileMeta, NodeState, Vfs, VfsNode, VirtualPath, VirtualPathBuf},
};

/// Can be used to easily create Vfs for tests
#[derive(Clone, Debug)]
pub(crate) enum TestNode<'a> {
    /// A file node with a name
    F(&'a str),
    /// A dir node with a name and children
    D(&'a str, Vec<TestNode<'a>>),
    /// A dir node with a name and a syncinfo hash
    FH(&'a str, u64),
    /// A dir node with a name, children and a syncinfo hash
    DH(&'a str, u64, Vec<TestNode<'a>>),
    /// A file node with a name and byte content
    FF(&'a str, &'a [u8]),
    /// A link with a name and a target
    L(&'a str, Option<&'a TestNode<'a>>),
    /// A file node with a name and and error status
    FE(&'a str, &'a str),
    /// A dir node with a name and and error status
    DE(&'a str, &'a str),
}

impl TestNode<'_> {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::F(name)
            | Self::D(name, _)
            | Self::FH(name, _)
            | Self::DH(name, _, _)
            | Self::FF(name, _)
            | Self::L(name, _)
            | Self::FE(name, _)
            | Self::DE(name, _) => name,
        }
    }

    pub(crate) fn into_node_recursive_diff(self) -> VfsNode<RecursiveTestSyncInfo> {
        self.into_node_recursive_diff_rec(VirtualPath::root())
    }

    pub(crate) fn into_node_recursive_diff_rec(
        self,
        parent: &VirtualPath,
    ) -> VfsNode<RecursiveTestSyncInfo> {
        match self {
            Self::F(name) => {
                let sync = RecursiveTestSyncInfo::new(0);
                VfsNode::File(FileMeta::new(name, 0, sync))
            }
            Self::FF(name, content) => {
                let sync = RecursiveTestSyncInfo::new(xxh3_64(content));
                VfsNode::File(FileMeta::new(name, content.len() as u64, sync))
            }
            Self::D(name, children) => {
                let mut path = parent.to_owned();
                path.push(name);
                let children_nodes: Vec<_> = children
                    .into_iter()
                    .map(|child| child.into_node_recursive_diff_rec(&path))
                    .collect();
                // Compute recursive hash
                let hash = xxh3_64(
                    &children_nodes
                        .iter()
                        .flat_map(|node| {
                            match node.state() {
                                NodeState::Ok(info) => info.hash,
                                NodeState::NeedResync => xxh3_64(b"Resync"),
                                NodeState::Error(failed_update) => {
                                    xxh3_64(failed_update.error().to_string().as_bytes())
                                }
                                NodeState::Conflict(_) => xxh3_64(b"Conflict"),
                            }
                            .to_le_bytes()
                        })
                        .collect::<Vec<_>>(),
                );

                let sync = RecursiveTestSyncInfo::new(hash);
                VfsNode::Dir(DirTree::new_with_children(name, sync, children_nodes))
            }
            Self::FH(name, hash) => {
                let sync = RecursiveTestSyncInfo::new(hash);
                VfsNode::File(FileMeta::new(name, 0, sync))
            }
            Self::DH(name, hash, children) => {
                let mut path = parent.to_owned();
                path.push(name);

                let sync = RecursiveTestSyncInfo::new(hash);
                VfsNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node_recursive_diff_rec(&path))
                        .collect(),
                ))
            }
            Self::L(name, target) => {
                let mut path = parent.to_owned();
                path.push(name);

                if let Some(node) = target {
                    node.clone().into_node_recursive_diff_rec(&path)
                } else {
                    panic!("Invalid symlink")
                }
            }
            Self::FE(name, error) => {
                let mut path = parent.to_owned();
                path.push(name);

                let failed_update = FailedUpdateApplication::new(
                    VfsDiff::file_created(path),
                    FsBackendError::from(io::Error::new(io::ErrorKind::InvalidInput, error)),
                );
                VfsNode::File(FileMeta::new_error(name, 0, failed_update))
            }
            Self::DE(name, error) => {
                let mut path = parent.to_owned();
                path.push(name);

                let failed_update = FailedUpdateApplication::new(
                    VfsDiff::dir_created(path),
                    FsBackendError::from(io::Error::new(io::ErrorKind::InvalidInput, error)),
                );

                VfsNode::Dir(DirTree::new_error(name, failed_update))
            }
        }
    }

    pub(crate) fn into_node(self) -> VfsNode<ShallowTestSyncInfo> {
        self.into_node_shallow_diff()
    }

    pub(crate) fn into_node_shallow_diff(self) -> VfsNode<ShallowTestSyncInfo> {
        self.into_node_shallow_diff_rec(VirtualPath::root())
    }

    pub(crate) fn into_node_shallow_diff_rec(
        self,
        parent: &VirtualPath,
    ) -> VfsNode<ShallowTestSyncInfo> {
        match self {
            Self::F(name) => {
                let sync = ShallowTestSyncInfo::new(0);
                VfsNode::File(FileMeta::new(name, 0, sync))
            }
            Self::FF(name, content) => {
                let sync = ShallowTestSyncInfo::new(xxh3_64(content));
                VfsNode::File(FileMeta::new(name, content.len() as u64, sync))
            }
            Self::D(name, children) => {
                let mut path = parent.to_owned();
                path.push(name);

                let children_nodes: Vec<_> = children
                    .into_iter()
                    .map(|child| child.into_node_shallow_diff_rec(&path))
                    .collect();

                // Since syncinfo is not recursive, only hash the names of the children
                let hash = xxh3_64(
                    &children_nodes
                        .iter()
                        .flat_map(|node| xxh3_64(node.name().as_bytes()).to_le_bytes())
                        .collect::<Vec<_>>(),
                );
                let sync = ShallowTestSyncInfo::new(hash);
                VfsNode::Dir(DirTree::new_with_children(name, sync, children_nodes))
            }
            Self::FH(name, hash) => {
                let sync = ShallowTestSyncInfo::new(hash);
                VfsNode::File(FileMeta::new(name, 0, sync))
            }
            Self::DH(name, hash, children) => {
                let mut path = parent.to_owned();
                path.push(name);

                let sync = ShallowTestSyncInfo::new(hash);
                VfsNode::Dir(DirTree::new_with_children(
                    name,
                    sync,
                    children
                        .into_iter()
                        .map(|child| child.into_node_shallow_diff_rec(&path))
                        .collect(),
                ))
            }
            Self::L(name, target) => {
                let mut path = parent.to_owned();
                path.push(name);

                if let Some(node) = target {
                    node.clone().into_node_shallow_diff_rec(&path)
                } else {
                    panic!("Invalid symlink")
                }
            }
            Self::FE(name, error) => {
                let mut path = parent.to_owned();
                path.push(name);

                let failed_update = FailedUpdateApplication::new(
                    VfsDiff::file_created(path),
                    FsBackendError::from(io::Error::new(io::ErrorKind::InvalidInput, error)),
                );
                VfsNode::File(FileMeta::new_error(name, 0, failed_update))
            }
            Self::DE(name, error) => {
                let mut path = parent.to_owned();
                path.push(name);

                let failed_update = FailedUpdateApplication::new(
                    VfsDiff::dir_created(path),
                    FsBackendError::from(io::Error::new(io::ErrorKind::InvalidInput, error)),
                );
                VfsNode::Dir(DirTree::new_error(name, failed_update))
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
                let children_nodes: Vec<_> = children
                    .into_iter()
                    .map(|child| child.into_node())
                    .collect();

                // Since syncinfo is not recursive, only hash the names of the children
                let hash = xxh3_64(
                    &children_nodes
                        .iter()
                        .flat_map(|node| xxh3_64(node.name().as_bytes()).to_le_bytes())
                        .collect::<Vec<_>>(),
                );

                let sync = ShallowTestSyncInfo::new(hash);
                DirTree::new_with_children(name, sync, children_nodes)
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
            Self::FE(_, _) => panic!(),
            Self::DE(name, error) => {
                let failed_update = FailedUpdateApplication::new(
                    VfsDiff::file_created(VirtualPathBuf::root()),
                    FsBackendError::from(io::Error::new(io::ErrorKind::InvalidInput, error)),
                );
                DirTree::new_error(name, failed_update)
            }
        }
    }
}

impl LocalPath for TestNode<'_> {
    type DirEntry = Self;
    fn is_file(&self) -> bool {
        match self {
            TestNode::F(_) | TestNode::FH(_, _) | TestNode::FF(_, _) | TestNode::FE(_, _) => true,
            TestNode::D(_, _) | TestNode::DH(_, _, _) | TestNode::DE(_, _) => false,
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
            TestNode::F(_) | TestNode::FH(_, _) | TestNode::FF(_, _) | TestNode::FE(_, _) => false,
            TestNode::D(_, _) | TestNode::DH(_, _, _) | TestNode::DE(_, _) => true,
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
            TestNode::F(_) | TestNode::FH(_, _) | TestNode::FF(_, _) | TestNode::FE(_, _) => Err(
                io::Error::new(io::ErrorKind::InvalidInput, "expected a dir"),
            ),
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
            TestNode::DE(_, err) => Err(io::Error::new(io::ErrorKind::InvalidInput, *err)),
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
            TestNode::FE(_, err) | TestNode::DE(_, err) => {
                Err(io::Error::new(io::ErrorKind::InvalidInput, *err))
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
            TestNode::FE(_, err) | TestNode::DE(_, err) => {
                Err(io::Error::new(io::ErrorKind::InvalidInput, *err))
            }
        }
    }
}

/// Like a TestNode, but own its content, which allows modifications.
///
/// Can be used to define a test concrete fs
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum InnerConcreteTestNode {
    D(String, Vec<InnerConcreteTestNode>),
    DH(String, u64, Vec<InnerConcreteTestNode>),
    FF(String, Vec<u8>),
    FE(String, String),
    DE(String, String),
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
            TestNode::FE(name, err) => Self::FE(name.to_string(), err.to_string()),
            TestNode::DE(name, err) => Self::DE(name.to_string(), err.to_string()),
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
            InnerConcreteTestNode::FE(name, _) => Self::F(name),
            InnerConcreteTestNode::DE(name, _) => Self::D(name, Vec::new()),
        }
    }
}

impl InnerConcreteTestNode {
    fn name(&self) -> &str {
        match self {
            Self::D(name, _)
            | Self::DH(name, _, _)
            | Self::FF(name, _)
            | Self::FE(name, _)
            | Self::DE(name, _) => name,
        }
    }

    fn content(&self) -> Option<&[u8]> {
        match self {
            Self::FF(_, content) => Some(content),
            _ => None,
        }
    }

    fn is_file(&self) -> bool {
        match self {
            Self::D(_, _) | Self::DH(_, _, _) | Self::DE(_, _) => false,
            Self::FF(_, _) | Self::FE(_, _) => true,
        }
    }

    fn is_dir(&self) -> bool {
        match self {
            Self::D(_, _) | Self::DH(_, _, _) | Self::DE(_, _) => true,
            Self::FF(_, _) | Self::FE(_, _) => false,
        }
    }

    fn get_node(&self, path: &VirtualPath) -> &Self {
        if path.is_root() {
            self
        } else {
            let (top_level, remainder) = path.top_level_split().unwrap();

            match self {
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
                _ => panic!(),
            }
        }
    }

    fn get_node_mut(&mut self, path: &VirtualPath) -> &mut Self {
        if path.is_root() {
            self
        } else {
            let (top_level, remainder) = path.top_level_split().unwrap();

            match self {
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
                _ => panic!(),
            }
        }
    }
}

/// Mock FS that can be used to test concrete operations.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ConcreteTestNode {
    inner: RefCell<InnerConcreteTestNode>,
    propagate_err_to_vfs: bool,
}

impl ConcreteTestNode {
    pub(crate) fn _propagate_err_to_vfs(&mut self) {
        self.propagate_err_to_vfs = true
    }
}

pub(crate) type TestFileSystem = FileSystem<ConcreteTestNode>;

impl From<TestNode<'_>> for ConcreteTestNode {
    fn from(value: TestNode) -> Self {
        Self {
            inner: RefCell::new(value.into()),
            propagate_err_to_vfs: false,
        }
    }
}

impl TryFrom<InnerConcreteTestNode> for ConcreteTestNode {
    type Error = <ConcreteTestNode as FSBackend>::IoError;

    fn try_from(value: InnerConcreteTestNode) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: RefCell::new(value),
            propagate_err_to_vfs: false,
        })
    }
}

impl<'a> From<&'a ConcreteTestNode> for VfsNode<ShallowTestSyncInfo> {
    fn from(value: &'a ConcreteTestNode) -> Self {
        let inner = value.inner.borrow();
        if !value.propagate_err_to_vfs {
            TestNode::from(inner.deref()).into_node()
        } else {
            todo!()
        }
    }
}

impl Display for InnerConcreteTestNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl From<InnerConcreteTestNode> for String {
    fn from(value: InnerConcreteTestNode) -> Self {
        format!("{}", value)
    }
}

impl FSBackend for ConcreteTestNode {
    type SyncInfo = ShallowTestSyncInfo;

    type IoError = TestError;

    type CreationInfo = InnerConcreteTestNode;

    type Description = String;

    async fn validate(_info: &Self::CreationInfo) -> Result<(), Self::IoError> {
        Ok(())
    }

    fn description(&self) -> Self::Description {
        self.inner.borrow().name().to_string()
    }

    async fn get_sync_info(&self, path: &VirtualPath) -> Result<Self::SyncInfo, Self::IoError> {
        let inner: VfsNode<_> = self.into();

        let node = inner.find_node(path).unwrap();
        match node.state() {
            NodeState::Ok(sync) => Ok(sync.clone()),
            _ => panic!(),
        }
    }

    async fn load_virtual(&self) -> Result<Vfs<Self::SyncInfo>, Self::IoError> {
        let root = self.into();

        Ok(Vfs::new(root))
    }

    async fn read_file(
        &self,
        path: &VirtualPath,
    ) -> Result<impl Stream<Item = Result<Bytes, Self::IoError>> + 'static, Self::IoError> {
        let inner = self.inner.borrow();

        let node = inner.get_node(path);

        if let InnerConcreteTestNode::FE(_, err) = node {
            return Err(FsBackendError::from(io::Error::new(
                io::ErrorKind::NotFound,
                err.as_str(),
            )));
        };

        if let Some(content) = node.content() {
            let owned = content.to_vec();
            let stream = stream::iter(owned.into_iter().map(|b| Ok(Bytes::from(vec![b]))));
            Ok(stream)
        } else {
            panic!("can't open node {path:?}")
        }
    }

    async fn write_file<Data: TryStream + Send + Unpin>(
        &self,
        path: &VirtualPath,
        data: Data,
    ) -> Result<Self::SyncInfo, Self::IoError>
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
        if let InnerConcreteTestNode::FE(_, err) = inner.deref() {
            return Err(FsBackendError::from(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                err.as_str(),
            )));
        };

        let parent = inner.get_node_mut(path.parent().unwrap());

        match parent {
            InnerConcreteTestNode::D(_, children) | InnerConcreteTestNode::DH(_, _, children) => {
                // Overwrite file
                for child in children.iter_mut() {
                    if child.name() == path.name() {
                        let hash = xxh3_64(&content);
                        *child = InnerConcreteTestNode::FF(path.name().to_owned(), content);
                        return Ok(ShallowTestSyncInfo::new(hash));
                    }
                }

                // Create file
                let hash = xxh3_64(&content);
                children.push(InnerConcreteTestNode::FF(path.name().to_owned(), content));
                Ok(ShallowTestSyncInfo::new(hash))
            }
            _ => panic!("{path:?}"),
        }
    }

    async fn rm(&self, path: &VirtualPath) -> Result<(), Self::IoError> {
        let mut inner = self.inner.borrow_mut();
        if let InnerConcreteTestNode::FE(_, err) = inner.deref() {
            return Err(FsBackendError::from(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                err.as_str(),
            )));
        };
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
            _ => panic!("{path:?}"),
        }
    }

    async fn mkdir(&self, path: &VirtualPath) -> Result<Self::SyncInfo, Self::IoError> {
        let mut inner = self.inner.borrow_mut();
        if let InnerConcreteTestNode::FE(_, err) = inner.deref() {
            return Err(FsBackendError::from(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                err.as_str(),
            )));
        };

        let parent = inner.get_node_mut(path.parent().unwrap());

        match parent {
            InnerConcreteTestNode::D(_, children) | InnerConcreteTestNode::DH(_, _, children) => {
                children.push(InnerConcreteTestNode::D(
                    path.name().to_string(),
                    Vec::new(),
                ));

                let hash = xxh3_64(
                    &children
                        .iter()
                        .flat_map(|node| xxh3_64(node.name().as_bytes()).to_le_bytes())
                        .collect::<Vec<_>>(),
                );

                Ok(ShallowTestSyncInfo::new(hash))
            }
            _ => panic!("{path:?}"),
        }
    }

    async fn rmdir(&self, path: &VirtualPath) -> Result<(), Self::IoError> {
        let mut inner = self.inner.borrow_mut();
        if let InnerConcreteTestNode::FE(_, err) = inner.deref() {
            return Err(FsBackendError::from(io::Error::new(
                io::ErrorKind::ReadOnlyFilesystem,
                err.as_str(),
            )));
        };

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
            _ => panic!("{path:?}"),
        }
    }
}

pub type TestError = FsBackendError;

impl FsInstanceDescription for String {
    fn name(&self) -> &str {
        self
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
    const TYPE_NAME: &'static str = "Test FileSystem";
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
