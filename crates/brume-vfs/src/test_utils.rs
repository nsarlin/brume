use xxhash_rust::xxh3::xxh3_64;

use crate::{
    DirTree, FileMeta, NodeState, VfsNode, VirtualPath, VirtualPathBuf,
    update::{FailedUpdateApplication, IsModified, ModificationState, VfsDiff},
};

/// Can be used to easily create Vfs for tests
#[derive(Clone, Debug)]
#[allow(unused)]
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

                let failed_update =
                    FailedUpdateApplication::new(VfsDiff::file_created(path), error.to_string());
                VfsNode::File(FileMeta::new_error(name, 0, failed_update))
            }
            Self::DE(name, error) => {
                let mut path = parent.to_owned();
                path.push(name);

                let failed_update =
                    FailedUpdateApplication::new(VfsDiff::dir_created(path), error.to_string());

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

                let failed_update =
                    FailedUpdateApplication::new(VfsDiff::file_created(path), error.to_string());
                VfsNode::File(FileMeta::new_error(name, 0, failed_update))
            }
            Self::DE(name, error) => {
                let mut path = parent.to_owned();
                path.push(name);

                let failed_update =
                    FailedUpdateApplication::new(VfsDiff::dir_created(path), error.to_string());
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
                    error.to_string(),
                );
                DirTree::new_error(name, failed_update)
            }
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

impl IsModified for ShallowTestSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.hash == reference.hash {
            ModificationState::ShallowUnmodified
        } else {
            ModificationState::Modified
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

impl IsModified for RecursiveTestSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.hash == reference.hash {
            ModificationState::RecursiveUnmodified
        } else {
            ModificationState::Modified
        }
    }
}
