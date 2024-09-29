use std::{ffi::OsStr, io};

use crate::{
    local::path::PathLike,
    vfs::{DirTree, FileInfo, TreeNode},
};

#[derive(Clone)]
pub(crate) enum TestNode {
    F(&'static str),
    D(&'static str, Vec<TestNode>),
}

impl From<&TestNode> for TreeNode {
    fn from(value: &TestNode) -> Self {
        match value {
            TestNode::F(name) => TreeNode::File(FileInfo::new(name)),
            TestNode::D(name, children) => TreeNode::Dir(DirTree::new_with_children(
                name,
                children.iter().map(|child| child.into()).collect(),
            )),
        }
    }
}

impl From<TestNode> for TreeNode {
    fn from(value: TestNode) -> Self {
        Self::from(&value)
    }
}

impl PathLike for TestNode {
    type DirEntry = Self;
    fn is_file(&self) -> bool {
        match self {
            TestNode::F(_) => true,
            TestNode::D(_, _) => false,
        }
    }

    fn is_dir(&self) -> bool {
        match self {
            TestNode::F(_) => false,
            TestNode::D(_, _) => true,
        }
    }

    fn file_name(&self) -> Option<&OsStr> {
        match self {
            TestNode::F(name) => Some(OsStr::new(name)),
            TestNode::D(name, _) => Some(OsStr::new(name)),
        }
    }

    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self>>> {
        match self {
            TestNode::F(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected a dir",
            )),
            TestNode::D(_, children) => Ok(children.iter().cloned().map(|val| Ok(val))),
        }
    }
}
