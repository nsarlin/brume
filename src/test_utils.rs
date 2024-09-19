use crate::vfs::{DirTree, FileInfo, TreeNode};

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
