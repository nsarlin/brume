//! File system manipulation

use std::cmp::Ordering;

use crate::{
    concrete::{concrete_eq_file, ConcreteFS},
    sorted_list::{Sortable, SortedList},
    vfs::{DirTree, SortedUpdateList, TreeNode, Vfs, VfsNodeUpdate, VirtualPath},
    Error,
};

/// Main representation of any kind of filesystem, remote or local.
///
/// It is composed of a generic Concrete FS backend that used for file operations, and a Virtual FS
/// that is its in-memory representation, using a tree-structure.
pub struct FileSystem<Concrete: ConcreteFS> {
    concrete: Concrete,
    vfs: Vfs<Concrete::SyncInfo>,
}

impl<Concrete: ConcreteFS> FileSystem<Concrete> {
    pub fn new(concrete: Concrete) -> Self {
        Self {
            concrete,
            vfs: Vfs::empty(),
        }
    }

    pub fn root(&self) -> FileSystemNode<'_, Concrete> {
        FileSystemNode {
            concrete: &self.concrete,
            node: self.vfs.root(),
        }
    }

    /// Query the concrete FS to update the in memory virtual representation
    pub async fn update_vfs(&mut self) -> Result<SortedUpdateList, Error>
    where
        Error: From<Concrete::Error>,
    {
        let new_vfs = self.concrete.load_virtual().await?;

        let updates = self.vfs.diff(&new_vfs)?;

        self.vfs = new_vfs;
        Ok(updates)
    }

    /// Return the virtual representation of this FileSystem
    pub fn vfs(&self) -> &Vfs<Concrete::SyncInfo> {
        &self.vfs
    }

    /// Return the concrete filesystem
    pub fn concrete(&self) -> &Concrete {
        &self.concrete
    }

    pub fn find_node(&self, path: &VirtualPath) -> Option<FileSystemNode<'_, Concrete>> {
        Some(FileSystemNode {
            concrete: &self.concrete,
            node: self.vfs.root().find_node(path)?,
        })
    }

    pub fn find_dir(&self, path: &VirtualPath) -> Result<FileSystemDir<'_, Concrete>, Error> {
        Ok(FileSystemDir {
            concrete: &self.concrete,
            dir: self.vfs.root().find_dir(path)?,
        })
    }
}

/// A directory in the [`FileSystem`]
pub struct FileSystemDir<'fs, Concrete: ConcreteFS> {
    concrete: &'fs Concrete,
    dir: &'fs DirTree<Concrete::SyncInfo>,
}

impl<'fs, Concrete: ConcreteFS> FileSystemDir<'fs, Concrete> {
    pub fn new(concrete: &'fs Concrete, dir: &'fs DirTree<Concrete::SyncInfo>) -> Self {
        Self { concrete, dir }
    }

    pub async fn diff<'otherfs, OtherConcrete: ConcreteFS>(
        &self,
        other: &FileSystemDir<'otherfs, OtherConcrete>,
        parent_path: &VirtualPath,
    ) -> Result<SortedUpdateList, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        // Here we cannot use `iter_zip_map` or an async variant of it because it does not seem
        // possible to express the lifetimes required by the async closures

        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.dir.name());

        let mut ret = SortedList::new();
        let mut self_iter = self.dir.children().iter();
        let mut other_iter = other.dir.children().iter();

        let mut self_item_opt = self_iter.next();
        let mut other_item_opt = other_iter.next();

        while let (Some(self_item), Some(other_item)) = (self_item_opt, other_item_opt) {
            match self_item.key().cmp(other_item.key()) {
                Ordering::Less => {
                    ret.insert(self_item.to_removed_diff(&dir_path));
                    self_item_opt = self_iter.next()
                }
                // We can use `unchecked_extend` because we know that the updates will be produced
                // in order
                Ordering::Equal => {
                    let self_child = FileSystemNode::new(self.concrete, self_item);
                    let other_child = FileSystemNode::new(other.concrete, other_item);

                    ret.unchecked_extend(Box::pin(self_child.diff(&other_child, &dir_path)).await?);
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

/// A node in a [`FileSystem`], can represent a file or a directory.
pub struct FileSystemNode<'fs, Concrete: ConcreteFS> {
    concrete: &'fs Concrete,
    node: &'fs TreeNode<Concrete::SyncInfo>,
}

impl<'fs, Concrete: ConcreteFS> FileSystemNode<'fs, Concrete> {
    pub fn new(concrete: &'fs Concrete, node: &'fs TreeNode<Concrete::SyncInfo>) -> Self {
        Self { concrete, node }
    }

    /// Get the differences between [`FileSystem`] trees, by checking file contents with their
    /// concrete backends.
    ///
    /// Compared to [``TreeNode::diff``], this will diff the files based on their content and not
    /// only on their SyncInfo.
    pub async fn diff<'otherfs, OtherConcrete: ConcreteFS>(
        &self,
        other: &FileSystemNode<'otherfs, OtherConcrete>,
        parent_path: &VirtualPath,
    ) -> Result<SortedUpdateList, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        if self.node.name() != other.node.name() {
            let mut path = parent_path.to_owned();
            path.push(self.node.name());
            return Err(Error::InvalidPath(path));
        }

        match (self.node, other.node) {
            (TreeNode::Dir(dself), TreeNode::Dir(dother)) => {
                let self_dir = FileSystemDir::new(self.concrete, dself);
                let other_dir = FileSystemDir::new(other.concrete, dother);
                self_dir.diff(&other_dir, parent_path).await
            }
            (TreeNode::File(fself), TreeNode::File(_fother)) => {
                // Diff the file based on their hash
                let mut file_path = parent_path.to_owned();
                file_path.push(fself.name());

                if !concrete_eq_file(self.concrete, other.concrete, &file_path).await? {
                    let diff = VfsNodeUpdate::FileModified(file_path);
                    Ok(SortedUpdateList::from([diff]))
                } else {
                    Ok(SortedUpdateList::new())
                }
            }
            (nself, nother) => Ok(SortedUpdateList::from_vec(vec![
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
        test_utils::TestNode::{D, FF},
        vfs::VirtualPathBuf,
    };

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
        let mut local_fs = FileSystem::new(local_base);
        local_fs.update_vfs().await.unwrap();

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
        let mut remote_fs = FileSystem::new(remote_base);
        remote_fs.update_vfs().await.unwrap();

        let diff = local_fs
            .root()
            .diff(&remote_fs.root(), VirtualPath::root())
            .await
            .unwrap();

        let reference_diff = [
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::FileCreated(VirtualPathBuf::new("/a/b/test.log").unwrap()),
            VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/e/g").unwrap()),
        ]
        .into();

        assert_eq!(diff, reference_diff);
    }
}
