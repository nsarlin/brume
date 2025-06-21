use std::{
    ffi::OsStr,
    fmt::Debug,
    io::{self},
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};

use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use tokio::fs::{self, DirEntry, ReadDir};
use tracing::{debug, warn};

use crate::vfs::{DirTree, FileInfo, VfsNode};

use super::{LocalDirError, LocalSyncInfo};

/// A path mapped to a local file-system that can be queried.
pub(crate) trait LocalPath: Debug {
    type DirEntry: LocalPath;
    async fn is_file(&self) -> bool;
    async fn is_dir(&self) -> bool;
    fn file_name(&self) -> Option<&OsStr>;
    async fn read_dir(&self) -> io::Result<BoxStream<'_, io::Result<Self::DirEntry>>>;
    async fn modification_time(&self) -> io::Result<SystemTime>;
    async fn file_size(&self) -> io::Result<u64>;
}

struct ReadDirStream {
    inner: ReadDir,
}

impl ReadDirStream {
    pub fn new(read_dir: ReadDir) -> Self {
        Self { inner: read_dir }
    }
}

impl Stream for ReadDirStream {
    type Item = io::Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_entry(cx).map(Result::transpose)
    }
}

impl LocalPath for Path {
    type DirEntry = PathBuf;
    async fn is_file(&self) -> bool {
        fs::metadata(self)
            .await
            .map(|meta| meta.is_file())
            .unwrap_or(false)
    }

    async fn is_dir(&self) -> bool {
        fs::metadata(self)
            .await
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
    }

    fn file_name(&self) -> Option<&OsStr> {
        self.file_name()
    }

    async fn read_dir(&self) -> io::Result<BoxStream<'_, io::Result<Self::DirEntry>>> {
        fs::read_dir(self).await.map(|entry_iter| {
            ReadDirStream::new(entry_iter)
                .map(|entry| entry.map(|inner| inner.path()))
                .boxed()
        })
    }

    async fn modification_time(&self) -> io::Result<SystemTime> {
        fs::metadata(self).await?.modified()
    }

    async fn file_size(&self) -> io::Result<u64> {
        Ok(fs::metadata(self).await?.len())
    }
}

impl LocalPath for PathBuf {
    type DirEntry = Self;
    async fn is_file(&self) -> bool {
        LocalPath::is_file(self.as_path()).await
    }

    async fn is_dir(&self) -> bool {
        LocalPath::is_dir(self.as_path()).await
    }

    fn file_name(&self) -> Option<&OsStr> {
        LocalPath::file_name(self.as_path())
    }

    async fn read_dir(&self) -> io::Result<BoxStream<'_, io::Result<Self::DirEntry>>> {
        LocalPath::read_dir(self.as_path()).await
    }

    async fn modification_time(&self) -> io::Result<SystemTime> {
        LocalPath::modification_time(self.as_path()).await
    }

    async fn file_size(&self) -> io::Result<u64> {
        LocalPath::file_size(self.as_path()).await
    }
}

/// Creates a VFS node by recursively scanning a list of path
///
/// Returns a vector of VfsNodes representing the children found.
pub(crate) async fn build_vfs_subtree<P: LocalPath>(
    children: &[P],
) -> Result<Vec<VfsNode<LocalSyncInfo>>, LocalDirError> {
    let mut nodes = Vec::new();
    for path in children {
        let time = match path.modification_time().await {
            Ok(time) => time.into(),
            Err(err) => {
                // File might be an invalid symlink, so we just log and skip it
                warn!("skipping file {path:?}: {err}");
                continue;
            }
        };

        let sync = LocalSyncInfo::new(time);

        if path.is_file().await {
            let file_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| LocalDirError::invalid_path(path))?;
            let node = VfsNode::File(FileInfo::new(
                file_name,
                path.file_size()
                    .await
                    .map_err(|e| LocalDirError::io(path, e))?,
                time,
                sync,
            ));
            nodes.push(node);
        } else if path.is_dir().await {
            let dir_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| LocalDirError::invalid_path(path))?;
            let mut dir_tree = DirTree::new(dir_name, time, sync);

            let sub_children = path
                .read_dir()
                .await
                .map_err(|e| LocalDirError::io(path, e))?
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| LocalDirError::io(path, e))?;

            let child_nodes = Box::pin(build_vfs_subtree(&sub_children)).await?;
            for child_node in child_nodes {
                dir_tree.insert_child(child_node);
            }
            nodes.push(VfsNode::Dir(dir_tree));
        } else {
            debug!("Skipping non-file/non-dir path: {:?}", path);
        }
    }

    Ok(nodes)
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use super::*;

    use crate::vfs::{DirTree, VfsNode};

    use crate::test_utils::TestNode::{D, F, L};

    #[tokio::test]
    async fn test_parse_path() {
        let base = vec![
            D("Doc", vec![F("f1.md"), F("f2.pdf")]),
            D("a", vec![D("b", vec![D("c", vec![])])]),
        ];

        let children_nodes = build_vfs_subtree(&base).await.unwrap();

        let mut root = DirTree::new("", Utc::now(), LocalSyncInfo::new(Utc::now()));
        children_nodes.into_iter().for_each(|n| {
            root.insert_child(n);
        });
        let parsed = VfsNode::Dir(root).as_ok();

        let reference = &D("", base).into_node();

        assert!(parsed.structural_eq(reference));
    }

    #[tokio::test]
    async fn test_parse_symlink() {
        // Check a link that points to a dir
        let target = D("Doc", vec![F("f1.md"), F("f2.pdf")]);
        let base = vec![L("link", Some(&target))];

        let children_nodes = build_vfs_subtree(&base).await.unwrap();

        let mut root = DirTree::new("", Utc::now(), LocalSyncInfo::new(Utc::now()));
        children_nodes.into_iter().for_each(|n| {
            root.insert_child(n);
        });
        let parsed = VfsNode::Dir(root).as_ok();

        let reference = D("", vec![D("link", vec![F("f1.md"), F("f2.pdf")])]).into_node();

        assert!(parsed.structural_eq(&reference));

        // Check a link that points to a file
        let target = F("f1.md");
        let base = vec![D("Doc", vec![L("link", Some(&target)), F("f2.pdf")])];

        let children_nodes = build_vfs_subtree(&base).await.unwrap();

        let mut root = DirTree::new("", Utc::now(), LocalSyncInfo::new(Utc::now()));
        children_nodes.into_iter().for_each(|n| {
            root.insert_child(n);
        });
        let parsed = VfsNode::Dir(root).as_ok();

        let reference = D("", vec![D("Doc", vec![F("link"), F("f2.pdf")])]).into_node();

        assert!(parsed.structural_eq(&reference));

        // Check that invalid links are skipped
        let base = vec![D("Doc", vec![L("link", None), F("f2.pdf")])];

        let children_nodes = build_vfs_subtree(&base).await.unwrap();

        let mut root = DirTree::new("", Utc::now(), LocalSyncInfo::new(Utc::now()));
        children_nodes.into_iter().for_each(|n| {
            root.insert_child(n);
        });
        let parsed = VfsNode::Dir(root).as_ok();

        let reference = D("", vec![D("Doc", vec![F("f2.pdf")])]).into_node();

        assert!(parsed.structural_eq(&reference));
    }
}
