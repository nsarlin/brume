//! How the VFS nodes are stored in the DB
use std::fmt::Debug;

use diesel::prelude::*;
use futures::future::Either;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

use brume::{
    update::VfsUpdate,
    vfs::{DirTree, FileMeta, NodeKind, NodeState, Vfs, VfsNode},
};
use brume_daemon_proto::VirtualPath;

use crate::schema::{filesystems, nodes};

use super::{Database, DatabaseError};

/// Information loaded from a VFS node in the db
#[derive(Clone, Debug, Queryable, Selectable, Identifiable, Associations)]
#[diesel(table_name = nodes)]
#[diesel(belongs_to(DbVfsNode, foreign_key = parent))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct DbVfsNode {
    id: i32,
    name: String,
    kind: String,
    size: Option<i64>,
    state: Option<Vec<u8>>,
    parent: Option<i32>,
}

impl<'a, SyncInfo> From<&'a DbVfsNode> for VfsNode<SyncInfo>
where
    SyncInfo: Deserialize<'a>,
{
    fn from(value: &'a DbVfsNode) -> Self {
        let kind = NodeKind::try_from(value.kind.as_str()).unwrap();
        let state = value
            .state
            .as_ref()
            .map(|st| bincode::deserialize(st).unwrap())
            .unwrap_or(NodeState::NeedResync);

        match kind {
            NodeKind::Dir => Self::Dir(DirTree::new_with_state(&value.name, state)),
            NodeKind::File => Self::File(FileMeta::new_with_state(
                &value.name,
                value.size.unwrap() as u64,
                state,
            )),
        }
    }
}

/// Information needed to insert a new VFS node in the db
#[derive(Insertable)]
#[diesel(table_name = nodes)]
struct DbNewVfsNode<'a> {
    name: &'a str,
    kind: &'a str,
    size: Option<i64>,
    state: Option<&'a [u8]>,
    parent: Option<i32>,
}

/// Information returned by an update of a VFS node in the db
#[derive(AsChangeset)]
#[diesel(table_name = nodes)]
struct DbUpdatedVfsNode<'a> {
    size: Option<i64>,
    state: Option<&'a [u8]>,
}

impl Default for DbNewVfsNode<'static> {
    fn default() -> Self {
        Self::root()
    }
}

impl DbNewVfsNode<'static> {
    pub(super) fn root() -> Self {
        Self {
            name: "",
            kind: NodeKind::Dir.as_str(),
            size: None,
            state: None,
            parent: None,
        }
    }
}

impl Database {
    pub async fn insert_vfs_root(&self) -> Result<i32, DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let conn = self.pool.get().await?;
        conn.interact(move |conn| {
            let vfs_root = DbNewVfsNode::root();
            diesel::insert_into(nodes)
                .values(&vfs_root)
                .returning(id)
                .get_result(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
    }

    #[cfg(test)]
    async fn list_all_nodes(&self) -> Result<Vec<DbVfsNode>, DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let conn = self.pool.get().await?;
        conn.interact(|conn| nodes.select(DbVfsNode::as_select()).load(conn))
            .await
            .unwrap() // This should never fail unless the inner closure panics
            .map_err(|e| e.into())
    }

    /// Loads recursively a node and its children
    async fn load_nodes_rec<SyncInfo: DeserializeOwned>(
        &self,
        parent_node: &DbVfsNode,
    ) -> Result<VfsNode<SyncInfo>, DatabaseError> {
        let mut vfs_node = parent_node.into();

        if let VfsNode::Dir(dir) = &mut vfs_node {
            let parent_node = parent_node.to_owned();

            let db_children = {
                let conn = self.pool.get().await?;
                conn.interact(move |conn| {
                    DbVfsNode::belonging_to(&parent_node)
                        .select(DbVfsNode::as_select())
                        .load(conn)
                })
                .await
                .unwrap()
            }?;

            for db_child in db_children {
                let child = Box::pin(self.load_nodes_rec(&db_child)).await?;

                dir.insert_child(child);
            }
        }
        Ok(vfs_node)
    }

    /// Loads a vfs with all its node from the DB
    pub async fn load_vfs<SyncInfo: DeserializeOwned + Debug>(
        &self,
        fs_uuid: Uuid,
    ) -> Result<Vfs<SyncInfo>, DatabaseError> {
        let vfs_root = self.get_vfs_root(fs_uuid).await?;

        let loaded_root = self.load_nodes_rec(&vfs_root).await?;

        Ok(Vfs::new(loaded_root))
    }

    /// Loads the node at `path` from the DB
    async fn find_node(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
    ) -> Result<DbVfsNode, DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let mut current_node = root.clone();

        for cur_name in path.iter() {
            let conn = self.pool.get().await?;

            let cur_name = cur_name.to_owned();
            current_node = conn
                .interact(move |conn| {
                    DbVfsNode::belonging_to(&current_node)
                        .select(DbVfsNode::as_select())
                        .filter(name.eq(cur_name))
                        .first(conn)
                })
                .await
                .unwrap() // This should never fail unless the inner closure panics
                ?
        }

        Ok(current_node)
    }

    /// Inserts a child node with the provided parent
    async fn insert_node_child<SyncInfo: Serialize>(
        &self,
        parent_node: i32,
        node: &VfsNode<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        match node {
            VfsNode::Dir(dir_tree) => Box::pin(self.insert_dir_child(parent_node, dir_tree)).await,
            VfsNode::File(file_meta) => self.insert_file_child(parent_node, file_meta).await,
        }
    }

    /// Inserts a file node with the provided parent
    async fn insert_file_child<SyncInfo: Serialize>(
        &self,
        parent_node: i32,
        file: &FileMeta<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let file_name = file.name().to_string();
        let file_size = file.size() as i64;
        let file_state = bincode::serialize(file.state()).unwrap();

        let conn = self.pool.get().await?;

        conn.interact(move |conn| {
            let db_node = DbNewVfsNode {
                name: &file_name,
                kind: NodeKind::File.as_str(),
                size: Some(file_size),
                state: Some(&file_state),
                parent: Some(parent_node),
            };

            diesel::insert_into(nodes).values(&db_node).execute(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
        .map(|_| ())
    }

    /// Inserts a dir node with the provided parent
    async fn insert_dir_child<SyncInfo: Serialize>(
        &self,
        parent_node: i32,
        tree: &DirTree<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let dir_name = tree.name().to_string();
        let dir_state = bincode::serialize(tree.state()).unwrap();

        let new_id = {
            let conn = self.pool.get().await?;

            conn.interact(move |conn| {
                let db_dir = DbNewVfsNode {
                    name: &dir_name,
                    kind: NodeKind::Dir.as_str(),
                    size: None,
                    state: Some(&dir_state),
                    parent: Some(parent_node),
                };

                diesel::insert_into(nodes)
                    .values(&db_dir)
                    .returning(id)
                    .get_result(conn)
            })
								.await
								.unwrap() // This should never fail unless the inner closure panics
								?
        };

        for child in tree.children().iter() {
            self.insert_node_child(new_id, child).await?;
        }
        Ok(())
    }

    /// Inserts a dir node at the provided path
    async fn insert_dir<SyncInfo: Serialize>(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        tree: &DirTree<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        let parent_node = path
            .parent()
            .map(|parent_path| Either::Left(self.find_node(root, parent_path)))
            .unwrap_or_else(|| Either::Right(async { Ok(root.clone()) }))
            .await?;

        self.insert_dir_child(parent_node.id, tree).await
    }

    /// Updates the metadata of the file node at the provided path
    async fn update_file<SyncInfo: Serialize>(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        file: &FileMeta<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let file_size = file.size() as i64;
        let file_state = bincode::serialize(file.state())?;

        let node = self.find_node(root, path).await?;

        let conn = self.pool.get().await?;

        conn.interact(move |conn| {
            let update = DbUpdatedVfsNode {
                size: Some(file_size),
                state: Some(&file_state),
            };

            diesel::update(nodes)
                .filter(id.eq(node.id))
                .set(update)
                .execute(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
        .map(|_| ())
    }

    /// Inserts a file node at the provided path
    async fn insert_file<SyncInfo: Serialize>(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        file: &FileMeta<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        let parent_node = path
            .parent()
            .map(|parent_path| Either::Left(self.find_node(root, parent_path)))
            .unwrap_or_else(|| Either::Right(async { Ok(root.clone()) }))
            .await?;

        self.insert_file_child(parent_node.id, file).await
    }

    /// Removes a node from DB at the provided path
    async fn delete_node(&self, root: &DbVfsNode, path: &VirtualPath) -> Result<(), DatabaseError> {
        let node = self.find_node(root, path).await?;

        let conn = self.pool.get().await?;
        conn.interact(move |conn| diesel::delete(&node).execute(conn))
            .await
            .unwrap() // This should never fail unless the inner closure panics
            .map_err(|e| e.into())
            .map(|_| ())
    }

    /// Updates the state of the node at the provided path
    async fn set_node_state<SyncInfo: Serialize>(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        node_state: &NodeState<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let db_state = bincode::serialize(&node_state)?;

        let node = self.find_node(root, path).await?;

        let conn = self.pool.get().await?;

        conn.interact(move |conn| {
            diesel::update(nodes)
                .filter(id.eq(node.id))
                .set(state.eq(db_state))
                .execute(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
        .map(|_| ())
    }

    /// Returns the root node of the FS
    async fn get_vfs_root(&self, fs_uuid: Uuid) -> Result<DbVfsNode, DatabaseError> {
        let conn = self.pool.get().await?;
        conn.interact(move |conn| {
            nodes::table
                .inner_join(filesystems::table)
                .filter(filesystems::uuid.eq(fs_uuid.as_bytes()))
                .select(DbVfsNode::as_select())
                .first(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
    }

    /// Updates the state of the node at `path` inside the FS with id `fs_uuid`
    pub async fn update_vfs_node_state<SyncInfo: Serialize>(
        &self,
        fs_uuid: Uuid,
        path: &VirtualPath,
        node_state: &NodeState<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        let vfs_root = self.get_vfs_root(fs_uuid).await?;

        self.set_node_state(&vfs_root, path, node_state).await
    }

    /// Removes from the DB the node at `path` inside the FS with id `fs_uuid`
    pub async fn delete_vfs_node(
        &self,
        fs_uuid: Uuid,
        path: &VirtualPath,
    ) -> Result<(), DatabaseError> {
        let vfs_root = self.get_vfs_root(fs_uuid).await?;

        self.delete_node(&vfs_root, path).await
    }

    /// Applies a VFS update to the nodes in the DB
    pub async fn update_vfs<SyncInfo: Serialize>(
        &self,
        fs_uuid: Uuid,
        update: &VfsUpdate<SyncInfo>,
    ) -> Result<(), DatabaseError> {
        let vfs_root = self.get_vfs_root(fs_uuid).await?;

        match update {
            VfsUpdate::DirCreated(dir_creation) => {
                self.insert_dir(&vfs_root, dir_creation.path(), dir_creation.dir())
                    .await
            }
            VfsUpdate::DirRemoved(path) => self.delete_node(&vfs_root, path).await,
            VfsUpdate::FileCreated(file_creation) => {
                let name = file_creation.path().name().to_owned();
                let path = file_creation.path().to_owned();
                let file =
                    FileMeta::new(&name, file_creation.file_size(), file_creation.sync_info());
                self.insert_file(&vfs_root, &path, &file).await
            }
            VfsUpdate::FileModified(file_modification) => {
                let name = file_modification.path().name().to_owned();
                let path = file_modification.path().to_owned();
                let file = FileMeta::new(
                    &name,
                    file_modification.file_size(),
                    file_modification.sync_info(),
                );

                self.update_file(&vfs_root, &path, &file).await
            }
            VfsUpdate::FileRemoved(path) => self.delete_node(&vfs_root, path).await,
            VfsUpdate::FailedApplication(failed_update) => {
                let path = failed_update.path().to_owned();
                let state = NodeState::<SyncInfo>::Error(failed_update.clone());
                self.set_node_state(&vfs_root, &path, &state).await
            }
            VfsUpdate::Conflict(vfs_diff) => {
                let path = vfs_diff.path().to_owned();
                let state = NodeState::<SyncInfo>::Conflict(vfs_diff.clone());
                self.set_node_state(&vfs_root, &path, &state).await
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::{self, ErrorKind};

    use brume::{
        concrete::local::LocalDirError,
        update::{FailedUpdateApplication, VfsDiff, VfsDirCreation, VfsFileUpdate, VfsUpdate},
        vfs::{DirTree, FileMeta, NodeKind, VfsNode},
    };
    use brume_daemon_proto::{
        AnyFsCreationInfo, AnyFsRef, LocalDirCreationInfo, VirtualPath, VirtualPathBuf,
    };

    use crate::db::{Database, DatabaseConfig};

    #[tokio::test]
    async fn test_update_db_vfs() {
        let db = Database::new(&DatabaseConfig::InMemory).await.unwrap();

        let mut a = DirTree::new("a", ());
        let f1 = FileMeta::new("f1", 10, ());
        let mut b = DirTree::new("b", ());
        let c = DirTree::new("c", ());
        let d = DirTree::new("d", ());

        b.insert_child(VfsNode::Dir(c));
        a.insert_child(VfsNode::File(f1));
        a.insert_child(VfsNode::Dir(b));

        let mut base_root = DirTree::new("", ());
        base_root.insert_child(VfsNode::Dir(a.clone()));
        let base_vfs = VfsNode::Dir(base_root);

        let creation_update = VfsUpdate::DirCreated(VfsDirCreation::new(VirtualPath::root(), a));

        let fs_info = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new("/tmp/test"));
        let fs_ref = AnyFsRef::from(fs_info.clone());
        db.insert_new_filesystem(fs_ref.id(), &fs_info)
            .await
            .unwrap();

        let nodes = db.list_all_nodes().await.unwrap();

        // Only the root is present
        assert_eq!(nodes.len(), 1);

        db.update_vfs(fs_ref.id(), &creation_update).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 5);

        let root = db.get_vfs_root(fs_ref.id()).await.unwrap();

        let node = db
            .find_node(&root, &VirtualPathBuf::new("/a/f1").unwrap())
            .await
            .unwrap();

        assert_eq!(node.kind, NodeKind::File.as_str());

        let node = db
            .find_node(&root, &VirtualPathBuf::new("/a/b").unwrap())
            .await
            .unwrap();

        assert_eq!(node.kind, NodeKind::Dir.as_str());

        let vfs_root = db.load_nodes_rec::<()>(&root).await.unwrap();
        assert!(vfs_root.structural_eq(&base_vfs));

        let creation_update =
            VfsUpdate::DirCreated(VfsDirCreation::new(&VirtualPathBuf::new("/a").unwrap(), d));
        db.update_vfs(fs_ref.id(), &creation_update).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 6);

        let node = db
            .find_node(&root, &VirtualPathBuf::new("/a/d").unwrap())
            .await
            .unwrap();

        assert_eq!(node.kind, NodeKind::Dir.as_str());

        let creation_update = VfsUpdate::FileCreated(VfsFileUpdate::new(
            &VirtualPathBuf::new("/a/b/f2").unwrap(),
            42,
            (),
        ));
        db.update_vfs(fs_ref.id(), &creation_update).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 7);

        let node = db
            .find_node(&root, &VirtualPathBuf::new("/a/b/f2").unwrap())
            .await
            .unwrap();

        assert_eq!(node.kind, NodeKind::File.as_str());
        assert_eq!(node.size, Some(42));

        let creation_update = VfsUpdate::FileModified(VfsFileUpdate::new(
            &VirtualPathBuf::new("/a/b/f2").unwrap(),
            54,
            (),
        ));
        db.update_vfs(fs_ref.id(), &creation_update).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 7);

        let node = db
            .find_node(&root, &VirtualPathBuf::new("/a/b/f2").unwrap())
            .await
            .unwrap();

        assert_eq!(node.kind, NodeKind::File.as_str());
        assert_eq!(node.size, Some(54));

        let diff = VfsDiff::file_modified(VirtualPathBuf::new("/a/b/f2").unwrap());
        let failed_diff = FailedUpdateApplication::new(
            diff.clone(),
            LocalDirError::io(
                &VirtualPathBuf::new("/a/b/f2").unwrap(),
                io::Error::new(ErrorKind::AlreadyExists, "Error"),
            )
            .into(),
        );

        let failed_update: VfsUpdate<()> = VfsUpdate::FailedApplication(failed_diff);
        db.update_vfs(fs_ref.id(), &failed_update).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 7);

        let node = db
            .find_node(&root, &VirtualPathBuf::new("/a/b/f2").unwrap())
            .await
            .unwrap();

        let vfs_node = VfsNode::<()>::from(&node);
        assert!(vfs_node.state().is_err());

        let conflict: VfsUpdate<()> = VfsUpdate::Conflict(diff);
        db.update_vfs(fs_ref.id(), &conflict).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 7);

        let node = db
            .find_node(&root, &VirtualPathBuf::new("/a/b/f2").unwrap())
            .await
            .unwrap();

        let vfs_node = VfsNode::<()>::from(&node);
        assert!(vfs_node.state().is_conflict());
    }
}
