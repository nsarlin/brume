//! How the VFS nodes are stored in the DB
use std::fmt::Debug;

use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::prelude::*;
use futures::future::Either;
use uuid::Uuid;

use brume::{
    update::{UpdateKind, VfsUpdate},
    vfs::{
        DirState, DirTree, FileInfo, FileState, NodeKind, NodeState, StatefulDirTree, StatefulVfs,
        StatefulVfsNode, Vfs, VfsNode,
    },
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
    last_modified: Option<NaiveDateTime>,
}

impl<'a> TryFrom<&'a DbVfsNode> for StatefulVfsNode<Vec<u8>> {
    type Error = DatabaseError;

    fn try_from(value: &'a DbVfsNode) -> Result<Self, Self::Error> {
        let kind = NodeKind::try_from(value.kind.as_str())
            .map_err(|_| DatabaseError::invalid_data("kind", "nodes", None))?;
        let state: NodeState<Vec<u8>> = value
            .state
            .as_ref()
            .map(|st| {
                bincode::deserialize(st)
                    .map_err(|e| DatabaseError::invalid_data("state", "nodes", Some(Box::new(e))))
            })
            .transpose()?
            .unwrap_or(NodeState::NeedResync);

        let last_modified = value
            .last_modified
            .map(|dt| dt.and_utc())
            .unwrap_or(DateTime::<Utc>::MIN_UTC);

        Ok(match kind {
            NodeKind::Dir => Self::Dir(DirTree::new(&value.name, last_modified, state)),
            NodeKind::File => Self::File(FileInfo::new(
                &value.name,
                value
                    .size
                    .ok_or_else(|| DatabaseError::invalid_data("size", "nodes", None))?
                    as u64,
                last_modified,
                state,
            )),
        })
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
    last_modified: NaiveDateTime,
}

/// Information returned by an update of a VFS node in the db
#[derive(AsChangeset)]
#[diesel(table_name = nodes)]
struct DbUpdatedVfsNode<'a> {
    size: Option<i64>,
    last_modified: NaiveDateTime,
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
            last_modified: NaiveDateTime::MIN,
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
    async fn load_nodes_rec(
        &self,
        parent_node: &DbVfsNode,
    ) -> Result<StatefulVfsNode<Vec<u8>>, DatabaseError> {
        let mut vfs_node = parent_node.try_into()?;

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
    pub async fn load_vfs(&self, fs_uuid: Uuid) -> Result<StatefulVfs<Vec<u8>>, DatabaseError> {
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
    async fn insert_node_child(
        &self,
        parent_node: i32,
        node: &StatefulVfsNode<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        match node {
            VfsNode::Dir(dir_tree) => Box::pin(self.insert_dir_child(parent_node, dir_tree)).await,
            VfsNode::File(file_meta) => self.insert_file_child(parent_node, file_meta).await,
        }
    }

    /// Inserts a file node with the provided parent
    async fn insert_file_child(
        &self,
        parent_node: i32,
        file: &FileState<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let file_name = file.name().to_string();
        let file_size = file.size() as i64;
        let file_last_modified = file.last_modified().naive_utc();
        let file_state = bincode::serialize(file.state()).unwrap();

        let conn = self.pool.get().await?;

        conn.interact(move |conn| {
            let db_node = DbNewVfsNode {
                name: &file_name,
                kind: NodeKind::File.as_str(),
                size: Some(file_size),
                state: Some(&file_state),
                parent: Some(parent_node),
                last_modified: file_last_modified,
            };

            diesel::insert_into(nodes).values(&db_node).execute(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
        .map(|_| ())
    }

    /// Inserts a dir node with the provided parent
    async fn insert_dir_child(
        &self,
        parent_node: i32,
        tree: &StatefulDirTree<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let dir_name = tree.name().to_string();
        let dir_last_modified = tree.last_modified().naive_utc();
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
                    last_modified: dir_last_modified
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
    async fn insert_dir(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        tree: &StatefulDirTree<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        if self.find_node(root, path).await.is_ok() {
            return Err(DatabaseError::invalid_data(
                "parent",
                "node",
                Some(format!("Trying to insert an already existing dir: {path}").into()),
            ));
        }

        let parent_node = path
            .parent()
            .map(|parent_path| Either::Left(self.find_node(root, parent_path)))
            .unwrap_or_else(|| Either::Right(async { Ok(root.clone()) }))
            .await?;

        self.insert_dir_child(parent_node.id, tree).await
    }

    /// Updates the metadata of the file node at the provided path
    async fn update_file(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        file: &FileState<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let file_size = file.size() as i64;
        let file_state = bincode::serialize(file.state())?;

        let node = self.find_node(root, path).await?;

        let file_last_modified = file.last_modified().naive_utc();
        let conn = self.pool.get().await?;

        conn.interact(move |conn| {
            let update = DbUpdatedVfsNode {
                size: Some(file_size),
                state: Some(&file_state),
                last_modified: file_last_modified,
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

    /// Updates the metadata of the dir node at the provided path
    async fn update_dir(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        dir: &DirState<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let dir_state = bincode::serialize(dir.state())?;

        let node = self.find_node(root, path).await?;

        let dir_last_modified = dir.last_modified().naive_utc();
        let conn = self.pool.get().await?;

        conn.interact(move |conn| {
            let update = DbUpdatedVfsNode {
                size: None,
                state: Some(&dir_state),
                last_modified: dir_last_modified,
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
    async fn insert_file(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        file: &FileState<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        if self.find_node(root, path).await.is_ok() {
            return Err(DatabaseError::invalid_data(
                "parent",
                "node",
                Some(format!("Trying to insert an already existing file: {path}").into()),
            ));
        }

        let parent_node = path
            .parent()
            .map(|parent_path| Either::Left(self.find_node(root, parent_path)))
            .unwrap_or_else(|| Either::Right(async { Ok(root.clone()) }))
            .await?;

        self.insert_file_child(parent_node.id, file).await
    }

    /// Removes a node from DB at the provided path
    async fn delete_node(&self, root: &DbVfsNode, path: &VirtualPath) -> Result<(), DatabaseError> {
        let node_to_delete = self.find_node(root, path).await?;

        self.delete_node_from_id(node_to_delete.id).await
    }

    async fn delete_node_from_id(&self, node_id: i32) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let conn = self.pool.get().await?;
        conn.interact(move |conn| diesel::delete(nodes.filter(id.eq(node_id))).execute(conn))
            .await
            .unwrap() // This should never fail unless the inner closure panics
            .map_err(|e| e.into())
            .map(|_| ())
    }

    /// Updates the state of the node at the provided path
    async fn set_node_state(
        &self,
        root: &DbVfsNode,
        path: &VirtualPath,
        node_state: &NodeState<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        use crate::schema::nodes::dsl::*;

        let node = self.find_node(root, path).await?;

        let db_state = bincode::serialize(&node_state)?;

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

    /// Returns the id of the root node of the FS
    pub async fn get_vfs_root_id(&self, fs_uuid: Uuid) -> Result<i32, DatabaseError> {
        let conn = self.pool.get().await?;
        let node = conn
            .interact(move |conn| {
                nodes::table
                    .inner_join(filesystems::table)
                    .filter(filesystems::uuid.eq(fs_uuid.as_bytes()))
                    .select(DbVfsNode::as_select())
                    .first(conn)
            })
            .await
            // This should never fail unless the inner closure panics
            .unwrap()?;

        Ok(node.id)
    }

    /// Updates the state of the node at `path` inside the FS with id `fs_uuid`
    pub async fn update_vfs_node_state(
        &self,
        fs_uuid: Uuid,
        path: &VirtualPath,
        node_state: &NodeState<Vec<u8>>,
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

    /// Removes the complete VFS from the DB
    pub async fn delete_vfs(&self, root_id: i32) -> Result<(), DatabaseError> {
        self.delete_node_from_id(root_id).await
    }

    /// Resets the VFS to the empty state
    pub async fn reset_vfs(&self, fs_uuid: Uuid) -> Result<(), DatabaseError> {
        use crate::schema::filesystems::dsl::*;

        let old_root = self.get_vfs_root(fs_uuid).await?;
        let new_root = self.insert_vfs_root().await?;

        {
            let conn = self.pool.get().await?;
            conn.interact(move |conn| {
                diesel::update(filesystems)
                    .filter(uuid.eq(fs_uuid.as_bytes()))
                    .set(root_node.eq(new_root))
                    .execute(conn)
            })
            .await
            .unwrap() // This should never fail unless the inner closure panics
            .map(|_| ())?;
        }

        let conn = self.pool.get().await?;
        conn.interact(move |conn| diesel::delete(&old_root).execute(conn))
            .await
            .unwrap() // This should never fail unless the inner closure panics
            .map(|_| ())
            .map_err(|e| e.into())
    }

    /// Applies a VFS update to the nodes in the DB
    pub async fn update_vfs(
        &self,
        fs_uuid: Uuid,
        update: &VfsUpdate<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        let vfs_root = self.get_vfs_root(fs_uuid).await?;

        match update {
            VfsUpdate::DirCreated(dir_creation) => {
                self.insert_dir(&vfs_root, dir_creation.path(), dir_creation.dir())
                    .await
            }
            VfsUpdate::DirRemoved(path) => self.delete_node(&vfs_root, path).await,
            VfsUpdate::DirModified(dir_modification) => {
                let path = dir_modification.path().to_owned();
                let dir = dir_modification.state();

                self.update_dir(&vfs_root, &path, dir).await
            }
            VfsUpdate::FileCreated(file_creation) => {
                let name = file_creation.path().name().to_owned();
                let path = file_creation.path().to_owned();
                let file = FileInfo::new_ok(
                    &name,
                    file_creation.file_size(),
                    file_creation.last_modified(),
                    file_creation.sync_info().clone(),
                );
                self.insert_file(&vfs_root, &path, &file).await
            }
            VfsUpdate::FileModified(file_modification) => {
                let name = file_modification.path().name().to_owned();
                let path = file_modification.path().to_owned();
                let file = FileInfo::new_ok(
                    &name,
                    file_modification.file_size(),
                    file_modification.last_modified(),
                    file_modification.sync_info().clone(),
                );

                self.update_file(&vfs_root, &path, &file).await
            }
            VfsUpdate::FileRemoved(path) => self.delete_node(&vfs_root, path).await,
            VfsUpdate::FailedApplication(failed_update) => {
                let path = failed_update.path().to_owned();

                // If this is a failed creation, we need to create the node in the db to store the
                // error
                match failed_update.update().kind() {
                    UpdateKind::DirCreated => {
                        let dir = DirTree::new_error(
                            path.name(),
                            DateTime::<Utc>::MIN_UTC,
                            failed_update.clone(),
                        );
                        self.insert_dir(&vfs_root, &path, &dir).await
                    }
                    UpdateKind::FileCreated => {
                        let file = FileInfo::new_error(
                            path.name(),
                            0,
                            DateTime::<Utc>::MIN_UTC,
                            failed_update.clone(),
                        );
                        self.insert_file(&vfs_root, &path, &file).await
                    }
                    UpdateKind::DirRemoved
                    | UpdateKind::FileRemoved
                    | UpdateKind::FileModified
                    | UpdateKind::DirModified => {
                        let state = NodeState::Error(failed_update.clone());
                        self.set_node_state(&vfs_root, &path, &state).await
                    }
                }
            }
            VfsUpdate::Conflict(conflict) => {
                let vfs_diff = conflict.update();
                let path = vfs_diff.path().to_owned();
                let state = NodeState::Conflict(conflict.clone());

                // If this is a creation, we need to create the node in the db to store the
                // conflict
                match vfs_diff.kind() {
                    UpdateKind::DirCreated => {
                        let dir = DirTree::new(path.name(), DateTime::<Utc>::MIN_UTC, state);
                        self.insert_dir(&vfs_root, &path, &dir).await
                    }
                    UpdateKind::FileCreated => {
                        let file = FileInfo::new(path.name(), 0, DateTime::<Utc>::MIN_UTC, state);
                        self.insert_file(&vfs_root, &path, &file).await
                    }
                    UpdateKind::DirRemoved
                    | UpdateKind::FileRemoved
                    | UpdateKind::FileModified
                    | UpdateKind::DirModified => {
                        self.set_node_state(&vfs_root, &path, &state).await
                    }
                }
            }
        }
    }

    /// Apply all updates to the db after a sync
    pub async fn apply_sync_updates(
        &self,
        local_updates: &[VfsUpdate<Vec<u8>>],
        local_fs_uuid: Uuid,
        remote_updates: &[VfsUpdate<Vec<u8>>],
        remote_fs_uuid: Uuid,
    ) -> Result<(), DatabaseError> {
        for update in local_updates {
            self.update_vfs(local_fs_uuid, update).await?;
        }

        for update in remote_updates {
            self.update_vfs(remote_fs_uuid, update).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::{self, ErrorKind};

    use brume::{
        concrete::{ToBytes, local::LocalDirError},
        update::{
            FailedUpdateApplication, VfsConflict, VfsDiff, VfsDirCreation, VfsFileUpdate, VfsUpdate,
        },
        vfs::{DirTree, FileInfo, NodeKind, NodeState, VfsNode},
    };
    use brume_daemon_proto::{
        AnyFsCreationInfo, FileSystemMeta, LocalDirCreationInfo, VirtualPath, VirtualPathBuf,
    };
    use chrono::Utc;

    use crate::db::{Database, DatabaseConfig};

    #[tokio::test]
    async fn test_update_db_vfs() {
        let db = Database::new(&DatabaseConfig::InMemory).await.unwrap();

        let mut a = DirTree::new_ok("a", Utc::now(), ());
        let f1 = FileInfo::new_ok("f1", 10, Utc::now(), ());
        let mut b = DirTree::new_ok("b", Utc::now(), ());
        let c = DirTree::new_ok("c", Utc::now(), ());
        let d = DirTree::new_ok("d", Utc::now(), ());

        b.insert_child(VfsNode::Dir(c));
        a.insert_child(VfsNode::File(f1));
        a.insert_child(VfsNode::Dir(b));

        let mut base_root = DirTree::new_ok("", Utc::now(), ());
        base_root.insert_child(VfsNode::Dir(a.clone()));
        let base_vfs = VfsNode::Dir(base_root);

        let f1_path = VirtualPathBuf::new("/a/f1").unwrap();
        let f2_path = VirtualPathBuf::new("/a/b/f2").unwrap();

        let creation_update = VfsUpdate::DirCreated(VfsDirCreation::new(VirtualPath::root(), a));

        let fs_info = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new("/tmp/test"));
        let fs_ref = FileSystemMeta::from(fs_info.clone());
        db.insert_new_filesystem(fs_ref.id(), &fs_info)
            .await
            .unwrap();

        let nodes = db.list_all_nodes().await.unwrap();

        // Only the root is present
        assert_eq!(nodes.len(), 1);

        db.update_vfs(fs_ref.id(), &creation_update.into())
            .await
            .unwrap();
        db.update_vfs_node_state(
            fs_ref.id(),
            VirtualPath::root(),
            &NodeState::Ok(().to_bytes()),
        )
        .await
        .unwrap();

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

        let vfs_root = db.load_nodes_rec(&root).await.unwrap();
        assert!(vfs_root.structural_eq(&base_vfs));

        let creation_update =
            VfsUpdate::DirCreated(VfsDirCreation::new(&VirtualPathBuf::new("/a").unwrap(), d));
        db.update_vfs(fs_ref.id(), &creation_update.into())
            .await
            .unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 6);

        let node = db
            .find_node(&root, &VirtualPathBuf::new("/a/d").unwrap())
            .await
            .unwrap();

        assert_eq!(node.kind, NodeKind::Dir.as_str());

        let creation_update =
            VfsUpdate::FileCreated(VfsFileUpdate::new(&f2_path, 42, Utc::now(), ()));
        db.update_vfs(fs_ref.id(), &creation_update.into())
            .await
            .unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 7);

        let node = db.find_node(&root, &f2_path).await.unwrap();

        assert_eq!(node.kind, NodeKind::File.as_str());
        assert_eq!(node.size, Some(42));

        let modification_update =
            VfsUpdate::FileModified(VfsFileUpdate::new(&f2_path, 54, Utc::now(), ()));
        db.update_vfs(fs_ref.id(), &modification_update.into())
            .await
            .unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 7);

        let node = db.find_node(&root, &f2_path).await.unwrap();

        assert_eq!(node.kind, NodeKind::File.as_str());
        assert_eq!(node.size, Some(54));

        let rm_update = VfsUpdate::<()>::FileRemoved(f1_path.clone());
        db.update_vfs(fs_ref.id(), &rm_update.into()).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 6);

        assert!(db.find_node(&root, &f1_path).await.is_err());

        let diff = VfsDiff::file_modified(f2_path.clone());
        let failed_diff = FailedUpdateApplication::new(
            diff.clone(),
            LocalDirError::io(
                f2_path.clone(),
                io::Error::new(ErrorKind::AlreadyExists, "Error"),
            ),
        );

        let failed_update = VfsUpdate::FailedApplication(failed_diff);
        db.update_vfs(fs_ref.id(), &failed_update).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 6);

        let node = db.find_node(&root, &f2_path).await.unwrap();

        let vfs_node = VfsNode::try_from(&node).unwrap();
        assert!(vfs_node.state().is_err());

        let conflict = VfsUpdate::Conflict(VfsConflict::new(diff, &[f2_path.clone()]));
        db.update_vfs(fs_ref.id(), &conflict).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 6);

        let node = db.find_node(&root, &f2_path).await.unwrap();

        let vfs_node = VfsNode::try_from(&node).unwrap();
        assert!(vfs_node.state().is_conflict());
    }

    #[tokio::test]
    async fn test_reset_vfs() {
        let db = Database::new(&DatabaseConfig::InMemory).await.unwrap();

        let mut a = DirTree::new_ok("a", Utc::now(), ());
        let f1 = FileInfo::new_ok("f1", 10, Utc::now(), ());
        let mut b = DirTree::new_ok("b", Utc::now(), ());
        let c = DirTree::new_ok("c", Utc::now(), ());

        b.insert_child(VfsNode::Dir(c));
        a.insert_child(VfsNode::File(f1));
        a.insert_child(VfsNode::Dir(b));

        let mut base_root = DirTree::new_ok("", Utc::now(), ());
        base_root.insert_child(VfsNode::Dir(a.clone()));

        let creation_update = VfsUpdate::DirCreated(VfsDirCreation::new(VirtualPath::root(), a));

        let fs_info = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new("/tmp/test"));
        let fs_ref = FileSystemMeta::from(fs_info.clone());

        db.insert_new_filesystem(fs_ref.id(), &fs_info)
            .await
            .unwrap();

        let nodes = db.list_all_nodes().await.unwrap();

        // Only the root is present
        assert_eq!(nodes.len(), 1);

        db.update_vfs(fs_ref.id(), &creation_update.into())
            .await
            .unwrap();

        let nodes = db.list_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 5);

        db.reset_vfs(fs_ref.id()).await.unwrap();

        let nodes = db.list_all_nodes().await.unwrap();

        // Only the root is present
        assert_eq!(nodes.len(), 1);
    }
}
