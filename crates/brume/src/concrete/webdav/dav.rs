//! Utilities to parse a dav response

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use reqwest_dav::list_cmd::ListEntity;
use thiserror::Error;
use urlencoding::decode;

use crate::{
    sorted_vec::{Sortable, SortedVec},
    vfs::{
        DirInfo, DirTree, FileInfo, NodeInfo, VfsNode, VirtualPath, VirtualPathBuf,
        VirtualPathError,
    },
};

use super::{WebDavError, WebDavSyncInfo};

/// Error encountered when parsing a tag in the WebDAV response
#[derive(Error, Debug)]
pub enum TagError {
    #[error("the tag is not present")]
    MissingTag,
    #[error("the tag is not a valid number: {0}")]
    InvalidTag(String),
}

impl Sortable for ListEntity {
    type Key = str;

    // By sorting the paths lexicographically, we make sure that children nodes are right after the
    // directory that contain them.
    fn key(&self) -> &Self::Key {
        dav_entity_href(self)
    }
}

/// Build a vfs from a Dav response. This may fail if the response entities does not represent a
/// valid tree structure.
pub(crate) fn dav_parse_vfs(
    entities: Vec<ListEntity>,
    dav_path: &VirtualPath,
) -> Result<VfsNode<WebDavSyncInfo>, WebDavError> {
    let entities = SortedVec::from_vec(entities);

    let mut entities_iter = entities.into_iter().map(|entity| DavEntity {
        entity,
        dav_path: dav_path.to_owned(),
    });

    let root_entity = entities_iter.next().ok_or(WebDavError::BadStructure)?;
    if !root_entity.name()?.is_empty() {
        return Err(WebDavError::BadStructure);
    }

    let empty_root_node = root_entity.try_into()?;

    let root = match empty_root_node {
        VfsNode::File(file) => VfsNode::File(file),
        VfsNode::Dir(root) => VfsNode::Dir(dav_build_tree(root, &mut entities_iter)?),
    };

    Ok(root)
}

pub(crate) fn dav_parse_entity_meta(
    entity: ListEntity,
) -> Result<NodeInfo<WebDavSyncInfo>, WebDavError> {
    let entity = DavEntity {
        entity,
        dav_path: VirtualPathBuf::root(),
    };

    let sync_info = entity.tag().map(WebDavSyncInfo::new)?;

    match &entity.entity {
        ListEntity::File(list_file) => Ok(NodeInfo::File(FileInfo::new(
            &entity.name()?,
            list_file.content_length as u64,
            list_file.last_modified,
            sync_info,
        ))),
        ListEntity::Folder(list_folder) => Ok(NodeInfo::Dir(DirInfo::new(
            &entity.name()?,
            list_folder.last_modified,
            sync_info,
        ))),
    }
}

/// Build the tree-like structure of a VFS from a flat list of paths. This function assumes that
/// this list have already been sorted in a way that a directory is directly followed by its
/// children.
fn dav_build_tree<I: ExactSizeIterator<Item = DavEntity>>(
    root: DirTree<WebDavSyncInfo>,
    entities: &mut I,
) -> Result<DirTree<WebDavSyncInfo>, WebDavError> {
    // This is used to store the currently worked on stack of directories
    let mut dirs: Vec<DirTree<WebDavSyncInfo>> = Vec::with_capacity(entities.len());
    let mut current_dir = root;

    for entity in entities {
        // if we arrive at the end of the children of a directory, we pop the stack and add the
        // directory as a child of popped value.
        while entity.parent()? != Some(current_dir.name()) {
            let parent_opt = dirs.pop();
            if let Some(mut parent) = parent_opt {
                parent.insert_child(VfsNode::Dir(current_dir));
                current_dir = parent;
            } else {
                return Err(WebDavError::BadStructure);
            }
        }

        let node = entity.try_into()?;

        if let VfsNode::Dir(folder) = node {
            dirs.push(current_dir);
            current_dir = folder;
        } else {
            current_dir.insert_child(node);
        }
    }

    // If there are still directories in the stack, we need to recursively add them as children of
    // their parent.
    while let Some(mut parent) = dirs.pop() {
        parent.insert_child(VfsNode::Dir(current_dir));
        current_dir = parent;
    }
    Ok(current_dir)
}

/// A single Entity in the dav response, representing a File or a Directory
struct DavEntity {
    entity: ListEntity,
    dav_path: VirtualPathBuf,
}

impl DavEntity {
    /// Return the name of the entity, without its path. Can fail if the path is not valid for the
    /// dav folder.
    fn name(&self) -> Result<Cow<'_, str>, WebDavError> {
        self.path()
            .map(|path| path.name())
            .map_err(|e| e.into())
            .and_then(|name| decode(name).map_err(|e| e.into()))
    }

    /// Return the name of the direct parent of the entity. Can fail if the path is not valid for
    /// the dav folder.
    fn parent(&self) -> Result<Option<&str>, VirtualPathError> {
        Ok(self.path()?.parent().map(|parent| parent.name()))
    }

    /// Return the relative url from the server root of the entity
    fn href(&self) -> &str {
        dav_entity_href(&self.entity)
    }

    /// Return the relative path of the entity from the "dav root", removing the dav url and the
    /// name of the folder.
    fn path(&self) -> Result<&VirtualPath, VirtualPathError> {
        let dav_path: &VirtualPath = self.href().try_into()?;

        dav_path.chroot(&self.dav_path)
    }

    fn tag(&self) -> Result<u128, TagError> {
        let tag_opt = match &self.entity {
            ListEntity::File(file) => file.tag.as_ref(),
            ListEntity::Folder(folder) => folder.tag.as_ref(),
        };

        tag_opt
            .ok_or(TagError::MissingTag)
            .and_then(|tag| parse_dav_tag(tag).map_err(|_| TagError::InvalidTag(tag.to_string())))
    }

    fn last_modified(&self) -> DateTime<Utc> {
        match &self.entity {
            ListEntity::File(file) => file.last_modified,
            ListEntity::Folder(folder) => folder.last_modified,
        }
    }
}

impl TryFrom<DavEntity> for VfsNode<WebDavSyncInfo> {
    type Error = WebDavError;

    fn try_from(value: DavEntity) -> Result<Self, Self::Error> {
        let name = value.name()?;
        let tag = value.tag()?;

        let sync = WebDavSyncInfo::new(tag);

        match &value.entity {
            ListEntity::File(file) => Ok(VfsNode::File(FileInfo::new(
                &name,
                file.content_length as u64,
                value.last_modified(),
                sync,
            ))),
            ListEntity::Folder(_) => Ok(VfsNode::Dir(DirTree::new(
                &name,
                value.last_modified(),
                sync,
            ))),
        }
    }
}

fn parse_dav_tag(tag: &str) -> Result<u128, ()> {
    let trimmed = tag.trim_matches('"');
    u128::from_str_radix(trimmed, 16).map_err(|_| ())
}

/// Return the relative url from the server root of the entity
fn dav_entity_href(entity: &ListEntity) -> &str {
    match entity {
        ListEntity::File(file) => &file.href,
        ListEntity::Folder(folder) => &folder.href,
    }
}

#[cfg(test)]
mod test {
    use reqwest_dav::list_cmd::ListEntity;

    use crate::vfs::VirtualPath;

    use super::dav_parse_vfs;

    #[test]
    fn test_parse_vfs() {
        use crate::test_utils::TestNode::{D, F};
        let reference = D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("a spaced file.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
            ],
        )
        .into_node();

        let dav_path: &VirtualPath = "/remote.php/dav/files/admin/".try_into().unwrap();

        let dav_folder = "[
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/\",
            last_modified: \"2024-09-24T23:06:38Z\",
            quota_used_bytes: Some(39044475),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fe2a304\"),
            address_book: false,
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/Doc/\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
            address_book: false,
        ),
    ),
    File(
        ListFile (
            href: \"/remote.php/dav/files/admin/Doc/f1.md\",
            last_modified: \"2024-09-24T23:06:37Z\",
            content_length: 1095,
            content_type: \"text/markdown\",
            tag: Some(\"ede1fda2e7e2acc2ca5311836516efba\"),
        ),
    ),
    File(
        ListFile (
            href: \"/remote.php/dav/files/admin/Doc/a%20spaced%20file.pdf\",
            last_modified: \"2024-09-24T23:06:37Z\",
            content_length: 1083339,
            content_type: \"application/pdf\",
            tag: Some(\"53767089464852abb96fec2ff31ab2de\"),
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/a/\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
            address_book: false,
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/a/b/\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
            address_book: false,
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/a/b/c/\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
            address_book: false,
        ),
    ),
]";

        let elements: Vec<ListEntity> = ron::from_str(dav_folder).unwrap();

        let res = dav_parse_vfs(elements, dav_path).unwrap().as_ok();

        assert!(res.structural_eq(&reference))
    }

    #[test]
    fn test_invalid_tree() {
        let dav_path: &VirtualPath = "/remote.php/dav/files/admin/".try_into().unwrap();

        let no_root = "[
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/a\",
            last_modified: \"2024-09-24T23:06:38Z\",
            quota_used_bytes: Some(39044475),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fe2a304\"),
            address_book: false,
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/b\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
            address_book: false,
        ),
    ),
]
";

        let elements: Vec<ListEntity> = ron::from_str(no_root).unwrap();

        assert!(dav_parse_vfs(elements, dav_path).is_err());

        let bad_hierarchy = "[
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/\",
            last_modified: \"2024-09-24T23:06:38Z\",
            quota_used_bytes: Some(39044475),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fe2a304\"),
            address_book: false,
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/b\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
            address_book: false,
        ),
    ),
    File(
        ListFile (
            href: \"/remote.php/dav/files/admin/Doc/f1.md\",
            last_modified: \"2024-09-24T23:06:37Z\",
            content_length: 1095,
            content_type: \"text/markdown\",
            tag: Some(\"ede1fda2e7e2acc2ca5311836516efba\"),
        ),
    ),
]
";

        let elements: Vec<ListEntity> = ron::from_str(bad_hierarchy).unwrap();

        assert!(dav_parse_vfs(elements, dav_path).is_err());

        let bad_root = "[
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/\",
            last_modified: \"2024-09-24T23:06:38Z\",
            quota_used_bytes: Some(39044475),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fe2a304\"),
            address_book: false,
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/user/b\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
            address_book: false,
        ),
    ),
    File(
        ListFile (
            href: \"/remote.php/dav/files/user/b/f1.md\",
            last_modified: \"2024-09-24T23:06:37Z\",
            content_length: 1095,
            content_type: \"text/markdown\",
            tag: Some(\"ede1fda2e7e2acc2ca5311836516efba\"),
        ),
    ),
]
";

        let elements: Vec<ListEntity> = ron::from_str(bad_root).unwrap();

        assert!(dav_parse_vfs(elements, dav_path).is_err());
    }
}
