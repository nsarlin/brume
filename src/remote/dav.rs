//! Utilities to parse a dav response

use reqwest_dav::list_cmd::ListEntity;

use crate::{
    vfs::{DirTree, FileInfo, TreeNode, Vfs},
    NC_DAV_PATH_STR,
};

/// Build a vfs from a Dav response. This may fail if the response entities does not represent a
/// valid tree structure.
pub(crate) fn dav_parse_vfs(mut entities: Vec<ListEntity>, folder_name: &str) -> Result<Vfs, ()> {
    // By sorting the paths lexicographically, we make sure that children nodes a right after the
    // directory that contain them.
    entities.sort_by(|ent_a, ent_b| {
        let path_a = dav_entity_href(ent_a);
        let path_b = dav_entity_href(ent_b);

        path_a.cmp(path_b)
    });

    let mut entities_iter = entities.into_iter().map(|entity| DavEntity {
        entity,
        folder_name: folder_name.to_string(),
    });

    let root_entity = entities_iter.next().ok_or(())?;
    if !root_entity.name()?.is_empty() {
        return Err(());
    }

    let empty_root_node = root_entity.try_into()?;

    match empty_root_node {
        TreeNode::File(file) => Ok(TreeNode::File(file).into()),
        TreeNode::Dir(root) => {
            Ok(TreeNode::Dir(dav_build_tree_inner(root, &mut entities_iter)?).into())
        }
    }
}

/// Build the tree-like structure of a VFS from a flat list of paths. This function assumes that
/// this list have already been sorted in a way that a directory is directly followed by its
/// children.
fn dav_build_tree_inner<I: ExactSizeIterator<Item = DavEntity>>(
    root: DirTree,
    entities: &mut I,
) -> Result<DirTree, ()> {
    // This is used to store the currently worked on stack of directories
    let mut dirs: Vec<DirTree> = Vec::with_capacity(entities.len());
    let mut current_dir = root;

    for entity in entities {
        // if we arrive at the end of the children of a directory, we pop the stack and add the
        // directory as a child of popped value.
        while entity.parent()? != Some(current_dir.name()) {
            let parent_opt = dirs.pop();
            if let Some(mut parent) = parent_opt {
                if parent.insert_child(TreeNode::Dir(current_dir)) {
                    current_dir = parent;
                } else {
                    return Err(());
                }
            } else {
                return Err(());
            }
        }

        let node = entity.try_into()?;

        if let TreeNode::Dir(folder) = node {
            dirs.push(current_dir);
            current_dir = folder;
        } else {
            current_dir.insert_child(node);
        }
    }

    // If there are still directories in the stack, we need to recursively add them as children of
    // their parent.
    while let Some(mut parent) = dirs.pop() {
        if parent.insert_child(TreeNode::Dir(current_dir)) {
            current_dir = parent;
        } else {
            return Err(());
        }
    }
    Ok(current_dir)
}

/// A single Entity in the dav response, representing a File or a Directory
struct DavEntity {
    entity: ListEntity,
    folder_name: String,
}

impl TryFrom<DavEntity> for TreeNode {
    type Error = ();

    fn try_from(value: DavEntity) -> Result<Self, Self::Error> {
        let name = value.name()?;
        match value.entity {
            ListEntity::File(_) => Ok(TreeNode::File(FileInfo::new(name))),
            ListEntity::Folder(_) => Ok(TreeNode::Dir(DirTree::new(name))),
        }
    }
}

impl DavEntity {
    /// Return the name of the entity, without its path. Can fail if the path is not valid for the
    /// dav folder.
    fn name(&self) -> Result<&str, ()> {
        let path = self.relative_path(&self.folder_name)?;

        if path.ends_with('/') {
            Ok(path.split('/').nth_back(1).unwrap_or(""))
        } else {
            Ok(path.split('/').last().unwrap_or(""))
        }
    }

    /// Return the name of the direct parent of the entity. Can fail if the path is not valid for
    /// the dav folder.
    fn parent(&self) -> Result<Option<&str>, ()> {
        let path = self.relative_path(&self.folder_name)?;

        if path.ends_with('/') {
            Ok(path.split('/').nth_back(2))
        } else {
            Ok(path.split('/').nth_back(1))
        }
    }

    /// Return the relative url from the server root of the entity
    fn href(&self) -> &str {
        dav_entity_href(&self.entity)
    }

    /// Return the relative path of the entity, from `folder_name`. `folder_name` should be a full
    /// path from the dav root url.
    fn relative_path<'a>(&'a self, folder_name: &str) -> Result<&'a str, ()> {
        let absolute_path = self.href();
        if let Some(without_dav_path) = absolute_path.strip_prefix(NC_DAV_PATH_STR) {
            if let Some(without_folder) = without_dav_path.strip_prefix(folder_name) {
                return Ok(without_folder);
            }
        }
        Err(())
    }
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

    use crate::vfs::TreeNode;

    use super::dav_parse_vfs;

    #[test]
    fn test_parse_vfs() {
        use crate::test_utils::TestNode::{D, F};
        let reference = TreeNode::from(&D(
            "",
            vec![
                D("Doc", vec![F("f1.md"), F("f2.pdf")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
            ],
        ));

        let dav_folder = "[
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/\",
            last_modified: \"2024-09-24T23:06:38Z\",
            quota_used_bytes: Some(39044475),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fe2a304\"),
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/Doc/\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
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
            href: \"/remote.php/dav/files/admin/Doc/f2.pdf\",
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
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/a/b/\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/a/b/c/\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
        ),
    ),
]";

        let elements: Vec<ListEntity> = ron::from_str(dav_folder).unwrap();

        let res = dav_parse_vfs(elements, "admin").unwrap();

        assert!(res.root().structural_eq(&reference))
    }

    #[test]
    fn test_invalid_tree() {
        let no_root = "[
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/a\",
            last_modified: \"2024-09-24T23:06:38Z\",
            quota_used_bytes: Some(39044475),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fe2a304\"),
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/b\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
        ),
    ),
]
";

        let elements: Vec<ListEntity> = ron::from_str(no_root).unwrap();

        assert!(dav_parse_vfs(elements, "admin").is_err());

        let bad_hierarchy = "[
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/\",
            last_modified: \"2024-09-24T23:06:38Z\",
            quota_used_bytes: Some(39044475),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fe2a304\"),
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/b\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
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

        assert!(dav_parse_vfs(elements, "admin").is_err());

        let bad_root = "[
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/admin/\",
            last_modified: \"2024-09-24T23:06:38Z\",
            quota_used_bytes: Some(39044475),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fe2a304\"),
        ),
    ),
    Folder(
        ListFolder (
            href: \"/remote.php/dav/files/user/b\",
            last_modified: \"2024-09-24T23:06:37Z\",
            quota_used_bytes: Some(1108865),
            quota_available_bytes: Some(-3),
            tag: Some(\"66f345fd36676\"),
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

        assert!(dav_parse_vfs(elements, "admin").is_err());
    }
}
