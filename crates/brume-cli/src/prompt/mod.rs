//! Various prompts that can be used for user interactions
mod autocomplete;

use std::{
    collections::HashMap,
    fmt::Display,
    fs::{self, create_dir_all},
    path::PathBuf,
};

use brume::{
    sorted_vec::SortedVec,
    update::UpdateKind,
    vfs::{NodeState, StatefulVfs, StatefulVfsNode},
};
use brume_daemon_proto::{
    AnyFsCreationInfo, LocalDirCreationInfo, NextcloudFsCreationInfo, SynchroId, SynchroMeta,
    SynchroSide, VirtualPath, VirtualPathBuf,
};
use chrono::{DateTime, Utc};
use comfy_table::Table;
use inquire::{Confirm, Password, Select, Text};

use crate::get_synchro;
use autocomplete::LocalFilePathCompleter;

/// A prompt for filesystem creation information
pub fn prompt_filesystem(side: SynchroSide) -> anyhow::Result<AnyFsCreationInfo> {
    let options = vec!["Local Folder", "Nextcloud"];

    // Start the selection at the most likely index for the synchro side
    let default_idx = match side {
        SynchroSide::Local => 0,  // Local folder
        SynchroSide::Remote => 1, // Nextcloud
    };

    let ans = Select::new(&format!("Select the \"{side}\" filesystem type:"), options)
        .with_starting_cursor(default_idx)
        .prompt()?;

    match ans {
        "Local Folder" => prompt_local_folder().map(AnyFsCreationInfo::LocalDir),
        "Nextcloud" => prompt_nextcloud().map(AnyFsCreationInfo::Nextcloud),
        _ => Err(anyhow::anyhow!("Invalid option")),
    }
}

/// A prompt for a local folder to synchronize
pub fn prompt_local_folder() -> anyhow::Result<LocalDirCreationInfo> {
    let path = Text::new("Path to the folder to sync:")
        .with_autocomplete(LocalFilePathCompleter::default())
        .prompt()?;

    let path = check_or_prompt_folder_creation(&path)?;
    Ok(LocalDirCreationInfo::new(path))
}

/// Checks if the user provided folder exists, or ask the user to create it.
///
/// Returns the path in a canonical form
/// Returns an error if the path does not exist and the user does not want to create it, or if the
/// path exists but is not a directory.
pub fn check_or_prompt_folder_creation(path: &str) -> anyhow::Result<PathBuf> {
    let canon = fs::canonicalize(path).or_else(|_| {
        if !Confirm::new(&format!("{path} does not exist locally, create it?"))
            .with_default(false)
            .prompt()?
        {
            Err(anyhow::anyhow!("{path} is not a valid path"))
        } else {
            create_dir_all(path)?;
            Ok(fs::canonicalize(path)?)
        }
    })?;

    if canon.is_dir() {
        Ok(canon)
    } else {
        Err(anyhow::anyhow!("{path} exists but is not a directory"))
    }
}

/// A prompt for nextcloud server connection information
pub fn prompt_nextcloud() -> anyhow::Result<NextcloudFsCreationInfo> {
    let url = Text::new("Url of the Nextcloud server:").prompt()?;

    let username = Text::new("Nextcloud username:").prompt()?;

    let password = Password::new("Nextcloud password:")
        .without_confirmation()
        .prompt()?;

    Ok(NextcloudFsCreationInfo::new(&url, &username, &password))
}

pub fn filter_synchro_list(
    list: HashMap<SynchroId, SynchroMeta>,
    mut filter: impl FnMut(&SynchroMeta) -> bool,
) -> HashMap<SynchroId, SynchroMeta> {
    list.into_iter().filter(|(_, meta)| filter(meta)).collect()
}

/// Prompt for a synchro from the list
pub fn prompt_synchro(
    synchro_list: &HashMap<SynchroId, SynchroMeta>,
) -> anyhow::Result<(SynchroId, SynchroMeta)> {
    let options = synchro_list.values().map(|sync| sync.name()).collect();

    let ans = Select::new("Which synchro?", options).prompt()?;

    Ok(get_synchro(synchro_list, ans).expect("Chosen synchro must be in the list"))
}

/// Prompt for a synchro side, remote or local
pub fn prompt_side() -> anyhow::Result<SynchroSide> {
    let options = vec!["Local", "Remote"];

    let ans = Select::new("Use content from which side?", options).prompt()?;

    match ans {
        "Local" => Ok(SynchroSide::Local),
        "Remote" => Ok(SynchroSide::Remote),
        _ => Err(anyhow::anyhow!("Invalid option")),
    }
}

/// Information needed to display a conflict for confirmation in the resolution prompt
struct PromptedConflict<'a> {
    path: &'a VirtualPath,
    update_kind: UpdateKind,
    last_modified: DateTime<Utc>,
}

impl<'a> PromptedConflict<'a> {
    fn new(path: &'a VirtualPath, node: &StatefulVfsNode<()>) -> Option<Self> {
        let last_modified = node.last_modified();

        let NodeState::Conflict(conflict) = node.state() else {
            return None;
        };

        Some(Self {
            path,
            update_kind: conflict.update().kind(),
            last_modified,
        })
    }

    fn find(
        path: &'a VirtualPath,
        local_vfs: &'a StatefulVfs<()>,
        remote_vfs: &'a StatefulVfs<()>,
    ) -> Option<(Vec<Self>, Vec<Self>)> {
        if let Some(local) = local_vfs.find_conflict(path) {
            let local_conflict = PromptedConflict::new(path, local_vfs.find_node(path)?)?;

            let remote_conflicts = local
                .otherside_conflict_paths()
                .iter()
                .flat_map(|path| {
                    let node = remote_vfs.find_node(path)?;
                    PromptedConflict::new(path, node)
                })
                .collect();

            Some((vec![local_conflict], remote_conflicts))
        } else {
            let remote = remote_vfs.find_conflict(path)?;
            let remote_conflict = PromptedConflict::new(path, remote_vfs.find_node(path)?)?;

            let local_conflicts = remote
                .otherside_conflict_paths()
                .iter()
                .flat_map(|path| {
                    let node = local_vfs.find_node(path)?;
                    PromptedConflict::new(path, node)
                })
                .collect();

            Some((local_conflicts, vec![remote_conflict]))
        }
    }
}

impl Display for PromptedConflict<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\n{}\nLast modified: {}",
            self.path, self.update_kind, self.last_modified
        )
    }
}

/// Prompt for a path with a conflict inside a synchro
pub fn prompt_conflict_path(
    local_vfs: &StatefulVfs<()>,
    remote_vfs: &StatefulVfs<()>,
) -> anyhow::Result<VirtualPathBuf> {
    let mut conflicts = local_vfs.get_conflicts();
    conflicts.extend(remote_vfs.get_conflicts());

    let conflicts = SortedVec::from_vec(conflicts);

    let selected = Select::new("Conflict path:", conflicts.into()).prompt()?;

    let (local_conflicts, remote_conflicts) =
        PromptedConflict::find(&selected, local_vfs, remote_vfs)
            .ok_or(anyhow::anyhow!("Invalid conflict path"))?;

    let max_len = local_conflicts.len().max(remote_conflicts.len());

    let mut table = Table::new();
    table.set_header(vec!["Local", "Remote"]);

    let local_column = local_conflicts
        .into_iter()
        .map(|conflict| format!("{conflict}"))
        .chain(std::iter::repeat(String::new()))
        .take(max_len);
    let remote_column = remote_conflicts
        .into_iter()
        .map(|conflict| format!("{conflict}"))
        .chain(std::iter::repeat(String::new()))
        .take(max_len);

    for (local_cell, remote_cell) in local_column.zip(remote_column) {
        table.add_row([local_cell, remote_cell]);
    }

    println!("{table}");

    Ok(selected)
}

#[cfg(test)]
mod test {
    use super::*;
    use brume::{
        filesystem::FileSystem,
        synchro::{FullSyncStatus, Synchro, Synchronized},
        test_utils::{
            TestFsBackend,
            TestNode::{D, FF},
        },
    };

    #[tokio::test]
    async fn test_find_conflict() {
        // Synchronize both fs
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(TestFsBackend::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![D(
                "Doc",
                vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")],
            )],
        );

        let remote_fs = FileSystem::new(TestFsBackend::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        synchro.full_sync().await.unwrap().status();

        // create a conflict: modify the remote and remove the local dir
        synchro
            .local_mut()
            .set_backend(TestFsBackend::from(local_base));

        let remote_mod = D(
            "",
            vec![D(
                "Doc",
                vec![
                    FF("f1.md", b"hallo"),
                    FF("f2.pdf", b"cruel"),
                    FF("f3.doc", b"world"),
                ],
            )],
        );
        synchro
            .remote_mut()
            .set_backend(TestFsBackend::from(remote_mod));
        synchro.full_sync().await.unwrap();
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        let path = VirtualPathBuf::new("/Doc").unwrap();
        let local_vfs = synchro.local_vfs();
        let remote_vfs = synchro.remote_vfs();
        let prompted = PromptedConflict::find(&path, &local_vfs, &remote_vfs).unwrap();

        let local_ref = vec![(VirtualPathBuf::new("/Doc").unwrap(), UpdateKind::DirRemoved)];

        let remote_ref = vec![
            (
                VirtualPathBuf::new("/Doc").unwrap(),
                UpdateKind::DirModified,
            ),
            (
                VirtualPathBuf::new("/Doc/f1.md").unwrap(),
                UpdateKind::FileModified,
            ),
            (
                VirtualPathBuf::new("/Doc/f2.pdf").unwrap(),
                UpdateKind::FileModified,
            ),
            (
                VirtualPathBuf::new("/Doc/f3.doc").unwrap(),
                UpdateKind::FileCreated,
            ),
        ];

        assert_eq!(
            local_ref,
            prompted
                .0
                .iter()
                .map(|prompt| (prompt.path.to_owned(), prompt.update_kind))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            remote_ref,
            prompted
                .1
                .iter()
                .map(|prompt| (prompt.path.to_owned(), prompt.update_kind))
                .collect::<Vec<_>>()
        )
    }
}
