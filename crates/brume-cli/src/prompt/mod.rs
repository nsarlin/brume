//! Various prompts that can be used for user interactions
mod autocomplete;

use std::{collections::HashMap, fmt::Display};

use brume::{
    sorted_vec::SortedVec,
    update::{UpdateKind, VfsConflict},
    vfs::StatefulVfs,
};
use brume_daemon_proto::{
    AnyFsCreationInfo, LocalDirCreationInfo, NextcloudFsCreationInfo, SynchroId, SynchroMeta,
    SynchroSide, VirtualPath, VirtualPathBuf,
};
use chrono::{DateTime, Utc};
use comfy_table::Table;
use inquire::{Password, Select, Text};

use crate::get_synchro;
use autocomplete::LocalFilePathCompleter;

/// A prompt for filesystem creation information
pub fn prompt_filesystem(side: SynchroSide) -> Result<AnyFsCreationInfo, String> {
    let options = vec!["Local Folder", "Nextcloud"];

    // Start the selection at the most likely index for the synchro side
    let default_idx = match side {
        SynchroSide::Local => 0,  // Local folder
        SynchroSide::Remote => 1, // Nextcloud
    };

    let ans = Select::new(
        &format!("Select the \"{}\" filesystem type:", side),
        options,
    )
    .with_starting_cursor(default_idx)
    .prompt()
    .map_err(|e| e.to_string())?;

    match ans {
        "Local Folder" => prompt_local_folder().map(AnyFsCreationInfo::LocalDir),
        "Nextcloud" => prompt_nextcloud().map(AnyFsCreationInfo::Nextcloud),
        _ => Err(String::from("Invalid option")),
    }
}

/// A prompt for a local folder to synchronize
pub fn prompt_local_folder() -> Result<LocalDirCreationInfo, String> {
    let path = Text::new("Path to the folder to sync:")
        .with_autocomplete(LocalFilePathCompleter::default())
        .prompt()
        .map_err(|e| e.to_string())?;

    Ok(LocalDirCreationInfo::new(path))
}

/// A prompt for nextcloud server connection information
pub fn prompt_nextcloud() -> Result<NextcloudFsCreationInfo, String> {
    let url = Text::new("Url of the Nextcloud server:")
        .prompt()
        .map_err(|e| e.to_string())?;

    let username = Text::new("Nextcloud username:")
        .prompt()
        .map_err(|e| e.to_string())?;

    let password = Password::new("Nextcloud password:")
        .without_confirmation()
        .prompt()
        .map_err(|e| e.to_string())?;

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
) -> Result<(SynchroId, SynchroMeta), String> {
    let options = synchro_list.values().map(|sync| sync.name()).collect();

    let ans = Select::new("Which synchro?", options)
        .prompt()
        .map_err(|e| e.to_string())?;

    Ok(get_synchro(synchro_list, ans).expect("Chosen synchro must be in the list"))
}

/// Prompt for a synchro side, remote or local
pub fn prompt_side() -> Result<SynchroSide, String> {
    let options = vec!["Local", "Remote"];

    let ans = Select::new("Use content from which side?", options)
        .prompt()
        .map_err(|e| e.to_string())?;

    match ans {
        "Local" => Ok(SynchroSide::Local),
        "Remote" => Ok(SynchroSide::Remote),
        _ => Err(String::from("Invalid option")),
    }
}

struct PromptedConlict<'a> {
    path: &'a VirtualPath,
    update_kind: UpdateKind,
    last_modified: DateTime<Utc>,
}

impl<'a> PromptedConlict<'a> {
    fn new(conflict: &VfsConflict) -> Self {
        todo!()
    }

    fn find(
        path: &VirtualPath,
        local_vfs: &StatefulVfs<()>,
        remote_vfs: &StatefulVfs<()>,
    ) -> Option<(Vec<Self>, Vec<Self>)> {
        if let Some(local) = local_vfs.find_conflict(&path) {
            let remote_conflicts = local
                .otherside_conflict_paths()
                .iter()
                .flat_map(|path| remote_vfs.find_conflict(path).cloned())
                .collect();

            Some((vec![local.clone()], remote_conflicts))
        } else {
            let remote = remote_vfs.find_conflict(&path)?;
            let local_conflicts = remote
                .otherside_conflict_paths()
                .iter()
                .flat_map(|path| local_vfs.find_conflict(path).cloned())
                .collect();

            Some((local_conflicts, vec![remote.clone()]))
        }
    }
}

impl<'a> Display for PromptedConlict<'a> {
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
) -> Result<VirtualPathBuf, String> {
    let mut conflicts = local_vfs.get_conflicts();
    conflicts.extend(remote_vfs.get_conflicts());

    let conflicts = SortedVec::from_vec(conflicts);

    let selected = Select::new("Conflict path:", conflicts.into())
        .prompt()
        .map_err(|e| e.to_string())?;

    let (local_conflicts, remote_conflicts) =
        PromptedConlict::find(&selected, local_vfs, remote_vfs);

    let max_len = local_conflicts.len().max(remote_conflicts.len());

    let mut table = Table::new();
    table.set_header(vec!["Local", "Remote"]);

    let local_column = local_conflicts
        .into_iter()
        .map(|conflict| format!("{}", conflict))
        .chain(std::iter::repeat(String::new()))
        .take(max_len);
    let remote_column = remote_conflicts
        .into_iter()
        .map(|conflict| format!("{}", conflict))
        .chain(std::iter::repeat(String::new()))
        .take(max_len);

    for (local_cell, remote_cell) in local_column.zip(remote_column) {
        table.add_row([local_cell, remote_cell]);
    }

    println!("{table}");

    Ok(selected)
}
