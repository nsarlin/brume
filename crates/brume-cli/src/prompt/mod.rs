//! Various prompts that can be used for user interactions
mod autocomplete;

use brume_daemon_proto::{
    AnyFsCreationInfo, LocalDirCreationInfo, NextcloudFsCreationInfo, SynchroSide,
};
use inquire::{Password, Select, Text};

use autocomplete::LocalFilePathCompleter;

/// A prompt for filesystem creation information
pub fn prompt_filesystem(side: SynchroSide) -> Result<AnyFsCreationInfo, String> {
    let options = vec!["Local Folder", "Nextcloud"];

    // Start the selection at the most likely index for the synchro side
    let default_idx = match side {
        SynchroSide::Local => 0,  // Local folder
        SynchroSide::Remote => 1, // Nextcloud
    };

    let ans = Select::new(&format!("Select the \"{}\" filesystem type", side), options)
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
