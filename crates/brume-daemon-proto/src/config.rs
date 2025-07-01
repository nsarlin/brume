use std::{path::PathBuf, time::Duration};

use serde::Deserialize;

/// How errors should be handled by the daemon
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Deserialize)]
pub enum ErrorMode {
    #[default]
    Log,
    Exit,
}

/// The config used for the daemon database
#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseUserConfig {
    /// Store the database in memory, used only in tests
    InMemory,
    /// Store the database on disk using the default path from XDG spec.
    OnDisk,
    /// Store the database on disk at a user specified path
    #[serde(untagged)]
    OnDiskPath(PathBuf),
}

/// Configuration of the brume daemon
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DaemonUserConfig {
    /// Time between two synchronizations
    pub sync_interval: Option<Duration>,
    /// How internal errors are handled
    pub error_mode: Option<ErrorMode>,
    /// Name of the unix socket used to communicate with the daemon
    pub sock_name: Option<String>,
    /// Config of the sqlite database
    pub db: Option<DatabaseUserConfig>,
}

/// Global config for all the brume programs
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BrumeUserConfig {
    pub daemon: DaemonUserConfig,
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use crate::config::{BrumeUserConfig, DatabaseUserConfig};

    #[test]
    fn test_db_config() {
        let toml_str = r#"
        [daemon]
        db = "path/to/db"
    "#;
        let config: BrumeUserConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(
            config.daemon.db.unwrap(),
            DatabaseUserConfig::OnDiskPath(PathBuf::from("path/to/db"))
        );

        let toml_str = r#"
        [daemon]
        db = "on_disk"
    "#;
        let config: BrumeUserConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(config.daemon.db.unwrap(), DatabaseUserConfig::OnDisk);

        let toml_str = r#"
        [daemon]
        db = "in_memory"
    "#;
        let config: BrumeUserConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(config.daemon.db.unwrap(), DatabaseUserConfig::InMemory);
    }
}
