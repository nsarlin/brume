//! Definitions of the base directories used to store data and config according to the
//! XDG specifications: <https://specifications.freedesktop.org/basedir-spec/latest/>

use std::{env, path::PathBuf};

use std::fs::create_dir_all;

const HOME_VAR: &str = "HOME";

pub const DATA_DIR: XdgDir = XdgDir {
    base_dir_var: "XDG_DATA_HOME",
    base_dir_default: ".local/share",
    additional_dirs_var: "XDG_DATA_DIRS",
    additional_dirs_default: "/usr/local/share/:/usr/share/",
};

const APP_NAME: &str = "brume";

pub struct XdgDir {
    base_dir_var: &'static str,
    base_dir_default: &'static str,
    additional_dirs_var: &'static str,
    additional_dirs_default: &'static str,
}

impl XdgDir {
    /// List of dirs that can be used as a base to store data according to XDG
    fn searched_dirs(&self) -> Vec<PathBuf> {
        // First add XDG_DATA_HOME to the search list
        let data_home = env::var(self.base_dir_var)
            .ok()
            .map(PathBuf::from)
            .or_else(|| {
                let home = env::var(HOME_VAR).ok().map(PathBuf::from)?;
                Some(home.join(self.base_dir_default))
            });

        let mut searched_dirs = data_home.map(|dir| vec![dir]).unwrap_or_default();

        // Then add the dirs from XDG_DATA_DIRS
        let data_dirs_str = env::var(self.additional_dirs_var)
            .ok()
            .unwrap_or(self.additional_dirs_default.to_string());

        searched_dirs.extend(data_dirs_str.split(':').map(PathBuf::from));

        searched_dirs
    }

    /// Resolve the data dir according to XDG specifications:
    /// - $XDG_DATA_HOME/$APP_NAME if it exists
    /// - any dir in $XDG_DATA_DIRS with a subfolder named $APP_NAME.
    pub fn resolve_dir(&self) -> Option<PathBuf> {
        let searched_dirs = self.searched_dirs();

        // Search for the app folder in any data dir
        for dir in searched_dirs.iter() {
            let app_path = dir.join(APP_NAME);
            if app_path.exists() {
                return Some(app_path);
            }
        }

        None
    }

    /// Resolve the data dir according to XDG specifications:
    /// - $XDG_DATA_HOME/$APP_NAME if it exists
    /// - any dir in $XDG_DATA_DIRS with a subfolder named $APP_NAME.
    ///
    /// If none is found, it will try to create it in any of these dir or return None.
    pub fn resolve_or_create_dir(&self) -> Option<PathBuf> {
        if let Some(data_dir) = self.resolve_dir() {
            return Some(data_dir);
        }

        let searched_dirs = self.searched_dirs();

        // Try to create in the first possible dir
        for dir in searched_dirs {
            let app_path = dir.join(APP_NAME);
            if create_dir_all(&app_path).is_ok() {
                return Some(app_path);
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use std::{
        env,
        fs::create_dir,
        sync::{LazyLock, Mutex},
    };

    use super::*;

    // Since these test modify the same env variables, we need to make sure they run sequentially
    static SEQUENTIAL_TEST: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    // Test the normal situation with XDG_DATA_HOME
    #[test]
    fn test_nominal() {
        let _lock = SEQUENTIAL_TEST.lock().unwrap();

        let home_data_dir = tempfile::tempdir().unwrap();
        let data_dir_1 = tempfile::tempdir().unwrap();
        let data_dir_2 = tempfile::tempdir().unwrap();

        unsafe { env::set_var(DATA_DIR.base_dir_var, home_data_dir.path()) }

        let data_dirs = format!(
            "{}:{}",
            data_dir_1.path().display(),
            data_dir_2.path().display()
        );
        unsafe { env::set_var(DATA_DIR.additional_dirs_var, data_dirs) }

        let app_data_dir = home_data_dir.into_path().join(APP_NAME);
        create_dir(&app_data_dir).unwrap();

        let resolved = DATA_DIR.resolve_dir().unwrap();

        assert_eq!(resolved, app_data_dir);
    }

    // Test the case where XDG_DATA_HOME is not set, so we generate it from the home path
    #[test]
    fn test_home() {
        let _lock = SEQUENTIAL_TEST.lock().unwrap();

        let home_dir = tempfile::tempdir().unwrap();
        let data_dir_1 = tempfile::tempdir().unwrap();
        let data_dir_2 = tempfile::tempdir().unwrap();

        unsafe { env::remove_var(DATA_DIR.base_dir_var) }
        unsafe { env::set_var(HOME_VAR, home_dir.path()) }

        let data_dirs = format!(
            "{}:{}",
            data_dir_1.path().display(),
            data_dir_2.path().display()
        );
        unsafe { env::set_var(DATA_DIR.additional_dirs_var, data_dirs) }

        let app_data_dir = home_dir
            .into_path()
            .join(DATA_DIR.base_dir_default)
            .join(APP_NAME);
        create_dir_all(&app_data_dir).unwrap();

        let resolved = DATA_DIR.resolve_dir().unwrap();

        assert_eq!(resolved, app_data_dir);
    }

    // Test the case where the app dir is in a path inside XDG_DATA_DIRS
    #[test]
    fn test_data_dirs() {
        let _lock = SEQUENTIAL_TEST.lock().unwrap();

        let home_data_dir = tempfile::tempdir().unwrap();
        let data_dir_1 = tempfile::tempdir().unwrap();
        let data_dir_2 = tempfile::tempdir().unwrap();

        unsafe { env::set_var(DATA_DIR.base_dir_var, home_data_dir.path()) }

        let data_dirs = format!(
            "{}:{}",
            data_dir_1.path().display(),
            data_dir_2.path().display()
        );
        unsafe { env::set_var(DATA_DIR.additional_dirs_var, data_dirs) }

        let app_data_dir = data_dir_1.into_path().join(APP_NAME);
        create_dir_all(&app_data_dir).unwrap();

        let resolved = DATA_DIR.resolve_dir().unwrap();

        assert_eq!(resolved, app_data_dir);
    }

    // Test the priority, favoring XDG_DATA_HOME if possible
    #[test]
    fn test_priority() {
        let _lock = SEQUENTIAL_TEST.lock().unwrap();

        let home_data_dir = tempfile::tempdir().unwrap();
        let data_dir_1 = tempfile::tempdir().unwrap();
        let data_dir_2 = tempfile::tempdir().unwrap();

        unsafe { env::set_var(DATA_DIR.base_dir_var, home_data_dir.path()) }

        let data_dirs = format!(
            "{}:{}",
            data_dir_1.path().display(),
            data_dir_2.path().display()
        );
        unsafe { env::set_var(DATA_DIR.additional_dirs_var, data_dirs) }

        let app_data_dir_home = home_data_dir.into_path().join(APP_NAME);
        create_dir_all(&app_data_dir_home).unwrap();
        let app_data_dir_1 = data_dir_1.into_path().join(APP_NAME);
        create_dir_all(&app_data_dir_1).unwrap();
        let app_data_dir_2 = data_dir_2.into_path().join(APP_NAME);
        create_dir_all(&app_data_dir_2).unwrap();

        let resolved = DATA_DIR.resolve_dir().unwrap();

        assert_eq!(resolved, app_data_dir_home);
    }

    // Test the case where the app dir has not been created yet
    #[test]
    fn test_not_found() {
        let _lock = SEQUENTIAL_TEST.lock().unwrap();

        let home_data_dir = tempfile::tempdir().unwrap();
        let data_dir_1 = tempfile::tempdir().unwrap();
        let data_dir_2 = tempfile::tempdir().unwrap();

        unsafe { env::set_var(DATA_DIR.base_dir_var, home_data_dir.path()) }

        let data_dirs = format!(
            "{}:{}",
            data_dir_1.path().display(),
            data_dir_2.path().display()
        );
        unsafe { env::set_var(DATA_DIR.additional_dirs_var, data_dirs) }

        assert!(DATA_DIR.resolve_dir().is_none());
    }
}
