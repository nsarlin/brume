/// Metadata of a File node
#[derive(Debug, Clone)]
pub struct FileInfo<SyncInfo> {
    name: String,
    sync: SyncInfo,
}

impl<SyncInfo> FileInfo<SyncInfo> {
    pub fn new(name: &str, sync: SyncInfo) -> Self {
        Self {
            name: name.to_string(),
            sync,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sync_info(&self) -> &SyncInfo {
        &self.sync
    }
}
