/// Metadata of a Directory node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirInfo<SyncInfo> {
    name: String,
    sync: SyncInfo,
}

impl<SyncInfo> DirInfo<SyncInfo> {
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
