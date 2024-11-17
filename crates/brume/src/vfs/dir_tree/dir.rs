/// Metadata of a Directory node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirMeta<SyncInfo> {
    name: String,
    sync: Option<SyncInfo>,
}

impl<SyncInfo> DirMeta<SyncInfo> {
    pub fn new(name: &str, sync: SyncInfo) -> Self {
        Self {
            name: name.to_string(),
            sync: Some(sync),
        }
    }

    pub fn new_without_syncinfo(name: &str) -> Self {
        Self {
            name: name.to_string(),
            sync: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sync_info(&self) -> &Option<SyncInfo> {
        &self.sync
    }

    pub fn sync_info_mut(&mut self) -> &mut Option<SyncInfo> {
        &mut self.sync
    }

    /// Invalidate the sync info to make them trigger a ConcreteFS sync on next run
    pub fn invalidate_sync_info(&mut self) {
        self.sync = None;
    }
}
