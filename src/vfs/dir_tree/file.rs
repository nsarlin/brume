/// Metadata of a File node
#[derive(Debug, Clone)]
pub struct FileMeta<SyncInfo> {
    name: String,
    sync: Option<SyncInfo>,
}

impl<SyncInfo> FileMeta<SyncInfo> {
    pub fn new(name: &str, sync: SyncInfo) -> Self {
        Self {
            name: name.to_string(),
            sync: Some(sync),
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
}
