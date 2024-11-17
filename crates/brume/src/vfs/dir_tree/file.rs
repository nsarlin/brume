/// Metadata of a File node
#[derive(Debug, Clone)]
pub struct FileMeta<SyncInfo> {
    name: String,
    size: u64,
    sync: Option<SyncInfo>,
}

impl<SyncInfo> FileMeta<SyncInfo> {
    pub fn new(name: &str, size: u64, sync: SyncInfo) -> Self {
        Self {
            name: name.to_string(),
            size,
            sync: Some(sync),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub fn sync_info(&self) -> &Option<SyncInfo> {
        &self.sync
    }

    pub fn sync_info_mut(&mut self) -> &mut Option<SyncInfo> {
        &mut self.sync
    }
}
