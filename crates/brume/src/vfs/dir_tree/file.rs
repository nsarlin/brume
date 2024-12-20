use crate::concrete::ConcreteFsError;

use super::NodeState;

/// Metadata of a File node
#[derive(Debug, Clone)]
pub struct FileMeta<SyncInfo> {
    name: String,
    size: u64,
    state: NodeState<SyncInfo>,
}

impl<SyncInfo> FileMeta<SyncInfo> {
    pub fn new(name: &str, size: u64, sync: SyncInfo) -> Self {
        Self {
            name: name.to_string(),
            size,
            state: NodeState::Ok(sync),
        }
    }

    pub fn new_error(name: &str, size: u64, error: ConcreteFsError) -> Self {
        Self {
            name: name.to_string(),
            size,
            state: NodeState::Error(error),
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

    pub fn state(&self) -> &NodeState<SyncInfo> {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut NodeState<SyncInfo> {
        &mut self.state
    }
}
