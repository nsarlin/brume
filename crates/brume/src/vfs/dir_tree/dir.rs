use crate::concrete::ConcreteFsError;

use super::NodeState;

/// Metadata of a Directory node
#[derive(Debug, Clone)]
pub struct DirMeta<SyncInfo> {
    name: String,
    state: NodeState<SyncInfo>,
}

impl<SyncInfo> DirMeta<SyncInfo> {
    pub fn new(name: &str, sync: SyncInfo) -> Self {
        Self {
            name: name.to_string(),
            state: NodeState::Ok(sync),
        }
    }

    pub fn new_without_syncinfo(name: &str) -> Self {
        Self {
            name: name.to_string(),
            state: NodeState::NeedResync,
        }
    }

    pub fn new_error(name: &str, error: ConcreteFsError) -> Self {
        Self {
            name: name.to_string(),
            state: NodeState::Error(error),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn state(&self) -> &NodeState<SyncInfo> {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut NodeState<SyncInfo> {
        &mut self.state
    }

    /// Invalidate the sync info to make them trigger a ConcreteFS sync on next run
    pub fn force_resync(&mut self) {
        self.state = NodeState::NeedResync;
    }
}
