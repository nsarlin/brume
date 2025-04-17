use crate::{
    concrete::{InvalidByteSyncInfo, ToBytes, TryFromBytes},
    update::FailedUpdateApplication,
};

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

    pub fn new_force_resync(name: &str) -> Self {
        Self {
            name: name.to_string(),
            state: NodeState::NeedResync,
        }
    }

    pub fn new_error(name: &str, error: FailedUpdateApplication) -> Self {
        Self {
            name: name.to_string(),
            state: NodeState::Error(error),
        }
    }

    pub fn new_with_state(name: &str, state: NodeState<SyncInfo>) -> Self {
        Self {
            name: name.to_string(),
            state,
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

    /// Invalidate the sync info to make them trigger a FSBackend sync on next run
    pub fn force_resync(&mut self) {
        self.state = NodeState::NeedResync;
    }
}

impl<SyncInfo> From<&DirMeta<SyncInfo>> for DirMeta<()> {
    fn from(value: &DirMeta<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            state: (&value.state).into(),
        }
    }
}

impl<SyncInfo: ToBytes> From<&DirMeta<SyncInfo>> for DirMeta<Vec<u8>> {
    fn from(value: &DirMeta<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            state: (&value.state).into(),
        }
    }
}

impl<SyncInfo: TryFromBytes> TryFrom<DirMeta<Vec<u8>>> for DirMeta<SyncInfo> {
    type Error = InvalidByteSyncInfo;

    fn try_from(value: DirMeta<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            state: value.state.try_into()?,
        })
    }
}
