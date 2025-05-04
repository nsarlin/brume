use serde::{Deserialize, Serialize};

use crate::{
    concrete::{InvalidByteSyncInfo, ToBytes, TryFromBytes},
    update::FailedUpdateApplication,
};

use super::NodeState;

/// Description of a Directory node, with arbitrary user provided metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirInfo<Meta> {
    name: String,
    metadata: Meta,
}

impl<Meta> DirInfo<Meta> {
    pub fn new(name: &str, meta: Meta) -> Self {
        Self {
            name: name.to_string(),
            metadata: meta,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn as_ok(self) -> DirState<Meta> {
        DirState {
            name: self.name,
            metadata: NodeState::Ok(self.metadata),
        }
    }

    pub fn metadata(&self) -> &Meta {
        &self.metadata
    }
}

pub type DirState<SyncInfo> = DirInfo<NodeState<SyncInfo>>;

impl<SyncInfo> DirState<SyncInfo> {
    pub fn new_ok(name: &str, info: SyncInfo) -> Self {
        Self::new(name, NodeState::Ok(info))
    }

    pub fn new_force_resync(name: &str) -> Self {
        Self {
            name: name.to_string(),
            metadata: NodeState::NeedResync,
        }
    }

    pub fn new_error(name: &str, error: FailedUpdateApplication) -> Self {
        Self {
            name: name.to_string(),
            metadata: NodeState::Error(error),
        }
    }

    pub fn state(&self) -> &NodeState<SyncInfo> {
        &self.metadata
    }

    pub fn state_mut(&mut self) -> &mut NodeState<SyncInfo> {
        &mut self.metadata
    }

    /// Invalidate the sync info to make them trigger a FSBackend sync on next run
    pub fn force_resync(&mut self) {
        self.metadata = NodeState::NeedResync;
    }

    /// Extract the Ok state of the node, or panic
    #[cfg(test)]
    pub fn unwrap(self) -> DirInfo<SyncInfo> {
        match self.metadata {
            NodeState::Ok(info) => DirInfo {
                name: self.name,
                metadata: info,
            },
            NodeState::NeedResync => panic!("NeedResync"),
            NodeState::Error(_) => panic!("Error"),
            NodeState::Conflict(_) => panic!("Conflict"),
        }
    }
}

impl<SyncInfo> From<&DirState<SyncInfo>> for DirState<()> {
    fn from(value: &DirState<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            metadata: (&value.metadata).into(),
        }
    }
}

impl<SyncInfo: ToBytes> From<&DirState<SyncInfo>> for DirState<Vec<u8>> {
    fn from(value: &DirState<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            metadata: (&value.metadata).into(),
        }
    }
}

impl<SyncInfo: TryFromBytes> TryFrom<DirState<Vec<u8>>> for DirState<SyncInfo> {
    type Error = InvalidByteSyncInfo;

    fn try_from(value: DirState<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            metadata: value.metadata.try_into()?,
        })
    }
}
