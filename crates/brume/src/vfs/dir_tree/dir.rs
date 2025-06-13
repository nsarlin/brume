use serde::{Deserialize, Serialize};

use crate::{
    concrete::{InvalidBytesSyncInfo, ToBytes, TryFromBytes},
    update::FailedUpdateApplication,
};

use super::NodeState;

/// Description of a Directory node, with arbitrary user provided metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirInfo<Data> {
    name: String,
    data: Data,
}

impl<Data> DirInfo<Data> {
    pub fn new(name: &str, meta: Data) -> Self {
        Self {
            name: name.to_string(),
            data: meta,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn into_ok(self) -> DirState<Data> {
        DirState {
            name: self.name,
            data: NodeState::Ok(self.data),
        }
    }

    pub fn metadata(&self) -> &Data {
        &self.data
    }

    pub fn into_metadata(self) -> Data {
        self.data
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
            data: NodeState::NeedResync,
        }
    }

    pub fn new_error(name: &str, error: FailedUpdateApplication) -> Self {
        Self {
            name: name.to_string(),
            data: NodeState::Error(error),
        }
    }

    pub fn state(&self) -> &NodeState<SyncInfo> {
        &self.data
    }

    pub fn state_mut(&mut self) -> &mut NodeState<SyncInfo> {
        &mut self.data
    }

    /// Invalidate the sync info to make them trigger a FSBackend sync on next run
    pub fn force_resync(&mut self) {
        self.data = NodeState::NeedResync;
    }

    /// Extract the Ok state of the node, or panic
    #[cfg(test)]
    pub fn unwrap(self) -> DirInfo<SyncInfo> {
        match self.data {
            NodeState::Ok(info) => DirInfo {
                name: self.name,
                data: info,
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
            data: (&value.data).into(),
        }
    }
}

impl<SyncInfo: ToBytes> From<&DirState<SyncInfo>> for DirState<Vec<u8>> {
    fn from(value: &DirState<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            data: (&value.data).into(),
        }
    }
}

impl<SyncInfo: TryFromBytes> TryFrom<DirState<Vec<u8>>> for DirState<SyncInfo> {
    type Error = InvalidBytesSyncInfo;

    fn try_from(value: DirState<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            data: value.data.try_into()?,
        })
    }
}
