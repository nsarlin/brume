use chrono::{DateTime, Utc};
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
    last_modified: DateTime<Utc>,
    data: Data,
}

impl<Data> DirInfo<Data> {
    pub fn new(name: &str, last_modified: DateTime<Utc>, meta: Data) -> Self {
        Self {
            name: name.to_string(),
            last_modified,
            data: meta,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn into_ok(self) -> DirState<Data> {
        DirState {
            name: self.name,
            last_modified: self.last_modified,
            data: NodeState::Ok(self.data),
        }
    }

    pub fn last_modified(&self) -> DateTime<Utc> {
        self.last_modified
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
    pub fn new_ok(name: &str, last_modified: DateTime<Utc>, info: SyncInfo) -> Self {
        Self::new(name, last_modified, NodeState::Ok(info))
    }

    pub fn new_force_resync(name: &str, last_modified: DateTime<Utc>) -> Self {
        Self {
            name: name.to_string(),
            last_modified,
            data: NodeState::NeedResync,
        }
    }

    pub fn new_error(
        name: &str,
        last_modified: DateTime<Utc>,
        error: FailedUpdateApplication,
    ) -> Self {
        Self {
            name: name.to_string(),
            last_modified,
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
                last_modified: self.last_modified,
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
            last_modified: value.last_modified,
            data: (&value.data).into(),
        }
    }
}

impl<SyncInfo: ToBytes> From<&DirState<SyncInfo>> for DirState<Vec<u8>> {
    fn from(value: &DirState<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            last_modified: value.last_modified,
            data: (&value.data).into(),
        }
    }
}

impl<SyncInfo: TryFromBytes> TryFrom<DirState<Vec<u8>>> for DirState<SyncInfo> {
    type Error = InvalidBytesSyncInfo;

    fn try_from(value: DirState<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            last_modified: value.last_modified,
            data: value.data.try_into()?,
        })
    }
}
