use serde::{Deserialize, Serialize};

use crate::{
    concrete::{InvalidByteSyncInfo, ToBytes, TryFromBytes},
    update::FailedUpdateApplication,
};

use super::NodeState;

/// Description of a File node, with arbitrary user provided metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo<Meta> {
    name: String,
    size: u64,
    metadata: Meta,
}

impl<Meta> FileInfo<Meta> {
    pub fn new(name: &str, size: u64, meta: Meta) -> Self {
        Self {
            name: name.to_string(),
            size,
            metadata: meta,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn metadata(&self) -> &Meta {
        &self.metadata
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub fn as_ok(self) -> FileState<Meta> {
        FileState {
            name: self.name,
            size: self.size,
            metadata: NodeState::Ok(self.metadata),
        }
    }
}

pub type FileState<SyncInfo> = FileInfo<NodeState<SyncInfo>>;

impl<SyncInfo> FileState<SyncInfo> {
    pub fn new_ok(name: &str, size: u64, info: SyncInfo) -> Self {
        Self::new(name, size, NodeState::Ok(info))
    }

    pub fn new_error(name: &str, size: u64, error: FailedUpdateApplication) -> Self {
        Self {
            name: name.to_string(),
            size,
            metadata: NodeState::Error(error),
        }
    }

    pub fn state(&self) -> &NodeState<SyncInfo> {
        &self.metadata
    }

    pub fn state_mut(&mut self) -> &mut NodeState<SyncInfo> {
        &mut self.metadata
    }

    /// Extract the Ok state of the node, or panic
    #[cfg(test)]
    pub fn unwrap(self) -> FileInfo<SyncInfo> {
        match self.metadata {
            NodeState::Ok(info) => FileInfo {
                name: self.name,
                size: self.size,
                metadata: info,
            },
            NodeState::NeedResync => panic!("NeedResync"),
            NodeState::Error(_) => panic!("Error"),
            NodeState::Conflict(_) => panic!("Conflict"),
        }
    }
}

impl<SyncInfo> From<&FileState<SyncInfo>> for FileState<()> {
    fn from(value: &FileState<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            size: value.size,
            metadata: (&value.metadata).into(),
        }
    }
}

impl<SyncInfo: ToBytes> From<&FileState<SyncInfo>> for FileState<Vec<u8>> {
    fn from(value: &FileState<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            size: value.size,
            metadata: (&value.metadata).into(),
        }
    }
}

impl<SyncInfo: TryFromBytes> TryFrom<FileState<Vec<u8>>> for FileState<SyncInfo> {
    type Error = InvalidByteSyncInfo;

    fn try_from(value: FileState<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            size: value.size,
            metadata: value.metadata.try_into()?,
        })
    }
}
