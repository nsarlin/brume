use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    concrete::{InvalidBytesSyncInfo, ToBytes, TryFromBytes},
    update::FailedUpdateApplication,
};

use super::NodeState;

/// Description of a File node, with arbitrary user provided metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo<Data> {
    name: String,
    size: u64,
    last_modified: DateTime<Utc>,
    data: Data,
}

impl<Data> FileInfo<Data> {
    pub fn new(name: &str, size: u64, last_modified: DateTime<Utc>, meta: Data) -> Self {
        Self {
            name: name.to_string(),
            size,
            last_modified,
            data: meta,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> u64 {
        self.size
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

    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub fn into_ok(self) -> FileState<Data> {
        FileState {
            name: self.name,
            size: self.size,
            last_modified: self.last_modified,
            data: NodeState::Ok(self.data),
        }
    }
}

pub type FileState<SyncInfo> = FileInfo<NodeState<SyncInfo>>;

impl<SyncInfo> FileState<SyncInfo> {
    pub fn new_ok(name: &str, size: u64, last_modified: DateTime<Utc>, info: SyncInfo) -> Self {
        Self::new(name, size, last_modified, NodeState::Ok(info))
    }

    pub fn new_error(
        name: &str,
        size: u64,
        last_modified: DateTime<Utc>,
        error: FailedUpdateApplication,
    ) -> Self {
        Self {
            name: name.to_string(),
            size,
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

    /// Extract the Ok state of the node, or panic
    #[cfg(any(test, feature = "test-utils"))]
    pub fn unwrap(self) -> FileInfo<SyncInfo> {
        match self.data {
            NodeState::Ok(info) => FileInfo {
                name: self.name,
                size: self.size,
                last_modified: self.last_modified,
                data: info,
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
            last_modified: value.last_modified,
            data: (&value.data).into(),
        }
    }
}

impl<SyncInfo: ToBytes> From<&FileState<SyncInfo>> for FileState<Vec<u8>> {
    fn from(value: &FileState<SyncInfo>) -> Self {
        Self {
            name: value.name.clone(),
            size: value.size,
            last_modified: value.last_modified,
            data: (&value.data).into(),
        }
    }
}

impl<SyncInfo: TryFromBytes> TryFrom<FileState<Vec<u8>>> for FileState<SyncInfo> {
    type Error = InvalidBytesSyncInfo;

    fn try_from(value: FileState<Vec<u8>>) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name.clone(),
            size: value.size,
            last_modified: value.last_modified,
            data: value.data.try_into()?,
        })
    }
}
