use crate::vfs::{IsModified, ModificationState};

/// State of the `SyncInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncInfoState<SyncInfo> {
    /// The info can be used normally
    Valid(SyncInfo),
    /// The info cannot be used and will always return "modified" when polled
    Invalid,
}

impl<SyncInfo: IsModified<SyncInfo>> IsModified<Self> for SyncInfoState<SyncInfo> {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        match (self, reference) {
            (SyncInfoState::Valid(valid_self), SyncInfoState::Valid(valid_other)) => {
                valid_self.modification_state(valid_other)
            }
            (SyncInfoState::Valid(_), SyncInfoState::Invalid)
            | (SyncInfoState::Invalid, SyncInfoState::Valid(_))
            | (SyncInfoState::Invalid, SyncInfoState::Invalid) => ModificationState::Modified,
        }
    }
}

/// Metadata of a Directory node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirInfo<SyncInfo> {
    name: String,
    sync: SyncInfoState<SyncInfo>,
}

impl<SyncInfo> DirInfo<SyncInfo> {
    pub fn new(name: &str, sync: SyncInfo) -> Self {
        Self {
            name: name.to_string(),
            sync: SyncInfoState::Valid(sync),
        }
    }

    pub fn new_invalid(name: &str) -> Self {
        Self {
            name: name.to_string(),
            sync: SyncInfoState::Invalid,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sync_info(&self) -> &SyncInfoState<SyncInfo> {
        &self.sync
    }
}
