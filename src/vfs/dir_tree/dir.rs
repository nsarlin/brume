/// Metadata of a Directory node
#[derive(Debug, Clone)]
pub struct DirInfo {
    name: String,
}

impl DirInfo {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
