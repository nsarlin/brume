/// Metadata of a File node
#[derive(Debug, Clone)]
pub struct FileInfo {
    name: String,
}

impl FileInfo {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
