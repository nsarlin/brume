/// A wrapper type that allows doing path operations on strings, without considerations for any
/// concrete file system. These paths are supposed to be absolute and should start with a '/'.
#[derive(Debug, Eq, PartialEq)]
pub struct VirtualPath<'a> {
    path: &'a str,
}

#[derive(Debug)]
pub enum VirtualPathError {
    InvalidPath(String),
    NotASubpath { base: String, subpath: String },
}

impl<'a> VirtualPath<'a> {
    /// Return the "root" path
    pub fn root() -> Self {
        Self { path: "/" }
    }

    /// Return the path without any trailing '/'
    fn trimmed_path(&self) -> &'a str {
        if let Some(prefix) = self.path.strip_suffix('/') {
            prefix
        } else {
            self.path
        }
    }

    /// Return a new [`VirtualPath`] with a new root
    pub fn chroot(&self, new_root: &VirtualPath<'_>) -> Result<Self, VirtualPathError> {
        let new_root_path = new_root.trimmed_path();

        if let Some(new_path) = self.path.strip_prefix(new_root_path) {
            new_path.try_into()
        } else {
            Err(VirtualPathError::NotASubpath {
                base: self.path.to_string(),
                subpath: new_root.path.to_string(),
            })
        }
    }

    /// Returns the name of the element pointed by this path
    pub fn name(&self) -> &'a str {
        let path = self.trimmed_path();

        // Since we have the invariant that all pathes should start with '/', if we don't find a '/'
        // we have removed it in the previous step, so our path is actually the root
        path.rsplit_once('/')
            .map(|(_, suffix)| suffix)
            .unwrap_or("")
    }

    /// Returns the parent of this path, if any
    pub fn parent(&self) -> Option<Self> {
        let path = self.trimmed_path();

        // It's ok to unwrap here, if "path" is valid we know that "prefix will be too
        path.rsplit_once('/').map(|(prefix, _)| {
            if prefix.is_empty() {
                Self::root()
            } else {
                VirtualPath::try_from(prefix).unwrap()
            }
        })
    }
}

impl<'a> TryFrom<&'a str> for VirtualPath<'a> {
    type Error = VirtualPathError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        if !value.starts_with('/') {
            return Err(VirtualPathError::InvalidPath(value.to_string()));
        }

        if value.contains("//") {
            return Err(VirtualPathError::InvalidPath(value.to_string()));
        }

        Ok(Self { path: value })
    }
}

impl<'a> From<VirtualPath<'a>> for &'a str {
    fn from(value: VirtualPath<'a>) -> Self {
        value.path
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_path() {
        let bad = "a/bad/path";

        assert!(VirtualPath::try_from(bad).is_err());

        let bad = "/a//bad/path";

        assert!(VirtualPath::try_from(bad).is_err());

        let bad = "/a///bad/path";

        assert!(VirtualPath::try_from(bad).is_err());

        let good = "/a/good/path";

        VirtualPath::try_from(good).unwrap();

        let good = "/a/good/path/";

        VirtualPath::try_from(good).unwrap();
    }

    #[test]
    fn test_chroot() {
        let base: VirtualPath = "/a/random/path".try_into().unwrap();

        let good_root: VirtualPath = "/a/random".try_into().unwrap();

        assert_eq!(
            base.chroot(&good_root).unwrap(),
            "/path".try_into().unwrap()
        );

        let other_good_root: VirtualPath = "/a/random/".try_into().unwrap();

        assert_eq!(
            base.chroot(&other_good_root).unwrap(),
            "/path".try_into().unwrap()
        );

        let bad_root: VirtualPath = "/b/random".try_into().unwrap();

        assert!(base.chroot(&bad_root).is_err());
    }

    #[test]
    fn test_name() {
        let base: VirtualPath = "/a/random/path".try_into().unwrap();

        assert_eq!(base.name(), "path");

        let trailing: VirtualPath = "/a/random/path/".try_into().unwrap();

        assert_eq!(trailing.name(), "path");

        let root = VirtualPath::root();

        assert_eq!(root.name(), "");
    }

    #[test]
    fn test_parent() {
        let base: VirtualPath = "/a/random/path".try_into().unwrap();

        assert_eq!(base.parent().unwrap(), "/a/random".try_into().unwrap());

        let trailing: VirtualPath = "/a/random/path/".try_into().unwrap();

        assert_eq!(trailing.parent().unwrap(), "/a/random".try_into().unwrap());

        let first_level: VirtualPath = "/a".try_into().unwrap();

        assert_eq!(first_level.parent().unwrap(), VirtualPath::root());

        let root: VirtualPath = "/".try_into().unwrap();

        assert_eq!(root.parent(), None);
    }
}
