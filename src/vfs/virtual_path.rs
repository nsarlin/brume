use std::{borrow::Borrow, ops::Deref, path::Path};

/// A wrapper type that allows doing path operations on strings, without considerations for any
/// concrete file system. These paths are supposed to be absolute and should start with a '/'.
#[derive(Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct VirtualPath {
    path: str,
}

#[derive(Debug)]
pub enum VirtualPathError {
    InvalidPath(String),
    NotASubpath { base: String, subpath: String },
}

impl VirtualPath {
    /// Return the root path: "/"
    pub const fn root() -> &'static Self {
        // safety: because of `#[repr(transparent)]`, &'a str can be safely converted to &'a Self
        unsafe { &*("/" as *const str as *const Self) }
    }

    /// Create a path from a &str, without any validation
    fn new(path: &str) -> &Self {
        // safety: because of `#[repr(transparent)]`, &'a str can be safely converted to &'a Self
        unsafe { &*(path as *const str as *const Self) }
    }

    /// Return the path without any trailing '/'
    fn trimmed_path(&self) -> &str {
        if let Some(prefix) = self.path.strip_suffix('/') {
            prefix
        } else {
            &self.path
        }
    }

    /// Return a new [`VirtualPath`] with a new root
    pub fn chroot(&self, new_root: &VirtualPath) -> Result<&Self, VirtualPathError> {
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
    pub fn name(&self) -> &str {
        let path = self.trimmed_path();

        // Since we have the invariant that all pathes should start with '/', if we don't find a '/'
        // we have removed it in the previous step, so our path is actually the root
        path.rsplit_once('/')
            .map(|(_, suffix)| suffix)
            .unwrap_or("")
    }

    /// Returns the parent of this path, if any
    pub fn parent(&self) -> Option<&Self> {
        let path = self.trimmed_path();

        // It's ok to unwrap here, if "path" is valid we know that "prefix will be too
        path.rsplit_once('/').map(|(prefix, _)| {
            if prefix.is_empty() {
                Self::root()
            } else {
                prefix.try_into().unwrap()
            }
        })
    }
}

impl<'a> TryFrom<&'a str> for &'a VirtualPath {
    type Error = VirtualPathError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        if !value.starts_with('/') {
            return Err(VirtualPathError::InvalidPath(value.to_string()));
        }

        if value.contains("//") {
            return Err(VirtualPathError::InvalidPath(value.to_string()));
        }

        Ok(VirtualPath::new(value))
    }
}

impl<'a> From<&'a VirtualPath> for &'a str {
    fn from(value: &'a VirtualPath) -> Self {
        &value.path
    }
}

impl Borrow<VirtualPath> for VirtualPathBuf {
    fn borrow(&self) -> &VirtualPath {
        self.deref()
    }
}

impl Deref for VirtualPathBuf {
    type Target = VirtualPath;

    fn deref(&self) -> &VirtualPath {
        VirtualPath::new(&self.path)
    }
}

impl AsRef<VirtualPath> for VirtualPathBuf {
    fn as_ref(&self) -> &VirtualPath {
        self.deref()
    }
}

impl ToOwned for VirtualPath {
    type Owned = VirtualPathBuf;

    fn to_owned(&self) -> Self::Owned {
        VirtualPathBuf {
            path: self.path.to_string(),
        }
    }
}

impl<'a> PartialEq<&'a VirtualPath> for VirtualPathBuf {
    fn eq(&self, other: &&'a VirtualPath) -> bool {
        self.path == other.path
    }
}

impl<'a> PartialEq<VirtualPathBuf> for &'a VirtualPath {
    fn eq(&self, other: &VirtualPathBuf) -> bool {
        self.path == other.path
    }
}

impl AsRef<Path> for VirtualPath {
    fn as_ref(&self) -> &Path {
        self.path.as_ref()
    }
}

/// Similar to the distinction with Path and PathBuf, this is a VirtualPath that owns the underlying
/// data.
#[derive(Debug, Eq, PartialEq, Clone)]
#[repr(transparent)]
pub struct VirtualPathBuf {
    path: String,
}

impl VirtualPathBuf {
    pub fn root() -> Self {
        Self {
            path: String::from('/'),
        }
    }

    pub fn new(path: &str) -> Result<Self, VirtualPathError> {
        let ref_path: &VirtualPath = path.try_into()?;

        Ok(ref_path.to_owned())
    }

    /// Extend a path with a new segment
    pub fn push(&mut self, value: &str) {
        if self.path.ends_with('/') {
            self.path.push_str(value.trim_matches('/'));
        } else {
            self.path.push('/');
            self.path.push_str(value.trim_matches('/'));
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_path() {
        let bad = VirtualPathBuf::new("a/bad/path");

        assert!(bad.is_err());

        let bad = VirtualPathBuf::new("/a//bad/path");

        assert!(bad.is_err());

        let bad = VirtualPathBuf::new("/a///bad/path");

        assert!(bad.is_err());

        VirtualPathBuf::new("/a/good/path").unwrap();

        VirtualPathBuf::new("/a/good/path/").unwrap();
    }

    #[test]
    fn test_chroot() {
        let base = VirtualPathBuf::new("/a/random/path").unwrap();

        let good_root = VirtualPathBuf::new("/a/random").unwrap();

        assert_eq!(
            base.chroot(&good_root).unwrap(),
            VirtualPathBuf::new("/path").unwrap()
        );

        let other_good_root = VirtualPathBuf::new("/a/random/").unwrap();

        assert_eq!(
            base.chroot(&other_good_root).unwrap(),
            VirtualPathBuf::new("/path").unwrap()
        );

        let bad_root = VirtualPathBuf::new("/b/random").unwrap();

        assert!(base.chroot(&bad_root).is_err());
    }

    #[test]
    fn test_name() {
        let base = VirtualPathBuf::new("/a/random/path").unwrap();

        assert_eq!(base.name(), "path");

        let trailing = VirtualPathBuf::new("/a/random/path/").unwrap();

        assert_eq!(trailing.name(), "path");

        let root = VirtualPath::root();

        assert_eq!(root.name(), "");
    }

    #[test]
    fn test_parent() {
        let base = VirtualPathBuf::new("/a/random/path").unwrap();

        assert_eq!(
            base.parent().unwrap(),
            VirtualPathBuf::new("/a/random").unwrap()
        );

        let trailing = VirtualPathBuf::new("/a/random/path/").unwrap();

        assert_eq!(
            trailing.parent().unwrap(),
            VirtualPathBuf::new("/a/random").unwrap()
        );

        let first_level = VirtualPathBuf::new("/a").unwrap();

        assert_eq!(first_level.parent().unwrap(), VirtualPath::root());

        let root = VirtualPathBuf::new("/").unwrap();

        assert_eq!(root.parent(), None);
    }

    #[test]
    fn test_push() {
        let reference = VirtualPathBuf::new("/a/random/path").unwrap();
        let mut base = VirtualPathBuf::new("/a/random").unwrap();
        base.push("path");

        assert_eq!(base, reference);

        let mut base = VirtualPathBuf::new("/a/random/").unwrap();
        base.push("path");

        assert_eq!(base, reference);

        let mut base = VirtualPathBuf::new("/a/random").unwrap();
        base.push("/path");

        assert_eq!(base, reference);

        let mut base = VirtualPathBuf::new("/a/random/").unwrap();
        base.push("/path");

        assert_eq!(base, reference);
    }
}
