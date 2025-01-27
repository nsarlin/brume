//! Representation of path objects that are not necessarily linked to the local filesystem.

use std::{borrow::Borrow, ops::Deref, path::Path};

use thiserror::Error;

use super::NodeKind;

#[derive(Error, Debug)]
pub enum InvalidPathError {
    #[error("path {0:?} not found in the VFS")]
    NotFound(VirtualPathBuf),
    #[error("expected a directory at {0:?}")]
    NotADir(VirtualPathBuf),
    #[error("expected a file at {0:?}")]
    NotAFile(VirtualPathBuf),
}

impl InvalidPathError {
    pub fn for_kind(kind: NodeKind, path: &VirtualPath) -> Self {
        match kind {
            NodeKind::Dir => Self::NotADir(path.to_owned()),
            NodeKind::File => Self::NotAFile(path.to_owned()),
        }
    }
}

/// A wrapper type that allows doing path operations on strings, without considerations for any
/// concrete file system. These paths are supposed to be absolute and should start with a '/'.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct VirtualPath {
    path: str,
}

#[derive(Error, Debug)]
pub enum VirtualPathError {
    #[error("this string cannot be converted into a path: {0}")]
    InvalidString(String),
    #[error("the path {subpath} is not a valid subpath for {base}")]
    NotASubpath { base: String, subpath: String },
}

impl VirtualPath {
    /// Return the root path: "/"
    pub const fn root() -> &'static Self {
        // safety: because of `#[repr(transparent)]`, &'a str can be safely converted to &'a Self
        unsafe { &*("/" as *const str as *const Self) }
    }

    /// Check if this path represent a "root"
    pub fn is_root(&self) -> bool {
        &self.path == "/"
    }

    /// Return the number of elements in the path
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// alias for [`Self::is_root`]
    pub fn is_empty(&self) -> bool {
        self.is_root()
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

    /// Return true if the node pointed by "self" is contained in "other", eventually recursively
    /// Return also true if self == other
    pub fn is_inside(&self, other: &VirtualPath) -> bool {
        if let Some(suffix) = self.path.strip_prefix(other.trimmed_path()) {
            // Check for false positives caused by files that are in the same dir and start with the
            // same name. For example: /some/file and /some/file_long should return false
            suffix.is_empty() || suffix.starts_with('/')
        } else {
            false
        }
    }

    /// Return the name of the element pointed by this path
    pub fn name(&self) -> &str {
        let path = self.trimmed_path();

        // Since we have the invariant that all paths should start with '/', if we don't find a '/'
        // we have removed it in the previous step, so our path is actually the root
        path.rsplit_once('/')
            .map(|(_, suffix)| suffix)
            .unwrap_or("")
    }

    /// Return the parent of this path, if any
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

    /// Return the top level directory of this path, for example "a" in "/a/b/c". Return `None` is
    /// the provided path is the root
    pub fn top_level(&self) -> Option<&str> {
        if self.is_root() {
            return None;
        }

        let noroot = &self.path[1..];

        if let Some(end_pos) = noroot.find('/') {
            Some(&noroot[..end_pos])
        } else {
            Some(self.name())
        }
    }

    /// Return the path split in two components, the top level and the rest. For example, the path
    /// "a/b/c" will return Some("a", "/b/c"). Return None when called on a root path.
    pub fn top_level_split(&self) -> Option<(&str, &Self)> {
        if self.is_root() {
            return None;
        }

        let noroot = &self.path[1..];

        if let Some(end_pos) = noroot.find('/') {
            // Ok to unwrap here because the path is known to be valid
            let top_level = &noroot[..end_pos];
            let remainder = self.path[(end_pos + 1)..].try_into().unwrap();
            Some((top_level, remainder))
        } else {
            Some((self.name(), Self::root()))
        }
    }

    /// Return an iterator over the components of the path
    pub fn iter(&self) -> VirtualPathIterator {
        VirtualPathIterator { path: self }
    }
}

impl<'a> TryFrom<&'a str> for &'a VirtualPath {
    type Error = VirtualPathError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        if !value.starts_with('/') {
            return Err(VirtualPathError::InvalidString(value.to_string()));
        }

        if value.contains("//") {
            return Err(VirtualPathError::InvalidString(value.to_string()));
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

impl PartialEq<VirtualPathBuf> for &VirtualPath {
    fn eq(&self, other: &VirtualPathBuf) -> bool {
        self.path == other.path
    }
}

impl AsRef<Path> for VirtualPath {
    fn as_ref(&self) -> &Path {
        self.path.as_ref()
    }
}

pub struct VirtualPathIterator<'a> {
    path: &'a VirtualPath,
}

impl<'a> Iterator for VirtualPathIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let (top_level, remainder) = self.path.top_level_split()?;

        self.path = remainder;

        Some(top_level)
    }
}

/// Similar to the distinction with Path and PathBuf, this is a VirtualPath that owns the underlying
/// data.
#[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Clone)]
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

    #[test]
    fn test_top_level() {
        let base = VirtualPathBuf::new("/a/random/path").unwrap();

        assert_eq!(base.top_level().unwrap(), "a");

        let root = VirtualPath::root();

        assert!(root.top_level().is_none());
    }

    #[test]
    fn test_top_level_split() {
        let base = VirtualPathBuf::new("/a/random/path").unwrap();

        assert_eq!(
            base.top_level_split().unwrap(),
            ("a", VirtualPathBuf::new("/random/path").unwrap().as_ref())
        );

        let base = VirtualPathBuf::new("/a/random/path/").unwrap();

        assert_eq!(
            base.top_level_split().unwrap(),
            ("a", VirtualPathBuf::new("/random/path/").unwrap().as_ref())
        );

        let base = VirtualPathBuf::new("/a/").unwrap();

        assert_eq!(base.top_level_split().unwrap(), ("a", VirtualPath::root()));

        let base = VirtualPathBuf::new("/a").unwrap();

        assert_eq!(base.top_level_split().unwrap(), ("a", VirtualPath::root()));

        let root = VirtualPath::root();

        assert!(root.top_level_split().is_none());
    }

    #[test]
    fn test_is_inside() {
        let base = VirtualPathBuf::new("/a/b").unwrap();
        let elem = VirtualPathBuf::new("/a/b/c").unwrap();

        assert!(elem.is_inside(&base));

        let elem = VirtualPathBuf::new("/a/b/c/d/e").unwrap();

        assert!(elem.is_inside(&base));

        let elem = VirtualPathBuf::new("/a/f/g").unwrap();

        assert!(!elem.is_inside(&base));

        let elem = VirtualPathBuf::new("/a/baba").unwrap();

        assert!(!elem.is_inside(&base));

        let elem = VirtualPathBuf::new("/a/b").unwrap();

        assert!(elem.is_inside(&base));
    }

    #[test]
    fn test_iter() {
        let elem = VirtualPathBuf::new("/a/b/c").unwrap();
        let mut iter = elem.iter();

        assert_eq!(iter.next(), Some("a"));
        assert_eq!(iter.next(), Some("b"));
        assert_eq!(iter.next(), Some("c"));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_len() {
        let elem = VirtualPathBuf::new("/a/b/c").unwrap();

        assert_eq!(elem.len(), 3);

        let elem = VirtualPathBuf::new("/a").unwrap();

        assert_eq!(elem.len(), 1);

        let elem = VirtualPathBuf::new("/").unwrap();

        assert_eq!(elem.len(), 0);
        assert!(elem.is_empty());
    }
}
