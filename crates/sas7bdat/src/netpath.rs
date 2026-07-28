//! Detects whether a path lives on a network share.
//!
//! Memory-mapping is the right default on local storage and close to the worst case on a
//! network share: each access becomes a page fault serviced by a round-trip, with no
//! sequential readahead, and a multi-gigabyte file turns into millions of them. `Auto`
//! therefore declines to map a file it can see is remote.
//!
//! Only Windows is probed, because that is where this crate's users mount network storage as
//! ordinary drive letters and hit the problem without realizing the file is remote. On other
//! platforms an NFS/SMB mount is a deliberate, visible act, and callers who need to override
//! the choice have [`IoBackendPreference`](crate::IoBackendPreference).

use std::path::Path;

/// True when `path` is known to live on a network share.
///
/// Conservative: an inconclusive probe answers `false`, so the caller keeps the local-storage
/// default rather than silently degrading a local scan.
#[cfg(windows)]
#[must_use]
pub(crate) fn is_network_path(path: &Path) -> bool {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDriveTypeW;
    use windows_sys::Win32::System::WindowsProgramming::DRIVE_REMOTE;

    // A UNC path (\\server\share, or \\?\UNC\server\share) is remote by construction and
    // needs no syscall.
    let text = path.to_string_lossy();
    let unc = text.starts_with("\\\\") && !text.starts_with("\\\\?\\")
        || text.starts_with("\\\\?\\UNC\\");
    if unc {
        return true;
    }

    // Otherwise the interesting case is a mapped drive (Z:\...), which looks local in the
    // path but is not. GetDriveTypeW wants a root ("Z:\") and a NUL-terminated wide string.
    let mut chars = text.chars();
    let (Some(letter), Some(':')) = (chars.next(), chars.next()) else {
        return false;
    };
    if !letter.is_ascii_alphabetic() {
        return false;
    }
    let root: Vec<u16> = std::ffi::OsString::from(format!("{letter}:\\"))
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: `root` is a NUL-terminated UTF-16 buffer that outlives the call, which is all
    // GetDriveTypeW reads. It returns a plain enum and cannot fail into an error state.
    #[allow(unsafe_code)]
    let kind = unsafe { GetDriveTypeW(root.as_ptr()) };
    kind == DRIVE_REMOTE
}

#[cfg(not(windows))]
#[must_use]
pub(crate) fn is_network_path(_path: &Path) -> bool {
    false
}

#[cfg(all(test, windows))]
mod tests {
    use super::is_network_path;
    use std::path::Path;

    #[test]
    fn unc_paths_are_remote() {
        assert!(is_network_path(Path::new(r"\\server\share\data.sas7bdat")));
        assert!(is_network_path(Path::new(
            r"\\?\UNC\server\share\data.sas7bdat"
        )));
    }

    #[test]
    fn local_paths_are_not_remote() {
        // C: is local on every Windows runner; \\?\C:\ is the verbatim form of the same.
        assert!(!is_network_path(Path::new(r"C:\Windows\notepad.exe")));
        assert!(!is_network_path(Path::new(r"\\?\C:\Windows\notepad.exe")));
        assert!(!is_network_path(Path::new("relative\\path.sas7bdat")));
    }
}
