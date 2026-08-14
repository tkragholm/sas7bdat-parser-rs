//! Detects whether a path lives on a network share.
//!
//! Mapping a remote file makes each access a page fault serviced by a round-trip, with no
//! sequential readahead. `Auto` skips mmap when it can tell the path is remote.
//!
//! Only Windows is probed: there a share mounts as an ordinary drive letter, so the path
//! alone does not identify it. Other platforms always answer `false`; callers select the
//! backend with [`IoBackendPreference`](crate::IoBackendPreference).

use std::path::Path;

// The one Win32 entry point this crate needs, declared directly.
//
// `windows-sys` supplies this, but it is 18 MB of generated bindings — by a wide
// margin the largest crate in the tree — for a single call. That is most of a
// vendored dependency bundle, which matters for a CRAN submission and is wasted
// download and build time everywhere else.
//
// `kernel32` is already linked by `std` on every Windows target, so naming it here
// is belt-and-braces rather than load-bearing.
//
// Note the `unsafe extern` form is required by edition 2024 and needs rustc >= 1.82.
// If the crate is ever moved back to edition 2021 for a lower MSRV, this becomes a
// plain `extern "system"` block.
#[cfg(windows)]
#[allow(unsafe_code)]
#[link(name = "kernel32")]
unsafe extern "system" {
    /// <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdrivetypew>
    ///
    /// Takes a NUL-terminated UTF-16 root path and returns a `DRIVE_*` constant. It
    /// has no failure mode: an unusable path answers `DRIVE_UNKNOWN` (0) or
    /// `DRIVE_NO_ROOT_DIR` (1), neither of which is `DRIVE_REMOTE`.
    fn GetDriveTypeW(lp_root_path_name: *const u16) -> u32;
}

/// `DRIVE_REMOTE` from `winbase.h` — the drive is a network share.
#[cfg(windows)]
const DRIVE_REMOTE: u32 = 4;

/// True when `path` is known to live on a network share.
///
/// Conservative: an inconclusive probe answers `false`, so the caller keeps the local-storage
/// default rather than silently degrading a local scan.
#[cfg(windows)]
#[must_use]
pub(crate) fn is_network_path(path: &Path) -> bool {
    use std::os::windows::ffi::OsStrExt;

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
