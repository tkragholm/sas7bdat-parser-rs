use std::{error::Error as StdError, fmt, path::PathBuf};

/// A `Result` whose error type defaults to [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An I/O failure while reading a file or stream.
#[derive(Debug, Clone)]
pub struct IoError {
    /// The file being read, when the failure came from opening or reading a path.
    pub path: Option<PathBuf>,
    /// The text of the underlying [`std::io::Error`].
    pub message: String,
}

/// A problem in the file header, found before any page is read.
#[derive(Debug, Clone)]
pub enum HeaderError {
    /// The first four bytes are not a SAS7BDAT or catalog magic number.
    InvalidMagic([u8; 4]),
    /// The header declares a format version this reader does not handle.
    UnsupportedVersion(u16),
    /// The byte-order marker contradicts the rest of the header.
    UnexpectedEndianness,
    /// The declared page size, in bytes, is too small to hold a page header.
    PageSizeTooSmall(u32),
    /// The declared header size, in bytes, is invalid.
    InvalidHeaderSize(u32),
    /// Any other header problem, described in the message.
    Other(String),
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic(magic) => write!(f, "invalid magic: {magic:02X?}"),
            Self::UnsupportedVersion(ver) => write!(f, "unsupported version: {ver}"),
            Self::UnexpectedEndianness => write!(f, "unexpected endianness"),
            Self::PageSizeTooSmall(size) => write!(f, "page size too small: {size}"),
            Self::InvalidHeaderSize(size) => write!(f, "invalid header size: {size}"),
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

/// A problem in the column and layout metadata that follows the header.
#[derive(Debug, Clone)]
pub enum MetadataError {
    /// A column declares a type code that is neither numeric nor character.
    InvalidColumnType(u8),
    /// The column at this 0-based index has no descriptor in the metadata.
    MissingColumnDescriptor(usize),
    /// The compression signature is not one this reader recognises.
    InvalidCompression(String),
    /// Any other metadata problem, described in the message.
    Other(String),
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidColumnType(t) => write!(f, "invalid column type: {t}"),
            Self::MissingColumnDescriptor(idx) => {
                write!(f, "missing column descriptor at index {idx}")
            }
            Self::InvalidCompression(c) => write!(f, "invalid compression: {c}"),
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

/// A column selection that cannot be built, such as an unknown column name.
#[derive(Debug, Clone)]
pub struct ProjectionError {
    /// What was wrong with the selection.
    pub message: String,
}

/// A cell value that cannot be decoded as requested.
#[derive(Debug, Clone)]
pub struct DecodeError {
    /// What failed to decode.
    pub message: String,
}

/// A compressed page or row that cannot be expanded.
#[derive(Debug, Clone)]
pub struct CompressionError {
    /// What failed to decompress.
    pub message: String,
}

/// A failure while building Arrow arrays or schemas.
#[derive(Debug, Clone)]
pub struct ArrowError {
    /// What failed in the Arrow conversion.
    pub message: String,
}

/// The file's own structure contradicts itself or points outside the data.
#[derive(Debug, Clone)]
pub enum CorruptionError {
    /// A page index is beyond the page count.
    PageOutOfBounds {
        /// The 0-based page index that was requested.
        index: u64,
        /// The number of pages in the file.
        limit: u64,
    },
    /// A pointer inside a page leads outside the page.
    PointerOutOfBounds {
        /// The offset the pointer refers to, in bytes.
        offset: usize,
        /// The page size, in bytes.
        limit: usize,
    },
    /// A subheader carries a signature that is not defined.
    InvalidSubheaderSignature(u32),
    /// A value has a different type from the one the structure requires.
    TypeMismatch {
        /// The type the structure requires.
        expected: String,
        /// The type that was found.
        found: String,
    },
    /// Any other inconsistency, described in the message.
    Other(String),
}

impl fmt::Display for CorruptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PageOutOfBounds { index, limit } => {
                write!(f, "page index {index} exceeds page count {limit}")
            }
            Self::PointerOutOfBounds { offset, limit } => {
                write!(f, "pointer offset {offset} exceeds page size {limit}")
            }
            Self::InvalidSubheaderSignature(sig) => {
                write!(f, "invalid subheader signature: {sig:08X}")
            }
            Self::TypeMismatch { expected, found } => {
                write!(f, "type mismatch: expected {expected}, found {found}")
            }
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

/// A valid file or request that this build cannot handle.
#[derive(Debug, Clone)]
pub struct UnsupportedError {
    /// What is not supported.
    pub feature: String,
}

/// An invariant inside the reader was violated — a bug here, not a problem with the file.
///
/// Kept distinct from [`CorruptionError`] because the two ask different things of whoever sees
/// them. Corruption means the input is not a readable SAS7BDAT and the caller should look at
/// their file; this means the reader compiled a plan it then failed to honour, a worker panicked,
/// or a lock was poisoned, and the caller should report it. Neither is `Unsupported`, which
/// promises the file is fine and this build simply cannot read that shape yet.
#[derive(Debug, Clone)]
pub struct InternalError {
    /// What invariant was violated.
    pub message: String,
}

/// Everything that can go wrong when opening or scanning a SAS7BDAT file.
///
/// The variant says whose problem it is. [`Error::Io`] is the environment. [`Error::Header`],
/// [`Error::Metadata`], [`Error::Corruption`] and [`Error::Compression`] mean the file is not a
/// readable SAS7BDAT. [`Error::Unsupported`] means the file may be fine but this build cannot
/// read that shape. [`Error::Projection`] and [`Error::Decode`] come from the request, and
/// [`Error::Internal`] is a bug in this crate. `Display` gives a one-line message that starts
/// with the kind.
///
/// ```
/// use sas7bdat::{Dataset, Error};
///
/// let err = Dataset::from_bytes(b"not a sas file".to_vec()).unwrap_err();
/// assert!(!matches!(err, Error::Internal(_)));
/// println!("{err}");
/// ```
#[derive(Debug, Clone)]
pub enum Error {
    /// An I/O failure.
    Io(IoError),
    /// The file header is invalid.
    Header(HeaderError),
    /// The column or layout metadata is invalid.
    Metadata(MetadataError),
    /// A column selection could not be built.
    Projection(ProjectionError),
    /// A cell value could not be decoded as requested.
    Decode(DecodeError),
    /// A compressed page or row could not be expanded.
    Compression(CompressionError),
    /// Building an Arrow array or schema failed.
    Arrow(ArrowError),
    /// The file's structure is inconsistent or points outside the data.
    Corruption(CorruptionError),
    /// The file or request needs a feature this build does not implement.
    Unsupported(UnsupportedError),
    /// A reader invariant was violated. This is a bug in the crate.
    Internal(InternalError),
}

impl Error {
    /// Constructor helpers for downstream adapters and convenience wrappers.
    ///
    /// These remain public so external crates can lift non-SAS failures into the
    /// crate's error type without matching on internal variants.
    #[must_use]
    pub fn io(message: impl Into<String>) -> Self {
        Self::Io(IoError {
            path: None,
            message: message.into(),
        })
    }

    /// An I/O error for `path`.
    #[must_use]
    pub fn io_with_path(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::Io(IoError {
            path: Some(path.into()),
            message: message.into(),
        })
    }

    /// An I/O error carrying the text of `err`.
    #[must_use]
    pub fn io_error(err: &std::io::Error) -> Self {
        Self::io(err.to_string())
    }

    /// An I/O error for `path` carrying the text of `err`.
    #[must_use]
    pub fn io_error_with_path(path: impl Into<PathBuf>, err: &std::io::Error) -> Self {
        Self::io_with_path(path, err.to_string())
    }

    /// The file is not a readable SAS7BDAT: its own declared geometry is inconsistent or
    /// points outside the data. Use this, not [`Self::unsupported`], for anything a
    /// hostile or truncated file could cause.
    #[must_use]
    pub fn corruption(message: impl Into<String>) -> Self {
        Self::Corruption(CorruptionError::Other(message.into()))
    }

    /// A reader invariant was violated. Not reachable from any input — if one of these fires,
    /// it is a bug in this crate.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(InternalError {
            message: message.into(),
        })
    }

    /// A file or request this build cannot handle. `feature` says what.
    #[must_use]
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported(UnsupportedError {
            feature: feature.into(),
        })
    }

    /// A cell value that could not be decoded as requested.
    #[must_use]
    pub fn decode(message: impl Into<String>) -> Self {
        Self::Decode(DecodeError {
            message: message.into(),
        })
    }

    /// A corrupt page. Builds the same [`Error::Corruption`] value as [`Error::corruption`].
    #[must_use]
    pub fn page_corruption(message: impl Into<String>) -> Self {
        Self::Corruption(CorruptionError::Other(message.into()))
    }

    /// An invalid header, described by `message`.
    #[must_use]
    pub fn header_corruption(message: impl Into<String>) -> Self {
        Self::Header(HeaderError::Other(message.into()))
    }

    /// Invalid column or layout metadata, described by `message`.
    #[must_use]
    pub fn metadata_corruption(message: impl Into<String>) -> Self {
        Self::Metadata(MetadataError::Other(message.into()))
    }

    /// An I/O failure while reading metadata, reported as invalid metadata with the text of `err`.
    #[must_use]
    pub fn metadata_io(err: &std::io::Error) -> Self {
        Self::metadata_corruption(err.to_string())
    }

    /// A failure while building an Arrow array or schema.
    #[must_use]
    pub fn arrow(message: impl Into<String>) -> Self {
        Self::Arrow(ArrowError {
            message: message.into(),
        })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {}", err.message),
            Self::Header(err) => write!(f, "header error: {err}"),
            Self::Metadata(err) => write!(f, "metadata error: {err}"),
            Self::Projection(err) => write!(f, "projection error: {}", err.message),
            Self::Decode(err) => write!(f, "decode error: {}", err.message),
            Self::Compression(err) => write!(f, "compression error: {}", err.message),
            Self::Arrow(err) => write!(f, "arrow error: {}", err.message),
            Self::Corruption(err) => write!(f, "corruption error: {err}"),
            Self::Unsupported(err) => write!(f, "unsupported: {}", err.feature),
            Self::Internal(err) => write!(f, "internal error: {} (please report)", err.message),
        }
    }
}

impl StdError for Error {}

#[cfg(test)]
mod tests {
    use super::Error;

    /// The three kinds ask different things of whoever reads them, so they must not render
    /// alike: corruption points at the file, internal points at this crate, unsupported says
    /// the file is fine but this build cannot read that shape.
    #[test]
    fn the_three_failure_kinds_render_distinctly() {
        assert_eq!(
            Error::corruption("row span exceeds page bounds").to_string(),
            "corruption error: row span exceeds page bounds"
        );
        assert_eq!(
            Error::internal("compiled plan did not match column builder").to_string(),
            "internal error: compiled plan did not match column builder (please report)"
        );
        assert_eq!(
            Error::unsupported("this compressed page layout is not implemented yet").to_string(),
            "unsupported: this compressed page layout is not implemented yet"
        );
    }

    #[test]
    fn constructors_pick_the_matching_variant() {
        assert!(matches!(Error::corruption("x"), Error::Corruption(_)));
        assert!(matches!(Error::internal("x"), Error::Internal(_)));
        assert!(matches!(Error::unsupported("x"), Error::Unsupported(_)));
    }
}
