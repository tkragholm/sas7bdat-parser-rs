use std::{error::Error as StdError, fmt, path::PathBuf};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct IoError {
    pub path: Option<PathBuf>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum HeaderError {
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u16),
    UnexpectedEndianness,
    PageSizeTooSmall(u32),
    InvalidHeaderSize(u32),
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

#[derive(Debug, Clone)]
pub enum MetadataError {
    InvalidColumnType(u8),
    MissingColumnDescriptor(usize),
    InvalidCompression(String),
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

#[derive(Debug, Clone)]
pub struct ProjectionError {
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct DecodeError {
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct CompressionError {
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ArrowError {
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum CorruptionError {
    PageOutOfBounds { index: u64, limit: u64 },
    PointerOutOfBounds { offset: usize, limit: usize },
    InvalidSubheaderSignature(u32),
    TypeMismatch { expected: String, found: String },
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

#[derive(Debug, Clone)]
pub struct UnsupportedError {
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
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum Error {
    Io(IoError),
    Header(HeaderError),
    Metadata(MetadataError),
    Projection(ProjectionError),
    Decode(DecodeError),
    Compression(CompressionError),
    Arrow(ArrowError),
    Corruption(CorruptionError),
    Unsupported(UnsupportedError),
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

    #[must_use]
    pub fn io_with_path(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::Io(IoError {
            path: Some(path.into()),
            message: message.into(),
        })
    }

    #[must_use]
    pub fn io_error(err: &std::io::Error) -> Self {
        Self::io(err.to_string())
    }

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

    #[must_use]
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported(UnsupportedError {
            feature: feature.into(),
        })
    }

    #[must_use]
    pub fn decode(message: impl Into<String>) -> Self {
        Self::Decode(DecodeError {
            message: message.into(),
        })
    }

    #[must_use]
    pub fn page_corruption(message: impl Into<String>) -> Self {
        Self::Corruption(CorruptionError::Other(message.into()))
    }

    #[must_use]
    pub fn header_corruption(message: impl Into<String>) -> Self {
        Self::Header(HeaderError::Other(message.into()))
    }

    #[must_use]
    pub fn metadata_corruption(message: impl Into<String>) -> Self {
        Self::Metadata(MetadataError::Other(message.into()))
    }

    #[must_use]
    pub fn metadata_io(err: &std::io::Error) -> Self {
        Self::metadata_corruption(err.to_string())
    }

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
