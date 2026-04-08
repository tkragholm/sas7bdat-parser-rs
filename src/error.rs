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
            Self::MissingColumnDescriptor(idx) => write!(f, "missing column descriptor at index {idx}"),
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

#[derive(Debug, Clone)]
pub enum Error {
    Io(IoError),
    Header(HeaderError),
    Metadata(MetadataError),
    Projection(ProjectionError),
    Decode(DecodeError),
    Compression(CompressionError),
    Corruption(CorruptionError),
    Unsupported(UnsupportedError),
}

impl Error {
    #[must_use]
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported(UnsupportedError {
            feature: feature.into(),
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
            Self::Corruption(err) => write!(f, "corruption error: {err}"),
            Self::Unsupported(err) => write!(f, "unsupported: {}", err.feature),
        }
    }
}

impl StdError for Error {}
