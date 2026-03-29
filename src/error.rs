use std::{error::Error as StdError, fmt, path::PathBuf};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct IoError {
    pub path: Option<PathBuf>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct HeaderError {
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct MetadataError {
    pub message: String,
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
pub struct CorruptionError {
    pub message: String,
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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {}", err.message),
            Self::Header(err) => write!(f, "header error: {}", err.message),
            Self::Metadata(err) => write!(f, "metadata error: {}", err.message),
            Self::Projection(err) => write!(f, "projection error: {}", err.message),
            Self::Decode(err) => write!(f, "decode error: {}", err.message),
            Self::Compression(err) => write!(f, "compression error: {}", err.message),
            Self::Corruption(err) => write!(f, "corruption error: {}", err.message),
            Self::Unsupported(err) => write!(f, "unsupported: {}", err.feature),
        }
    }
}

impl StdError for Error {}
