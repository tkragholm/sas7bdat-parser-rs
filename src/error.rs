use std::borrow::Cow;
use std::fmt;
use std::io;

use parquet::errors::ParquetError;

/// Result type used across the high-level SAS reader implementation.
pub type Result<T> = std::result::Result<T, Error>;

/// High-level error type surfaced by the idiomatic SAS reader.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O failure while reading from the underlying data source.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// Failure to convert bytes between character encodings.
    #[error("encoding conversion from {encoding} failed: {details}")]
    Encoding {
        encoding: Cow<'static, str>,
        details: Cow<'static, str>,
    },

    /// The file appears to be corrupt or inconsistent while processing a section.
    #[error("corrupted SAS file while processing {section}: {details}")]
    Corrupted {
        section: Section,
        details: Cow<'static, str>,
    },

    /// SAS features that are not yet implemented in the pure Rust reader.
    #[error("unsupported SAS feature: {feature}")]
    Unsupported { feature: Cow<'static, str> },

    /// Metadata or schema could not be interpreted according to expectations.
    #[error("invalid SAS metadata: {details}")]
    InvalidMetadata { details: Cow<'static, str> },

    /// Failure encountered while interacting with the Parquet writer.
    #[error("parquet error: {details}")]
    Parquet { details: Cow<'static, str> },

    /// Failed to allocate or grow internal buffers.
    #[error("allocation failed: {details}")]
    Allocation { details: Cow<'static, str> },
}

/// Logical section of the parser used for diagnostic reporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Section {
    Header,
    Page { index: u64 },
    Subheader { page_index: u64, signature: u32 },
    Row { index: u64 },
    Column { index: u32 },
    Decompression { page_index: u64 },
    Encoding,
}

impl Section {
    /// Helper constructor for subheader sections when the raw signature is known.
    #[must_use]
    pub const fn subheader(page_index: u64, signature: u32) -> Self {
        Self::Subheader {
            page_index,
            signature,
        }
    }
}

impl From<ParquetError> for Error {
    fn from(err: ParquetError) -> Self {
        Self::Parquet {
            details: Cow::Owned(err.to_string()),
        }
    }
}

impl fmt::Display for Section {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Header => write!(f, "file header"),
            Self::Page { index } => write!(f, "page {index}"),
            Self::Subheader {
                page_index,
                signature,
            } => write!(
                f,
                "subheader signature 0x{signature:08X} on page {page_index}"
            ),
            Self::Row { index } => write!(f, "row {index}"),
            Self::Column { index } => write!(f, "column {index}"),
            Self::Decompression { page_index } => {
                write!(f, "page {page_index} during decompression")
            }
            Self::Encoding => write!(f, "character encoding conversion"),
        }
    }
}
