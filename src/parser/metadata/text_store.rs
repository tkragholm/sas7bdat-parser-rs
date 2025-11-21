use std::borrow::Cow;

use crate::error::{Error, Result, Section};

/// Reference into the text blob storage used by SAS column metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextRef {
    pub index: u16,
    pub offset: u16,
    pub length: u16,
}

impl TextRef {
    pub const EMPTY: Self = Self {
        index: 0,
        offset: 0,
        length: 0,
    };

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }
}

/// Stores decoded text blobs referenced by column metadata subheaders.
#[derive(Debug, Default)]
pub struct TextStore {
    blobs: Vec<Vec<u8>>,
}

impl TextStore {
    #[must_use]
    pub const fn new() -> Self {
        Self { blobs: Vec::new() }
    }

    /// Adds a text blob extracted from a column text subheader.
    pub fn push_blob(&mut self, blob: &[u8]) {
        self.blobs.push(blob.to_vec());
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.blobs.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    #[must_use]
    pub fn blob(&self, index: usize) -> Option<&[u8]> {
        self.blobs.get(index).map(Vec::as_slice)
    }

    /// Resolves a `TextRef` into a UTF-8 string if possible.
    ///
    /// # Errors
    ///
    /// Returns an error if the reference points outside the stored blobs or
    /// the bytes cannot be decoded as UTF-8.
    pub fn resolve(&self, text_ref: TextRef) -> Result<Option<Cow<'_, str>>> {
        if text_ref.length == 0 {
            return Ok(None);
        }
        let blob = self
            .blobs
            .get(text_ref.index as usize)
            .ok_or_else(|| Error::Corrupted {
                section: Section::Column {
                    index: u32::from(text_ref.index),
                },
                details: Cow::from("text reference points outside blob storage"),
            })?;
        let end = text_ref
            .offset
            .checked_add(text_ref.length)
            .ok_or_else(|| Error::Corrupted {
                section: Section::Column {
                    index: u32::from(text_ref.index),
                },
                details: Cow::from("text reference overflow"),
            })? as usize;
        let offset = text_ref.offset as usize;
        if end > blob.len() {
            return Err(Error::Corrupted {
                section: Section::Column {
                    index: u32::from(text_ref.index),
                },
                details: Cow::from("text reference exceeds blob length"),
            });
        }
        let bytes = &blob[offset..end];
        let decoded = String::from_utf8(bytes.to_vec()).map_err(|_| Error::Encoding {
            encoding: Cow::from("unknown"),
            details: Cow::from("failed to decode column text blob as UTF-8"),
        })?;
        Ok(Some(Cow::Owned(decoded)))
    }
}
