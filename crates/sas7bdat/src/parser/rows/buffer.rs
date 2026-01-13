use crate::error::{Error, Result, Section};
use std::borrow::Cow;

/// Borrowed or owned row data used while paging.
pub enum RowData {
    Borrowed(usize),
    Owned(Vec<u8>),
}

impl RowData {
    pub fn as_slice<'data>(
        &'data self,
        row_length: usize,
        page_buffer: &'data [u8],
        row_index: u64,
    ) -> Result<&'data [u8]> {
        match self {
            Self::Borrowed(offset) => {
                let start = *offset;
                let end = start.saturating_add(row_length);
                if end > page_buffer.len() {
                    return Err(Error::Corrupted {
                        section: Section::Row { index: row_index },
                        details: Cow::from("row offset exceeds page bounds"),
                    });
                }
                Ok(&page_buffer[start..end])
            }
            Self::Owned(buffer) => Ok(buffer.as_slice()),
        }
    }
}
