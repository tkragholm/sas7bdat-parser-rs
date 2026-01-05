use std::io::{Read, Seek};

use crate::cell::CellValue;
use crate::error::{Error, Result};
use crate::iter_utils::next_from_result;
use crate::parser::RowIterator;

pub struct ProjectedRowIter<'a, R: Read + Seek> {
    pub(crate) inner: RowIterator<'a, R>,
    pub(crate) selected_indices: Vec<usize>,
    pub(crate) sorted_projection: Vec<(usize, usize)>,
    pub(crate) exhausted: bool,
}

impl<R: Read + Seek> ProjectedRowIter<'_, R> {
    /// Advances the projection iterator.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or if a requested column is
    /// missing from the row data.
    pub fn try_next(&mut self) -> Result<Option<Vec<CellValue<'static>>>> {
        if self.exhausted {
            return Ok(None);
        }
        let maybe_row = match self.inner.try_next() {
            Ok(value) => value,
            Err(err) => {
                self.exhausted = true;
                return Err(err);
            }
        };
        if let Some(row) = maybe_row {
            let mut slots: Vec<Option<CellValue<'static>>> =
                vec![None; self.selected_indices.len()];
            let mut sorted_pos = 0usize;
            let sorted_len = self.sorted_projection.len();
            let mut filled = 0usize;
            for (column_index, value) in row.into_iter().enumerate() {
                if sorted_pos < sorted_len {
                    let (target_index, result_position) = self.sorted_projection[sorted_pos];
                    if target_index < column_index {
                        return Err(Error::InvalidMetadata {
                            details: format!(
                                "projected column index {target_index} missing from row data"
                            )
                            .into(),
                        });
                    }
                    if target_index == column_index {
                        slots[result_position] = Some(value.into_owned());
                        sorted_pos += 1;
                        filled += 1;
                        if filled == sorted_len {
                            break;
                        }
                        continue;
                    }
                }
                if filled == sorted_len {
                    break;
                }
            }
            if filled != sorted_len {
                return Err(Error::InvalidMetadata {
                    details: "row did not contain all projected columns".into(),
                });
            }
            let mut projected = Vec::with_capacity(self.selected_indices.len());
            for slot in slots {
                if let Some(value) = slot {
                    projected.push(value);
                } else {
                    return Err(Error::InvalidMetadata {
                        details: "projected column resolved to empty slot".into(),
                    });
                }
            }
            Ok(Some(projected))
        } else {
            self.exhausted = true;
            Ok(None)
        }
    }
}

impl<R: Read + Seek> Iterator for ProjectedRowIter<'_, R> {
    type Item = Result<Vec<CellValue<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.try_next();
        next_from_result(
            result,
            |row| row,
            || {
                self.exhausted = true;
            },
        )
    }
}
