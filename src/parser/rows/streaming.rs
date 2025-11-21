use std::borrow::Cow;

use encoding_rs::Encoding;
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::metadata::Endianness;
use crate::parser::metadata::ColumnKind;
use crate::value::Value;

use super::decode::{decode_value_inner, is_blank, numeric_bits, numeric_bits_is_missing};
use super::runtime_column::RuntimeColumn;

/// Lightweight view over a row slice with associated metadata for streaming sinks.
pub struct StreamingRow<'data, 'meta> {
    pub(crate) data: &'data [u8],
    pub(crate) columns: &'meta [RuntimeColumn],
    pub(crate) encoding: &'static Encoding,
    pub(crate) endianness: Endianness,
}

/// Lightweight accessor for a single column within a streaming row.
pub struct StreamingCell<'data, 'meta> {
    column: &'meta RuntimeColumn,
    slice: &'data [u8],
    encoding: &'static Encoding,
    endianness: Endianness,
}

impl<'data, 'meta> StreamingRow<'data, 'meta> {
    #[must_use]
    pub const fn new(
        data: &'data [u8],
        columns: &'meta [RuntimeColumn],
        encoding: &'static Encoding,
        endianness: Endianness,
    ) -> Self {
        Self {
            data,
            columns,
            encoding,
            endianness,
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.columns.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Returns the streaming cell at `index`.
    ///
    /// # Errors
    ///
    /// Returns an error when the index is out of bounds or the column slice
    /// exceeds the row buffer.
    pub fn cell(&self, index: usize) -> Result<StreamingCell<'data, 'meta>> {
        let column = self
            .columns
            .get(index)
            .ok_or_else(|| Error::InvalidMetadata {
                details: Cow::Owned(format!("column index {index} out of bounds")),
            })?;
        self.cell_from_column(column)
    }

    pub fn cell_from_column(
        &self,
        column: &'meta RuntimeColumn,
    ) -> Result<StreamingCell<'data, 'meta>> {
        if column.offset + column.width > self.data.len() {
            return Err(Error::Corrupted {
                section: crate::error::Section::Column {
                    index: column.index,
                },
                details: Cow::from("column slice out of bounds"),
            });
        }
        Ok(StreamingCell {
            column,
            slice: &self.data[column.offset..column.offset + column.width],
            encoding: self.encoding,
            endianness: self.endianness,
        })
    }

    #[must_use]
    pub const fn iter(&self) -> StreamingRowIter<'_, 'data, 'meta> {
        StreamingRowIter {
            row: self,
            index: 0,
        }
    }

    /// Materialises the row into an owned vector of values.
    ///
    /// # Errors
    ///
    /// Propagates decoding failures for individual cells.
    pub fn materialize(&self) -> Result<Vec<Value<'data>>> {
        let mut values = SmallVec::<[Value<'data>; 16]>::with_capacity(self.columns.len());
        self.materialize_into(&mut values)?;
        Ok(values.into_vec())
    }

    /// Materialises the row into the provided buffer, reusing its capacity.
    ///
    /// # Errors
    ///
    /// Propagates decoding failures for individual cells.
    pub fn materialize_into(&self, values: &mut SmallVec<[Value<'data>; 16]>) -> Result<()> {
        values.clear();
        values.reserve(self.columns.len());
        for cell in self {
            let cell = cell?;
            values.push(cell.decode_value()?);
        }
        Ok(())
    }
}

impl<'data> StreamingCell<'data, '_> {
    #[must_use]
    pub const fn column_index(&self) -> u32 {
        self.column.index
    }

    #[must_use]
    pub const fn width(&self) -> usize {
        self.column.width
    }

    #[must_use]
    pub const fn kind(&self) -> ColumnKind {
        self.column.kind
    }

    #[must_use]
    pub const fn raw_slice(&self) -> &'data [u8] {
        self.slice
    }

    #[must_use]
    pub fn is_missing(&self) -> bool {
        match self.column.kind {
            ColumnKind::Character => is_blank(self.slice),
            ColumnKind::Numeric(_) => {
                let raw = numeric_bits(self.slice, self.endianness);
                numeric_bits_is_missing(raw)
            }
        }
    }

    /// Decodes the cell into a `Value`.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding fails (e.g. invalid metadata).
    pub fn decode_value(&self) -> Result<Value<'data>> {
        Ok(decode_value_inner(
            self.column.kind,
            self.column.raw_width,
            self.slice,
            self.encoding,
            self.endianness,
        ))
    }
}

impl<'row, 'data, 'meta> IntoIterator for &'row StreamingRow<'data, 'meta> {
    type Item = Result<StreamingCell<'data, 'meta>>;
    type IntoIter = StreamingRowIter<'row, 'data, 'meta>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct StreamingRowIter<'row, 'data, 'meta> {
    row: &'row StreamingRow<'data, 'meta>,
    index: usize,
}

impl<'data, 'meta> Iterator for StreamingRowIter<'_, 'data, 'meta> {
    type Item = Result<StreamingCell<'data, 'meta>>;

    fn next(&mut self) -> Option<Self::Item> {
        let column = self.row.columns.get(self.index)?;
        self.index += 1;
        if column.offset + column.width > self.row.data.len() {
            return Some(Err(Error::Corrupted {
                section: crate::error::Section::Column {
                    index: column.index,
                },
                details: Cow::from("column slice out of bounds"),
            }));
        }
        Some(Ok(StreamingCell {
            column,
            slice: &self.row.data[column.offset..column.offset + column.width],
            encoding: self.row.encoding,
            endianness: self.row.endianness,
        }))
    }
}
