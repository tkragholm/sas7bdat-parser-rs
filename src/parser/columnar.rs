use crate::metadata::Endianness;
use crate::parser::column::ColumnKind;

/// Column-oriented view over a slice of row buffers.
pub struct ColumnarBatch<'data, 'meta> {
    pub row_count: usize,
    rows: Vec<&'data [u8]>,
    columns: &'meta [RuntimeColumnRef],
    endianness: Endianness,
}

/// Lightweight copy of runtime column metadata used by columnar batches.
#[derive(Clone, Copy)]
pub struct RuntimeColumnRef {
    pub index: u32,
    pub offset: usize,
    pub width: usize,
    pub kind: ColumnKind,
}

impl<'data, 'meta> ColumnarBatch<'data, 'meta> {
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn new(
        rows: Vec<&'data [u8]>,
        columns: &'meta [RuntimeColumnRef],
        endianness: Endianness,
    ) -> Self {
        Self {
            row_count: rows.len(),
            rows,
            columns,
            endianness,
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    #[must_use]
    pub fn column(&self, index: usize) -> Option<ColumnarColumn<'_, 'data>> {
        let column = self.columns.get(index)?;
        Some(ColumnarColumn {
            column,
            rows: &self.rows,
            endianness: self.endianness,
        })
    }

    pub fn columns(&self) -> impl Iterator<Item = ColumnarColumn<'_, 'data>> {
        self.columns.iter().map(move |column| ColumnarColumn {
            column,
            rows: &self.rows,
            endianness: self.endianness,
        })
    }
    #[cfg(feature = "parallel-rows")]
    pub fn par_columns(&self) -> impl rayon::prelude::ParallelIterator<Item = ColumnarColumn<'_, 'data>> {
        use rayon::prelude::*;
        self.columns.par_iter().map(move |column| ColumnarColumn {
            column,
            rows: &self.rows,
            endianness: self.endianness,
        })
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<&'data [u8]> {
        self.rows
    }
}

pub struct ColumnarColumn<'meta, 'data> {
    column: &'meta RuntimeColumnRef,
    rows: &'meta [&'data [u8]],
    endianness: Endianness,
}

impl<'data> ColumnarColumn<'_, 'data> {
    #[must_use]
    pub const fn index(&self) -> u32 {
        self.column.index
    }

    #[must_use]
    pub const fn kind(&self) -> ColumnKind {
        self.column.kind
    }

    pub fn non_null_count(&self) -> u64 {
        match self.column.kind {
            ColumnKind::Numeric(_) => {
                let mut count = 0u64;
                for row in self.rows {
                    if let Some(slice) =
                        row.get(self.column.offset..self.column.offset + self.column.width)
                    {
                        let bits = load_numeric_bits(slice, self.endianness);
                        if !numeric_bits_is_missing(bits) {
                            count += 1;
                        }
                    }
                }
                count
            }
            ColumnKind::Character => {
                let mut count = 0u64;
                for row in self.rows {
                    if let Some(slice) =
                        row.get(self.column.offset..self.column.offset + self.column.width)
                    {
                        if !is_blank(slice) {
                            count += 1;
                        }
                    }
                }
                count
            }
        }
    }
}

#[inline]
fn load_numeric_bits(slice: &[u8], endian: Endianness) -> u64 {
    let mut buf = [0u8; 8];
    if slice.len() >= 8 {
        buf.copy_from_slice(&slice[..8]);
        match endian {
            Endianness::Little => u64::from_le_bytes(buf),
            Endianness::Big => u64::from_be_bytes(buf),
        }
    } else {
        match endian {
            Endianness::Big => {
                buf[..slice.len()].copy_from_slice(slice);
            }
            Endianness::Little => {
                for (idx, &byte) in slice.iter().rev().enumerate() {
                    buf[idx] = byte;
                }
            }
        }
        u64::from_be_bytes(buf)
    }
}

#[inline]
const fn numeric_bits_is_missing(raw: u64) -> bool {
    const EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
    const FRACTION_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
    (raw & EXP_MASK) == EXP_MASK && (raw & FRACTION_MASK) != 0
}

#[inline]
fn is_blank(slice: &[u8]) -> bool {
    !slice.iter().any(|&b| b != 0 && b != b' ')
}
