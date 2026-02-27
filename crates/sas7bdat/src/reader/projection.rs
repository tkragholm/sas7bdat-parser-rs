use crate::{
    cell::CellValue,
    error::Result,
    iter_utils::next_from_result,
    parser::{CompiledRuntimeColumnRef, RowIterator},
};
use std::io::{Read, Seek};

pub struct ProjectedRowIter<'a, R: Read + Seek> {
    pub(crate) inner: RowIterator<'a, R>,
    pub(crate) compiled_columns: Vec<CompiledRuntimeColumnRef>,
    pub(crate) exhausted: bool,
}

impl<R: Read + Seek> ProjectedRowIter<'_, R> {
    /// Advances the projection iterator.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub fn try_next(&mut self) -> Result<Option<Vec<CellValue<'static>>>> {
        if self.exhausted {
            return Ok(None);
        }
        let maybe_row = match self
            .inner
            .try_next_projected_compiled_columns_owned(&self.compiled_columns)
        {
            Ok(value) => value,
            Err(err) => {
                self.exhausted = true;
                return Err(err);
            }
        };
        if let Some(row) = maybe_row {
            Ok(Some(row))
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
