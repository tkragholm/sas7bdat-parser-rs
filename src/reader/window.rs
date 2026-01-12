use std::io::{Read, Seek};

use crate::cell::CellValue;
use crate::error::Result;
use crate::parser::RowIterator;

use super::projection::ProjectedRowIter;

pub struct RowWindow<'a, R: Read + Seek>(RowWindowInner<RowIterator<'a, R>>);

pub struct ProjectedRowWindow<'a, R: Read + Seek>(RowWindowInner<ProjectedRowIter<'a, R>>);

trait SkippableRows {
    fn advance(&mut self) -> Result<bool>;
}

impl<R: Read + Seek> SkippableRows for RowIterator<'_, R> {
    fn advance(&mut self) -> Result<bool> {
        Ok(self.try_next()?.is_some())
    }
}

impl<R: Read + Seek> SkippableRows for ProjectedRowIter<'_, R> {
    fn advance(&mut self) -> Result<bool> {
        Ok(self.try_next()?.is_some())
    }
}

trait RowSource: SkippableRows {
    type Row<'a>
    where
        Self: 'a;

    fn next_row(&mut self) -> Result<Option<Self::Row<'_>>>;
}

impl<R: Read + Seek> RowSource for RowIterator<'_, R> {
    type Row<'s>
        = Vec<CellValue<'s>>
    where
        Self: 's;

    fn next_row(&mut self) -> Result<Option<Self::Row<'_>>> {
        self.try_next()
    }
}

impl<R: Read + Seek> RowSource for ProjectedRowIter<'_, R> {
    type Row<'s>
        = Vec<CellValue<'static>>
    where
        Self: 's;

    fn next_row(&mut self) -> Result<Option<Self::Row<'_>>> {
        self.try_next()
    }
}

struct RowWindowState<I> {
    inner: I,
    skip_remaining: u64,
    remaining: Option<u64>,
    skipped: bool,
}

impl<I> RowWindowState<I> {
    const fn new(inner: I, skip: u64, remaining: Option<u64>) -> Self {
        Self {
            inner,
            skip_remaining: skip,
            remaining,
            skipped: skip == 0,
        }
    }
}

impl<I: SkippableRows> RowWindowState<I> {
    fn consume_skip(&mut self) -> Result<Option<()>> {
        consume_skip_helper(&mut self.skip_remaining, &mut self.skipped, &mut self.inner)
    }
}

impl<I: RowSource> RowWindowState<I> {
    fn try_next(&mut self) -> Result<Option<I::Row<'_>>> {
        if !self.skipped && self.consume_skip()?.is_none() {
            return Ok(None);
        }
        fetch_with_remaining(&mut self.remaining, self.inner.next_row())
    }
}

struct RowWindowInner<I> {
    state: RowWindowState<I>,
}

impl<I> RowWindowInner<I> {
    const fn new(inner: I, skip: u64, remaining: Option<u64>) -> Self {
        Self {
            state: RowWindowState::new(inner, skip, remaining),
        }
    }
}

impl<I: RowSource> RowWindowInner<I> {
    fn try_next(&mut self) -> Result<Option<I::Row<'_>>> {
        self.state.try_next()
    }
}

macro_rules! impl_row_window {
    ($name:ident => $inner:ty, $row:ty) => {
        impl<'a, R: Read + Seek> $name<'a, R> {
            pub(super) const fn new(inner: $inner, skip: u64, remaining: Option<u64>) -> Self {
                Self(RowWindowInner::new(inner, skip, remaining))
            }

            /// Advances the iterator by one row.
            ///
            /// # Errors
            ///
            /// Returns an error if row decoding fails.
            #[cfg_attr(feature = "hotpath", hotpath::measure)]
            pub fn try_next(&mut self) -> Result<Option<$row>> {
                self.0.try_next()
            }
        }
    };
}

impl_row_window!(RowWindow => RowIterator<'a, R>, Vec<CellValue<'_>>);
impl_row_window!(
    ProjectedRowWindow => ProjectedRowIter<'a, R>,
    Vec<CellValue<'static>>
);

impl<R: Read + Seek> Iterator for RowWindow<'_, R> {
    type Item = Result<Vec<CellValue<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        map_next(self.try_next(), |row| {
            row.into_iter().map(CellValue::into_owned).collect()
        })
    }
}

impl<R: Read + Seek> Iterator for ProjectedRowWindow<'_, R> {
    type Item = Result<Vec<CellValue<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        map_next(self.try_next(), |row| row)
    }
}

fn map_next<T, F>(
    result: Result<Option<T>>,
    mut map_row: F,
) -> Option<Result<Vec<CellValue<'static>>>>
where
    F: FnMut(T) -> Vec<CellValue<'static>>,
{
    match result {
        Ok(Some(row)) => Some(Ok(map_row(row))),
        Ok(None) => None,
        Err(err) => Some(Err(err)),
    }
}

fn fetch_with_remaining<T>(
    remaining: &mut Option<u64>,
    row: Result<Option<T>>,
) -> Result<Option<T>> {
    if matches!(remaining, Some(0)) {
        return Ok(None);
    }
    let row = row?;
    row.map_or_else(
        || Ok(None),
        |row| {
            if let Some(rem) = remaining.as_mut() {
                *rem = rem.saturating_sub(1);
            }
            Ok(Some(row))
        },
    )
}

fn consume_skip_helper<S: SkippableRows>(
    skip_remaining: &mut u64,
    skipped: &mut bool,
    source: &mut S,
) -> Result<Option<()>> {
    while *skip_remaining > 0 {
        if source.advance()? {
            *skip_remaining = skip_remaining.saturating_sub(1);
        } else {
            *skipped = true;
            return Ok(None);
        }
    }
    *skipped = true;
    Ok(Some(()))
}
