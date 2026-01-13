use super::{
    decode::{
        decode_string, is_blank, numeric_bits, numeric_bits_is_missing, sas_days_to_datetime,
        sas_seconds_to_datetime, sas_seconds_to_time, trim_trailing_space_or_nul_simd,
    },
    runtime_column::RuntimeColumnRef,
};
use crate::{
    dataset::Endianness,
    error::{Error, Result},
    parser::metadata::{ColumnKind, NumericKind},
};
use encoding_rs::{Encoding, UTF_8};
use hashbrown::{HashMap, hash_map::RawEntryMut};
use rustc_hash::FxHasher;
use simdutf8::basic;
use smallvec::SmallVec;
use std::{
    borrow::Cow,
    cell::{Ref, RefCell},
    convert::TryFrom,
    hash::BuildHasherDefault,
};

pub const COLUMNAR_BATCH_ROWS: usize = 256;
pub const COLUMNAR_INLINE_ROWS: usize = 32;
pub const STAGED_UTF8_DICTIONARY_LIMIT: usize = 2_048;
const SECONDS_PER_DAY_I64: i64 = 86_400;

pub struct ColumnarBatch<'rows> {
    pub row_count: usize,
    row_slices: SmallVec<[&'rows [u8]; COLUMNAR_INLINE_ROWS]>,
    columns: &'rows [RuntimeColumnRef],
    endianness: Endianness,
    encoding: &'static Encoding,
    typed_numeric: RefCell<Vec<Option<TypedNumericColumn>>>,
    utf8_staged: RefCell<Vec<Option<MaterializedUtf8Column>>>,
    stage_utf8: bool,
}

pub struct MaterializedColumn<T> {
    values: Vec<T>,
    def_levels: Vec<i16>,
}

pub enum TypedNumericColumn {
    Double(MaterializedColumn<f64>),
    Date(MaterializedColumn<i32>),
    DateTime(MaterializedColumn<i64>),
    Time(MaterializedColumn<i64>),
}

pub struct MaterializedUtf8Column {
    row_count: usize,
    def_levels: Vec<i16>,
    values: Vec<StagedUtf8Value>,
    dictionary: Vec<Vec<u8>>,
}

impl MaterializedUtf8Column {
    pub(crate) const fn new(
        row_count: usize,
        def_levels: Vec<i16>,
        values: Vec<StagedUtf8Value>,
        dictionary: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            row_count,
            def_levels,
            values,
            dictionary,
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    #[must_use]
    pub const fn non_null_count(&self) -> usize {
        self.values.len()
    }

    #[must_use]
    pub fn def_levels(&self) -> &[i16] {
        &self.def_levels
    }

    #[must_use]
    pub fn dictionary(&self) -> &[Vec<u8>] {
        &self.dictionary
    }

    #[must_use]
    pub fn values(&self) -> &[StagedUtf8Value] {
        &self.values
    }
}

#[derive(Debug)]
pub enum StagedUtf8Value {
    Dictionary(u32),
    Inline(Vec<u8>),
}

impl<'rows> ColumnarBatch<'rows> {
    #[must_use]
    pub(crate) fn new(
        row_slices: SmallVec<[&'rows [u8]; COLUMNAR_INLINE_ROWS]>,
        columns: &'rows [RuntimeColumnRef],
        endianness: Endianness,
        encoding: &'static Encoding,
        stage_utf8: bool,
    ) -> Self {
        let row_count = row_slices.len();
        let mut typed_numeric = Vec::with_capacity(columns.len());
        typed_numeric.resize_with(columns.len(), || None);
        let mut utf8_staged = Vec::with_capacity(columns.len());
        utf8_staged.resize_with(columns.len(), || None);
        Self {
            row_count,
            row_slices,
            columns,
            endianness,
            encoding,
            typed_numeric: RefCell::new(typed_numeric),
            utf8_staged: RefCell::new(utf8_staged),
            stage_utf8,
        }
    }

    pub fn truncate_front(&mut self, rows: usize) {
        if rows >= self.row_count {
            self.row_slices.clear();
            self.row_count = 0;
            return;
        }
        self.row_slices.drain(0..rows);
        self.row_count -= rows;
    }

    pub fn truncate(&mut self, rows: usize) {
        if rows >= self.row_count {
            return;
        }
        self.row_slices.truncate(rows);
        self.row_count = rows;
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    #[must_use]
    pub fn column(&self, index: usize) -> Option<ColumnarColumn<'_, 'rows>> {
        let column = self.columns.get(index)?;
        Some(ColumnarColumn {
            column,
            rows: self.row_slices.as_slice(),
            endianness: self.endianness,
            encoding: self.encoding,
        })
    }

    #[must_use]
    pub const fn encoding(&self) -> &'static Encoding {
        self.encoding
    }

    /// Materialises a numeric column into a typed buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding numeric values fails.
    ///
    /// # Panics
    ///
    /// Panics if the cache slot is unexpectedly empty after insertion.
    pub fn materialize_numeric(&self, index: usize) -> Result<Option<Ref<'_, TypedNumericColumn>>> {
        let Some(column) = self.columns.get(index) else {
            return Ok(None);
        };
        let ColumnKind::Numeric(kind) = column.kind else {
            return Ok(None);
        };

        {
            let cache = self.typed_numeric.borrow();
            if let Some(Some(_)) = cache.get(index) {
                return Ok(Some(Ref::map(cache, |vec| {
                    vec[index].as_ref().expect("typed numeric missing")
                })));
            }
        }

        let materialized = self.materialize_numeric_column(index, kind)?;
        self.typed_numeric.borrow_mut()[index] = Some(materialized);
        let cache = self.typed_numeric.borrow();
        Ok(Some(Ref::map(cache, |vec| {
            vec[index].as_ref().expect("typed numeric missing")
        })))
    }

    /// Materialises a character column into staged UTF-8 buffers.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding string values fails.
    ///
    /// # Panics
    ///
    /// Panics if the cache slot is unexpectedly empty after insertion.
    pub fn materialize_utf8(
        &self,
        index: usize,
    ) -> Result<Option<Ref<'_, MaterializedUtf8Column>>> {
        if !self.stage_utf8 {
            return Ok(None);
        }

        let Some(column) = self.columns.get(index) else {
            return Ok(None);
        };
        let ColumnKind::Character = column.kind else {
            return Ok(None);
        };

        {
            let cache = self.utf8_staged.borrow();
            if let Some(Some(_)) = cache.get(index) {
                return Ok(Some(Ref::map(cache, |vec| {
                    vec[index].as_ref().expect("staged utf8 missing")
                })));
            }
        }

        let materialized = self.materialize_utf8_column(index);
        self.utf8_staged.borrow_mut()[index] = Some(materialized);
        let cache = self.utf8_staged.borrow();
        Ok(Some(Ref::map(cache, |vec| {
            vec[index].as_ref().expect("staged utf8 missing")
        })))
    }

    fn materialize_numeric_column(
        &self,
        index: usize,
        kind: NumericKind,
    ) -> Result<TypedNumericColumn> {
        let column = self.column(index).expect("column index out of bounds");
        match kind {
            NumericKind::Double => Ok(TypedNumericColumn::Double(Self::materialize_f64(&column))),
            NumericKind::Date => Ok(TypedNumericColumn::Date(Self::materialize_date(&column)?)),
            NumericKind::DateTime => Ok(TypedNumericColumn::DateTime(Self::materialize_datetime(
                &column,
            )?)),
            NumericKind::Time => Ok(TypedNumericColumn::Time(Self::materialize_time(&column)?)),
        }
    }

    fn materialize_f64(column: &ColumnarColumn<'_, '_>) -> MaterializedColumn<f64> {
        Self::materialize_numeric_mapped(column, |value| value)
    }

    fn materialize_date(column: &ColumnarColumn<'_, '_>) -> Result<MaterializedColumn<i32>> {
        Self::materialize_numeric_result(column, |days| {
            let datetime = sas_days_to_datetime(days).ok_or_else(|| Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "column '{}' contains date outside supported range",
                    column.index()
                )),
            })?;
            let seconds = datetime.unix_timestamp();
            let day = seconds.div_euclid(SECONDS_PER_DAY_I64);
            i32::try_from(day).map_err(|_| Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "column '{}' contains date outside Parquet range",
                    column.index()
                )),
            })
        })
    }

    fn materialize_numeric_mapped<T>(
        column: &ColumnarColumn<'_, '_>,
        mut map: impl FnMut(f64) -> T,
    ) -> MaterializedColumn<T> {
        Self::materialize_numeric_result(column, |value| Ok(map(value)))
            .expect("infallible numeric mapping")
    }

    fn materialize_numeric_result<T>(
        column: &ColumnarColumn<'_, '_>,
        mut map: impl FnMut(f64) -> Result<T>,
    ) -> Result<MaterializedColumn<T>> {
        let mut values = Vec::with_capacity(column.len());
        let mut def_levels = Vec::with_capacity(column.len());
        for maybe_bits in column.iter_numeric_bits() {
            if let Some(bits) = maybe_bits {
                let seconds = f64::from_bits(bits);
                let value = map(seconds)?;
                def_levels.push(1);
                values.push(value);
            } else {
                def_levels.push(0);
            }
        }
        let materialized = MaterializedColumn { values, def_levels };
        debug_assert!(materialized.values.len() <= materialized.def_levels.len());
        Ok(materialized)
    }

    fn materialize_i64_mapped(
        column: &ColumnarColumn<'_, '_>,
        map: impl FnMut(f64) -> Result<i64>,
    ) -> Result<MaterializedColumn<i64>> {
        Self::materialize_numeric_result(column, map)
    }

    fn materialize_datetime(column: &ColumnarColumn<'_, '_>) -> Result<MaterializedColumn<i64>> {
        Self::materialize_i64_mapped(column, |seconds| {
            let datetime =
                sas_seconds_to_datetime(seconds).ok_or_else(|| Error::InvalidMetadata {
                    details: Cow::Owned(format!(
                        "column '{}' contains timestamp outside supported range",
                        column.index()
                    )),
                })?;
            let micros = datetime.unix_timestamp_nanos().div_euclid(1_000);
            i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "column '{}' contains timestamp outside Parquet range",
                    column.index()
                )),
            })
        })
    }

    fn materialize_time(column: &ColumnarColumn<'_, '_>) -> Result<MaterializedColumn<i64>> {
        Self::materialize_i64_mapped(column, |seconds| {
            let duration = sas_seconds_to_time(seconds).ok_or_else(|| Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "column '{}' contains time outside supported range",
                    column.index()
                )),
            })?;
            let micros = duration.whole_microseconds();
            i64::try_from(micros).map_err(|_| Error::InvalidMetadata {
                details: Cow::Owned(format!(
                    "column '{}' contains time outside Parquet range",
                    column.index()
                )),
            })
        })
    }

    fn materialize_utf8_column(&self, index: usize) -> MaterializedUtf8Column {
        let column = self.column(index).expect("column index out of bounds");
        let row_count = column.len();
        let mut def_levels = Vec::with_capacity(row_count);
        let mut values = Vec::with_capacity(row_count);
        let mut dictionary = Vec::with_capacity(STAGED_UTF8_DICTIONARY_LIMIT.min(row_count));
        let mut dictionary_lookup: HashMap<Vec<u8>, u32, BuildHasherDefault<FxHasher>> =
            HashMap::with_capacity_and_hasher(
                STAGED_UTF8_DICTIONARY_LIMIT.min(row_count),
                BuildHasherDefault::<FxHasher>::default(),
            );
        let mut dictionary_enabled = true;
        let mut non_null_count = 0usize;
        let high_card_sample = 256usize.min(row_count);
        let mut unique_sample = 0usize;

        for row_index in 0..row_count {
            let Some(raw) = column.raw_cell(row_index) else {
                def_levels.push(0);
                continue;
            };
            let trimmed = trim_trailing_space_or_nul_simd(raw);
            if trimmed.is_empty() {
                def_levels.push(0);
                continue;
            }

            let bytes: Cow<'_, [u8]> = if self.encoding == UTF_8 {
                match basic::from_utf8(trimmed) {
                    Ok(_) => Cow::Borrowed(trimmed),
                    Err(_) => Cow::Owned(
                        decode_string(trimmed, self.encoding)
                            .into_owned()
                            .into_bytes(),
                    ),
                }
            } else {
                Cow::Owned(
                    decode_string(trimmed, self.encoding)
                        .into_owned()
                        .into_bytes(),
                )
            };

            def_levels.push(1);
            non_null_count = non_null_count.saturating_add(1);
            let bytes = bytes.as_ref();

            if dictionary_enabled {
                if non_null_count <= high_card_sample {
                    if dictionary_lookup.len() > unique_sample {
                        unique_sample = dictionary_lookup.len();
                    }
                    if non_null_count == high_card_sample && unique_sample * 4 >= non_null_count * 3
                    {
                        dictionary_lookup.clear();
                        dictionary_enabled = false;
                        values.push(StagedUtf8Value::Inline(bytes.to_vec()));
                        continue;
                    }
                }

                if dictionary_lookup.len() >= STAGED_UTF8_DICTIONARY_LIMIT {
                    dictionary_lookup.clear();
                    dictionary_enabled = false;
                    values.push(StagedUtf8Value::Inline(bytes.to_vec()));
                    continue;
                }
                match dictionary_lookup.raw_entry_mut().from_key(bytes) {
                    RawEntryMut::Occupied(entry) => {
                        values.push(StagedUtf8Value::Dictionary(*entry.get()));
                    }
                    RawEntryMut::Vacant(vacant) => {
                        let dict_index = dictionary.len();
                        let Ok(dict_index) = u32::try_from(dict_index) else {
                            dictionary_enabled = false;
                            dictionary_lookup.clear();
                            values.push(StagedUtf8Value::Inline(bytes.to_vec()));
                            continue;
                        };
                        let owned = bytes.to_vec();
                        vacant.insert(owned.clone(), dict_index);
                        dictionary.push(owned);
                        values.push(StagedUtf8Value::Dictionary(dict_index));
                    }
                }
            } else {
                values.push(StagedUtf8Value::Inline(bytes.to_vec()));
            }
        }

        MaterializedUtf8Column::new(row_count, def_levels, values, dictionary)
    }
}

pub struct ColumnarColumn<'batch, 'rows> {
    column: &'batch RuntimeColumnRef,
    rows: &'batch [&'rows [u8]],
    endianness: Endianness,
    encoding: &'static Encoding,
}

impl ColumnarColumn<'_, '_> {
    #[must_use]
    pub const fn index(&self) -> u32 {
        self.column.index
    }

    #[must_use]
    pub const fn kind(&self) -> ColumnKind {
        self.column.kind
    }

    fn row_slice(&self, row_index: usize) -> Option<&[u8]> {
        self.rows.get(row_index).copied()
    }

    #[inline]
    fn column_slice<'a>(&self, row: &'a [u8]) -> Option<&'a [u8]> {
        row.get(self.column.offset..self.column.offset + self.column.width)
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[must_use]
    pub fn raw_cell(&self, row_index: usize) -> Option<&[u8]> {
        self.row_slice(row_index)
            .and_then(|row| self.column_slice(row))
    }

    #[must_use]
    pub const fn endianness(&self) -> Endianness {
        self.endianness
    }

    #[must_use]
    pub const fn encoding(&self) -> &'static Encoding {
        self.encoding
    }

    #[must_use]
    pub const fn raw_width(&self) -> u32 {
        self.column.raw_width
    }

    pub fn iter_strings(&self) -> impl Iterator<Item = Option<Cow<'_, str>>> {
        self.iter_strings_range(0, self.rows.len())
    }

    pub fn iter_strings_range(
        &self,
        start: usize,
        len: usize,
    ) -> impl Iterator<Item = Option<Cow<'_, str>>> {
        let end = start.saturating_add(len).min(self.rows.len());
        self.iter_string_indices(start..end)
    }

    fn iter_string_indices(
        &self,
        range: std::ops::Range<usize>,
    ) -> impl Iterator<Item = Option<Cow<'_, str>>> + '_ {
        range.map(move |idx| {
            self.row_slice(idx).and_then(|row| {
                self.column_slice(row).and_then(|slice| {
                    if is_blank(slice) {
                        None
                    } else {
                        Some(decode_string(slice, self.encoding))
                    }
                })
            })
        })
    }

    pub fn iter_numeric_bits(&self) -> impl Iterator<Item = Option<u64>> + '_ {
        self.iter_numeric_bits_range(0, self.rows.len())
    }

    pub fn iter_numeric_bits_range(
        &self,
        start: usize,
        len: usize,
    ) -> impl Iterator<Item = Option<u64>> + '_ {
        let end = start.saturating_add(len).min(self.rows.len());
        (start..end).map(move |idx| {
            self.row_slice(idx)
                .and_then(|row| self.column_slice(row))
                .map(|slice| numeric_bits(slice, self.endianness))
                .and_then(|bits| {
                    if numeric_bits_is_missing(bits) {
                        None
                    } else {
                        Some(bits)
                    }
                })
        })
    }
}
