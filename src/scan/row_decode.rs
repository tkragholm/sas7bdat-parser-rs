use super::{
    Arc, CompiledColumnPlan, CompiledDecodeKernel, DateNumericValue, DateTimeNumericValue,
    DecodeMode, Encoding, Endianness, Error, OwnedCellMaterializationKind, Result, SasDate,
    SasDateTime, SasTime, ScanBuilder, StringDecodeOptions, TimeNumericValue, TypedNumericValue,
    UTF_8, Utf8ValidationMode, classify_date_numeric_value, classify_datetime_numeric_value,
    classify_time_numeric_value, classify_typed_numeric_value, compile_column_plan,
    compile_compiled_projection_column_plan, compile_owned_materialization_kind,
    compile_string_decode_kernel, decode_numeric_cell, is_blank_after_trim_mode,
    maybe_fix_mojibake, mojibake_fix_maybe_needed_for_encoded_bytes, numeric_bits,
    resolve_encoding, trim_and_classify_for_mode,
};
use bstr::ByteSlice;

#[derive(Debug)]
pub(super) struct RowDecodePlan {
    pub(super) columns: Vec<CompiledColumnPlan>,
    pub(super) owned_kinds: Vec<OwnedCellMaterializationKind>,
    pub(super) max_end: usize,
    pub(super) names: Arc<[String]>,
    pub(super) encoding: &'static Encoding,
    pub(super) string_kernel: StringDecodeKernel,
    pub(super) decode_mode: DecodeMode,
    pub(super) string_options: StringDecodeOptions,
    pub(super) endianness: Endianness,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum PlannedCell<'a> {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    StrBorrowed(&'a str),
    StrOwned(usize),
    Bytes(&'a [u8]),
    Date(SasDate),
    DateTime(SasDateTime),
    Time(SasTime),
}

#[derive(Debug)]
pub(super) enum DecodedUtf8BatchValue<'a> {
    Borrowed(&'a [u8]),
    Scratch,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TrimmedString<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) is_ascii: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum StringDecodeKernel {
    Utf8Strict,
    Utf8Lenient,
    EncodedStrict,
    EncodedLenient,
}

impl RowDecodePlan {
    pub(super) fn new(builder: &ScanBuilder<'_>) -> Result<Self> {
        let names: Arc<[String]> = builder.projection.map_or_else(
            || {
                Arc::from(
                    builder
                        .ds
                        .layout
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect::<Vec<_>>(),
                )
            },
            |projection| Arc::clone(&projection.names),
        );

        let encoding = resolve_encoding(builder.ds.metadata.encoding.as_deref());
        let (columns, max_end) = builder.projection.map_or_else(
            || {
                let columns = builder
                    .ds
                    .layout
                    .columns
                    .iter()
                    .map(|column| compile_column_plan(builder, column))
                    .collect::<Result<Vec<_>>>()?;
                let max_end = columns.iter().map(|column| column.end).max().unwrap_or(0);
                Ok((columns, max_end))
            },
            |projection| {
                Ok((
                    projection
                        .inner
                        .columns
                        .iter()
                        .map(|column| compile_compiled_projection_column_plan(builder, column))
                        .collect::<Vec<_>>(),
                    usize::from(projection.inner.max_end),
                ))
            },
        )?;
        let owned_kinds = columns
            .iter()
            .map(|column| compile_owned_materialization_kind(column.kernel))
            .collect();

        Ok(Self {
            columns,
            owned_kinds,
            max_end,
            names,
            encoding,
            string_kernel: compile_string_decode_kernel(encoding, builder.string_options),
            decode_mode: builder.decode,
            string_options: builder.string_options,
            endianness: builder.ds.layout.header.endianness,
        })
    }

    pub(super) fn plan_cells<'row>(
        &self,
        row: &'row [u8],
        owned_strings: &mut Vec<String>,
    ) -> Result<Vec<PlannedCell<'row>>> {
        let mut planned = Vec::with_capacity(self.columns.len());
        self.plan_cells_into(row, owned_strings, &mut planned)?;
        Ok(planned)
    }

    pub(super) fn plan_cells_into<'row>(
        &self,
        row: &'row [u8],
        owned_strings: &mut Vec<String>,
        planned: &mut Vec<PlannedCell<'row>>,
    ) -> Result<()> {
        self.validate_row_bounds(row)?;
        owned_strings.clear();
        planned.clear();
        if planned.capacity() < self.columns.len() {
            planned.reserve(self.columns.len() - planned.capacity());
        }
        for column in &self.columns {
            planned.push(self.plan_cell_in_bounds(row, column, owned_strings)?);
        }
        Ok(())
    }

    pub(super) fn validate_row_bounds(&self, row: &[u8]) -> Result<()> {
        if row.len() < self.max_end {
            return Err(Error::unsupported("column slice exceeds row bounds"));
        }
        Ok(())
    }

    pub(super) fn slice_in_bounds<'a>(row: &'a [u8], column: &CompiledColumnPlan) -> &'a [u8] {
        debug_assert!(row.len() >= column.end);
        &row[column.start..column.end]
    }

    pub(super) fn plan_cell_in_bounds<'row>(
        &self,
        row: &'row [u8],
        column: &CompiledColumnPlan,
        owned_strings: &mut Vec<String>,
    ) -> Result<PlannedCell<'row>> {
        let slice = Self::slice_in_bounds(row, column);

        match column.kernel {
            CompiledDecodeKernel::Utf8 => self.plan_string(slice, owned_strings),
            CompiledDecodeKernel::RawBytes => Ok(PlannedCell::Bytes(slice)),
            CompiledDecodeKernel::Date => Ok(self.plan_numeric_date(slice)),
            CompiledDecodeKernel::DateAsNumeric
            | CompiledDecodeKernel::Integer
            | CompiledDecodeKernel::Float
            | CompiledDecodeKernel::DateTimeAsNumeric
            | CompiledDecodeKernel::TimeAsNumeric => self.plan_numeric_value(slice, column.width),
            CompiledDecodeKernel::DateTime => Ok(self.plan_numeric_datetime(slice)),
            CompiledDecodeKernel::Time => Ok(self.plan_numeric_time(slice)),
            CompiledDecodeKernel::NumericLossless => Ok(self.plan_numeric_lossless(slice)),
        }
    }

    pub(super) fn plan_string<'row>(
        &self,
        slice: &'row [u8],
        owned_strings: &mut Vec<String>,
    ) -> Result<PlannedCell<'row>> {
        if matches!(self.decode_mode, DecodeMode::TypedLossless) {
            return Ok(PlannedCell::Bytes(slice));
        }

        let trimmed = trim_and_classify_for_mode(slice, self.string_options.trim_mode);
        let slice = trimmed.bytes;
        if slice.is_empty() || is_blank_after_trim_mode(slice, self.string_options.trim_mode) {
            return Ok(PlannedCell::StrBorrowed(""));
        }

        if trimmed.is_ascii {
            return Ok(PlannedCell::StrBorrowed(
                std::str::from_utf8(slice).expect("ASCII is valid UTF-8"),
            ));
        }

        self.decode_trimmed_string(slice, owned_strings)
    }

    #[inline]
    pub(super) fn decode_string_bytes_for_batch_borrowed<'row>(
        &self,
        slice: &'row [u8],
    ) -> Result<&'row [u8]> {
        let trimmed = trim_and_classify_for_mode(slice, self.string_options.trim_mode);
        let slice = trimmed.bytes;
        if slice.is_empty()
            || is_blank_after_trim_mode(slice, self.string_options.trim_mode)
            || trimmed.is_ascii
        {
            return Ok(slice);
        }

        match self.string_kernel {
            StringDecodeKernel::Utf8Strict => match std::str::from_utf8(slice) {
                Ok(_) => Ok(slice),
                Err(_) => Err(Error::Decode(crate::error::DecodeError {
                    message: "invalid UTF-8 in fixed-width string cell".to_owned(),
                })),
            },
            _ => Err(Error::unsupported(
                "borrowed-only batch string decode requires strict UTF-8 kernel",
            )),
        }
    }

    #[inline]
    pub(super) fn decode_utf8_lenient_trimmed_bytes_for_batch_direct<'row>(
        &self,
        trimmed: TrimmedString<'row>,
        scratch: &mut String,
    ) -> DecodedUtf8BatchValue<'row> {
        let slice = trimmed.bytes;
        if slice.is_empty() || trimmed.is_ascii {
            return DecodedUtf8BatchValue::Borrowed(slice);
        }

        if std::str::from_utf8(slice).is_ok() {
            DecodedUtf8BatchValue::Borrowed(slice)
        } else {
            *scratch = maybe_fix_mojibake(
                slice.to_str_lossy().into_owned(),
                self.string_options.mojibake_fix,
            );
            DecodedUtf8BatchValue::Scratch
        }
    }

    #[inline]
    pub(super) fn decode_encoded_strict_trimmed_bytes_for_batch_direct<'row>(
        &self,
        trimmed: TrimmedString<'row>,
        scratch: &mut String,
    ) -> Result<DecodedUtf8BatchValue<'row>> {
        let slice = trimmed.bytes;
        if slice.is_empty() || trimmed.is_ascii {
            return Ok(DecodedUtf8BatchValue::Borrowed(slice));
        }

        let (decoded, had_errors) = self.encoding.decode_without_bom_handling(slice);
        if had_errors {
            return Err(Error::Decode(crate::error::DecodeError {
                message: "string decode failed under strict validation".to_owned(),
            }));
        }
        match decoded {
            std::borrow::Cow::Borrowed(value) => {
                Ok(DecodedUtf8BatchValue::Borrowed(value.as_bytes()))
            }
            std::borrow::Cow::Owned(value) => {
                *scratch = if mojibake_fix_maybe_needed_for_encoded_bytes(
                    self.encoding,
                    slice,
                    self.string_options.mojibake_fix,
                ) {
                    maybe_fix_mojibake(value, self.string_options.mojibake_fix)
                } else {
                    value
                };
                Ok(DecodedUtf8BatchValue::Scratch)
            }
        }
    }

    #[inline]
    pub(super) fn decode_encoded_lenient_trimmed_bytes_for_batch_direct<'row>(
        &self,
        trimmed: TrimmedString<'row>,
        scratch: &mut String,
    ) -> DecodedUtf8BatchValue<'row> {
        let slice = trimmed.bytes;
        if slice.is_empty() || trimmed.is_ascii {
            return DecodedUtf8BatchValue::Borrowed(slice);
        }

        let (decoded, _) = self.encoding.decode_without_bom_handling(slice);
        match decoded {
            std::borrow::Cow::Borrowed(value) => DecodedUtf8BatchValue::Borrowed(value.as_bytes()),
            std::borrow::Cow::Owned(value) => {
                *scratch = if mojibake_fix_maybe_needed_for_encoded_bytes(
                    self.encoding,
                    slice,
                    self.string_options.mojibake_fix,
                ) {
                    maybe_fix_mojibake(value, self.string_options.mojibake_fix)
                } else {
                    value
                };
                DecodedUtf8BatchValue::Scratch
            }
        }
    }

    pub(super) fn decode_trimmed_string<'row>(
        &self,
        slice: &'row [u8],
        owned_strings: &mut Vec<String>,
    ) -> Result<PlannedCell<'row>> {
        match self.string_kernel {
            StringDecodeKernel::Utf8Strict => std::str::from_utf8(slice).map_or_else(
                |_| {
                    Err(Error::Decode(crate::error::DecodeError {
                        message: "invalid UTF-8 in fixed-width string cell".to_owned(),
                    }))
                },
                |value| Ok(PlannedCell::StrBorrowed(value)),
            ),
            StringDecodeKernel::Utf8Lenient => std::str::from_utf8(slice).map_or_else(
                |_| {
                    owned_strings.push(maybe_fix_mojibake(
                        slice.to_str_lossy().into_owned(),
                        self.string_options.mojibake_fix,
                    ));
                    Ok(PlannedCell::StrOwned(owned_strings.len() - 1))
                },
                |value| Ok(PlannedCell::StrBorrowed(value)),
            ),
            StringDecodeKernel::EncodedStrict => {
                let (decoded, had_errors) = self.encoding.decode_without_bom_handling(slice);
                if had_errors {
                    return Err(Error::Decode(crate::error::DecodeError {
                        message: "string decode failed under strict validation".to_owned(),
                    }));
                }
                match decoded {
                    std::borrow::Cow::Borrowed(value) => Ok(PlannedCell::StrBorrowed(value)),
                    std::borrow::Cow::Owned(value) => {
                        Ok(self.push_owned_string(owned_strings, slice, value))
                    }
                }
            }
            StringDecodeKernel::EncodedLenient => {
                let (decoded, _) = self.encoding.decode_without_bom_handling(slice);
                match decoded {
                    std::borrow::Cow::Borrowed(value) => Ok(PlannedCell::StrBorrowed(value)),
                    std::borrow::Cow::Owned(value) => {
                        Ok(self.push_owned_string(owned_strings, slice, value))
                    }
                }
            }
        }
    }

    fn push_owned_string<'row>(
        &self,
        owned_strings: &mut Vec<String>,
        slice: &[u8],
        value: String,
    ) -> PlannedCell<'row> {
        owned_strings.push(
            if mojibake_fix_maybe_needed_for_encoded_bytes(
                self.encoding,
                slice,
                self.string_options.mojibake_fix,
            ) {
                maybe_fix_mojibake(value, self.string_options.mojibake_fix)
            } else {
                value
            },
        );
        PlannedCell::StrOwned(owned_strings.len() - 1)
    }

    pub(super) fn plan_numeric_value<'row>(
        &self,
        slice: &[u8],
        width: u32,
    ) -> Result<PlannedCell<'row>> {
        match self.decode_mode {
            DecodeMode::Typed => Ok(
                match classify_typed_numeric_value(
                    decode_numeric_cell(slice, self.endianness),
                    width <= 4,
                ) {
                    TypedNumericValue::Null => PlannedCell::Null,
                    TypedNumericValue::Int32(value) => PlannedCell::Int32(value),
                    TypedNumericValue::Int64(value) => PlannedCell::Int64(value),
                    TypedNumericValue::Float64(value) => PlannedCell::Float64(value),
                },
            ),
            DecodeMode::TypedLossless => Ok(self.plan_numeric_lossless(slice)),
            DecodeMode::Raw => Err(Error::unsupported(
                "visit_rows does not support DecodeMode::Raw; use visit_raw_rows instead",
            )),
        }
    }

    pub(super) fn plan_numeric_date<'row>(&self, slice: &[u8]) -> PlannedCell<'row> {
        match classify_date_numeric_value(decode_numeric_cell(slice, self.endianness)) {
            DateNumericValue::Null => PlannedCell::Null,
            DateNumericValue::Date(value) => PlannedCell::Date(value),
            DateNumericValue::Float64(value) => PlannedCell::Float64(value),
        }
    }

    pub(super) fn plan_numeric_datetime<'row>(&self, slice: &[u8]) -> PlannedCell<'row> {
        match classify_datetime_numeric_value(decode_numeric_cell(slice, self.endianness)) {
            DateTimeNumericValue::Null => PlannedCell::Null,
            DateTimeNumericValue::DateTime(value) => PlannedCell::DateTime(value),
            DateTimeNumericValue::Float64(value) => PlannedCell::Float64(value),
        }
    }

    pub(super) fn plan_numeric_time<'row>(&self, slice: &[u8]) -> PlannedCell<'row> {
        match classify_time_numeric_value(decode_numeric_cell(slice, self.endianness)) {
            TimeNumericValue::Null => PlannedCell::Null,
            TimeNumericValue::Time(value) => PlannedCell::Time(value),
            TimeNumericValue::Float64(value) => PlannedCell::Float64(value),
        }
    }

    pub(super) fn materialize_owned_cell_fast(
        &self,
        row: &[u8],
        column: &CompiledColumnPlan,
        kind: OwnedCellMaterializationKind,
    ) -> Result<crate::row::OwnedCellValue> {
        let slice = Self::slice_in_bounds(row, column);
        match kind {
            OwnedCellMaterializationKind::Utf8 => self.materialize_owned_string_typed(slice),
            OwnedCellMaterializationKind::RawBytes => {
                Ok(crate::row::OwnedCellValue::Bytes(slice.to_vec()))
            }
            OwnedCellMaterializationKind::Date => Ok(self.materialize_owned_date(slice)),
            OwnedCellMaterializationKind::DateAsNumeric
            | OwnedCellMaterializationKind::NumericTyped
            | OwnedCellMaterializationKind::DateTimeAsNumeric
            | OwnedCellMaterializationKind::TimeAsNumeric => {
                Ok(self.materialize_owned_numeric_typed(slice, column.width))
            }
            OwnedCellMaterializationKind::DateTime => Ok(self.materialize_owned_datetime(slice)),
            OwnedCellMaterializationKind::Time => Ok(self.materialize_owned_time(slice)),
            OwnedCellMaterializationKind::NumericLossless => {
                Ok(self.materialize_owned_numeric_lossless(slice))
            }
        }
    }

    pub(super) fn materialize_owned_string_typed(
        &self,
        slice: &[u8],
    ) -> Result<crate::row::OwnedCellValue> {
        let trimmed = trim_and_classify_for_mode(slice, self.string_options.trim_mode);
        let slice = trimmed.bytes;

        if slice.is_empty() || is_blank_after_trim_mode(slice, self.string_options.trim_mode) {
            return Ok(crate::row::OwnedCellValue::String(String::new()));
        }

        if trimmed.is_ascii {
            return Ok(crate::row::OwnedCellValue::String(
                std::str::from_utf8(slice)
                    .expect("ASCII is valid UTF-8")
                    .to_owned(),
            ));
        }

        if self.encoding == UTF_8 {
            return match std::str::from_utf8(slice) {
                Ok(value) => Ok(crate::row::OwnedCellValue::String(value.to_owned())),
                Err(_)
                    if matches!(
                        self.string_options.utf8_validation,
                        Utf8ValidationMode::Strict
                    ) =>
                {
                    Err(Error::Decode(crate::error::DecodeError {
                        message: "invalid UTF-8 in fixed-width string cell".to_owned(),
                    }))
                }
                Err(_) => Ok(crate::row::OwnedCellValue::String(maybe_fix_mojibake(
                    slice.to_str_lossy().into_owned(),
                    self.string_options.mojibake_fix,
                ))),
            };
        }

        let (decoded, had_errors) = self.encoding.decode_without_bom_handling(slice);
        if had_errors
            && matches!(
                self.string_options.utf8_validation,
                Utf8ValidationMode::Strict
            )
        {
            return Err(Error::Decode(crate::error::DecodeError {
                message: "string decode failed under strict validation".to_owned(),
            }));
        }

        let value = decoded.into_owned();
        Ok(crate::row::OwnedCellValue::String(
            if mojibake_fix_maybe_needed_for_encoded_bytes(
                self.encoding,
                slice,
                self.string_options.mojibake_fix,
            ) {
                maybe_fix_mojibake(value, self.string_options.mojibake_fix)
            } else {
                value
            },
        ))
    }

    pub(super) fn materialize_owned_date(&self, slice: &[u8]) -> crate::row::OwnedCellValue {
        match classify_date_numeric_value(decode_numeric_cell(slice, self.endianness)) {
            DateNumericValue::Null => crate::row::OwnedCellValue::Null,
            DateNumericValue::Date(value) => crate::row::OwnedCellValue::Date(value),
            DateNumericValue::Float64(value) => crate::row::OwnedCellValue::Float64(value),
        }
    }

    pub(super) fn materialize_owned_datetime(&self, slice: &[u8]) -> crate::row::OwnedCellValue {
        match classify_datetime_numeric_value(decode_numeric_cell(slice, self.endianness)) {
            DateTimeNumericValue::Null => crate::row::OwnedCellValue::Null,
            DateTimeNumericValue::DateTime(value) => crate::row::OwnedCellValue::DateTime(value),
            DateTimeNumericValue::Float64(value) => crate::row::OwnedCellValue::Float64(value),
        }
    }

    pub(super) fn materialize_owned_time(&self, slice: &[u8]) -> crate::row::OwnedCellValue {
        match classify_time_numeric_value(decode_numeric_cell(slice, self.endianness)) {
            TimeNumericValue::Null => crate::row::OwnedCellValue::Null,
            TimeNumericValue::Time(value) => crate::row::OwnedCellValue::Time(value),
            TimeNumericValue::Float64(value) => crate::row::OwnedCellValue::Float64(value),
        }
    }

    pub(super) fn materialize_owned_numeric_typed(
        &self,
        slice: &[u8],
        width: u32,
    ) -> crate::row::OwnedCellValue {
        match classify_typed_numeric_value(decode_numeric_cell(slice, self.endianness), width <= 4)
        {
            TypedNumericValue::Null => crate::row::OwnedCellValue::Null,
            TypedNumericValue::Int32(value) => crate::row::OwnedCellValue::Int32(value),
            TypedNumericValue::Int64(value) => crate::row::OwnedCellValue::Int64(value),
            TypedNumericValue::Float64(value) => crate::row::OwnedCellValue::Float64(value),
        }
    }

    pub(super) fn materialize_owned_numeric_lossless(
        &self,
        slice: &[u8],
    ) -> crate::row::OwnedCellValue {
        if slice.is_empty() {
            return crate::row::OwnedCellValue::Null;
        }
        crate::row::OwnedCellValue::Float64(f64::from_bits(numeric_bits(slice, self.endianness)))
    }

    pub(super) fn plan_numeric_lossless<'row>(&self, slice: &[u8]) -> PlannedCell<'row> {
        if slice.is_empty() {
            return PlannedCell::Null;
        }
        PlannedCell::Float64(f64::from_bits(numeric_bits(slice, self.endianness)))
    }
}

pub(super) fn materialize_planned_cells<'a>(
    planned: &[PlannedCell<'a>],
    owned_strings: &'a [String],
) -> Result<Vec<crate::row::CellValue<'a>>> {
    let mut cells = Vec::with_capacity(planned.len());
    for cell in planned {
        cells.push(match *cell {
            PlannedCell::Null => crate::row::CellValue::Null,
            PlannedCell::Int32(value) => crate::row::CellValue::Int32(value),
            PlannedCell::Int64(value) => crate::row::CellValue::Int64(value),
            PlannedCell::Float64(value) => crate::row::CellValue::Float64(value),
            PlannedCell::StrBorrowed(value) => crate::row::CellValue::Str(value),
            PlannedCell::StrOwned(index) => crate::row::CellValue::Str(
                owned_strings
                    .get(index)
                    .ok_or_else(|| Error::unsupported("owned string index out of range"))?
                    .as_str(),
            ),
            PlannedCell::Bytes(value) => crate::row::CellValue::Bytes(value),
            PlannedCell::Date(value) => crate::row::CellValue::Date(value),
            PlannedCell::DateTime(value) => crate::row::CellValue::DateTime(value),
            PlannedCell::Time(value) => crate::row::CellValue::Time(value),
        });
    }
    Ok(cells)
}
