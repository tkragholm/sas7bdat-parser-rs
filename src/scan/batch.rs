use super::*;
#[derive(Debug)]
pub(super) struct BatchDecodePlan {
    pub(super) row_plan: RowDecodePlan,
    pub(super) column_kinds: Vec<ColumnMaterializationKind>,
    pub(super) families: BatchColumnFamilies,
    pub(super) all_columns_staged_numeric: bool,
    pub(super) needs_owned_string_scratch: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct BatchColumnFamilies {
    pub(super) staged_numeric: Vec<usize>,
    pub(super) direct_numeric: Vec<usize>,
    pub(super) direct_raw_bytes: Vec<usize>,
    pub(super) direct_utf8_borrowed: Vec<usize>,
    pub(super) direct_utf8_owned: Vec<usize>,
    pub(super) fallback: Vec<usize>,
}

#[derive(Debug)]
pub(super) struct BatchAccumulator {
    plan: BatchDecodePlan,
    target_rows: usize,
    capacity_hint_rows: usize,
    row_base: Option<u64>,
    row_count: usize,
    columns: Vec<OwnedBatchColumnBuilder>,
    owned_strings: Vec<String>,
    utf8_decode_scratch: String,
}

#[derive(Debug)]
pub(super) enum OwnedBatchColumnBuilder {
    I32 {
        values: Vec<i32>,
        valid: Option<Vec<u8>>,
    },
    I64 {
        values: Vec<i64>,
        valid: Option<Vec<u8>>,
    },
    F64 {
        values: Vec<f64>,
        valid: Option<Vec<u8>>,
    },
    StagedNumeric {
        raw_bits: Vec<u64>,
        mode: NumericTileMode,
        has_missing: bool,
    },
    Date {
        values: Vec<SasDate>,
        valid: Option<Vec<u8>>,
    },
    DateTime {
        values: Vec<SasDateTime>,
        valid: Option<Vec<u8>>,
    },
    Time {
        values: Vec<SasTime>,
        valid: Option<Vec<u8>>,
    },
    Utf8 {
        offsets: Vec<u32>,
        data: Vec<u8>,
        valid: Option<Vec<u8>>,
    },
    RawBytes {
        offsets: Vec<u32>,
        data: Vec<u8>,
        valid: Option<Vec<u8>>,
    },
}

impl BatchDecodePlan {
    pub(super) fn new(builder: &ScanBuilder<'_>) -> Result<Self> {
        let row_plan = RowDecodePlan::new(builder)?;
        let column_kinds = row_plan
            .columns
            .iter()
            .map(|column| column_materialization_kind(column.kernel, column.width))
            .collect::<Vec<_>>();
        let families =
            compile_batch_column_families(&row_plan.columns, &column_kinds, row_plan.string_kernel);
        let all_columns_staged_numeric =
            !row_plan.columns.is_empty() && families.staged_numeric.len() == row_plan.columns.len();
        let needs_owned_string_scratch = families
            .fallback
            .iter()
            .any(|&idx| matches!(row_plan.columns[idx].kernel, CompiledDecodeKernel::Utf8));
        Ok(Self {
            row_plan,
            column_kinds,
            families,
            all_columns_staged_numeric,
            needs_owned_string_scratch,
        })
    }
}

impl BatchAccumulator {
    pub(super) fn new(
        plan: BatchDecodePlan,
        target_rows: usize,
        capacity_hint_rows: usize,
    ) -> Self {
        let columns = plan
            .row_plan
            .columns
            .iter()
            .zip(plan.column_kinds.iter().copied())
            .map(|(column, kind)| {
                OwnedBatchColumnBuilder::with_capacity_hint(
                    kind,
                    capacity_hint_rows,
                    column.width,
                    column.numeric_tile,
                )
            })
            .collect();
        Self {
            plan,
            target_rows: target_rows.max(1),
            capacity_hint_rows: capacity_hint_rows.max(1),
            row_base: None,
            row_count: 0,
            columns,
            owned_strings: Vec::new(),
            utf8_decode_scratch: String::new(),
        }
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub(super) const fn is_full(&self) -> bool {
        self.row_count >= self.target_rows
    }

    pub(super) fn push_row(&mut self, row_index: u64, row: &[u8]) -> Result<()> {
        if self.row_base.is_none() {
            self.row_base = Some(row_index);
        }

        self.plan.row_plan.validate_row_bounds(row)?;
        if self.plan.all_columns_staged_numeric {
            for &idx in &self.plan.families.staged_numeric {
                let batch_column = &mut self.columns[idx];
                let column = &self.plan.row_plan.columns[idx];
                let slice = self.plan.row_plan.slice_in_bounds(row, column);
                let raw = decode_numeric_raw_bits_or_missing(slice, self.plan.row_plan.endianness);
                let appended = batch_column.append_staged_numeric_bits_fast(raw)?;
                debug_assert!(appended, "compiled staged numeric batch must match builder");
                if !appended {
                    return Err(Error::unsupported(
                        "compiled staged numeric batch plan did not match column builder",
                    ));
                }
            }
            self.row_count += 1;
            return Ok(());
        }

        if self.plan.needs_owned_string_scratch {
            self.owned_strings.clear();
        }
        for &idx in &self.plan.families.staged_numeric {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let slice = self.plan.row_plan.slice_in_bounds(row, column);
            let raw = decode_numeric_raw_bits_or_missing(slice, self.plan.row_plan.endianness);
            let appended = batch_column.append_staged_numeric_bits_fast(raw)?;
            debug_assert!(appended, "compiled staged numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled staged numeric batch plan did not match column builder",
                ));
            }
        }

        for &idx in &self.plan.families.direct_numeric {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let appended =
                append_direct_numeric_batch_column(&self.plan.row_plan, batch_column, column, row)?;
            debug_assert!(appended, "compiled direct numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled direct numeric batch plan did not match column builder",
                ));
            }
        }

        for &idx in &self.plan.families.direct_raw_bytes {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let appended = append_direct_raw_bytes_batch_column(
                &self.plan.row_plan,
                batch_column,
                column,
                row,
            )?;
            debug_assert!(appended, "compiled raw-bytes batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled raw-bytes batch plan did not match column builder",
                ));
            }
        }

        for &idx in &self.plan.families.direct_utf8_borrowed {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let appended = append_direct_utf8_borrowed_batch_column(
                &self.plan.row_plan,
                batch_column,
                column,
                row,
            )?;
            debug_assert!(appended, "compiled borrowed utf8 batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled borrowed utf8 batch plan did not match column builder",
                ));
            }
        }

        for &idx in &self.plan.families.direct_utf8_owned {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let appended = append_direct_utf8_owned_batch_column(
                &self.plan.row_plan,
                batch_column,
                column,
                row,
                &mut self.utf8_decode_scratch,
            )?;
            debug_assert!(appended, "compiled utf8 batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled owned utf8 batch plan did not match column builder",
                ));
            }
        }

        for &idx in &self.plan.families.fallback {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let cell =
                self.plan
                    .row_plan
                    .plan_cell_in_bounds(row, column, &mut self.owned_strings)?;
            batch_column.append(cell, &self.owned_strings)?;
        }
        self.row_count += 1;
        Ok(())
    }

    pub(super) fn take_batch(&mut self) -> OwnedColumnarBatch {
        let row_base = self.row_base.unwrap_or(0);
        let row_count = self.row_count;
        let columns = std::mem::take(&mut self.columns)
            .into_iter()
            .map(OwnedBatchColumnBuilder::finish)
            .collect();
        self.row_base = None;
        self.row_count = 0;
        OwnedColumnarBatch {
            row_base,
            row_count,
            columns,
        }
    }

    pub(super) fn reset_after_flush(&mut self) {
        self.columns = self
            .plan
            .row_plan
            .columns
            .iter()
            .zip(self.plan.column_kinds.iter().copied())
            .map(|(column, kind)| {
                OwnedBatchColumnBuilder::with_capacity_hint(
                    kind,
                    self.capacity_hint_rows,
                    column.width,
                    column.numeric_tile,
                )
            })
            .collect();
        self.owned_strings.clear();
        self.utf8_decode_scratch.clear();
    }
}

impl OwnedBatchColumnBuilder {
    pub(super) fn with_capacity_hint(
        kind: ColumnMaterializationKind,
        target_rows: usize,
        width_hint: u32,
        numeric_tile: Option<NumericTileMode>,
    ) -> Self {
        let variable_capacity =
            target_rows.saturating_mul(usize::try_from(width_hint).unwrap_or(0));
        match kind {
            ColumnMaterializationKind::I32 => Self::I32 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::I64 => Self::I64 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::F64 => Self::F64 {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Date => Self::Date {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::DateTime => Self::DateTime {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Time => Self::Time {
                values: Vec::with_capacity(target_rows),
                valid: None,
            },
            ColumnMaterializationKind::Utf8 => Self::Utf8 {
                offsets: Vec::with_capacity(target_rows.saturating_add(1)),
                data: Vec::with_capacity(variable_capacity),
                valid: None,
            },
            ColumnMaterializationKind::RawBytes => Self::RawBytes {
                offsets: Vec::with_capacity(target_rows.saturating_add(1)),
                data: Vec::with_capacity(variable_capacity),
                valid: None,
            },
        }
        .with_numeric_tile(target_rows, numeric_tile)
        .with_initial_offset()
    }

    pub(super) fn with_numeric_tile(
        self,
        target_rows: usize,
        numeric_tile: Option<NumericTileMode>,
    ) -> Self {
        match (self, numeric_tile) {
            (_, Some(mode)) => Self::StagedNumeric {
                raw_bits: Vec::with_capacity(target_rows),
                mode,
                has_missing: false,
            },
            (builder, _) => builder,
        }
    }

    pub(super) fn with_initial_offset(mut self) -> Self {
        match &mut self {
            Self::Utf8 { offsets, .. } | Self::RawBytes { offsets, .. } => offsets.push(0),
            _ => {}
        }
        self
    }

    pub(super) fn append_integer_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::I32 { values, valid } => match number {
                None => {
                    push_primitive_null(values, valid, 0);
                    Ok(true)
                }
                Some(value) => {
                    if let Some(value32) = try_i32_from_f64(value) {
                        push_primitive_valid(values, valid, value32);
                        Ok(true)
                    } else {
                        self.widen_integer_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::I64 { values, valid } => match number {
                None => {
                    push_primitive_null(values, valid, 0);
                    Ok(true)
                }
                Some(value) => {
                    if let Some(value64) = try_i64_from_f64(value) {
                        push_primitive_valid(values, valid, value64);
                        Ok(true)
                    } else {
                        self.widen_integer_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => Ok(false),
        }
    }

    pub(super) fn append_f64_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::F64 { values, valid } => {
                match number {
                    None => push_primitive_null(values, valid, 0.0),
                    Some(value) => push_primitive_valid(values, valid, value),
                }
                Ok(true)
            }
            Self::StagedNumeric {
                raw_bits,
                mode: NumericTileMode::F64RawBits,
                has_missing,
            } => {
                let raw = number.map_or(SAS_NUMERIC_MISSING_SENTINEL, f64::to_bits);
                *has_missing |= numeric_bits_is_missing(raw);
                raw_bits.push(raw);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    #[inline(always)]
    pub(super) fn append_staged_numeric_bits_fast(&mut self, raw: u64) -> Result<bool> {
        match self {
            Self::StagedNumeric {
                raw_bits,
                has_missing,
                ..
            } => {
                *has_missing |= numeric_bits_is_missing(raw);
                raw_bits.push(raw);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(super) fn append_date_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::Date { values, valid } => match number {
                None => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDate {
                            days_since_sas_epoch: 0,
                        },
                    );
                    Ok(true)
                }
                Some(value) => {
                    if let Some(days) = try_i32_from_f64(value) {
                        push_primitive_valid(
                            values,
                            valid,
                            SasDate {
                                days_since_sas_epoch: days,
                            },
                        );
                        Ok(true)
                    } else {
                        self.widen_temporal_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => Ok(false),
        }
    }

    pub(super) fn append_datetime_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::DateTime { values, valid } => match number {
                None => {
                    push_primitive_null(
                        values,
                        valid,
                        SasDateTime {
                            seconds_since_sas_epoch: 0,
                        },
                    );
                    Ok(true)
                }
                Some(value) => {
                    if let Some(seconds) = try_i64_from_f64(value) {
                        push_primitive_valid(
                            values,
                            valid,
                            SasDateTime {
                                seconds_since_sas_epoch: seconds,
                            },
                        );
                        Ok(true)
                    } else {
                        self.widen_temporal_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => Ok(false),
        }
    }

    pub(super) fn append_time_fast(&mut self, number: Option<f64>) -> Result<bool> {
        match self {
            Self::Time { values, valid } => match number {
                None => {
                    push_primitive_null(
                        values,
                        valid,
                        SasTime {
                            seconds_since_midnight: 0,
                        },
                    );
                    Ok(true)
                }
                Some(value) => {
                    if let Some(seconds) = try_i64_from_f64(value) {
                        push_primitive_valid(
                            values,
                            valid,
                            SasTime {
                                seconds_since_midnight: seconds,
                            },
                        );
                        Ok(true)
                    } else {
                        self.widen_temporal_to_f64();
                        self.append_f64_fast(number)
                    }
                }
            },
            Self::F64 { .. } => self.append_f64_fast(number),
            _ => Ok(false),
        }
    }

    pub(super) fn append(&mut self, cell: PlannedCell<'_>, owned_strings: &[String]) -> Result<()> {
        match self {
            Self::I32 { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(values, valid, 0),
                PlannedCell::Int32(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Int64(value) => {
                    if let Ok(value32) = i32::try_from(value) {
                        push_primitive_valid(values, valid, value32);
                    } else {
                        self.widen_integer_to_f64();
                        return self.append(PlannedCell::Int64(value), owned_strings);
                    }
                }
                PlannedCell::Float64(value) => {
                    if let Some(value32) = try_i32_from_f64(value) {
                        push_primitive_valid(values, valid, value32);
                    } else {
                        self.widen_integer_to_f64();
                        return self.append(PlannedCell::Float64(value), owned_strings);
                    }
                }
                other => return Err(unexpected_batch_cell("i32", other)),
            },
            Self::I64 { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(values, valid, 0),
                PlannedCell::Int32(value) => push_primitive_valid(values, valid, i64::from(value)),
                PlannedCell::Int64(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Float64(value) => {
                    if let Some(value64) = try_i64_from_f64(value) {
                        push_primitive_valid(values, valid, value64);
                    } else {
                        self.widen_integer_to_f64();
                        return self.append(PlannedCell::Float64(value), owned_strings);
                    }
                }
                other => return Err(unexpected_batch_cell("i64", other)),
            },
            Self::F64 { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(values, valid, 0.0),
                PlannedCell::Int32(value) => push_primitive_valid(values, valid, f64::from(value)),
                PlannedCell::Int64(value) => push_primitive_valid(values, valid, value as f64),
                PlannedCell::Float64(value) => push_primitive_valid(values, valid, value),
                other => return Err(unexpected_batch_cell("f64", other)),
            },
            Self::StagedNumeric { raw_bits, .. } => {
                raw_bits.push(staged_numeric_raw_bits_from_planned_cell(cell)?);
            }
            Self::Date { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(
                    values,
                    valid,
                    SasDate {
                        days_since_sas_epoch: 0,
                    },
                ),
                PlannedCell::Date(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Int32(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int32(value), owned_strings);
                }
                PlannedCell::Int64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int64(value), owned_strings);
                }
                PlannedCell::Float64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Float64(value), owned_strings);
                }
                other => return Err(unexpected_batch_cell("date", other)),
            },
            Self::DateTime { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(
                    values,
                    valid,
                    SasDateTime {
                        seconds_since_sas_epoch: 0,
                    },
                ),
                PlannedCell::DateTime(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Int32(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int32(value), owned_strings);
                }
                PlannedCell::Int64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int64(value), owned_strings);
                }
                PlannedCell::Float64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Float64(value), owned_strings);
                }
                other => return Err(unexpected_batch_cell("datetime", other)),
            },
            Self::Time { values, valid } => match cell {
                PlannedCell::Null => push_primitive_null(
                    values,
                    valid,
                    SasTime {
                        seconds_since_midnight: 0,
                    },
                ),
                PlannedCell::Time(value) => push_primitive_valid(values, valid, value),
                PlannedCell::Int32(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int32(value), owned_strings);
                }
                PlannedCell::Int64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Int64(value), owned_strings);
                }
                PlannedCell::Float64(value) => {
                    self.widen_temporal_to_f64();
                    return self.append(PlannedCell::Float64(value), owned_strings);
                }
                other => return Err(unexpected_batch_cell("time", other)),
            },
            Self::Utf8 {
                offsets,
                data,
                valid,
            } => match cell {
                PlannedCell::Null => push_variable_null(offsets, data, valid),
                PlannedCell::StrBorrowed(value) => {
                    push_variable_valid(offsets, data, valid, value.as_bytes())?;
                }
                PlannedCell::StrOwned(index) => push_variable_valid(
                    offsets,
                    data,
                    valid,
                    owned_strings
                        .get(index)
                        .ok_or_else(|| Error::unsupported("owned string index out of range"))?
                        .as_bytes(),
                )?,
                other => return Err(unexpected_batch_cell("utf8", other)),
            },
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => match cell {
                PlannedCell::Null => push_variable_null(offsets, data, valid),
                PlannedCell::Bytes(value) => push_variable_valid(offsets, data, valid, value)?,
                other => return Err(unexpected_batch_cell("raw-bytes", other)),
            },
        }
        Ok(())
    }

    pub(super) fn widen_temporal_to_f64(&mut self) {
        let widened = match std::mem::replace(
            self,
            Self::F64 {
                values: Vec::new(),
                valid: None,
            },
        ) {
            Self::Date { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| f64::from(value.days_since_sas_epoch))
                    .collect(),
                valid,
            },
            Self::DateTime { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| value.seconds_since_sas_epoch as f64)
                    .collect(),
                valid,
            },
            Self::Time { values, valid } => Self::F64 {
                values: values
                    .into_iter()
                    .map(|value| value.seconds_since_midnight as f64)
                    .collect(),
                valid,
            },
            other => other,
        };
        *self = widened;
    }

    pub(super) fn widen_integer_to_f64(&mut self) {
        let widened = match std::mem::replace(
            self,
            Self::F64 {
                values: Vec::new(),
                valid: None,
            },
        ) {
            Self::I32 { values, valid } => Self::F64 {
                values: values.into_iter().map(f64::from).collect(),
                valid,
            },
            Self::I64 { values, valid } => Self::F64 {
                values: values.into_iter().map(|value| value as f64).collect(),
                valid,
            },
            other => other,
        };
        *self = widened;
    }

    pub(super) fn finish(self) -> OwnedColumnBuffer {
        match self {
            Self::I32 { values, valid } => OwnedColumnBuffer::I32 { values, valid },
            Self::I64 { values, valid } => OwnedColumnBuffer::I64 { values, valid },
            Self::F64 { values, valid } => OwnedColumnBuffer::F64 { values, valid },
            Self::StagedNumeric {
                raw_bits,
                mode,
                has_missing,
            } => materialize_staged_numeric_column(raw_bits, mode, has_missing),
            Self::Date { values, valid } => OwnedColumnBuffer::Date { values, valid },
            Self::DateTime { values, valid } => OwnedColumnBuffer::DateTime { values, valid },
            Self::Time { values, valid } => OwnedColumnBuffer::Time { values, valid },
            Self::Utf8 {
                offsets,
                data,
                valid,
            } => OwnedColumnBuffer::Utf8 {
                offsets,
                data,
                valid,
            },
            Self::RawBytes {
                offsets,
                data,
                valid,
            } => OwnedColumnBuffer::RawBytes {
                offsets,
                data,
                valid,
            },
        }
    }
}

#[inline(always)]
pub(super) fn append_direct_numeric_batch_column(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
) -> Result<bool> {
    let slice = row_plan.slice_in_bounds(row, column);

    if column.numeric_tile.is_some() {
        let raw = decode_numeric_raw_bits_or_missing(slice, row_plan.endianness);
        return batch_column.append_staged_numeric_bits_fast(raw);
    }

    match column.kernel {
        CompiledDecodeKernel::Integer => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_integer_fast(number)
        }
        CompiledDecodeKernel::Float => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_f64_fast(number)
        }
        CompiledDecodeKernel::DateAsNumeric
        | CompiledDecodeKernel::DateTimeAsNumeric
        | CompiledDecodeKernel::TimeAsNumeric
        | CompiledDecodeKernel::NumericLossless => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_f64_fast(number)
        }
        CompiledDecodeKernel::Date => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_date_fast(number)
        }
        CompiledDecodeKernel::DateTime => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_datetime_fast(number)
        }
        CompiledDecodeKernel::Time => {
            let number = decode_numeric_cell(slice, row_plan.endianness);
            batch_column.append_time_fast(number)
        }
        _ => Ok(false),
    }
}

#[inline(always)]
pub(super) fn append_direct_raw_bytes_batch_column(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
) -> Result<bool> {
    let slice = row_plan.slice_in_bounds(row, column);
    match batch_column {
        OwnedBatchColumnBuilder::RawBytes {
            offsets,
            data,
            valid,
        } if matches!(column.kernel, CompiledDecodeKernel::RawBytes) => {
            push_variable_valid(offsets, data, valid, slice)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[inline(always)]
pub(super) fn append_direct_utf8_borrowed_batch_column(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
) -> Result<bool> {
    let slice = row_plan.slice_in_bounds(row, column);
    match (column.kernel, batch_column) {
        (
            CompiledDecodeKernel::Utf8,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
            },
        ) => {
            let bytes = row_plan.decode_string_bytes_for_batch_borrowed(slice)?;
            push_variable_valid(offsets, data, valid, bytes)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[inline(always)]
pub(super) fn append_direct_utf8_owned_batch_column(
    row_plan: &RowDecodePlan,
    batch_column: &mut OwnedBatchColumnBuilder,
    column: &CompiledColumnPlan,
    row: &[u8],
    utf8_decode_scratch: &mut String,
) -> Result<bool> {
    let slice = row_plan.slice_in_bounds(row, column);
    match (column.kernel, batch_column) {
        (
            CompiledDecodeKernel::Utf8,
            OwnedBatchColumnBuilder::Utf8 {
                offsets,
                data,
                valid,
            },
        ) => {
            let trimmed = if row_plan.string_options.trim_fixed_width {
                trim_and_classify_ascii(slice)
            } else {
                TrimmedString {
                    bytes: slice,
                    is_ascii: slice.is_ascii(),
                }
            };
            let slice = trimmed.bytes;
            if slice.is_empty() || trimmed.is_ascii {
                push_variable_valid(offsets, data, valid, slice)?;
                return Ok(true);
            }
            match row_plan.decode_utf8_bytes_for_batch_direct(slice, utf8_decode_scratch)? {
                DecodedUtf8BatchValue::Borrowed(bytes) => {
                    push_variable_valid(offsets, data, valid, bytes)?;
                }
                DecodedUtf8BatchValue::Scratch => {
                    push_variable_valid(offsets, data, valid, utf8_decode_scratch.as_bytes())?;
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

pub(super) fn compile_batch_column_families(
    columns: &[CompiledColumnPlan],
    column_kinds: &[ColumnMaterializationKind],
    string_kernel: StringDecodeKernel,
) -> BatchColumnFamilies {
    let mut families = BatchColumnFamilies::default();
    for (idx, (column, kind)) in columns.iter().zip(column_kinds.iter().copied()).enumerate() {
        if column.numeric_tile.is_some() {
            families.staged_numeric.push(idx);
            continue;
        }

        match (column.kernel, kind) {
            (
                CompiledDecodeKernel::Integer
                | CompiledDecodeKernel::Float
                | CompiledDecodeKernel::Date
                | CompiledDecodeKernel::DateAsNumeric
                | CompiledDecodeKernel::DateTime
                | CompiledDecodeKernel::DateTimeAsNumeric
                | CompiledDecodeKernel::Time
                | CompiledDecodeKernel::TimeAsNumeric
                | CompiledDecodeKernel::NumericLossless,
                ColumnMaterializationKind::I32
                | ColumnMaterializationKind::I64
                | ColumnMaterializationKind::F64
                | ColumnMaterializationKind::Date
                | ColumnMaterializationKind::DateTime
                | ColumnMaterializationKind::Time,
            ) => families.direct_numeric.push(idx),
            (CompiledDecodeKernel::RawBytes, ColumnMaterializationKind::RawBytes) => {
                families.direct_raw_bytes.push(idx)
            }
            (CompiledDecodeKernel::Utf8, ColumnMaterializationKind::Utf8) => match string_kernel {
                StringDecodeKernel::Utf8Strict => families.direct_utf8_borrowed.push(idx),
                StringDecodeKernel::Utf8Lenient
                | StringDecodeKernel::EncodedStrict
                | StringDecodeKernel::EncodedLenient => families.direct_utf8_owned.push(idx),
            },
            _ => families.fallback.push(idx),
        }
    }
    families
}

pub(super) const fn column_materialization_kind(
    kernel: CompiledDecodeKernel,
    width: u32,
) -> ColumnMaterializationKind {
    match kernel {
        CompiledDecodeKernel::Integer => {
            if width <= 4 {
                ColumnMaterializationKind::I32
            } else {
                ColumnMaterializationKind::I64
            }
        }
        CompiledDecodeKernel::Float => ColumnMaterializationKind::F64,
        CompiledDecodeKernel::NumericLossless
        | CompiledDecodeKernel::DateAsNumeric
        | CompiledDecodeKernel::DateTimeAsNumeric
        | CompiledDecodeKernel::TimeAsNumeric => ColumnMaterializationKind::F64,
        CompiledDecodeKernel::Utf8 => ColumnMaterializationKind::Utf8,
        CompiledDecodeKernel::RawBytes => ColumnMaterializationKind::RawBytes,
        CompiledDecodeKernel::Date => ColumnMaterializationKind::Date,
        CompiledDecodeKernel::DateTime => ColumnMaterializationKind::DateTime,
        CompiledDecodeKernel::Time => ColumnMaterializationKind::Time,
    }
}

pub(super) fn borrow_column_buffers(columns: &[OwnedColumnBuffer]) -> Vec<ColumnBuffer<'_>> {
    columns.iter().map(OwnedColumnBuffer::as_borrowed).collect()
}

#[inline(always)]
pub(super) fn push_primitive_valid<T>(values: &mut Vec<T>, valid: &mut Option<Vec<u8>>, value: T) {
    values.push(value);
    if let Some(valid) = valid {
        valid.push(1);
    }
}

#[inline(always)]
pub(super) fn push_primitive_null<T: Copy>(
    values: &mut Vec<T>,
    valid: &mut Option<Vec<u8>>,
    default: T,
) {
    if valid.is_none() {
        *valid = Some(vec![1; values.len()]);
    }
    values.push(default);
    valid.as_mut().expect("validity initialized").push(0);
}

#[inline(always)]
pub(super) fn push_variable_valid(
    offsets: &mut Vec<u32>,
    data: &mut Vec<u8>,
    valid: &mut Option<Vec<u8>>,
    value: &[u8],
) -> Result<()> {
    data.extend_from_slice(value);
    let next_offset = u32::try_from(data.len())
        .map_err(|_| Error::unsupported("columnar variable buffer exceeds u32 offset range"))?;
    offsets.push(next_offset);
    if let Some(valid) = valid {
        valid.push(1);
    }
    Ok(())
}

#[inline(always)]
pub(super) fn push_variable_null(
    offsets: &mut Vec<u32>,
    _data: &mut Vec<u8>,
    valid: &mut Option<Vec<u8>>,
) {
    if valid.is_none() {
        *valid = Some(vec![1; offsets.len().saturating_sub(1)]);
    }
    let last = *offsets.last().unwrap_or(&0);
    offsets.push(last);
    valid.as_mut().expect("validity initialized").push(0);
}

pub(super) fn unexpected_batch_cell(expected: &str, actual: PlannedCell<'_>) -> Error {
    Error::Decode(crate::error::DecodeError {
        message: format!("columnar decode expected {expected} cell but saw {actual:?}"),
    })
}
