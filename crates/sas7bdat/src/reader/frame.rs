use crate::{
    cell::{CellValue, MissingValue},
    dataset::{
        DatasetMetadata, MissingLiteral, MissingRange, MissingValuePolicy, TaggedMissing, Variable,
        VariableKind,
    },
    error::{Error, Result},
};
use std::borrow::Cow;
#[cfg(feature = "arrow")]
use std::{collections::HashMap, sync::Arc};

const SECONDS_PER_DAY: i64 = 86_400;
const NANOS_PER_MICRO: i128 = 1_000;

#[cfg(feature = "arrow")]
use arrow_array::{
    ArrayRef, BinaryArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
#[cfg(feature = "arrow")]
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
#[cfg(feature = "arrow")]
use arrow_schema::{DataType, Field, Schema, TimeUnit};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MissingSummary {
    pub has_system: bool,
    pub tags: Vec<TaggedMissing>,
    pub ranges: Vec<MissingRange>,
}

impl MissingSummary {
    #[must_use]
    pub fn from_policy(policy: &MissingValuePolicy) -> Self {
        Self {
            has_system: policy.system_missing,
            tags: policy.tagged_missing.clone(),
            ranges: policy.ranges.clone(),
        }
    }

    fn record(&mut self, value: &MissingValue) {
        match value {
            MissingValue::System => self.has_system = true,
            MissingValue::Tagged(tag) => {
                if !self.tags.iter().any(|existing| existing == tag) {
                    self.tags.push(tag.clone());
                }
            }
            MissingValue::Range { lower, upper } => {
                let range = match (lower, upper) {
                    (MissingLiteral::Numeric(start), MissingLiteral::Numeric(end)) => {
                        MissingRange::Numeric {
                            start: *start,
                            end: *end,
                        }
                    }
                    (MissingLiteral::String(start), MissingLiteral::String(end)) => {
                        MissingRange::String {
                            start: start.clone(),
                            end: end.clone(),
                        }
                    }
                    _ => MissingRange::String {
                        start: literal_to_string(lower),
                        end: literal_to_string(upper),
                    },
                };
                if !self.ranges.iter().any(|existing| existing == &range) {
                    self.ranges.push(range);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameColumnType {
    I64,
    F64,
    Utf8,
    Binary,
    Date32,
    DateTime64,
    Time64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FrameSchema {
    pub fields: Vec<FrameSchemaField>,
}

impl FrameSchema {
    #[must_use]
    pub fn fields(&self) -> &[FrameSchemaField] {
        &self.fields
    }

    #[must_use]
    pub fn field(&self, index: usize) -> Option<&FrameSchemaField> {
        self.fields.get(index)
    }

    #[must_use]
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }

    #[must_use]
    pub fn field_by_name(&self, name: &str) -> Option<&FrameSchemaField> {
        self.field_index(name).and_then(|index| self.field(index))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FrameSchemaField {
    pub name: String,
    pub label: Option<String>,
    pub sas_format: Option<String>,
    pub value_labels: Option<String>,
    pub missing: MissingSummary,
    pub physical_type: FrameColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimitiveCol<T> {
    pub values: Vec<T>,
    pub validity: Vec<u8>,
    pub row_count: usize,
}

impl<T> PrimitiveCol<T> {
    #[must_use]
    pub const fn len(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    #[must_use]
    pub fn is_valid(&self, row: usize) -> bool {
        row < self.row_count && validity_is_set(&self.validity, row)
    }

    #[must_use]
    pub fn value(&self, row: usize) -> Option<&T> {
        if !self.is_valid(row) {
            return None;
        }
        self.values.get(row)
    }
}

impl<T: Copy> PrimitiveCol<T> {
    #[must_use]
    pub fn value_copied(&self, row: usize) -> Option<T> {
        self.value(row).copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Utf8Col {
    pub offsets: Vec<i32>,
    pub values: Vec<u8>,
    pub validity: Vec<u8>,
    pub row_count: usize,
}

impl Utf8Col {
    #[must_use]
    pub const fn len(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    #[must_use]
    pub fn is_valid(&self, row: usize) -> bool {
        row < self.row_count && validity_is_set(&self.validity, row)
    }

    /// Returns a UTF-8 value for `row` when valid, otherwise `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if offsets are malformed or the underlying bytes are not valid UTF-8.
    pub fn value(&self, row: usize) -> Result<Option<&str>> {
        if !self.is_valid(row) {
            return Ok(None);
        }
        let (start, end) = offset_range("utf8", row, &self.offsets, self.values.len())?;
        let slice = self
            .values
            .get(start..end)
            .ok_or_else(|| Error::InvalidConfiguration {
                details: Cow::Owned(format!(
                    "utf8 column offset range {start}..{end} out of bounds"
                )),
            })?;
        let value = std::str::from_utf8(slice).map_err(|err| Error::Encoding {
            encoding: Cow::from("UTF-8"),
            details: Cow::Owned(format!("invalid utf8 in frame column: {err}")),
        })?;
        Ok(Some(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryCol {
    pub offsets: Vec<i32>,
    pub values: Vec<u8>,
    pub validity: Vec<u8>,
    pub row_count: usize,
}

impl BinaryCol {
    #[must_use]
    pub const fn len(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    #[must_use]
    pub fn is_valid(&self, row: usize) -> bool {
        row < self.row_count && validity_is_set(&self.validity, row)
    }

    /// Returns a binary value for `row` when valid, otherwise `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if offsets are malformed.
    pub fn value(&self, row: usize) -> Result<Option<&[u8]>> {
        if !self.is_valid(row) {
            return Ok(None);
        }
        let (start, end) = offset_range("binary", row, &self.offsets, self.values.len())?;
        let slice = self
            .values
            .get(start..end)
            .ok_or_else(|| Error::InvalidConfiguration {
                details: Cow::Owned(format!(
                    "binary column offset range {start}..{end} out of bounds"
                )),
            })?;
        Ok(Some(slice))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FrameColumn {
    I64(PrimitiveCol<i64>),
    F64(PrimitiveCol<f64>),
    Utf8(Utf8Col),
    Binary(BinaryCol),
    Date32(PrimitiveCol<i32>),
    DateTime64(PrimitiveCol<i64>),
    Time64(PrimitiveCol<i64>),
}

impl FrameColumn {
    #[must_use]
    pub const fn physical_type(&self) -> FrameColumnType {
        match self {
            Self::I64(_) => FrameColumnType::I64,
            Self::F64(_) => FrameColumnType::F64,
            Self::Utf8(_) => FrameColumnType::Utf8,
            Self::Binary(_) => FrameColumnType::Binary,
            Self::Date32(_) => FrameColumnType::Date32,
            Self::DateTime64(_) => FrameColumnType::DateTime64,
            Self::Time64(_) => FrameColumnType::Time64,
        }
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        match self {
            Self::F64(col) => col.row_count,
            Self::Utf8(col) => col.row_count,
            Self::Binary(col) => col.row_count,
            Self::Date32(col) => col.row_count,
            Self::I64(col) | Self::DateTime64(col) | Self::Time64(col) => col.row_count,
        }
    }

    #[must_use]
    pub const fn as_i64(&self) -> Option<&PrimitiveCol<i64>> {
        match self {
            Self::I64(col) | Self::DateTime64(col) | Self::Time64(col) => Some(col),
            Self::F64(_) | Self::Utf8(_) | Self::Binary(_) | Self::Date32(_) => None,
        }
    }

    #[must_use]
    pub const fn as_f64(&self) -> Option<&PrimitiveCol<f64>> {
        match self {
            Self::F64(col) => Some(col),
            Self::I64(_)
            | Self::Utf8(_)
            | Self::Binary(_)
            | Self::Date32(_)
            | Self::DateTime64(_)
            | Self::Time64(_) => None,
        }
    }

    #[must_use]
    pub const fn as_utf8(&self) -> Option<&Utf8Col> {
        match self {
            Self::Utf8(col) => Some(col),
            Self::I64(_)
            | Self::F64(_)
            | Self::Binary(_)
            | Self::Date32(_)
            | Self::DateTime64(_)
            | Self::Time64(_) => None,
        }
    }

    #[must_use]
    pub const fn as_binary(&self) -> Option<&BinaryCol> {
        match self {
            Self::Binary(col) => Some(col),
            Self::I64(_)
            | Self::F64(_)
            | Self::Utf8(_)
            | Self::Date32(_)
            | Self::DateTime64(_)
            | Self::Time64(_) => None,
        }
    }

    #[must_use]
    pub const fn as_date32(&self) -> Option<&PrimitiveCol<i32>> {
        match self {
            Self::Date32(col) => Some(col),
            Self::I64(_)
            | Self::F64(_)
            | Self::Utf8(_)
            | Self::Binary(_)
            | Self::DateTime64(_)
            | Self::Time64(_) => None,
        }
    }
}

#[cfg(feature = "arrow")]
impl FrameBatch {
    /// Converts this frame batch into an Arrow `RecordBatch`, consuming the frame.
    ///
    /// This is the lowest-overhead conversion path because it can move backing
    /// vectors directly into Arrow buffers.
    ///
    /// # Errors
    ///
    /// Returns an error if Arrow batch construction fails.
    pub fn into_arrow_record_batch(self) -> Result<RecordBatch> {
        let mut fields = Vec::with_capacity(self.schema.fields.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for (field, column) in self.schema.fields.into_iter().zip(self.columns.into_iter()) {
            fields.push(frame_field_to_arrow(&field, column.physical_type())?);
            arrays.push(column_into_arrow_array(column)?);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|err| Error::InvalidConfiguration {
            details: Cow::Owned(format!("failed building Arrow RecordBatch: {err}")),
        })
    }

    /// Converts this frame batch into an Arrow `RecordBatch`.
    ///
    /// Prefer [`Self::into_arrow_record_batch`] when possible for the lowest
    /// conversion overhead.
    ///
    /// # Errors
    ///
    /// Returns an error if Arrow batch construction fails.
    pub fn to_arrow_record_batch(&self) -> Result<RecordBatch> {
        let mut fields = Vec::with_capacity(self.schema.fields.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for (field, column) in self.schema.fields.iter().zip(self.columns.iter()) {
            fields.push(frame_field_to_arrow(field, column.physical_type())?);
            arrays.push(column_to_arrow_array(column)?);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|err| Error::InvalidConfiguration {
            details: Cow::Owned(format!("failed building Arrow RecordBatch: {err}")),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FrameBatch {
    pub schema: FrameSchema,
    pub row_count: usize,
    pub columns: Vec<FrameColumn>,
}

impl FrameBatch {
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub const fn column_count(&self) -> usize {
        self.columns.len()
    }

    #[must_use]
    pub const fn schema(&self) -> &FrameSchema {
        &self.schema
    }

    #[must_use]
    pub fn columns(&self) -> &[FrameColumn] {
        &self.columns
    }

    #[must_use]
    pub fn column(&self, index: usize) -> Option<&FrameColumn> {
        self.columns.get(index)
    }

    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.schema.field_index(name)
    }

    #[must_use]
    pub fn column_by_name(&self, name: &str) -> Option<&FrameColumn> {
        self.column_index(name).and_then(|index| self.column(index))
    }

    #[must_use]
    pub fn field_by_name(&self, name: &str) -> Option<&FrameSchemaField> {
        self.schema.field_by_name(name)
    }
}

#[derive(Debug, Clone)]
pub(super) struct FrameBlueprint {
    fields: Vec<FrameSchemaField>,
}

impl FrameBlueprint {
    pub(super) fn from_metadata(metadata: &DatasetMetadata, projection: &[usize]) -> Result<Self> {
        let mut fields = Vec::with_capacity(projection.len());
        for &index in projection {
            let variable =
                metadata
                    .variables
                    .get(index)
                    .ok_or_else(|| Error::InvalidConfiguration {
                        details: Cow::Owned(format!(
                            "frame projection index {index} exceeds variable count {}",
                            metadata.variables.len()
                        )),
                    })?;
            fields.push(schema_field_from_variable(variable));
        }
        Ok(Self { fields })
    }

    pub(super) fn collector_with_capacity(&self, rows: usize) -> FrameCollector {
        FrameCollector::from_fields(&self.fields, rows)
    }
}

pub(super) struct FrameCollector {
    fields: Vec<FrameSchemaField>,
    columns: Vec<ColumnBuilder>,
    row_count: usize,
}

impl FrameCollector {
    fn from_fields(fields: &[FrameSchemaField], expected_rows: usize) -> Self {
        let mut columns = Vec::with_capacity(fields.len());
        for field in fields {
            columns.push(ColumnBuilder::Unknown {
                fallback: field.physical_type,
                null_rows: 0,
                expected_rows,
            });
        }
        Self {
            fields: fields.to_vec(),
            columns,
            row_count: 0,
        }
    }

    #[must_use]
    pub(crate) const fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn push_row(&mut self, values: &[CellValue<'_>]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(Error::InvalidConfiguration {
                details: Cow::Owned(format!(
                    "frame row width {} does not match schema width {}",
                    values.len(),
                    self.columns.len()
                )),
            });
        }

        for (index, value) in values.iter().enumerate() {
            let field = self
                .fields
                .get_mut(index)
                .ok_or_else(|| Error::InvalidConfiguration {
                    details: "frame field index out of bounds".into(),
                })?;
            let column =
                self.columns
                    .get_mut(index)
                    .ok_or_else(|| Error::InvalidConfiguration {
                        details: "frame column index out of bounds".into(),
                    })?;
            column.push_cell(value, &mut field.missing)?;
        }

        self.row_count = self.row_count.saturating_add(1);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> FrameBatch {
        let mut columns = Vec::with_capacity(self.columns.len());
        for (index, column) in self.columns.drain(..).enumerate() {
            let finalized = column.finalize();
            if let Some(field) = self.fields.get_mut(index) {
                field.physical_type = finalized.physical_type();
            }
            columns.push(finalized);
        }

        FrameBatch {
            schema: FrameSchema {
                fields: self.fields,
            },
            row_count: self.row_count,
            columns,
        }
    }
}

enum ColumnBuilder {
    Unknown {
        fallback: FrameColumnType,
        null_rows: usize,
        expected_rows: usize,
    },
    I64(PrimitiveBuilder<i64>),
    F64(PrimitiveBuilder<f64>),
    Date32(PrimitiveBuilder<i32>),
    DateTime64(PrimitiveBuilder<i64>),
    Time64(PrimitiveBuilder<i64>),
    Utf8(VarWidthBuilder),
    Binary(VarWidthBuilder),
}

impl ColumnBuilder {
    fn push_cell(&mut self, value: &CellValue<'_>, missing: &mut MissingSummary) -> Result<()> {
        if let CellValue::Missing(missing_value) = value {
            missing.record(missing_value);
            self.push_null();
            return Ok(());
        }

        let expected_rows = match self {
            Self::Unknown { expected_rows, .. } => *expected_rows,
            Self::F64(builder) => builder.capacity_rows,
            Self::Date32(builder) => builder.capacity_rows,
            Self::I64(builder) | Self::DateTime64(builder) | Self::Time64(builder) => {
                builder.capacity_rows
            }
            Self::Utf8(builder) | Self::Binary(builder) => builder.capacity_rows,
        };
        if let Self::Unknown { null_rows, .. } = self {
            let replacement = Self::from_first_non_missing(value, *null_rows, expected_rows)?;
            *self = replacement;
        }

        match (self, value) {
            (Self::I64(builder), CellValue::Int32(v)) => builder.push_some(i64::from(*v)),
            (Self::I64(builder), CellValue::Int64(v)) => builder.push_some(*v),
            (this @ Self::I64(_), CellValue::Float(v)) => {
                let upgraded = match std::mem::replace(
                    this,
                    Self::Unknown {
                        fallback: FrameColumnType::F64,
                        null_rows: 0,
                        expected_rows: 0,
                    },
                ) {
                    Self::I64(builder) => Self::F64(builder.into_f64()),
                    _ => unreachable!("column variant mismatch"),
                };
                *this = upgraded;
                match this {
                    Self::F64(builder) => builder.push_some(*v),
                    _ => unreachable!("column upgrade failed"),
                }
            }
            (Self::F64(builder), CellValue::Int32(v)) => builder.push_some(f64::from(*v)),
            (Self::F64(builder), CellValue::Int64(v)) => {
                #[allow(clippy::cast_precision_loss)]
                let value = *v as f64;
                builder.push_some(value);
            }
            (Self::F64(builder), CellValue::Float(v)) => builder.push_some(*v),
            (Self::Date32(builder), CellValue::Date(value)) => {
                builder.push_some(date_to_days(*value)?);
            }
            (Self::DateTime64(builder), CellValue::DateTime(value)) => {
                builder.push_some(datetime_to_micros(*value)?);
            }
            (Self::Time64(builder), CellValue::Time(value)) => {
                builder.push_some(duration_to_micros(*value)?);
            }
            (
                Self::Utf8(builder) | Self::Binary(builder),
                CellValue::Str(value) | CellValue::NumericString(value),
            ) => {
                builder.push_some(value.as_bytes())?;
            }
            (this @ Self::Utf8(_), CellValue::Bytes(value)) => {
                let upgraded = match std::mem::replace(
                    this,
                    Self::Unknown {
                        fallback: FrameColumnType::Binary,
                        null_rows: 0,
                        expected_rows: 0,
                    },
                ) {
                    Self::Utf8(builder) => Self::Binary(builder),
                    _ => unreachable!("column variant mismatch"),
                };
                *this = upgraded;
                match this {
                    Self::Binary(builder) => builder.push_some(value.as_ref())?,
                    _ => unreachable!("column upgrade failed"),
                }
            }
            (Self::Binary(builder), CellValue::Bytes(value)) => {
                builder.push_some(value.as_ref())?;
            }
            (Self::Unknown { .. }, _) => unreachable!("unknown column was not initialized"),
            (_, other) => {
                return Err(Error::InvalidConfiguration {
                    details: Cow::Owned(format!(
                        "frame column type cannot accept cell variant {other:?}"
                    )),
                });
            }
        }

        Ok(())
    }

    fn push_null(&mut self) {
        match self {
            Self::Unknown { null_rows, .. } => *null_rows = null_rows.saturating_add(1),
            Self::F64(builder) => builder.push_null(),
            Self::Date32(builder) => builder.push_null(),
            Self::I64(builder) | Self::DateTime64(builder) | Self::Time64(builder) => {
                builder.push_null();
            }
            Self::Utf8(builder) | Self::Binary(builder) => builder.push_null(),
        }
    }

    fn from_first_non_missing(
        value: &CellValue<'_>,
        null_rows: usize,
        expected_rows: usize,
    ) -> Result<Self> {
        match value {
            CellValue::Int32(_) | CellValue::Int64(_) => Ok(Self::I64(
                PrimitiveBuilder::<i64>::with_nulls_and_capacity(null_rows, expected_rows),
            )),
            CellValue::Float(_) => Ok(Self::F64(PrimitiveBuilder::<f64>::with_nulls_and_capacity(
                null_rows,
                expected_rows,
            ))),
            CellValue::Date(_) => Ok(Self::Date32(
                PrimitiveBuilder::<i32>::with_nulls_and_capacity(null_rows, expected_rows),
            )),
            CellValue::DateTime(_) => Ok(Self::DateTime64(
                PrimitiveBuilder::<i64>::with_nulls_and_capacity(null_rows, expected_rows),
            )),
            CellValue::Time(_) => Ok(Self::Time64(
                PrimitiveBuilder::<i64>::with_nulls_and_capacity(null_rows, expected_rows),
            )),
            CellValue::Str(_) | CellValue::NumericString(_) => Ok(Self::Utf8(
                VarWidthBuilder::with_nulls_and_capacity(null_rows, expected_rows),
            )),
            CellValue::Bytes(_) => Ok(Self::Binary(VarWidthBuilder::with_nulls_and_capacity(
                null_rows,
                expected_rows,
            ))),
            CellValue::Missing(_) => Err(Error::InvalidConfiguration {
                details: "missing cell cannot initialize frame column type".into(),
            }),
        }
    }

    fn finalize(self) -> FrameColumn {
        match self {
            Self::Unknown {
                fallback,
                null_rows,
                ..
            } => Self::null_column(fallback, null_rows),
            Self::I64(builder) => FrameColumn::I64(builder.finish()),
            Self::F64(builder) => FrameColumn::F64(builder.finish()),
            Self::Date32(builder) => FrameColumn::Date32(builder.finish()),
            Self::DateTime64(builder) => FrameColumn::DateTime64(builder.finish()),
            Self::Time64(builder) => FrameColumn::Time64(builder.finish()),
            Self::Utf8(builder) => FrameColumn::Utf8(builder.finish_utf8()),
            Self::Binary(builder) => FrameColumn::Binary(builder.finish_binary()),
        }
    }

    fn null_column(kind: FrameColumnType, rows: usize) -> FrameColumn {
        match kind {
            FrameColumnType::I64 => {
                FrameColumn::I64(PrimitiveBuilder::<i64>::with_nulls(rows).finish())
            }
            FrameColumnType::F64 => {
                FrameColumn::F64(PrimitiveBuilder::<f64>::with_nulls(rows).finish())
            }
            FrameColumnType::Date32 => {
                FrameColumn::Date32(PrimitiveBuilder::<i32>::with_nulls(rows).finish())
            }
            FrameColumnType::DateTime64 => {
                FrameColumn::DateTime64(PrimitiveBuilder::<i64>::with_nulls(rows).finish())
            }
            FrameColumnType::Time64 => {
                FrameColumn::Time64(PrimitiveBuilder::<i64>::with_nulls(rows).finish())
            }
            FrameColumnType::Utf8 => {
                FrameColumn::Utf8(VarWidthBuilder::with_nulls(rows).finish_utf8())
            }
            FrameColumnType::Binary => {
                FrameColumn::Binary(VarWidthBuilder::with_nulls(rows).finish_binary())
            }
        }
    }
}

struct PrimitiveBuilder<T> {
    values: Vec<T>,
    validity: Vec<u8>,
    row_count: usize,
    capacity_rows: usize,
}

impl<T> PrimitiveBuilder<T>
where
    T: Default + Clone,
{
    fn with_nulls(row_count: usize) -> Self {
        Self::with_nulls_and_capacity(row_count, row_count)
    }

    fn with_nulls_and_capacity(row_count: usize, capacity_rows: usize) -> Self {
        let capacity_rows = capacity_rows.max(row_count);
        let mut values = vec![T::default(); row_count];
        values.reserve(capacity_rows.saturating_sub(row_count));
        let mut validity = vec![0; validity_bytes_for_rows(row_count)];
        validity.reserve(validity_bytes_for_rows(capacity_rows).saturating_sub(validity.len()));
        Self {
            values,
            validity,
            row_count,
            capacity_rows,
        }
    }

    fn push_some(&mut self, value: T) {
        self.values.push(value);
        set_validity_bit(&mut self.validity, self.row_count);
        self.row_count = self.row_count.saturating_add(1);
    }

    fn push_null(&mut self) {
        self.values.push(T::default());
        ensure_validity_len(&mut self.validity, self.row_count);
        self.row_count = self.row_count.saturating_add(1);
    }

    fn finish(self) -> PrimitiveCol<T> {
        PrimitiveCol {
            values: self.values,
            validity: self.validity,
            row_count: self.row_count,
        }
    }
}

impl PrimitiveBuilder<i64> {
    fn into_f64(self) -> PrimitiveBuilder<f64> {
        let mut values = Vec::with_capacity(self.values.len());
        for value in self.values {
            #[allow(clippy::cast_precision_loss)]
            let converted = value as f64;
            values.push(converted);
        }
        PrimitiveBuilder {
            values,
            validity: self.validity,
            row_count: self.row_count,
            capacity_rows: self.capacity_rows,
        }
    }
}

struct VarWidthBuilder {
    offsets: Vec<i32>,
    values: Vec<u8>,
    validity: Vec<u8>,
    row_count: usize,
    capacity_rows: usize,
}

impl VarWidthBuilder {
    fn with_nulls(row_count: usize) -> Self {
        Self::with_nulls_and_capacity(row_count, row_count)
    }

    fn with_nulls_and_capacity(row_count: usize, capacity_rows: usize) -> Self {
        let capacity_rows = capacity_rows.max(row_count);
        let mut offsets = vec![0; row_count.saturating_add(1)];
        offsets.reserve(capacity_rows.saturating_sub(row_count));
        let mut validity = vec![0; validity_bytes_for_rows(row_count)];
        validity.reserve(validity_bytes_for_rows(capacity_rows).saturating_sub(validity.len()));
        Self {
            offsets,
            values: Vec::new(),
            validity,
            row_count,
            capacity_rows,
        }
    }

    fn push_some(&mut self, value: &[u8]) -> Result<()> {
        self.values.extend_from_slice(value);
        let offset = i32::try_from(self.values.len()).map_err(|_| Error::Allocation {
            details: Cow::from("variable-width frame column exceeded i32 offset range"),
        })?;
        self.offsets.push(offset);
        set_validity_bit(&mut self.validity, self.row_count);
        self.row_count = self.row_count.saturating_add(1);
        Ok(())
    }

    fn push_null(&mut self) {
        let offset = self.offsets.last().copied().unwrap_or(0);
        self.offsets.push(offset);
        ensure_validity_len(&mut self.validity, self.row_count);
        self.row_count = self.row_count.saturating_add(1);
    }

    fn finish_utf8(self) -> Utf8Col {
        Utf8Col {
            offsets: self.offsets,
            values: self.values,
            validity: self.validity,
            row_count: self.row_count,
        }
    }

    fn finish_binary(self) -> BinaryCol {
        BinaryCol {
            offsets: self.offsets,
            values: self.values,
            validity: self.validity,
            row_count: self.row_count,
        }
    }
}

fn schema_field_from_variable(variable: &Variable) -> FrameSchemaField {
    FrameSchemaField {
        name: variable.name.clone(),
        label: variable.label.clone(),
        sas_format: variable.format.as_ref().map(format_to_string),
        value_labels: variable.value_labels.clone(),
        missing: MissingSummary::from_policy(&variable.missing),
        physical_type: default_column_type(&variable.kind),
    }
}

const fn default_column_type(kind: &VariableKind) -> FrameColumnType {
    match kind {
        VariableKind::Numeric => FrameColumnType::F64,
        VariableKind::Character => FrameColumnType::Utf8,
    }
}

fn format_to_string(format: &crate::dataset::Format) -> String {
    match (format.width, format.decimals) {
        (Some(width), Some(decimals)) => format!("{}{}.{}", format.name, width, decimals),
        (Some(width), None) => format!("{}{}", format.name, width),
        _ => format.name.clone(),
    }
}

fn literal_to_string(value: &MissingLiteral) -> String {
    match value {
        MissingLiteral::Numeric(number) => number.to_string(),
        MissingLiteral::String(text) => text.clone(),
    }
}

const fn validity_bytes_for_rows(rows: usize) -> usize {
    rows.saturating_add(7) / 8
}

fn ensure_validity_len(validity: &mut Vec<u8>, row: usize) {
    let target_len = validity_bytes_for_rows(row.saturating_add(1));
    if validity.len() < target_len {
        validity.resize(target_len, 0);
    }
}

fn set_validity_bit(validity: &mut Vec<u8>, row: usize) {
    ensure_validity_len(validity, row);
    let byte_index = row / 8;
    let bit_index = row % 8;
    validity[byte_index] |= 1u8 << bit_index;
}

fn validity_is_set(validity: &[u8], row: usize) -> bool {
    let byte = row / 8;
    let bit = row % 8;
    validity
        .get(byte)
        .is_some_and(|value| (value & (1u8 << bit)) != 0)
}

fn offset_range(
    kind: &str,
    row: usize,
    offsets: &[i32],
    values_len: usize,
) -> Result<(usize, usize)> {
    let start = *offsets
        .get(row)
        .ok_or_else(|| Error::InvalidConfiguration {
            details: Cow::Owned(format!("missing start offset for {kind} row {row}")),
        })?;
    let end = *offsets
        .get(row.saturating_add(1))
        .ok_or_else(|| Error::InvalidConfiguration {
            details: Cow::Owned(format!(
                "missing end offset for {kind} row {}",
                row.saturating_add(1)
            )),
        })?;
    if end < start {
        return Err(Error::InvalidConfiguration {
            details: Cow::Owned(format!(
                "{kind} offsets are not monotonic at row {row}: {start}>{end}"
            )),
        });
    }

    let start = usize::try_from(start).map_err(|_| Error::InvalidConfiguration {
        details: Cow::Owned(format!(
            "{kind} start offset is negative at row {row}: {start}"
        )),
    })?;
    let end = usize::try_from(end).map_err(|_| Error::InvalidConfiguration {
        details: Cow::Owned(format!("{kind} end offset is negative at row {row}: {end}")),
    })?;
    if end > values_len {
        return Err(Error::InvalidConfiguration {
            details: Cow::Owned(format!(
                "{kind} end offset {end} exceeds values length {values_len}"
            )),
        });
    }
    Ok((start, end))
}

fn datetime_to_micros(value: time::OffsetDateTime) -> Result<i64> {
    let nanos = value.unix_timestamp_nanos();
    let micros = nanos / NANOS_PER_MICRO;
    i64::try_from(micros).map_err(|_| Error::InvalidConfiguration {
        details: Cow::from("datetime is out of microsecond i64 range"),
    })
}

fn date_to_days(value: time::OffsetDateTime) -> Result<i32> {
    let unix_days = value.unix_timestamp().div_euclid(SECONDS_PER_DAY);
    i32::try_from(unix_days).map_err(|_| Error::InvalidConfiguration {
        details: Cow::from("date is out of day i32 range"),
    })
}

fn duration_to_micros(value: time::Duration) -> Result<i64> {
    let micros = value.whole_nanoseconds() / NANOS_PER_MICRO;
    i64::try_from(micros).map_err(|_| Error::InvalidConfiguration {
        details: Cow::from("time duration is out of microsecond i64 range"),
    })
}

#[cfg(feature = "arrow")]
fn frame_field_to_arrow(field: &FrameSchemaField, kind: FrameColumnType) -> Result<Field> {
    let data_type = match kind {
        FrameColumnType::I64 => DataType::Int64,
        FrameColumnType::F64 => DataType::Float64,
        FrameColumnType::Utf8 => DataType::Utf8,
        FrameColumnType::Binary => DataType::Binary,
        FrameColumnType::Date32 => DataType::Date32,
        FrameColumnType::DateTime64 => DataType::Timestamp(TimeUnit::Microsecond, None),
        FrameColumnType::Time64 => DataType::Time64(TimeUnit::Microsecond),
    };

    let mut metadata = HashMap::new();
    if let Some(label) = &field.label {
        metadata.insert("sas.label".to_string(), label.clone());
    }
    if let Some(format) = &field.sas_format {
        metadata.insert("sas.format".to_string(), format.clone());
    }
    if let Some(labels) = &field.value_labels {
        metadata.insert("sas.value_labels".to_string(), labels.clone());
    }
    if field.missing.has_system {
        metadata.insert("sas.missing.system".to_string(), "true".to_string());
    }
    if !field.missing.tags.is_empty() {
        let tags = serde_json::to_string(
            &field
                .missing
                .tags
                .iter()
                .map(|t| literal_to_string(&t.literal))
                .collect::<Vec<_>>(),
        )
        .map_err(|err| Error::InvalidConfiguration {
            details: Cow::Owned(format!("failed to serialize missing tags metadata: {err}")),
        })?;
        metadata.insert("sas.missing.tags".to_string(), tags);
    }
    if !field.missing.ranges.is_empty() {
        metadata.insert(
            "sas.missing.ranges".to_string(),
            field.missing.ranges.len().to_string(),
        );
    }

    Ok(Field::new(&field.name, data_type, true).with_metadata(metadata))
}

#[cfg(feature = "arrow")]
fn column_to_arrow_array(column: &FrameColumn) -> Result<ArrayRef> {
    match column {
        FrameColumn::I64(col) => {
            i64_array_from_parts(col.values.clone(), col.validity.clone(), col.row_count)
        }
        FrameColumn::F64(col) => {
            f64_array_from_parts(col.values.clone(), col.validity.clone(), col.row_count)
        }
        FrameColumn::Date32(col) => {
            date32_array_from_parts(col.values.clone(), col.validity.clone(), col.row_count)
        }
        FrameColumn::DateTime64(col) => timestamp_micros_array_from_parts(
            col.values.clone(),
            col.validity.clone(),
            col.row_count,
        ),
        FrameColumn::Time64(col) => {
            time64_micros_array_from_parts(col.values.clone(), col.validity.clone(), col.row_count)
        }
        FrameColumn::Utf8(col) => utf8_array_from_parts(
            col.offsets.clone(),
            col.values.clone(),
            col.validity.clone(),
            col.row_count,
        ),
        FrameColumn::Binary(col) => binary_array_from_parts(
            col.offsets.clone(),
            col.values.clone(),
            col.validity.clone(),
            col.row_count,
        ),
    }
}

#[cfg(feature = "arrow")]
fn column_into_arrow_array(column: FrameColumn) -> Result<ArrayRef> {
    match column {
        FrameColumn::I64(col) => i64_array_from_parts(col.values, col.validity, col.row_count),
        FrameColumn::F64(col) => f64_array_from_parts(col.values, col.validity, col.row_count),
        FrameColumn::Date32(col) => {
            date32_array_from_parts(col.values, col.validity, col.row_count)
        }
        FrameColumn::DateTime64(col) => {
            timestamp_micros_array_from_parts(col.values, col.validity, col.row_count)
        }
        FrameColumn::Time64(col) => {
            time64_micros_array_from_parts(col.values, col.validity, col.row_count)
        }
        FrameColumn::Utf8(col) => {
            utf8_array_from_parts(col.offsets, col.values, col.validity, col.row_count)
        }
        FrameColumn::Binary(col) => {
            binary_array_from_parts(col.offsets, col.values, col.validity, col.row_count)
        }
    }
}

#[cfg(feature = "arrow")]
fn i64_array_from_parts(values: Vec<i64>, validity: Vec<u8>, row_count: usize) -> Result<ArrayRef> {
    let nulls = null_buffer_from_validity(validity, row_count)?;
    let array =
        Int64Array::try_new(values.into(), nulls).map_err(|err| Error::InvalidConfiguration {
            details: Cow::Owned(format!("failed building Arrow Int64Array: {err}")),
        })?;
    Ok(Arc::new(array))
}

#[cfg(feature = "arrow")]
fn f64_array_from_parts(values: Vec<f64>, validity: Vec<u8>, row_count: usize) -> Result<ArrayRef> {
    let nulls = null_buffer_from_validity(validity, row_count)?;
    let array =
        Float64Array::try_new(values.into(), nulls).map_err(|err| Error::InvalidConfiguration {
            details: Cow::Owned(format!("failed building Arrow Float64Array: {err}")),
        })?;
    Ok(Arc::new(array))
}

#[cfg(feature = "arrow")]
fn date32_array_from_parts(
    values: Vec<i32>,
    validity: Vec<u8>,
    row_count: usize,
) -> Result<ArrayRef> {
    let nulls = null_buffer_from_validity(validity, row_count)?;
    let array =
        Date32Array::try_new(values.into(), nulls).map_err(|err| Error::InvalidConfiguration {
            details: Cow::Owned(format!("failed building Arrow Date32Array: {err}")),
        })?;
    Ok(Arc::new(array))
}

#[cfg(feature = "arrow")]
fn timestamp_micros_array_from_parts(
    values: Vec<i64>,
    validity: Vec<u8>,
    row_count: usize,
) -> Result<ArrayRef> {
    let nulls = null_buffer_from_validity(validity, row_count)?;
    let array = TimestampMicrosecondArray::try_new(values.into(), nulls).map_err(|err| {
        Error::InvalidConfiguration {
            details: Cow::Owned(format!(
                "failed building Arrow TimestampMicrosecondArray: {err}"
            )),
        }
    })?;
    Ok(Arc::new(array))
}

#[cfg(feature = "arrow")]
fn time64_micros_array_from_parts(
    values: Vec<i64>,
    validity: Vec<u8>,
    row_count: usize,
) -> Result<ArrayRef> {
    let nulls = null_buffer_from_validity(validity, row_count)?;
    let array = Time64MicrosecondArray::try_new(values.into(), nulls).map_err(|err| {
        Error::InvalidConfiguration {
            details: Cow::Owned(format!(
                "failed building Arrow Time64MicrosecondArray: {err}"
            )),
        }
    })?;
    Ok(Arc::new(array))
}

#[cfg(feature = "arrow")]
fn utf8_array_from_parts(
    offsets: Vec<i32>,
    values: Vec<u8>,
    validity: Vec<u8>,
    row_count: usize,
) -> Result<ArrayRef> {
    let offsets = offset_buffer_from_vec("utf8", offsets, row_count, values.len())?;
    let values = Buffer::from_vec(values);
    let nulls = null_buffer_from_validity(validity, row_count)?;
    let array = StringArray::try_new(offsets, values, nulls).map_err(|err| {
        Error::InvalidConfiguration {
            details: Cow::Owned(format!("failed building Arrow StringArray: {err}")),
        }
    })?;
    Ok(Arc::new(array))
}

#[cfg(feature = "arrow")]
fn binary_array_from_parts(
    offsets: Vec<i32>,
    values: Vec<u8>,
    validity: Vec<u8>,
    row_count: usize,
) -> Result<ArrayRef> {
    let offsets = offset_buffer_from_vec("binary", offsets, row_count, values.len())?;
    let values = Buffer::from_vec(values);
    let nulls = null_buffer_from_validity(validity, row_count)?;
    let array = BinaryArray::try_new(offsets, values, nulls).map_err(|err| {
        Error::InvalidConfiguration {
            details: Cow::Owned(format!("failed building Arrow BinaryArray: {err}")),
        }
    })?;
    Ok(Arc::new(array))
}

#[cfg(feature = "arrow")]
fn offset_buffer_from_vec(
    kind: &str,
    offsets: Vec<i32>,
    row_count: usize,
    values_len: usize,
) -> Result<OffsetBuffer<i32>> {
    validate_offsets(kind, &offsets, row_count, values_len)?;
    // SAFETY: `validate_offsets` enforces non-empty, non-negative, monotonic offsets.
    Ok(unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) })
}

#[cfg(feature = "arrow")]
fn validate_offsets(
    kind: &str,
    offsets: &[i32],
    row_count: usize,
    values_len: usize,
) -> Result<()> {
    if offsets.len() != row_count.saturating_add(1) {
        return Err(Error::InvalidConfiguration {
            details: Cow::Owned(format!(
                "{kind} column offset length {} does not match row_count + 1 ({})",
                offsets.len(),
                row_count.saturating_add(1)
            )),
        });
    }

    let mut previous = 0usize;
    for (index, offset) in offsets.iter().enumerate() {
        let current = usize::try_from(*offset).map_err(|_| Error::InvalidConfiguration {
            details: Cow::Owned(format!(
                "{kind} column has negative offset at index {index}: {offset}"
            )),
        })?;
        if index > 0 && current < previous {
            return Err(Error::InvalidConfiguration {
                details: Cow::Owned(format!(
                    "{kind} column offsets are not monotonic at index {index}: {previous}>{current}"
                )),
            });
        }
        if current > values_len {
            return Err(Error::InvalidConfiguration {
                details: Cow::Owned(format!(
                    "{kind} column offset {current} exceeds values length {values_len}"
                )),
            });
        }
        previous = current;
    }
    Ok(())
}

#[cfg(feature = "arrow")]
fn null_buffer_from_validity(
    mut validity: Vec<u8>,
    row_count: usize,
) -> Result<Option<NullBuffer>> {
    if row_count == 0 {
        return Ok(None);
    }

    let expected_len = validity_bytes_for_rows(row_count);
    if validity.len() < expected_len {
        return Err(Error::InvalidConfiguration {
            details: Cow::Owned(format!(
                "validity buffer length {} is smaller than required {} for row_count {row_count}",
                validity.len(),
                expected_len
            )),
        });
    }
    validity.truncate(expected_len);

    let mut valid_count = 0usize;
    for (index, byte) in validity.iter_mut().enumerate() {
        let is_last = index + 1 == expected_len;
        if is_last {
            let used_bits = row_count % 8;
            if used_bits != 0 {
                let mask = (1u8 << used_bits) - 1;
                *byte &= mask;
            }
        }
        valid_count = valid_count.saturating_add(byte.count_ones() as usize);
    }
    if valid_count == row_count {
        return Ok(None);
    }

    let bitmap = BooleanBuffer::new(Buffer::from_vec(validity), 0, row_count);
    Ok(Some(NullBuffer::new(bitmap)))
}
