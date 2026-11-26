//! The shape of the written columns: their names, constant columns appended after
//! them, and the Arrow types that strings, timestamps and times are written as.
//!
//! A [`ColumnPlan`] is set once for a run. [`ColumnPlan::for_input`] fills in the
//! values that depend on the file, [`ColumnPlan::resolve`] fixes the plan against
//! that file's Arrow schema, and the [`ResolvedColumns`] it returns rewrites each
//! record batch. The default plan keeps the names and writes SAS times as
//! Parquet's `TIME`, the one type every reader shows as a time of day. A file the
//! plan leaves unchanged, such as one without time columns, is not rewritten at all.
//!
//! Names are a rule rather than a list, so one plan fits files with different
//! columns: a case rule for every column, and a rename map that overrides it for
//! the columns it names. SAS names are case-insensitive, so the map's keys match
//! the file's names regardless of case. A key that matches no column is an error,
//! as is any rule that gives two columns the same name.
//!
//! The type conversions only rescale or reinterpret values that are already
//! decoded:
//!
//! - `string` to `large_string` widens the offsets and shares the text buffer.
//! - Timestamps and durations change unit by floor division, so an instant
//!   before 1970 rounds towards the past like any other. A finer unit multiplies,
//!   and a value that no longer fits in 64 bits is an error.
//! - The reader decodes SAS times as durations, since SAS stores elapsed times in
//!   the same type. A duration becomes a time of day only when every value lies in
//!   `[0, 24 h)`. A value outside that range fails the file and names the column
//!   and the setting that keeps it, rather than wrapping 25 hours into 1 a.m.
//!   [`TimeType::Duration`] writes elapsed times as they are.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use arrow_array::{
    Array, ArrayRef, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, LargeStringArray, RecordBatch, StringArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, cast::AsArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

pub use arrow_schema::TimeUnit;

/// The case rule applied to every column the rename map does not name.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NameCase {
    /// The names as stored in the file.
    #[default]
    AsIs,
    Lower,
    Upper,
}

/// The Arrow type text columns are written as.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StringType {
    /// `string`, with 32-bit offsets.
    #[default]
    Utf8,
    /// `large_string`, with 64-bit offsets.
    LargeUtf8,
}

/// The Arrow type SAS time values are written as.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeType {
    /// An elapsed time, which may exceed a day or be negative. Parquet has no
    /// duration type, so a reader that ignores Arrow's embedded schema, such as
    /// DuckDB's own Parquet reader, sees a plain integer.
    Duration(TimeUnit),
    /// A time of day, Parquet's `TIME`. Seconds and milliseconds are `time32`,
    /// microseconds and nanoseconds `time64`. R's arrow reads it as `hms` and DuckDB
    /// as `TIME`.
    TimeOfDay(TimeUnit),
}

impl Default for TimeType {
    /// Microseconds, which is DuckDB's own `TIME` precision. SAS stores times as
    /// seconds with a fraction, so a coarser unit would floor part of it away.
    fn default() -> Self {
        Self::TimeOfDay(TimeUnit::Microsecond)
    }
}

/// The value of a constant column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstantValue {
    Text(String),
    /// The input file's path as it was given, filled in by [`ColumnPlan::for_input`].
    InputPath,
}

/// How the written columns are named and typed. The default keeps the names and
/// changes only the time columns, to [`TimeType::default`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColumnPlan {
    pub case: NameCase,
    /// `(name in the file, name to write)`. Keys match case-insensitively, and the
    /// new name is written as given, without the case rule.
    pub rename: Vec<(String, String)>,
    /// Columns appended after the file's own, in order, holding one value in every row.
    pub constants: Vec<(String, ConstantValue)>,
    pub strings: StringType,
    /// The unit timestamps are written in. `None` keeps the reader's microseconds.
    pub timestamps: Option<TimeUnit>,
    /// How times are written.
    pub times: TimeType,
}

impl ColumnPlan {
    /// Whether the plan changes any column's name or adds a column.
    #[must_use]
    pub fn renames_or_adds(&self) -> bool {
        self.case != NameCase::AsIs || !self.rename.is_empty() || !self.constants.is_empty()
    }

    /// Whether any type setting differs from its default. CSV and TSV write values as
    /// text, so the delimited writers refuse a plan for which this is true and
    /// ignore the default.
    #[must_use]
    pub fn sets_types(&self) -> bool {
        self.strings != StringType::default()
            || self.timestamps.is_some()
            || self.times != TimeType::default()
    }

    /// This plan with every [`ConstantValue::InputPath`] replaced by `input`.
    #[must_use]
    pub fn for_input(&self, input: &Path) -> Self {
        let mut plan = self.clone();
        for (_, value) in &mut plan.constants {
            if *value == ConstantValue::InputPath {
                *value = ConstantValue::Text(input.to_string_lossy().into_owned());
            }
        }
        plan
    }

    /// The names written for columns named `names` in the file, followed by the
    /// constant columns' names.
    ///
    /// # Errors
    ///
    /// Returns an error if a rename key matches no column, or if two written columns
    /// would share a name.
    pub fn output_names(&self, names: &[String]) -> Result<Vec<String>> {
        for (from, _) in &self.rename {
            if !names.iter().any(|name| name.eq_ignore_ascii_case(from)) {
                bail!("cannot rename column `{from}`: the file has no column of that name");
            }
        }
        let mut out: Vec<String> = names
            .iter()
            .map(|name| {
                self.rename
                    .iter()
                    .find(|(from, _)| name.eq_ignore_ascii_case(from))
                    .map_or_else(
                        || match self.case {
                            NameCase::AsIs => name.clone(),
                            NameCase::Lower => name.to_lowercase(),
                            NameCase::Upper => name.to_uppercase(),
                        },
                        |(_, to)| to.clone(),
                    )
            })
            .collect();
        out.extend(self.constants.iter().map(|(name, _)| name.clone()));
        for (index, name) in out.iter().enumerate() {
            if name.is_empty() {
                bail!("a written column would have an empty name");
            }
            if let Some(first) = out[..index].iter().position(|earlier| earlier == name) {
                let source = |position: usize| {
                    names.get(position).map_or_else(
                        || "an added constant column".to_string(),
                        |name| format!("column `{name}`"),
                    )
                };
                bail!(
                    "{} and {} would both be written as `{name}`",
                    source(first),
                    source(index)
                );
            }
        }
        Ok(out)
    }

    /// Fix the plan against one file's schema.
    ///
    /// # Errors
    ///
    /// As [`Self::output_names`], and if a constant still holds
    /// [`ConstantValue::InputPath`], which [`Self::for_input`] replaces.
    pub fn resolve(&self, input: &SchemaRef) -> Result<ResolvedColumns> {
        let names: Vec<String> = input.fields().iter().map(|f| f.name().clone()).collect();
        let out_names = self.output_names(&names)?;
        let mut fields: Vec<Field> = input
            .fields()
            .iter()
            .zip(&out_names)
            .map(|(field, name)| {
                field
                    .as_ref()
                    .clone()
                    .with_name(name.clone())
                    .with_data_type(self.output_type(field.data_type()))
            })
            .collect();
        let string_type = match self.strings {
            StringType::Utf8 => DataType::Utf8,
            StringType::LargeUtf8 => DataType::LargeUtf8,
        };
        let mut constants = Vec::with_capacity(self.constants.len());
        for ((_, value), name) in self.constants.iter().zip(&out_names[names.len()..]) {
            let ConstantValue::Text(text) = value else {
                bail!("constant column `{name}` takes the input path, which was not filled in");
            };
            fields.push(Field::new(name.clone(), string_type.clone(), false));
            constants.push(text.clone());
        }
        Ok(ResolvedColumns {
            schema: Arc::new(Schema::new_with_metadata(fields, input.metadata().clone())),
            input_columns: names.len(),
            constants,
        })
    }

    fn output_type(&self, input: &DataType) -> DataType {
        match input {
            DataType::Utf8 if self.strings == StringType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Timestamp(_, tz) => self.timestamps.map_or_else(
                || input.clone(),
                |unit| DataType::Timestamp(unit, tz.clone()),
            ),
            DataType::Duration(_) => match self.times {
                TimeType::Duration(unit) => DataType::Duration(unit),
                TimeType::TimeOfDay(unit @ (TimeUnit::Second | TimeUnit::Millisecond)) => {
                    DataType::Time32(unit)
                }
                TimeType::TimeOfDay(unit) => DataType::Time64(unit),
            },
            other => other.clone(),
        }
    }
}

/// A [`ColumnPlan`] fixed against one file's schema.
#[derive(Debug)]
pub struct ResolvedColumns {
    schema: SchemaRef,
    input_columns: usize,
    constants: Vec<String>,
}

impl ResolvedColumns {
    /// The schema of the batches [`Self::apply`] returns.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Whether the rewrite changes anything for a file whose schema is `input`: a
    /// name, a type, or an added column. Field metadata is carried over unchanged
    /// and does not count.
    #[must_use]
    pub fn changes(&self, input: &Schema) -> bool {
        !self.constants.is_empty()
            || input.fields().len() != self.input_columns
            || input
                .fields()
                .iter()
                .zip(self.schema.fields())
                .any(|(from, to)| from.name() != to.name() || from.data_type() != to.data_type())
    }

    /// The written name of the file's column `index`.
    #[must_use]
    pub fn name_of(&self, index: usize) -> Option<&str> {
        (index < self.input_columns).then(|| self.schema.field(index).name().as_str())
    }

    /// Rewrite one batch of the file's columns into the planned schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch does not have the file's columns, if a rescaled
    /// value overflows, or if a time of day is outside `[0, 24 h)`.
    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.num_columns() != self.input_columns {
            bail!(
                "batch has {} columns, the plan was resolved for {}",
                batch.num_columns(),
                self.input_columns
            );
        }
        let rows = batch.num_rows();
        let mut arrays = Vec::with_capacity(self.schema.fields().len());
        for (index, column) in batch.columns().iter().enumerate() {
            let field = self.schema.field(index);
            arrays.push(convert(column, field.data_type(), field.name())?);
        }
        for (offset, text) in self.constants.iter().enumerate() {
            let field = self.schema.field(self.input_columns + offset);
            arrays.push(constant(text, rows, field.data_type()));
        }
        RecordBatch::try_new(SchemaRef::clone(&self.schema), arrays)
            .map_err(|err| anyhow!("rewriting a batch: {err}"))
    }
}

fn constant(text: &str, rows: usize, data_type: &DataType) -> ArrayRef {
    if *data_type == DataType::LargeUtf8 {
        Arc::new(LargeStringArray::from_iter_values(std::iter::repeat_n(
            text, rows,
        )))
    } else {
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            text, rows,
        )))
    }
}

fn convert(array: &ArrayRef, target: &DataType, name: &str) -> Result<ArrayRef> {
    let source = array.data_type();
    if source == target {
        return Ok(ArrayRef::clone(array));
    }
    match (source, target) {
        (DataType::Utf8, DataType::LargeUtf8) => {
            let strings = array.as_string::<i32>();
            let offsets: Vec<i64> = strings
                .value_offsets()
                .iter()
                .map(|&offset| i64::from(offset))
                .collect();
            Ok(Arc::new(LargeStringArray::new(
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                strings.values().clone(),
                strings.nulls().cloned(),
            )))
        }
        (
            DataType::Timestamp(from, _) | DataType::Duration(from),
            DataType::Timestamp(to, _) | DataType::Duration(to),
        ) => {
            let (values, nulls) = rescaled(array, *from, *to, name)?;
            Ok(i64_array(target, values, nulls))
        }
        (DataType::Duration(from), DataType::Time32(to) | DataType::Time64(to)) => {
            let (values, nulls) = rescaled(array, *from, *to, name)?;
            let day = 86_400 * units_per_second(*to);
            if let Some(bad) = values
                .iter()
                .enumerate()
                .find(|(row, value)| is_set(nulls.as_ref(), *row) && !(0..day).contains(*value))
                .map(|(_, value)| *value)
            {
                #[allow(clippy::cast_precision_loss)]
                let hours = bad as f64 / (3_600 * units_per_second(*to)) as f64;
                bail!(
                    "column `{name}` holds a time of {hours:.2} hours, outside one day, which \
                     Parquet's TIME cannot hold. Write times as durations to keep it: \
                     times = \"duration\" in R, --time-type duration on the command line"
                );
            }
            Ok(i64_array(target, values, nulls))
        }
        _ => bail!("column `{name}`: no conversion from {source} to {target}"),
    }
}

fn is_set(nulls: Option<&NullBuffer>, row: usize) -> bool {
    nulls.is_none_or(|nulls| nulls.is_valid(row))
}

const fn units_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// The 64-bit values of a timestamp or duration array in unit `to`. Null slots
/// become 0, so whatever they held cannot overflow.
fn rescaled(
    array: &ArrayRef,
    from: TimeUnit,
    to: TimeUnit,
    name: &str,
) -> Result<(Vec<i64>, Option<NullBuffer>)> {
    let data = array.to_data();
    let raw: ScalarBuffer<i64> =
        ScalarBuffer::new(data.buffers()[0].clone(), data.offset(), data.len());
    let nulls = array.nulls().cloned();
    let (from, to) = (units_per_second(from), units_per_second(to));
    let mut values = Vec::with_capacity(raw.len());
    for (row, &value) in raw.iter().enumerate() {
        let value = if !is_set(nulls.as_ref(), row) {
            0
        } else if to >= from {
            value.checked_mul(to / from).ok_or_else(|| {
                anyhow!("column `{name}`: a value does not fit in 64 bits in the finer unit")
            })?
        } else {
            value.div_euclid(from / to)
        };
        values.push(value);
    }
    Ok((values, nulls))
}

#[allow(clippy::cast_possible_truncation)]
fn i64_array(target: &DataType, values: Vec<i64>, nulls: Option<NullBuffer>) -> ArrayRef {
    let values64 = || ScalarBuffer::from(values.clone());
    // Time32 values were range-checked against one day, so they fit in 32 bits.
    let values32 = || ScalarBuffer::from(values.iter().map(|&v| v as i32).collect::<Vec<_>>());
    match target {
        DataType::Timestamp(unit, tz) => {
            let tz = tz.clone();
            match unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::new(values64(), nulls).with_timezone_opt(tz))
                }
                TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::new(values64(), nulls).with_timezone_opt(tz),
                ),
                TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::new(values64(), nulls).with_timezone_opt(tz),
                ),
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondArray::new(values64(), nulls).with_timezone_opt(tz))
                }
            }
        }
        DataType::Duration(TimeUnit::Second) => {
            Arc::new(DurationSecondArray::new(values64(), nulls))
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::new(values64(), nulls))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::new(values64(), nulls))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Arc::new(DurationNanosecondArray::new(values64(), nulls))
        }
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::new(values32(), nulls)),
        DataType::Time32(_) => Arc::new(Time32MillisecondArray::new(values32(), nulls)),
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::new(values64(), nulls))
        }
        _ => Arc::new(Time64NanosecondArray::new(values64(), nulls)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn default_plan_sets_nothing_but_writes_times_of_day() {
        let plan = ColumnPlan::default();
        assert!(!plan.renames_or_adds());
        assert!(!plan.sets_types());
        assert_eq!(plan.times, TimeType::TimeOfDay(TimeUnit::Microsecond));
        let lower = ColumnPlan {
            case: NameCase::Lower,
            ..ColumnPlan::default()
        };
        assert!(lower.renames_or_adds());
        assert!(!lower.sets_types());
        let durations = ColumnPlan {
            times: TimeType::Duration(TimeUnit::Nanosecond),
            ..ColumnPlan::default()
        };
        assert!(durations.sets_types());
    }

    #[test]
    fn default_plan_leaves_a_file_without_times_unchanged() {
        let input = schema(vec![
            Field::new("A", DataType::Int64, true),
            Field::new("S", DataType::Utf8, true),
            Field::new("T", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]);
        assert!(
            !ColumnPlan::default()
                .resolve(&input)
                .unwrap()
                .changes(&input)
        );
        let with_time = schema(vec![Field::new(
            "TM",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        )]);
        assert!(
            ColumnPlan::default()
                .resolve(&with_time)
                .unwrap()
                .changes(&with_time)
        );
    }

    #[test]
    fn case_rule_and_rename_map() {
        let plan = ColumnPlan {
            case: NameCase::Lower,
            rename: vec![("pnr".into(), "PersonID".into())],
            constants: vec![("source_file".into(), ConstantValue::Text("x".into()))],
            ..ColumnPlan::default()
        };
        let out = plan
            .output_names(&names(&["PNR", "Dato", "ÆNDRET"]))
            .unwrap();
        assert_eq!(out, names(&["PersonID", "dato", "ændret", "source_file"]));
    }

    #[test]
    fn unknown_rename_key_is_an_error() {
        let plan = ColumnPlan {
            rename: vec![("nope".into(), "x".into())],
            ..ColumnPlan::default()
        };
        let err = plan.output_names(&names(&["A"])).unwrap_err().to_string();
        assert!(err.contains("`nope`"), "{err}");
    }

    #[test]
    fn collisions_name_both_columns() {
        let plan = ColumnPlan {
            case: NameCase::Lower,
            ..ColumnPlan::default()
        };
        let err = plan
            .output_names(&names(&["Id", "ID"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`Id`") && err.contains("`ID`"), "{err}");

        let plan = ColumnPlan {
            constants: vec![("A".into(), ConstantValue::Text(String::new()))],
            ..ColumnPlan::default()
        };
        let err = plan.output_names(&names(&["A"])).unwrap_err().to_string();
        assert!(err.contains("constant"), "{err}");
    }

    #[test]
    fn input_path_is_filled_per_file() {
        let plan = ColumnPlan {
            constants: vec![("source_file".into(), ConstantValue::InputPath)],
            ..ColumnPlan::default()
        };
        let input = schema(vec![Field::new("A", DataType::Int64, true)]);
        assert!(
            plan.resolve(&input).is_err(),
            "unfilled path must not resolve"
        );
        let resolved = plan
            .for_input(Path::new("E/rawdata/bef2020.sas7bdat"))
            .resolve(&input)
            .unwrap();
        let batch =
            RecordBatch::try_new(input, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let out = resolved.apply(&batch).unwrap();
        let source = out.column(1).as_string::<i32>();
        assert_eq!(source.value(0), "E/rawdata/bef2020.sas7bdat");
        assert_eq!(source.value(1), "E/rawdata/bef2020.sas7bdat");
        assert!(!out.schema().field(1).is_nullable());
    }

    #[test]
    fn large_strings_keep_values_nulls_and_slices() {
        let plan = ColumnPlan {
            strings: StringType::LargeUtf8,
            ..ColumnPlan::default()
        };
        let input = schema(vec![Field::new("S", DataType::Utf8, true)]);
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("æøå"),
            Some(""),
        ]));
        let batch = RecordBatch::try_new(SchemaRef::clone(&input), vec![strings]).unwrap();
        let resolved = plan.resolve(&input).unwrap();
        for batch in [batch.clone(), batch.slice(1, 3)] {
            let out = resolved.apply(&batch).unwrap();
            let large = out.column(0).as_string::<i64>();
            let small = batch.column(0).as_string::<i32>();
            assert_eq!(large.len(), small.len());
            for row in 0..small.len() {
                assert_eq!(large.is_null(row), small.is_null(row));
                if small.is_valid(row) {
                    assert_eq!(large.value(row), small.value(row));
                }
            }
        }
    }

    #[test]
    fn timestamps_floor_towards_the_past() {
        let plan = ColumnPlan {
            timestamps: Some(TimeUnit::Second),
            ..ColumnPlan::default()
        };
        let input = schema(vec![Field::new(
            "T",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        let micros: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1_500_000),
            Some(-1_500_000),
            None,
            Some(i64::MAX),
        ]));
        let batch = RecordBatch::try_new(SchemaRef::clone(&input), vec![micros]).unwrap();
        let out = plan.resolve(&input).unwrap().apply(&batch).unwrap();
        let seconds = out
            .column(0)
            .as_primitive::<arrow_array::types::TimestampSecondType>();
        assert_eq!(seconds.value(0), 1);
        assert_eq!(seconds.value(1), -2);
        assert!(seconds.is_null(2));
        assert_eq!(seconds.value(3), i64::MAX.div_euclid(1_000_000));
    }

    #[test]
    fn finer_unit_overflow_is_an_error() {
        let plan = ColumnPlan {
            timestamps: Some(TimeUnit::Nanosecond),
            ..ColumnPlan::default()
        };
        let input = schema(vec![Field::new(
            "T",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        let micros: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX), None]));
        let batch = RecordBatch::try_new(SchemaRef::clone(&input), vec![micros]).unwrap();
        assert!(plan.resolve(&input).unwrap().apply(&batch).is_err());
        // A null slot's contents are never rescaled.
        let micros: ArrayRef = Arc::new(TimestampMicrosecondArray::new(
            vec![i64::MAX].into(),
            Some(NullBuffer::new_null(1)),
        ));
        let batch = RecordBatch::try_new(SchemaRef::clone(&input), vec![micros]).unwrap();
        assert!(plan.resolve(&input).unwrap().apply(&batch).is_ok());
    }

    #[test]
    fn times_of_day_and_the_one_day_limit() {
        let plan = ColumnPlan {
            times: TimeType::TimeOfDay(TimeUnit::Millisecond),
            ..ColumnPlan::default()
        };
        let input = schema(vec![Field::new(
            "TM",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        )]);
        let resolved = plan.resolve(&input).unwrap();
        assert_eq!(
            *resolved.schema().field(0).data_type(),
            DataType::Time32(TimeUnit::Millisecond)
        );
        let nanos: ArrayRef = Arc::new(DurationNanosecondArray::from(vec![
            Some(3_661_500_000_000),
            None,
            Some(0),
        ]));
        let batch = RecordBatch::try_new(SchemaRef::clone(&input), vec![nanos]).unwrap();
        let out = resolved.apply(&batch).unwrap();
        let times = out
            .column(0)
            .as_primitive::<arrow_array::types::Time32MillisecondType>();
        assert_eq!(times.value(0), 3_661_500);
        assert!(times.is_null(1));

        let late: ArrayRef = Arc::new(DurationNanosecondArray::from(vec![Some(
            25 * 3_600 * 1_000_000_000,
        )]));
        let batch = RecordBatch::try_new(SchemaRef::clone(&input), vec![late]).unwrap();
        let err = resolved.apply(&batch).unwrap_err().to_string();
        assert!(err.contains("`TM`") && err.contains("25.00 hours"), "{err}");
        assert!(
            err.contains("times = \"duration\"") && err.contains("--time-type duration"),
            "{err}"
        );
        let early: ArrayRef = Arc::new(DurationNanosecondArray::from(vec![Some(-1)]));
        let batch = RecordBatch::try_new(SchemaRef::clone(&input), vec![early]).unwrap();
        assert!(resolved.apply(&batch).is_err());
    }

    #[test]
    fn time64_for_finer_units() {
        let plan = ColumnPlan {
            times: TimeType::TimeOfDay(TimeUnit::Microsecond),
            ..ColumnPlan::default()
        };
        let input = schema(vec![Field::new(
            "TM",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        )]);
        let resolved = plan.resolve(&input).unwrap();
        assert_eq!(
            *resolved.schema().field(0).data_type(),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }

    #[test]
    fn field_metadata_survives_a_rename() {
        let plan = ColumnPlan {
            case: NameCase::Lower,
            ..ColumnPlan::default()
        };
        let field = Field::new("A", DataType::Int64, true)
            .with_metadata([("sas.label".to_string(), "Alder".to_string())]);
        let resolved = plan.resolve(&schema(vec![field])).unwrap();
        let out = resolved.schema().field(0);
        assert_eq!(out.name(), "a");
        assert_eq!(
            out.metadata().get("sas.label").map(String::as_str),
            Some("Alder")
        );
        assert_eq!(resolved.name_of(0), Some("a"));
        assert_eq!(resolved.name_of(1), None);
    }
}
