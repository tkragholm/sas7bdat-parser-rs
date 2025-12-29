// Example functions

use savvy::NotAvailableValue;
use savvy::savvy;
use savvy::{
    IntegerSexp, OwnedIntegerSexp, OwnedListSexp, OwnedRealSexp, OwnedStringSexp, StringSexp,
};
use std::convert::TryFrom;
use std::fs::File;
use std::io::BufWriter;

// Bring in the core crate
use sas7bdat::SasFile;
use sas7bdat::metadata::{VariableKind, Vendor};
use sas7bdat::sinks::{CsvSink, ParquetSink};
use sas7bdat::value::Value;

/// Convert Input To Upper-Case
///
/// @param x A character vector.
/// @returns A character vector with upper case version of the input.
/// @export
#[savvy]
fn to_upper(x: StringSexp) -> savvy::Result<savvy::Sexp> {
    let mut out = OwnedStringSexp::new(x.len())?;

    for (i, e) in x.iter().enumerate() {
        if e.is_na() {
            out.set_na(i)?;
            continue;
        }

        let e_upper = e.to_uppercase();
        out.set_elt(i, &e_upper)?;
    }

    Ok(out.into())
}

/// Multiply Input By Another Input
///
/// @param x An integer vector.
/// @param y An integer to multiply.
/// @returns An integer vector with values multiplied by `y`.
/// @export
#[savvy]
fn int_times_int(x: IntegerSexp, y: i32) -> savvy::Result<savvy::Sexp> {
    let mut out = OwnedIntegerSexp::new(x.len())?;

    for (i, e) in x.iter().enumerate() {
        if e.is_na() {
            out.set_na(i)?;
        } else {
            out[i] = e * y;
        }
    }

    Ok(out.into())
}

#[savvy]
struct Person {
    pub name: String,
}

/// A person with a name
///
/// @export
#[savvy]
impl Person {
    fn new() -> Self {
        Self {
            name: String::new(),
        }
    }

    fn set_name(&mut self, name: &str) -> savvy::Result<()> {
        self.name = name.to_string();
        Ok(())
    }

    fn name(&self) -> savvy::Result<savvy::Sexp> {
        let mut out = OwnedStringSexp::new(1)?;
        out.set_elt(0, &self.name)?;
        Ok(out.into())
    }

    fn associated_function() -> savvy::Result<savvy::Sexp> {
        let mut out = OwnedStringSexp::new(1)?;
        out.set_elt(0, "associated_function")?;
        Ok(out.into())
    }
}

/// @export
#[savvy]
fn hello() -> savvy::Result<()> {
    savvy::r_println!("Hello world!");
    Ok(())
}

// This test is run by `cargo test`. You can put tests that don't need a real
// R session here.
#[cfg(test)]
mod test1 {
    #[test]
    fn test_person() {
        let mut p = super::Person::new();
        p.set_name("foo").expect("set_name() must succeed");
        assert_eq!(&p.name, "foo");
    }
}

// Tests marked under `#[cfg(feature = "savvy-test")]` are run by `savvy-cli test`, which
// executes the Rust code on a real R session so that you can use R things for
// testing.
#[cfg(feature = "savvy-test")]
mod test1 {
    // The return type must be `savvy::Result<()>`
    #[test]
    fn test_to_upper() -> savvy::Result<()> {
        // You can create a non-owned version of input by `.as_read_only()`
        let x = savvy::OwnedStringSexp::try_from_slice(["foo", "bar"])?.as_read_only();

        let result = super::to_upper(x)?;

        // This function compares an SEXP with the result of R code specified in
        // the second argument.
        savvy::assert_eq_r_code(result, r#"c("FOO", "BAR")"#);

        Ok(())
    }
}

// --- New bindings using the core crate ---

fn map_core_err<E>(e: E) -> savvy::Error
where
    E: std::fmt::Display,
{
    savvy::Error::new(format!("sas7bdat error: {e}"))
}

fn map_io_err(action: &str, path: &str, err: &std::io::Error) -> savvy::Error {
    savvy::Error::new(format!("failed to {action} '{path}': {err}"))
}

/// Count rows in a SAS7BDAT file
///
/// @param path Path to a .sas7bdat file
/// @return Integer scalar with the row count (capped at 2^31-1 if larger)
/// @export
#[savvy]
fn sas_row_count(path: &str) -> savvy::Result<savvy::Sexp> {
    let file = SasFile::open(path).map_err(map_core_err)?;
    // Prefer metadata row_count when available
    let rc = file.metadata().row_count;
    let mut out = OwnedIntegerSexp::new(1)?;
    // R integers are 32-bit; cap if exceeded
    let val = i32::try_from(rc).unwrap_or(i32::MAX);
    out[0] = val;
    Ok(out.into())
}

/// Column names of a SAS7BDAT file
///
/// @param path Path to a .sas7bdat file
/// @return Character vector of column names
/// @export
#[savvy]
fn sas_column_names(path: &str) -> savvy::Result<savvy::Sexp> {
    let file = SasFile::open(path).map_err(map_core_err)?;
    let names: Vec<String> = file
        .metadata()
        .variables
        .iter()
        .map(|v| v.name.clone())
        .collect();
    let mut out = OwnedStringSexp::new(names.len())?;
    for (i, n) in names.iter().enumerate() {
        out.set_elt(i, n)?;
    }
    Ok(out.into())
}

/// Basic metadata as JSON (for convenient consumption in R)
///
/// @param path Path to a .sas7bdat file
/// @return Length-1 character vector with a JSON string
/// @export
#[savvy]
fn sas_metadata_json(path: &str) -> savvy::Result<savvy::Sexp> {
    let file = SasFile::open(path).map_err(map_core_err)?;
    let md = file.metadata();

    let vendor = match md.vendor {
        Vendor::Sas => "SAS",
        Vendor::StatTransfer => "StatTransfer",
        Vendor::Other(_) => "Other",
    };
    let compression = match md.compression {
        sas7bdat::metadata::Compression::None => "none",
        sas7bdat::metadata::Compression::Row => "row",
        sas7bdat::metadata::Compression::Binary => "binary",
        sas7bdat::metadata::Compression::Unknown(_) => "unknown",
    };
    let endianness = match md.endianness {
        sas7bdat::metadata::Endianness::Little => "little",
        sas7bdat::metadata::Endianness::Big => "big",
    };

    let column_names: Vec<&str> = md.variables.iter().map(|v| v.name.as_str()).collect();
    let column_types: Vec<&str> = md
        .variables
        .iter()
        .map(|v| match v.kind {
            VariableKind::Numeric => "numeric",
            VariableKind::Character => "character",
        })
        .collect();

    let obj = serde_json::json!({
        "row_count": md.row_count,
        "column_count": md.column_count,
        "table_name": md.table_name,
        "file_label": md.file_label,
        "file_encoding": md.file_encoding,
        "vendor": vendor,
        "compression": compression,
        "endianness": endianness,
        "version": { "major": md.version.major, "minor": md.version.minor, "revision": md.version.revision },
        "timestamps": {
            "created": md.timestamps.created.as_ref().map(std::string::ToString::to_string),
            "modified": md.timestamps.modified.as_ref().map(std::string::ToString::to_string)
        },
        "columns": {
            "names": column_names,
            "types": column_types
        }
    });

    let json = obj.to_string();
    let mut out = OwnedStringSexp::new(1)?;
    out.set_elt(0, &json)?;
    Ok(out.into())
}

enum NumericRole {
    General,
    Date,
    DateTime,
    Time,
}

impl NumericRole {
    const fn update(&mut self, new_role: Self) {
        if matches!(self, Self::General) {
            *self = new_role;
        }
    }

    const fn label(&self) -> &'static str {
        match self {
            Self::General => "double",
            Self::Date => "date",
            Self::DateTime => "datetime",
            Self::Time => "time",
        }
    }
}

struct NumericColumn {
    values: Vec<f64>,
    role: NumericRole,
}

impl NumericColumn {
    fn new(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            role: NumericRole::General,
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn push(&mut self, value: &Value<'_>, column_name: &str) -> savvy::Result<()> {
        match value {
            Value::Missing(_) => self.values.push(f64::NAN),
            Value::Float(v) => self.values.push(*v),
            Value::Int32(v) => self.values.push(f64::from(*v)),
            Value::Int64(v) => self.values.push(*v as f64),
            Value::NumericString(text) | Value::Str(text) => {
                match parse_numeric(text.as_ref(), column_name)? {
                    Some(num) => self.values.push(num),
                    None => self.values.push(f64::NAN),
                }
            }
            Value::Bytes(bytes) => {
                let text = std::str::from_utf8(bytes.as_ref()).map_err(|_| {
                    savvy::Error::new(format!(
                        "column '{column_name}' contains non-UTF8 bytes in numeric field"
                    ))
                })?;
                match parse_numeric(text, column_name)? {
                    Some(num) => self.values.push(num),
                    None => self.values.push(f64::NAN),
                }
            }
            Value::Date(datetime) => {
                self.role.update(NumericRole::Date);
            let seconds = datetime.unix_timestamp() as f64
                + f64::from(datetime.nanosecond()) / 1_000_000_000.0;
            self.values.push(seconds / 86_400.0);
        }
        Value::DateTime(datetime) => {
            self.role.update(NumericRole::DateTime);
            let seconds = datetime.unix_timestamp() as f64
                + f64::from(datetime.nanosecond()) / 1_000_000_000.0;
            self.values.push(seconds);
        }
            Value::Time(duration) => {
                self.role.update(NumericRole::Time);
                self.values.push(duration.as_seconds_f64());
            }
        }
        Ok(())
    }

    fn finalize(self) -> savvy::Result<(savvy::Sexp, String)> {
        let owned = OwnedRealSexp::try_from_slice(&self.values)?;
        let sexp: savvy::Result<savvy::Sexp> = owned.into();
        let sexp = sexp?;
        Ok((sexp, self.role.label().to_string()))
    }
}

struct StringColumn {
    values: Vec<Option<String>>,
}

impl StringColumn {
    fn new(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: &Value<'_>) -> savvy::Result<()> {
        match value {
            Value::Missing(_) => self.values.push(None),
            Value::Str(text) | Value::NumericString(text) => {
                self.values.push(Some(text.as_ref().to_string()));
            }
            Value::Bytes(bytes) => {
                let text = std::str::from_utf8(bytes.as_ref()).map_err(|_| {
                    savvy::Error::new("character column contains non-UTF8 bytes".to_string())
                })?;
                self.values.push(Some(text.to_string()));
            }
            Value::Float(v) => self.values.push(Some(format!("{v}"))),
            Value::Int32(v) => self.values.push(Some(v.to_string())),
            Value::Int64(v) => self.values.push(Some(v.to_string())),
            Value::DateTime(datetime) => self.values.push(Some(datetime.to_string())),
            Value::Date(datetime) => self.values.push(Some(datetime.date().to_string())),
            Value::Time(duration) => self.values.push(Some(duration.to_string())),
        }
        Ok(())
    }

    fn finalize(self) -> savvy::Result<(savvy::Sexp, String)> {
        let mut out = OwnedStringSexp::new(self.values.len())?;
        for (idx, value) in self.values.iter().enumerate() {
            match value {
                Some(text) => out.set_elt(idx, text)?,
                None => out.set_na(idx)?,
            }
        }
        let sexp: savvy::Result<savvy::Sexp> = out.into();
        Ok((sexp?, "character".to_string()))
    }
}

enum ColumnData {
    Numeric(NumericColumn),
    Character(StringColumn),
}

impl ColumnData {
    fn push(&mut self, value: &Value<'_>, column_name: &str) -> savvy::Result<()> {
        match self {
            Self::Numeric(col) => col.push(value, column_name),
            Self::Character(col) => col.push(value),
        }
    }

    fn finalize(self) -> savvy::Result<(savvy::Sexp, String)> {
        match self {
            Self::Numeric(col) => col.finalize(),
            Self::Character(col) => col.finalize(),
        }
    }
}

fn parse_numeric(text: &str, column_name: &str) -> savvy::Result<Option<f64>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed.parse::<f64>().map(Some).map_err(|_| {
        savvy::Error::new(format!(
            "column '{column_name}' value '{trimmed}' cannot be parsed as double"
        ))
    })
}

/// Read a SAS7BDAT file into a column-oriented representation
///
/// @param path Path to a .sas7bdat file
/// @return Named list of vectors, with column metadata attached as attributes
/// @export
#[savvy]
fn read_sas(path: &str) -> savvy::Result<savvy::Sexp> {
    let mut file = SasFile::open(path).map_err(map_core_err)?;
    let metadata = file.metadata().clone();

    let column_count = metadata.variables.len();
    let capacity = usize::try_from(metadata.row_count).map_err(|_| {
        savvy::Error::new(format!(
            "row count {} exceeds supported size on this platform",
            metadata.row_count
        ))
    })?;

    let mut columns: Vec<ColumnData> = Vec::with_capacity(column_count);
    let mut names: Vec<String> = Vec::with_capacity(column_count);

    for variable in &metadata.variables {
        names.push(variable.name.trim_end().to_string());
        match variable.kind {
            VariableKind::Numeric => {
                columns.push(ColumnData::Numeric(NumericColumn::new(capacity)));
            }
            VariableKind::Character => {
                columns.push(ColumnData::Character(StringColumn::new(capacity)));
            }
        }
    }

    let mut rows_seen = 0usize;
    {
        let mut rows = file.rows().map_err(map_core_err)?;
        while let Some(row) = rows.try_next().map_err(map_core_err)? {
            if row.len() != columns.len() {
                return Err(savvy::Error::new(format!(
                    "expected {} columns, but got {} in row {}",
                    columns.len(),
                    row.len(),
                    rows_seen + 1
                )));
            }
            for (idx, value) in row.iter().enumerate() {
                columns[idx].push(value, &names[idx])?;
            }
            rows_seen += 1;
        }
    }

    let mut out = OwnedListSexp::new(column_count, true)?;
    let mut type_labels: Vec<String> = Vec::with_capacity(column_count);

    for (idx, (column, name)) in columns.into_iter().zip(names.iter()).enumerate() {
        let (sexp, label) = column.finalize()?;
        type_labels.push(label);
        out.set_name_and_value(idx, name, sexp)?;
    }

    let types_attr =
        OwnedStringSexp::try_from_iter(type_labels.iter().map(std::string::String::as_str))?;
    let types_attr: savvy::Result<savvy::Sexp> = types_attr.into();
    let types_attr = types_attr?;
    out.set_attrib("column_types", types_attr)?;

    let mut row_count_attr = OwnedIntegerSexp::new(1)?;
    row_count_attr[0] = if rows_seen > i32::MAX as usize {
        i32::MAX
    } else {
        i32::try_from(rows_seen)?
    };
    let row_count_attr: savvy::Result<savvy::Sexp> = row_count_attr.into();
    let row_count_attr = row_count_attr?;
    out.set_attrib("row_count", row_count_attr)?;

    out.into()
}

/// Stream a SAS7BDAT file into an on-disk sink.
///
/// @param path Path to the input `.sas7bdat` file.
/// @param sink Output sink identifier (`"parquet"` or `"csv"`).
/// @param output Destination file path for the sink output.
/// @export
#[savvy]
fn write_sas(path: &str, sink: &str, output: &str) -> savvy::Result<()> {
    let mut sas = SasFile::open(path).map_err(map_core_err)?;
    let sink_kind = sink.trim().to_ascii_lowercase();
    match sink_kind.as_str() {
        "parquet" => {
            let file =
                File::create(output).map_err(|e| map_io_err("create parquet file", output, &e))?;
            let mut writer = ParquetSink::new(file);
            sas.write_into_sink(&mut writer).map_err(map_core_err)?;
        }
        "csv" => {
            let file =
                File::create(output).map_err(|e| map_io_err("create csv file", output, &e))?;
            let buf = BufWriter::new(file);
            let mut writer = CsvSink::new(buf);
            sas.write_into_sink(&mut writer).map_err(map_core_err)?;
        }
        other => {
            return Err(savvy::Error::new(format!(
                "unsupported sink '{other}'. expected 'parquet' or 'csv'"
            )));
        }
    }
    Ok(())
}
