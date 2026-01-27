use crate::{
    cell::CellValue,
    dataset::DatasetMetadata,
    error::{Error, Result},
};
use std::{
    collections::HashMap,
    sync::Arc,
};

#[derive(Debug)]
pub struct RowLookup {
    name_to_index: HashMap<String, usize>,
}

impl RowLookup {
    #[must_use] 
    pub fn from_metadata(metadata: &DatasetMetadata) -> Self {
        let mut name_to_index = HashMap::with_capacity(metadata.variables.len() * 2);
        for variable in &metadata.variables {
            let trimmed = variable.name.trim_end();
            name_to_index
                .entry(variable.name.clone())
                .or_insert(variable.index as usize);
            name_to_index
                .entry(trimmed.to_owned())
                .or_insert(variable.index as usize);
        }
        Self { name_to_index }
    }

    #[must_use]
    pub fn index(&self, name: &str) -> Option<usize> {
        if let Some(index) = self.name_to_index.get(name) {
            return Some(*index);
        }
        let trimmed = name.trim_end();
        if trimmed != name {
            return self.name_to_index.get(trimmed).copied();
        }
        None
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    values: Vec<CellValue<'static>>,
    lookup: Arc<RowLookup>,
}

impl Row {
    pub(crate) const fn new(values: Vec<CellValue<'static>>, lookup: Arc<RowLookup>) -> Self {
        Self { values, lookup }
    }

    #[must_use]
    pub fn values(&self) -> &[CellValue<'static>] {
        &self.values
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&CellValue<'static>> {
        self.lookup
            .index(name)
            .and_then(|index| self.values.get(index))
    }

    /// Returns a typed value from the row by column name.
    ///
    /// Missing values resolve to `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns an error when the column name is unknown or the value cannot be converted.
    pub fn get_as<T: RowValue>(&self, name: &str) -> Result<Option<T>> {
        let cell = self.get(name).ok_or_else(|| Error::InvalidMetadata {
            details: format!("column name '{name}' not found in row").into(),
        })?;
        T::from_cell(cell)
    }
}

pub trait RowValue: Sized {
    /// Convert a cell value to a typed value.
    ///
    /// # Errors
    ///
    /// Returns an error if the cell cannot be converted to the requested type.
    fn from_cell(cell: &CellValue<'_>) -> Result<Option<Self>>;
}

impl RowValue for i64 {
    fn from_cell(cell: &CellValue<'_>) -> Result<Option<Self>> {
        match cell {
            CellValue::Missing(_) => Ok(None),
            CellValue::Int64(value) => Ok(Some(*value)),
            CellValue::Int32(value) => Ok(Some(Self::from(*value))),
            CellValue::Float(value) => {
                if !value.is_finite() {
                    return Err(Error::InvalidMetadata {
                        details: "non-finite float cannot be converted to i64".into(),
                    });
                }
                let integer = value.trunc();
                if (value - integer).abs() > f64::EPSILON {
                    return Err(Error::InvalidMetadata {
                        details: "float has fractional component; cannot convert to i64".into(),
                    });
                }
                #[allow(clippy::cast_precision_loss)]
                let min = Self::MIN as f64;
                #[allow(clippy::cast_precision_loss)]
                let max = Self::MAX as f64;
                if integer < min || integer > max {
                    return Err(Error::InvalidMetadata {
                        details: "float exceeds i64 range".into(),
                    });
                }
                #[allow(clippy::cast_possible_truncation)]
                let casted = integer as Self;
                Ok(Some(casted))
            }
            CellValue::NumericString(text) | CellValue::Str(text) => text
                .parse::<Self>()
                .map(Some)
                .map_err(|_| Error::InvalidMetadata {
                    details: "string cannot be parsed as i64".into(),
                }),
            _ => Err(Error::InvalidMetadata {
                details: "cell type cannot be converted to i64".into(),
            }),
        }
    }
}

impl RowValue for f64 {
    fn from_cell(cell: &CellValue<'_>) -> Result<Option<Self>> {
        match cell {
            CellValue::Missing(_) => Ok(None),
            CellValue::Float(value) => Ok(Some(*value)),
            CellValue::Int32(value) => Ok(Some(Self::from(*value))),
            CellValue::Int64(value) => {
                #[allow(clippy::cast_precision_loss)]
                let value = *value as Self;
                Ok(Some(value))
            }
            CellValue::NumericString(text) | CellValue::Str(text) => text
                .parse::<Self>()
                .map(Some)
                .map_err(|_| Error::InvalidMetadata {
                    details: "string cannot be parsed as f64".into(),
                }),
            _ => Err(Error::InvalidMetadata {
                details: "cell type cannot be converted to f64".into(),
            }),
        }
    }
}

impl RowValue for String {
    fn from_cell(cell: &CellValue<'_>) -> Result<Option<Self>> {
        match cell {
            CellValue::Missing(_) => Ok(None),
            CellValue::Str(text) | CellValue::NumericString(text) => {
                Ok(Some(text.to_string()))
            }
            _ => Err(Error::InvalidMetadata {
                details: "cell type cannot be converted to String".into(),
            }),
        }
    }
}

#[cfg(feature = "time")]
impl RowValue for time::OffsetDateTime {
    fn from_cell(cell: &CellValue<'_>) -> Result<Option<Self>> {
        match cell {
            CellValue::Missing(_) => Ok(None),
            CellValue::DateTime(value) | CellValue::Date(value) => Ok(Some(*value)),
            _ => Err(Error::InvalidMetadata {
                details: "cell type cannot be converted to OffsetDateTime".into(),
            }),
        }
    }
}

#[cfg(feature = "chrono")]
impl RowValue for chrono::NaiveDate {
    fn from_cell(cell: &CellValue<'_>) -> Result<Option<Self>> {
        let dt = match cell {
            CellValue::Missing(_) => return Ok(None),
            CellValue::Date(value) | CellValue::DateTime(value) => *value,
            _ => {
                return Err(Error::InvalidMetadata {
                    details: "cell type cannot be converted to chrono::NaiveDate".into(),
                });
            }
        };
        let date = dt.date();
        chrono::NaiveDate::from_ymd_opt(
            date.year(),
            u32::from(date.month().number_from_month()),
            date.day(),
        )
        .map(Some)
        .ok_or_else(|| Error::InvalidMetadata {
            details: "date out of range for chrono::NaiveDate".into(),
        })
    }
}

#[cfg(feature = "chrono")]
impl RowValue for chrono::NaiveDateTime {
    fn from_cell(cell: &CellValue<'_>) -> Result<Option<Self>> {
        let dt = match cell {
            CellValue::Missing(_) => return Ok(None),
            CellValue::DateTime(value) | CellValue::Date(value) => *value,
            _ => {
                return Err(Error::InvalidMetadata {
                    details: "cell type cannot be converted to chrono::NaiveDateTime".into(),
                });
            }
        };
        chrono::NaiveDateTime::from_timestamp_opt(dt.unix_timestamp(), dt.nanosecond())
            .map(Some)
            .ok_or_else(|| Error::InvalidMetadata {
                details: "datetime out of range for chrono::NaiveDateTime".into(),
            })
    }
}

pub struct RowIter<'a, R: std::io::Read + std::io::Seek> {
    inner: crate::parser::RowIterator<'a, R>,
    lookup: Arc<RowLookup>,
}

impl<'a, R: std::io::Read + std::io::Seek> RowIter<'a, R> {
    pub(crate) const fn new(
        inner: crate::parser::RowIterator<'a, R>,
        lookup: Arc<RowLookup>,
    ) -> Self {
        Self { inner, lookup }
    }

    /// Advances the iterator by one row.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails.
    pub fn try_next(&mut self) -> Result<Option<Row>> {
        match self.inner.try_next()? {
            Some(row) => Ok(Some(Row::new(
                row.into_iter().map(CellValue::into_owned).collect(),
                Arc::clone(&self.lookup),
            ))),
            None => Ok(None),
        }
    }
}

impl<R: std::io::Read + std::io::Seek> Iterator for RowIter<'_, R> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_next() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Row, RowLookup};
    use crate::{
        cell::CellValue,
        dataset::{DatasetMetadata, Variable, VariableKind},
        error::Error,
    };
    use std::{borrow::Cow, sync::Arc};

    fn sample_metadata() -> DatasetMetadata {
        let mut metadata = DatasetMetadata::new(4);
        metadata.variables.push(Variable::new(
            0,
            "id".to_string(),
            VariableKind::Numeric,
            8,
        ));
        metadata.variables.push(Variable::new(
            1,
            "name".to_string(),
            VariableKind::Character,
            16,
        ));
        metadata.variables.push(Variable::new(
            2,
            "score".to_string(),
            VariableKind::Numeric,
            8,
        ));
        metadata.variables.push(Variable::new(
            3,
            "missing".to_string(),
            VariableKind::Numeric,
            8,
        ));
        metadata
    }

    fn make_row(values: Vec<CellValue<'static>>) -> Row {
        let lookup = Arc::new(RowLookup::from_metadata(&sample_metadata()));
        Row::new(values, lookup)
    }

    #[test]
    fn get_as_i64_from_numeric_cells() {
        let row = make_row(vec![
            CellValue::Int64(42),
            CellValue::Str(Cow::Borrowed("Alice")),
            CellValue::Float(3.0),
            CellValue::Missing(crate::cell::MissingValue::system()),
        ]);
        assert_eq!(row.get_as::<i64>("id").unwrap(), Some(42));
        assert_eq!(row.get_as::<i64>("score").unwrap(), Some(3));
        assert_eq!(row.get_as::<i64>("missing").unwrap(), None);
    }

    #[test]
    fn get_as_string_from_str_cell() {
        let row = make_row(vec![
            CellValue::Int64(1),
            CellValue::Str(Cow::Borrowed("bob")),
            CellValue::Float(1.5),
            CellValue::Missing(crate::cell::MissingValue::system()),
        ]);
        assert_eq!(row.get_as::<String>("name").unwrap(), Some("bob".to_string()));
    }

    #[test]
    fn get_as_f64_from_integer_cell() {
        let row = make_row(vec![
            CellValue::Int64(7),
            CellValue::Str(Cow::Borrowed("x")),
            CellValue::Int32(12),
            CellValue::Missing(crate::cell::MissingValue::system()),
        ]);
        assert_eq!(row.get_as::<f64>("id").unwrap(), Some(7.0));
        assert_eq!(row.get_as::<f64>("score").unwrap(), Some(12.0));
    }

    #[test]
    fn get_as_i64_rejects_fractional_float() {
        let row = make_row(vec![
            CellValue::Int64(1),
            CellValue::Str(Cow::Borrowed("x")),
            CellValue::Float(1.5),
            CellValue::Missing(crate::cell::MissingValue::system()),
        ]);
        let err = row.get_as::<i64>("score").unwrap_err();
        assert!(matches!(err, Error::InvalidMetadata { .. }));
    }

    #[test]
    fn get_as_unknown_column_errors() {
        let row = make_row(vec![
            CellValue::Int64(1),
            CellValue::Str(Cow::Borrowed("x")),
            CellValue::Float(2.0),
            CellValue::Missing(crate::cell::MissingValue::system()),
        ]);
        let err = row.get_as::<i64>("missing_column").unwrap_err();
        assert!(matches!(err, Error::InvalidMetadata { .. }));
    }

    #[cfg(feature = "time")]
    #[test]
    fn get_as_offset_datetime_from_date() {
        let dt = time::OffsetDateTime::UNIX_EPOCH;
        let row = make_row(vec![
            CellValue::DateTime(dt),
            CellValue::Str(Cow::Borrowed("x")),
            CellValue::Float(2.0),
            CellValue::Missing(crate::cell::MissingValue::system()),
        ]);
        assert_eq!(row.get_as::<time::OffsetDateTime>("id").unwrap(), Some(dt));
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn get_as_chrono_date() {
        let dt = time::OffsetDateTime::UNIX_EPOCH;
        let row = make_row(vec![
            CellValue::Date(dt),
            CellValue::Str(Cow::Borrowed("x")),
            CellValue::Float(2.0),
            CellValue::Missing(crate::cell::MissingValue::system()),
        ]);
        let date = row.get_as::<chrono::NaiveDate>("id").unwrap().unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    }
}
