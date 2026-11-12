//! Value-label sets decoded from a `.sas7bcat` catalog.
//!
//! `Serialize` is derived so downstream tools can persist a label set verbatim — the CLI
//! embeds it in Parquet field metadata under `sas.value_labels`.

use serde::Serialize;

/// The value labels of one SAS format, as stored in a catalog.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LabelSet {
    /// The format name, as the catalog spells it.
    pub name: String,
    /// Whether the keys are numbers or strings.
    pub value_type: ValueType,
    /// The labels, in catalog order.
    pub labels: Vec<ValueLabel>,
}

impl LabelSet {
    /// Creates an empty set.
    #[must_use]
    pub const fn new(name: String, value_type: ValueType) -> Self {
        Self {
            name,
            value_type,
            labels: Vec::new(),
        }
    }

    /// Returns the label string for a numeric key, if one exists.
    #[must_use]
    pub fn lookup_numeric(&self, value: f64) -> Option<&str> {
        for label in &self.labels {
            match &label.key {
                ValueKey::Numeric(v) if (*v - value).abs() < f64::EPSILON => {
                    return Some(&label.label);
                }
                ValueKey::Integer(v) if (f64::from(*v) - value).abs() < f64::EPSILON => {
                    return Some(&label.label);
                }
                _ => {}
            }
        }
        None
    }

    /// Returns the label string for a string key, if one exists.
    #[must_use]
    pub fn lookup_string(&self, value: &str) -> Option<&str> {
        for label in &self.labels {
            if let ValueKey::String(k) = &label.key
                && k.trim_end() == value.trim_end()
            {
                return Some(&label.label);
            }
        }
        None
    }
}

/// One key and the text SAS displays for it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ValueLabel {
    /// The value being labelled.
    pub key: ValueKey,
    /// The label text.
    pub label: String,
}

/// The value a label is attached to.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum ValueKey {
    /// A non-integral number.
    Numeric(f64),
    /// A whole number that fits in `i32`.
    Integer(i32),
    /// A special missing value: `A` to `Z` for `.A` to `.Z`, `_` for `._`, and `.` for the plain missing value.
    Tagged(char),
    /// A string value.
    String(String),
}

/// Whether a format's keys are numbers or strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ValueType {
    /// The keys are numbers.
    Numeric,
    /// The keys are strings.
    String,
}
