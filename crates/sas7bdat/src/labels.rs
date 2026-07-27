//! Value-label sets decoded from a `.sas7bcat` catalog.
//!
//! `Serialize` is derived so downstream tools can persist a label set verbatim — the CLI
//! embeds it in Parquet field metadata under `sas.value_labels`.

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LabelSet {
    pub name: String,
    pub value_type: ValueType,
    pub labels: Vec<ValueLabel>,
}

impl LabelSet {
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ValueLabel {
    pub key: ValueKey,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum ValueKey {
    Numeric(f64),
    Integer(i32),
    Tagged(char),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ValueType {
    Numeric,
    String,
}
