use std::borrow::Cow;
use std::convert::TryFrom;

use crate::error::{Error, Result};
use crate::metadata::{Alignment, Format, Measure, MissingValuePolicy, Variable, VariableKind};

use super::text_store::{TextRef, TextStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Numeric(NumericKind),
    Character,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericKind {
    Double,
    Date,
    DateTime,
    Time,
}

impl ColumnKind {
    #[must_use]
    pub const fn from_type_code(code: u8) -> Option<Self> {
        match code {
            0x01 => Some(Self::Numeric(NumericKind::Double)),
            0x02 => Some(Self::Character),
            _ => None,
        }
    }
}

/// Tracks column offsets and widths for row parsing.
#[derive(Debug, Clone, Copy)]
pub struct ColumnOffsets {
    pub offset: u64,
    pub width: u32,
}

/// Intermediate column information aggregated from the SAS meta pages.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub index: u32,
    pub offsets: ColumnOffsets,
    pub kind: ColumnKind,
    pub format_width: Option<u16>,
    pub format_decimals: Option<u16>,
    pub name_ref: TextRef,
    pub label_ref: TextRef,
    pub format_ref: TextRef,
    pub measure: Measure,
    pub alignment: Alignment,
}

impl ColumnInfo {
    /// Populates the provided [`Variable`] with the decoded column metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the associated text blobs cannot be resolved.
    pub fn apply_to_variable(&self, text_store: &TextStore, variable: &mut Variable) -> Result<()> {
        variable.index = self.index;
        variable.kind = match self.kind {
            ColumnKind::Numeric(_) => VariableKind::Numeric,
            ColumnKind::Character => VariableKind::Character,
        };
        variable.storage_width =
            usize::try_from(self.offsets.width).map_err(|_| Error::Unsupported {
                feature: Cow::from("column width exceeds platform pointer width"),
            })?;
        variable.missing = MissingValuePolicy::default();
        if matches!(variable.kind, VariableKind::Numeric) {
            variable.missing.system_missing = true;
        }
        variable.measure = self.measure;
        variable.alignment = self.alignment;

        if let Some(name) = text_store.resolve(self.name_ref)? {
            variable.name = name.into_owned();
        }
        if let Some(label) = text_store.resolve(self.label_ref)? {
            variable.label = Some(label.into_owned());
        }
        if let Some(fmt_name) = text_store.resolve(self.format_ref)? {
            let format = Format {
                name: fmt_name.into_owned(),
                width: self.format_width,
                decimals: self.format_decimals,
            };
            variable.format = Some(format);
        }
        Ok(())
    }
}

pub fn infer_numeric_kind(format_name: &str) -> Option<NumericKind> {
    if format_name.is_empty() {
        return None;
    }
    let cleaned = format_name.trim().trim_matches('.').to_ascii_uppercase();
    if cleaned.is_empty() {
        return None;
    }
    if cleaned.contains("DATETIME")
        || cleaned.ends_with("DT")
        || cleaned.starts_with("E8601DT")
        || cleaned.starts_with("B8601DT")
    {
        return Some(NumericKind::DateTime);
    }
    if cleaned.contains("TIME") || cleaned.ends_with("TM") || cleaned.starts_with("E8601TM") {
        return Some(NumericKind::Time);
    }
    if cleaned.contains("DATE")
        || cleaned.contains("YY")
        || cleaned.contains("MON")
        || cleaned.contains("WEEK")
        || cleaned.contains("YEAR")
        || cleaned.contains("MINGUO")
        || cleaned.ends_with("DA")
        || cleaned.starts_with("E8601DA")
        || cleaned.starts_with("B8601DA")
    {
        return Some(NumericKind::Date);
    }
    None
}
