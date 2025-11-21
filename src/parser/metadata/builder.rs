use crate::metadata::{Alignment, Measure};
use std::borrow::Cow;

use super::column_info::{ColumnInfo, ColumnKind, ColumnOffsets, NumericKind, infer_numeric_kind};
use super::text_store::TextStore;

#[derive(Debug, Default)]
pub struct ColumnMetadataBuilder {
    text_store: TextStore,
    columns: Vec<ColumnInfo>,
    column_count: Option<u32>,
    names_seen: usize,
    attrs_seen: usize,
    formats_seen: usize,
    max_width: u32,
    column_list: Option<Vec<i16>>,
}

impl ColumnMetadataBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            text_store: TextStore::new(),
            columns: Vec::new(),
            column_count: None,
            names_seen: 0,
            attrs_seen: 0,
            formats_seen: 0,
            max_width: 0,
            column_list: None,
        }
    }

    #[must_use]
    pub const fn text_store(&self) -> &TextStore {
        &self.text_store
    }

    pub const fn text_store_mut(&mut self) -> &mut TextStore {
        &mut self.text_store
    }

    #[must_use]
    pub const fn column_count(&self) -> Option<u32> {
        self.column_count
    }

    pub const fn set_column_count(&mut self, count: u32) {
        self.column_count = Some(count);
    }

    #[must_use]
    pub const fn max_width(&self) -> u32 {
        self.max_width
    }

    pub fn ensure_column(&mut self, index: u32) -> &mut ColumnInfo {
        let len = self.columns.len();
        if index as usize >= len {
            self.columns.resize_with(index as usize + 1, || ColumnInfo {
                index: 0,
                offsets: ColumnOffsets {
                    offset: 0,
                    width: 0,
                },
                kind: ColumnKind::Numeric(NumericKind::Double),
                format_width: None,
                format_decimals: None,
                name_ref: super::text_store::TextRef::EMPTY,
                label_ref: super::text_store::TextRef::EMPTY,
                format_ref: super::text_store::TextRef::EMPTY,
                measure: Measure::Unknown,
                alignment: Alignment::Unknown,
            });
        }
        let column = &mut self.columns[index as usize];
        column.index = index;
        column
    }

    /// Returns a mutable reference to the column at `index`, creating it if necessary.
    ///
    /// # Panics
    ///
    /// Panics if the column could not be created.
    pub fn column_mut(&mut self, index: u32) -> &mut ColumnInfo {
        let _ = self.ensure_column(index);
        self.columns
            .get_mut(index as usize)
            .expect("column ensured but not present")
    }

    pub const fn note_names_processed(&mut self, count: usize) {
        self.names_seen += count;
    }

    #[must_use]
    pub const fn names_seen(&self) -> usize {
        self.names_seen
    }

    pub const fn note_attrs_processed(&mut self, count: usize) {
        self.attrs_seen += count;
    }

    #[must_use]
    pub const fn attrs_seen(&self) -> usize {
        self.attrs_seen
    }

    pub const fn note_formats_processed(&mut self) {
        self.formats_seen += 1;
    }

    #[must_use]
    pub const fn formats_seen(&self) -> usize {
        self.formats_seen
    }

    pub const fn update_max_width(&mut self, width: u32) {
        if width > self.max_width {
            self.max_width = width;
        }
    }

    pub fn append_column_list(&mut self, values: Vec<i16>) {
        let entry = self.column_list.get_or_insert_with(Vec::new);
        if entry.is_empty() {
            entry.extend(values);
        } else if entry.len() < values.len() {
            entry.extend(values.into_iter().skip(entry.len()));
        }
    }

    #[must_use]
    pub fn column_list(&self) -> Option<&[i16]> {
        self.column_list.as_deref()
    }

    #[must_use]
    pub fn finalize(self) -> (TextStore, Vec<ColumnInfo>, Option<Vec<i16>>) {
        let mut columns = self.columns;
        let mut inferred_formats: Vec<Option<String>> = Vec::with_capacity(columns.len());
        for column in &columns {
            inferred_formats.push(
                self.text_store
                    .resolve(column.format_ref)
                    .ok()
                    .and_then(|opt| opt.map(Cow::into_owned)),
            );
        }

        for (column, format_name) in columns.iter_mut().zip(inferred_formats.into_iter()) {
            if let (ColumnKind::Numeric(kind), Some(format_name)) = (&mut column.kind, format_name)
                && let Some(inferred) = infer_numeric_kind(&format_name)
            {
                *kind = inferred;
            }
        }

        (self.text_store, columns, self.column_list)
    }
}
