mod labels;
mod missing;
mod projection;
mod selection;
mod window;

use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::dataset::{DatasetMetadata, MissingValuePolicy};
use crate::error::{Error, Result};
use crate::parser::{DatasetLayout, RowIterator, parse_catalog, parse_metadata};
use crate::sinks::{RowSink, SinkContext};

use labels::{build_label_lookup, normalize_label_name};
use missing::{dedup_missing_ranges, dedup_tagged_missing, merge_label_set_missing};

pub struct SasReader<R: Read + Seek> {
    reader: R,
    layout: DatasetLayout,
}

pub use projection::ProjectedRowIter;
pub use selection::RowSelection;
pub use window::{ProjectedRowWindow, RowWindow};

impl SasReader<File> {
    /// Opens a SAS7BDAT file from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or if the metadata
    /// cannot be parsed.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Self::from_reader(file)
    }
}

impl<R: Read + Seek> SasReader<R> {
    /// Builds a reader from any `Read + Seek` implementor.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata parsing fails.
    pub fn from_reader(mut reader: R) -> Result<Self> {
        let layout = parse_metadata(&mut reader)?;
        reader.seek(SeekFrom::Start(0))?;
        Ok(Self { reader, layout })
    }

    pub const fn metadata(&self) -> &DatasetMetadata {
        &self.layout.header.metadata
    }

    /// Loads value-label catalog metadata from a companion file.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be opened or parsed.
    pub fn attach_catalog<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let mut file = File::open(path)?;
        self.attach_catalog_reader(&mut file)
    }

    /// Loads value-label catalog metadata from the provided reader.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be parsed.
    pub fn attach_catalog_reader<C: Read + Seek>(&mut self, reader: &mut C) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        let catalog = parse_catalog(reader)?;

        {
            let metadata = &mut self.layout.header.metadata;

            for set in catalog.label_sets {
                metadata.label_sets.insert(set.name.clone(), set);
            }

            let lookup = build_label_lookup(&metadata.label_sets);
            for variable in &mut metadata.variables {
                if let Some(format) = &variable.format {
                    let normalized = normalize_label_name(&format.name);
                    if let Some(matched) = lookup.get(&normalized) {
                        variable.value_labels = Some(matched.clone());
                    } else if !normalized.starts_with('$') {
                        let prefixed = format!("${normalized}");
                        if let Some(matched) = lookup.get(&prefixed) {
                            variable.value_labels = Some(matched.clone());
                        }
                    }
                }

                if let Some(label_name) = variable.value_labels.clone()
                    && let Some(set) = metadata.label_sets.get(&label_name)
                {
                    merge_label_set_missing(&mut variable.missing, set);
                }
            }
        }

        self.scan_missing_policies()?;
        Ok(())
    }

    /// Populates missing-value policies by scanning the dataset.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails.
    pub fn scan_missing_policies(&mut self) -> Result<()> {
        let variable_count = self.layout.header.metadata.variables.len();
        if variable_count == 0 {
            return Ok(());
        }

        let mut policies: Vec<MissingValuePolicy> = self
            .layout
            .header
            .metadata
            .variables
            .iter()
            .map(|var| var.missing.clone())
            .collect();

        self.reader.seek(SeekFrom::Start(0))?;
        {
            let mut rows = self.layout.row_iterator(&mut self.reader)?;
            for row in rows.by_ref() {
                let row = row?;
                for (idx, value) in row.iter().enumerate() {
                    if let crate::cell::CellValue::Missing(missing) = value {
                        missing::record_missing_observation(&mut policies[idx], missing);
                    }
                }
            }
        }
        self.reader.seek(SeekFrom::Start(0))?;

        for (variable, policy) in self
            .layout
            .header
            .metadata
            .variables
            .iter_mut()
            .zip(policies.into_iter())
        {
            let mut normalized_policy = policy;
            dedup_tagged_missing(&mut normalized_policy.tagged_missing);
            dedup_missing_ranges(&mut normalized_policy.ranges);
            variable.missing = normalized_policy;
        }

        Ok(())
    }

    /// Creates a row iterator over the dataset.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    pub fn rows(&mut self) -> Result<RowIterator<'_, R>> {
        self.reader.seek(SeekFrom::Start(0))?;
        self.layout.row_iterator(&mut self.reader)
    }

    /// Creates a row iterator configured by the provided selection.
    ///
    /// This method is intended for pagination without column projection. Use
    /// [`select_with`] when selecting a subset of columns.
    ///
    /// # Errors
    ///
    /// Returns an error if the selection specifies a projection, if the reader
    /// cannot be positioned, or if row iteration cannot be initialised.
    pub fn rows_windowed(&mut self, selection: &RowSelection) -> Result<RowWindow<'_, R>> {
        if selection.has_projection() {
            return Err(Error::InvalidMetadata {
                details: "rows_windowed does not accept column projection; use select_with instead"
                    .into(),
            });
        }
        self.reader.seek(SeekFrom::Start(0))?;
        let iterator = self.layout.row_iterator(&mut self.reader)?;
        Ok(RowWindow::new(
            iterator,
            selection.skip_count(),
            selection.max_count(),
        ))
    }

    /// Creates an iterator that yields a subset of columns for each row.
    ///
    /// # Errors
    ///
    /// Returns an error if any requested column index is invalid or if row
    /// decoding fails.
    pub fn select_columns(&mut self, indices: &[usize]) -> Result<ProjectedRowIter<'_, R>> {
        let column_count = self.layout.header.metadata.column_count as usize;
        if indices.is_empty() {
            return Err(Error::InvalidMetadata {
                details: "projected column list may not be empty".into(),
            });
        }
        let mut normalized = Vec::with_capacity(indices.len());
        let mut seen = HashSet::with_capacity(indices.len());
        for &idx in indices {
            if idx >= column_count {
                return Err(Error::InvalidMetadata {
                    details: format!(
                        "column projection index {idx} exceeds column count {column_count}"
                    )
                    .into(),
                });
            }
            if !seen.insert(idx) {
                return Err(Error::InvalidMetadata {
                    details: format!("duplicate column projection index {idx}").into(),
                });
            }
            normalized.push(idx);
        }
        self.reader.seek(SeekFrom::Start(0))?;
        let inner = self.layout.row_iterator(&mut self.reader)?;
        let mut sorted_projection: Vec<(usize, usize)> = normalized
            .iter()
            .copied()
            .enumerate()
            .map(|(position, column_index)| (column_index, position))
            .collect();
        sorted_projection.sort_unstable_by_key(|entry| entry.0);
        Ok(ProjectedRowIter {
            inner,
            selected_indices: normalized,
            sorted_projection,
            exhausted: false,
        })
    }

    /// Creates an iterator configured by selection with column projection.
    ///
    /// # Errors
    ///
    /// Returns an error when projection cannot be resolved or row decoding fails.
    pub fn select_with(&mut self, selection: &RowSelection) -> Result<ProjectedRowWindow<'_, R>> {
        let metadata = &self.layout.header.metadata;
        let indices =
            selection
                .resolve_projection(metadata)?
                .ok_or_else(|| Error::InvalidMetadata {
                    details: "column projection not specified".into(),
                })?;
        let projected = self.select_columns(&indices)?;
        Ok(ProjectedRowWindow::new(
            projected,
            selection.skip_count(),
            selection.max_count(),
        ))
    }

    /// Streams the full dataset into a custom sink implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or if the sink reports a failure.
    pub fn stream_into<S: RowSink>(&mut self, sink: &mut S) -> Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        let context = SinkContext::new(&self.layout);
        sink.begin(context)?;
        let mut iterator = self.layout.row_iterator(&mut self.reader)?;
        iterator.stream_all(|row| sink.write_streaming_row(row))?;
        sink.finish()?;
        self.reader.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn into_parts(self) -> (R, DatasetLayout) {
        (self.reader, self.layout)
    }
}
