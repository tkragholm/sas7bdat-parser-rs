use crate::{
    catalog::{normalize_format_name, parse_catalog_file},
    columnar::OwnedColumnarBatch,
    error::{Error, Result},
    internal::{FileInner, FileSource, LayoutPlan, PageDescriptorTable},
    layout::parse_layout,
    metadata::{ColumnMeta, DatasetMetadata},
    options::{BatchHint, IoBackendPreference, OpenOptions, RowSelection},
    pages::{
        DescriptorBreakdown, compile_page_descriptors, compile_page_descriptors_breakdown,
        compile_page_descriptors_breakdown_in_memory, compile_page_descriptors_in_memory,
    },
    probe::probe_header,
    projection::{Projection, ProjectionBuilder},
    row::OwnedRow,
    scan::ScanBuilder,
};
use memmap2::Mmap;
use std::{
    fs::{self, File},
    io::{Cursor, Read, Seek},
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Debug, Clone)]
pub struct OpenBreakdown {
    pub metadata_ns: u128,
    pub file_open_ns: u128,
    pub mmap_ns: Option<u128>,
    pub probe_header_ns: u128,
    pub rewind_ns: u128,
    pub parse_layout_ns: u128,
    pub total_ns: u128,
    pub used_mmap: bool,
}

#[derive(Debug, Clone)]
pub struct Dataset {
    pub(crate) file: Arc<FileInner>,
    pub(crate) metadata: Arc<DatasetMetadata>,
    pub(crate) layout: Arc<LayoutPlan>,
    pub(crate) descriptors: Arc<Mutex<Option<Arc<PageDescriptorTable>>>>,
}

impl Dataset {
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, mapped, or its
    /// SAS7BDAT structure cannot be parsed.
    pub fn open_breakdown(path: impl AsRef<Path>, options: OpenOptions) -> Result<OpenBreakdown> {
        let path = path.as_ref();

        let metadata_start = Instant::now();
        let _meta = fs::metadata(path).map_err(|err| Error::io_error_with_path(path, &err))?;
        let metadata_ns = metadata_start.elapsed().as_nanos();

        let file_open_start = Instant::now();
        let file = File::open(path).map_err(|err| Error::io_error_with_path(path, &err))?;
        let file_open_ns = file_open_start.elapsed().as_nanos();

        if should_try_mmap(options.io_backend, path) {
            let mmap_start = Instant::now();
            if let Some(mmap) = try_map_file(path, &file)? {
                let mmap_ns = mmap_start.elapsed().as_nanos();
                let mut cursor = Cursor::new(&mmap[..]);

                let probe_start = Instant::now();
                let (header, metadata) = probe_header(&mut cursor)?;
                let probe_header_ns = probe_start.elapsed().as_nanos();

                let rewind_start = Instant::now();
                cursor.rewind().map_err(|err| Error::io_error(&err))?;
                let rewind_ns = rewind_start.elapsed().as_nanos();

                let layout_start = Instant::now();
                let _ = parse_layout(&mut cursor, header, metadata)?;
                let parse_layout_ns = layout_start.elapsed().as_nanos();

                return Ok(OpenBreakdown {
                    metadata_ns,
                    file_open_ns,
                    mmap_ns: Some(mmap_ns),
                    probe_header_ns,
                    rewind_ns,
                    parse_layout_ns,
                    total_ns: metadata_ns
                        + file_open_ns
                        + mmap_ns
                        + probe_header_ns
                        + rewind_ns
                        + parse_layout_ns,
                    used_mmap: true,
                });
            }
        }

        let probe_start = Instant::now();
        let mut file = file;
        let (header, metadata) = probe_header(&mut file).map_err(|mut err| {
            if let Error::Io(ref mut io_err) = err {
                io_err.path = Some(path.to_path_buf());
            }
            err
        })?;
        let probe_header_ns = probe_start.elapsed().as_nanos();

        let rewind_start = Instant::now();
        file.rewind()
            .map_err(|err| Error::io_error_with_path(path, &err))?;
        let rewind_ns = rewind_start.elapsed().as_nanos();

        let layout_start = Instant::now();
        let _ = parse_layout(&mut file, header, metadata).map_err(|mut err| {
            if let Error::Io(ref mut io_err) = err {
                io_err.path = Some(path.to_path_buf());
            }
            err
        })?;
        let parse_layout_ns = layout_start.elapsed().as_nanos();

        Ok(OpenBreakdown {
            metadata_ns,
            file_open_ns,
            mmap_ns: None,
            probe_header_ns,
            rewind_ns,
            parse_layout_ns,
            total_ns: metadata_ns + file_open_ns + probe_header_ns + rewind_ns + parse_layout_ns,
            used_mmap: false,
        })
    }

    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or its SAS7BDAT
    /// structure cannot be parsed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, OpenOptions::default())
    }

    /// Builds a dataset from any `Read` implementor by buffering the input in memory first.
    ///
    /// # Errors
    ///
    /// Returns an error if reading the input or parsing the SAS7BDAT payload fails.
    pub fn from_reader<R: Read>(reader: R) -> Result<Self> {
        Self::from_reader_with_options(reader, OpenOptions::default())
    }

    /// Builds a dataset from any `Read` implementor by buffering the input in memory first.
    ///
    /// # Errors
    ///
    /// Returns an error if reading the input or parsing the SAS7BDAT payload fails.
    pub fn from_reader_with_options<R: Read>(mut reader: R, options: OpenOptions) -> Result<Self> {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|err| Error::io_error(&err))?;
        Self::from_bytes_with_options(bytes, options)
    }

    /// # Errors
    ///
    /// Returns an error if descriptor compilation fails for the dataset source.
    pub fn descriptor_breakdown(&self) -> Result<DescriptorBreakdown> {
        match &self.file.source {
            FileSource::Bytes(bytes) => {
                compile_page_descriptors_breakdown_in_memory(bytes.as_ref(), &self.layout)
            }
            FileSource::Mmap(mmap) => {
                compile_page_descriptors_breakdown_in_memory(&mmap[..], &self.layout)
            }
            FileSource::Path(path) => {
                let mut file =
                    File::open(path).map_err(|err| Error::io_error_with_path(path, &err))?;
                compile_page_descriptors_breakdown(&mut file, &self.layout)
            }
        }
    }

    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, mapped, or its
    /// SAS7BDAT structure cannot be parsed.
    pub fn open_with(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref();
        let _meta = fs::metadata(path).map_err(|err| Error::io_error_with_path(path, &err))?;
        let file = File::open(path).map_err(|err| Error::io_error_with_path(path, &err))?;
        if should_try_mmap(options.io_backend, path)
            && let Some(mmap) = try_map_file(path, &file)?
        {
            return Self::from_mmap(mmap, options);
        }

        Self::from_buffered_file(path, file, options)
    }

    /// # Errors
    ///
    /// Returns an error if the provided bytes do not contain a valid
    /// SAS7BDAT payload.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Self::from_bytes_with_options(bytes, OpenOptions::default())
    }

    fn from_bytes_with_options(bytes: Vec<u8>, options: OpenOptions) -> Result<Self> {
        let bytes = Arc::<[u8]>::from(bytes);
        let mut cursor = Cursor::new(&*bytes);
        let (layout, metadata) = Self::parse_from_reader(&mut cursor)?;
        Ok(Self {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options,
            }),
            metadata: Arc::new(metadata),
            layout: Arc::new(layout),
            descriptors: Arc::new(Mutex::new(None)),
        })
    }

    #[must_use]
    pub fn metadata(&self) -> &DatasetMetadata {
        &self.metadata
    }

    #[must_use]
    pub fn columns(&self) -> &[ColumnMeta] {
        &self.layout.columns
    }

    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnMeta> {
        self.columns().iter().find(|column| column.name() == name)
    }

    #[must_use]
    pub const fn projection(&self) -> ProjectionBuilder<'_> {
        ProjectionBuilder::new(self)
    }

    /// Create a scan builder for this dataset.
    ///
    /// This is the canonical entry point for all scan operations. Every shortcut method on
    /// `Dataset` (`visit_rows`, `collect_batches`, etc.) calls `scan()` internally with
    /// default settings. Use `scan()` directly when you need to configure projection, row
    /// selection, batch sizing, or other scan options.
    #[must_use]
    pub fn scan(&self) -> ScanBuilder<'_> {
        ScanBuilder::new(self)
    }

    /// Loads a companion SAS catalog (`.sas7bcat`) and merges its value-label sets
    /// into this dataset's metadata.
    ///
    /// After calling this, [`DatasetMetadata::label_sets`] contains the parsed
    /// formats, keyed by normalized format name (uppercase, trimmed). Columns whose
    /// [`ColumnMeta::format`] matches a key in `label_sets` can be hydrated to
    /// labelled values by the caller.
    ///
    /// Calling this multiple times is safe: label sets are inserted, not replaced in
    /// bulk, so repeated calls with different catalog files accumulate formats.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or its catalog structure cannot
    /// be decoded.
    pub fn attach_catalog(&mut self, path: impl AsRef<std::path::Path>) -> Result<()> {
        use std::sync::Arc;
        let layout = parse_catalog_file(path.as_ref())?;
        let metadata = Arc::make_mut(&mut self.metadata);
        for label_set in layout.label_sets {
            let key = normalize_format_name(&label_set.name);
            if !key.is_empty() {
                metadata.label_sets.insert(key, label_set);
            }
        }
        Ok(())
    }

    /// Overrides the inferred [`LogicalType`](crate::LogicalType) of named columns for
    /// all subsequent scans on this dataset.
    ///
    /// SAS stores every numeric column as an IEEE double, so integer-coded columns
    /// (registry codes, category numbers, ...) are inferred as
    /// [`LogicalType::Float`](crate::LogicalType::Float) and emitted as `f64`. Overriding
    /// such a column to [`LogicalType::Integer`](crate::LogicalType::Integer) makes scans
    /// emit it as `i64` instead — and fail with a decode error if the file contains a
    /// non-integral or out-of-range value, rather than silently changing the column type.
    ///
    /// Override names that do not match any column in this file are ignored, so a
    /// register-wide override catalog can be applied wholesale to files whose column
    /// sets vary.
    ///
    /// # Errors
    ///
    /// Returns an error if an override would reinterpret a column across the
    /// numeric/character boundary (e.g. a character column overridden to `Integer`),
    /// which cannot be expressed by re-typing the stored values.
    pub fn apply_schema_overrides<I, S>(&mut self, overrides: I) -> Result<()>
    where
        I: IntoIterator<Item = (S, crate::LogicalType)>,
        S: AsRef<str>,
    {
        use crate::LogicalType;
        let layout = Arc::make_mut(&mut self.layout);
        for (name, requested) in overrides {
            let name = name.as_ref();
            let Some(column) = layout.columns.iter_mut().find(|column| column.name == name) else {
                continue;
            };
            let current = column.logical_type;
            let compatible = match current {
                LogicalType::Integer
                | LogicalType::Float
                | LogicalType::Date
                | LogicalType::DateTime
                | LogicalType::Time => matches!(
                    requested,
                    LogicalType::Integer
                        | LogicalType::Float
                        | LogicalType::Date
                        | LogicalType::DateTime
                        | LogicalType::Time
                ),
                LogicalType::String => {
                    matches!(requested, LogicalType::String | LogicalType::Bytes)
                }
                LogicalType::Bytes => matches!(requested, LogicalType::Bytes),
            };
            if !compatible {
                return Err(Error::unsupported(format!(
                    "schema override for column '{name}': a {current:?} column cannot be decoded as {requested:?}"
                )));
            }
            column.logical_type = requested;
        }
        Ok(())
    }

    /// Scans raw rows and returns the collected scan statistics.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails due to I/O or decompression errors.
    pub fn visit_raw_rows<F>(&self, f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(crate::RawRow<'_>) -> Result<std::ops::ControlFlow<()>>,
    {
        self.scan().visit_raw_rows(f)
    }

    /// Scans decoded rows and returns the collected scan statistics.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn visit_rows<F>(&self, f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(crate::RowView<'_>) -> Result<std::ops::ControlFlow<()>>,
    {
        self.scan().visit_rows(f)
    }

    /// Scans decoded rows after applying a named projection.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name is not found, or if the scan fails.
    pub fn rows_with_projection<F>(&self, names: &[&str], f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(crate::RowView<'_>) -> Result<std::ops::ControlFlow<()>>,
    {
        let projection = self.select_with(names)?;
        self.scan().with_projection(&projection).visit_rows(f)
    }

    /// Scans decoded rows after applying a row window.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn rows_windowed<F>(&self, selection: RowSelection, f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(crate::RowView<'_>) -> Result<std::ops::ControlFlow<()>>,
    {
        self.scan().select(selection).visit_rows(f)
    }

    /// Scans decoded batches and returns the collected scan statistics.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn visit_batches<F>(&self, f: F) -> Result<crate::ScanStatsSummary>
    where
        F: FnMut(crate::ColumnarBatch<'_>) -> Result<std::ops::ControlFlow<()>>,
    {
        self.scan().visit_batches(f)
    }

    /// Collects decoded rows into owned row values.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn collect_rows(&self) -> Result<Vec<OwnedRow>> {
        self.scan().collect_rows()
    }

    /// Collects decoded rows for a named projection into owned row values.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name is not found, or if the scan fails.
    pub fn collect_rows_with_projection(&self, names: &[&str]) -> Result<Vec<OwnedRow>> {
        let projection = self.select_with(names)?;
        self.scan().with_projection(&projection).collect_rows()
    }

    /// Collects decoded rows within a row window into owned row values.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or row decoding fails.
    pub fn collect_rows_windowed(&self, selection: RowSelection) -> Result<Vec<OwnedRow>> {
        self.scan().select(selection).collect_rows()
    }

    /// Collects decoded batches into owned columnar batches.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn collect_batches(&self) -> Result<Vec<OwnedColumnarBatch>> {
        self.scan().collect_batches()
    }

    /// Collects decoded batches using a fixed target batch size.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan or batch decoding fails.
    pub fn collect_batches_with_rows(&self, batch_rows: usize) -> Result<Vec<OwnedColumnarBatch>> {
        self.scan()
            .with_batch_hint(BatchHint::Rows(batch_rows))
            .collect_batches()
    }

    /// Collects decoded batches for a named projection.
    ///
    /// # Errors
    ///
    /// Returns an error if any column name is not found, or if the scan fails.
    pub fn collect_batches_with_projection(
        &self,
        names: &[&str],
    ) -> Result<Vec<OwnedColumnarBatch>> {
        let projection = self.select_with(names)?;
        self.scan().with_projection(&projection).collect_batches()
    }

    pub(crate) fn select_with(&self, names: &[&str]) -> Result<Projection> {
        self.projection().columns(names.iter().copied()).build()
    }

    /// Whether the descriptor table has already been compiled and cached.
    ///
    /// The fused scan compiles descriptors as it decodes, which is only worth doing when the
    /// table isn't already sitting in the cache from an earlier scan.
    pub(crate) fn has_cached_descriptors(&self) -> bool {
        self.descriptors.lock().is_ok_and(|guard| guard.is_some())
    }

    pub(crate) fn descriptors(&self) -> Result<Arc<PageDescriptorTable>> {
        let descriptors = {
            let mut guard = self
                .descriptors
                .lock()
                .map_err(|_| Error::unsupported("descriptor cache poisoned"))?;
            if let Some(descriptors) = guard.as_ref() {
                return Ok(Arc::clone(descriptors));
            }

            let descriptors = Arc::new(self.load_descriptors()?);
            *guard = Some(Arc::clone(&descriptors));
            descriptors
        };
        Ok(descriptors)
    }
}

impl Dataset {
    fn parse_from_reader<R: Read + Seek>(reader: &mut R) -> Result<(LayoutPlan, DatasetMetadata)> {
        let (header, metadata) = probe_header(reader)?;
        reader.rewind().map_err(|err| Error::io_error(&err))?;
        let (layout, metadata) = parse_layout(reader, header, metadata)?;
        Ok((layout, metadata))
    }

    fn from_buffered_file(path: &Path, mut file: File, options: OpenOptions) -> Result<Self> {
        let (layout, metadata) = Self::parse_from_reader(&mut file).map_err(|mut err| {
            if let Error::Io(ref mut io_err) = err {
                io_err.path = Some(path.to_path_buf());
            }
            err
        })?;
        Ok(Self {
            file: Arc::new(FileInner {
                source: FileSource::Path(path.to_path_buf()),
                options,
            }),
            metadata: Arc::new(metadata),
            layout: Arc::new(layout),
            descriptors: Arc::new(Mutex::new(None)),
        })
    }

    fn from_mmap(mmap: Mmap, options: OpenOptions) -> Result<Self> {
        let mmap = Arc::new(mmap);
        let mut cursor = Cursor::new(&mmap[..]);
        let (layout, metadata) = Self::parse_from_reader(&mut cursor)?;
        Ok(Self {
            file: Arc::new(FileInner {
                source: FileSource::Mmap(Arc::clone(&mmap)),
                options,
            }),
            metadata: Arc::new(metadata),
            layout: Arc::new(layout),
            descriptors: Arc::new(Mutex::new(None)),
        })
    }

    fn load_descriptors(&self) -> Result<PageDescriptorTable> {
        match &self.file.source {
            FileSource::Bytes(bytes) => {
                compile_page_descriptors_in_memory(bytes.as_ref(), &self.layout)
            }
            FileSource::Mmap(mmap) => compile_page_descriptors_in_memory(&mmap[..], &self.layout),
            FileSource::Path(path) => {
                let mut file =
                    File::open(path).map_err(|err| Error::io_error_with_path(path, &err))?;
                compile_page_descriptors(&mut file, &self.layout)
            }
        }
    }
}

/// Whether to memory-map `path` under `preference`.
///
/// `Auto` maps local storage and declines to map a network share: over SMB every access is a
/// page fault serviced by a round-trip with no readahead, which turns a large file into
/// millions of them. `MmapPreferred` still maps a remote file — it is an explicit request.
fn should_try_mmap(preference: IoBackendPreference, path: &Path) -> bool {
    match preference {
        IoBackendPreference::MmapPreferred => true,
        IoBackendPreference::Auto => !crate::netpath::is_network_path(path),
        IoBackendPreference::BufferedPreferred | IoBackendPreference::BufferedOnly => false,
    }
}

// The crate's single `unsafe`: a read-only mmap. Explicitly allowed so it stands out.
#[allow(unsafe_code)]
fn try_map_file(path: &Path, file: &File) -> Result<Option<Mmap>> {
    // SAFETY: The mapping is created read-only from an open file descriptor and stored
    // in the dataset source, so all later access stays within the immutable mapped range.
    match unsafe { Mmap::map(file) } {
        Ok(mmap) => Ok(Some(mmap)),
        Err(err) => {
            if cfg!(target_family = "unix") || cfg!(target_family = "windows") {
                Ok(None)
            } else {
                Err(Error::io_error_with_path(path, &err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn open_with_mmap_preferred_uses_mapped_source_when_fixture_is_available() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/raw_data/csharp/charset_utf8.sas7bdat");
        if !path.exists() {
            return;
        }

        let ds = Dataset::open_with(
            &path,
            OpenOptions {
                io_backend: IoBackendPreference::MmapPreferred,
                ..OpenOptions::default()
            },
        )
        .expect("mmap-preferred dataset open");

        assert!(matches!(ds.file.source, FileSource::Mmap(_)));
    }

    #[test]
    fn open_with_buffered_only_keeps_path_source_when_fixture_is_available() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/raw_data/csharp/charset_utf8.sas7bdat");
        if !path.exists() {
            return;
        }

        let ds = Dataset::open_with(
            &path,
            OpenOptions {
                io_backend: IoBackendPreference::BufferedOnly,
                ..OpenOptions::default()
            },
        )
        .expect("buffered-only dataset open");

        assert!(matches!(ds.file.source, FileSource::Path(_)));
    }

    #[test]
    fn from_reader_matches_open_on_fixture_when_fixture_is_available() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/raw_data/other/cars.sas7bdat");
        if !path.exists() {
            return;
        }

        let bytes = std::fs::read(&path).expect("fixture bytes");
        let from_reader = Dataset::from_reader(Cursor::new(bytes)).expect("reader dataset");
        let opened = Dataset::open(&path).expect("opened dataset");

        assert_eq!(
            from_reader.metadata().row_count,
            opened.metadata().row_count
        );
        assert_eq!(from_reader.columns().len(), opened.columns().len());
    }

    #[test]
    fn collect_batches_with_rows_matches_scan_builder_when_fixture_is_available() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/raw_data/other/cars.sas7bdat");
        if !path.exists() {
            return;
        }

        let ds = Dataset::open(&path).expect("opened dataset");
        let aliased = ds.collect_batches_with_rows(128).expect("alias batches");
        let direct = ds
            .scan()
            .with_batch_hint(BatchHint::Rows(128))
            .collect_batches()
            .expect("direct batches");

        assert_eq!(aliased.len(), direct.len());
        assert_eq!(
            aliased.iter().map(|batch| batch.row_count).sum::<usize>(),
            direct.iter().map(|batch| batch.row_count).sum::<usize>()
        );
    }

    #[test]
    fn rows_with_projection_matches_collect_rows_with_projection_when_fixture_is_available() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/raw_data/other/cars.sas7bdat");
        if !path.exists() {
            return;
        }

        let ds = Dataset::open(&path).expect("opened dataset");
        let names: Vec<String> = ds
            .columns()
            .iter()
            .take(2)
            .map(|column| column.name().to_owned())
            .collect();
        if names.is_empty() {
            return;
        }

        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let projection = ds.select_with(&name_refs).expect("projection");
        assert_eq!(projection.columns().len(), name_refs.len());

        let collected = ds
            .collect_rows_with_projection(&name_refs)
            .expect("projected rows");
        let mut visited = 0usize;
        let stats = ds
            .rows_with_projection(&name_refs, |row| {
                visited += 1;
                assert_eq!(row.len(), name_refs.len());
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .expect("projected row scan");

        assert_eq!(visited, collected.len());
        assert_eq!(stats.rows_emitted, collected.len() as u64);
    }

    /// A small, git-tracked fixture (shipped with the R plugin) so the timing-breakdown
    /// tests actually run in CI rather than skipping like the untracked-corpus tests above.
    fn tracked_fixture() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../r-plugin/inst/extdata/people.sas7bdat")
    }

    #[test]
    fn open_breakdown_via_mmap_records_mapped_timings() {
        let breakdown = Dataset::open_breakdown(
            tracked_fixture(),
            OpenOptions {
                io_backend: IoBackendPreference::MmapPreferred,
                ..OpenOptions::default()
            },
        )
        .expect("mmap-preferred breakdown");

        assert!(breakdown.used_mmap);
        assert!(breakdown.mmap_ns.is_some());
        // total_ns is the sum of every recorded phase, including the mmap phase.
        assert_eq!(
            breakdown.total_ns,
            breakdown.metadata_ns
                + breakdown.file_open_ns
                + breakdown.mmap_ns.expect("mmap phase recorded")
                + breakdown.probe_header_ns
                + breakdown.rewind_ns
                + breakdown.parse_layout_ns,
        );
    }

    #[test]
    fn open_breakdown_buffered_skips_the_mmap_phase() {
        let breakdown = Dataset::open_breakdown(
            tracked_fixture(),
            OpenOptions {
                io_backend: IoBackendPreference::BufferedOnly,
                ..OpenOptions::default()
            },
        )
        .expect("buffered-only breakdown");

        assert!(!breakdown.used_mmap);
        assert!(breakdown.mmap_ns.is_none());
        assert_eq!(
            breakdown.total_ns,
            breakdown.metadata_ns
                + breakdown.file_open_ns
                + breakdown.probe_header_ns
                + breakdown.rewind_ns
                + breakdown.parse_layout_ns,
        );
    }

    #[test]
    fn open_breakdown_reports_io_error_for_missing_file() {
        let missing = Path::new(env!("CARGO_MANIFEST_DIR")).join("does-not-exist.sas7bdat");
        assert!(Dataset::open_breakdown(&missing, OpenOptions::default()).is_err());
    }

    /// End-to-end coverage for the columnar decoder's non-ASCII transcode branch (the
    /// direct UTF-8 owned path). The fixture is `people.sas7bdat` (WINDOWS-1252) with row
    /// 0's GENDER cell byte-patched from `M` (0x4D) to 0xE9, i.e. `é` — so the batch
    /// decoder must transcode a high byte to UTF-8 instead of taking the ASCII fast path.
    /// See tests/fixtures/README.md.
    #[test]
    fn batch_decode_transcodes_non_ascii_windows_1252_string() {
        use crate::columnar::OwnedColumnBuffer;

        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/people_nonascii.sas7bdat");
        let ds = Dataset::open(&path).expect("open patched non-ASCII fixture");

        // GENDER is the 4th column (index 3); collect it across all batches.
        let mut genders = Vec::new();
        for batch in ds.scan().collect_batches().expect("columnar batches") {
            let OwnedColumnBuffer::Utf8 { offsets, data, .. } = &batch.columns[3] else {
                panic!("GENDER should decode to a utf8 column");
            };
            let bounds = offsets.as_slice();
            for row in 0..batch.row_count {
                let start = usize::try_from(bounds[row]).expect("offset fits usize");
                let end = usize::try_from(bounds[row + 1]).expect("offset fits usize");
                genders.push(
                    std::str::from_utf8(&data[start..end])
                        .expect("decoded cell is valid utf-8")
                        .to_owned(),
                );
            }
        }

        // Row 0 transcodes 0xE9 -> "é" (UTF-8 0xC3 0xA9); the rest stay ASCII.
        assert_eq!(genders, ["é", "F", "M", "F", "F"]);
    }

    /// Regression: a datetime column with fractional (sub-second) SAS values widens to an
    /// f64 buffer internally. The Arrow path used to declare `Timestamp(Second)` but build
    /// an `f64` array, crashing `RecordBatch::try_new`. It now emits `Timestamp(Microsecond)`
    /// (so Parquet stores a real timestamp) and preserves sub-second precision.
    #[cfg(feature = "arrow")]
    #[test]
    fn fractional_datetime_columns_export_as_microsecond_timestamps() {
        use arrow_schema::{DataType, TimeUnit};

        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../r-plugin/inst/extdata/dtdate.sas7bdat");
        let ds = Dataset::open(&path).expect("open dtdate");
        let batches = ds
            .scan()
            .collect_arrow_batches()
            .expect("arrow conversion must not crash on widened datetime columns");
        let batch = batches.first().expect("at least one batch");

        // Every column in this fixture is a (sub-second) datetime -> microsecond timestamp.
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert!(batch.num_rows() > 0);
    }

    /// Regression: SAS TIME is a signed count of seconds since midnight, not a clock time.
    /// This fixture holds values well past 24h (row 51 is 359,280s = 99h48m). Declaring
    /// `Time64(Nanosecond)`, whose domain is `[0, 24h)`, wrote correct integers under a
    /// logical type that forbids them — so arrow-rs, pyarrow and `DuckDB` all read them back
    /// as null. `Duration(Nanosecond)` has the same i64 payload and the whole SAS range.
    #[cfg(feature = "arrow")]
    #[test]
    fn out_of_range_time_columns_export_as_durations_without_nulling() {
        use arrow_array::{Array, DurationNanosecondArray};
        use arrow_schema::{DataType, TimeUnit};

        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/raw_data/csharp/date_format_time.sas7bdat");
        let ds = Dataset::open(&path).expect("open date_format_time");
        let batches = ds.scan().collect_arrow_batches().expect("arrow conversion");
        let batch = batches.first().expect("at least one batch");

        let field = batch.schema_ref().field(0).clone();
        assert_eq!(
            field.data_type(),
            &DataType::Duration(TimeUnit::Nanosecond),
            "a TIME column must not be declared over a domain its values escape"
        );
        // The clock-time intent survives the widened type, for consumers that want it back.
        assert_eq!(
            field.metadata().get(crate::scan::SAS_LOGICAL_TYPE_KEY),
            Some(&"TIME".to_owned())
        );

        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<DurationNanosecondArray>()
            .expect("Duration(ns) column");
        // 359,280s is past 24h and used to survive the write but not the read.
        assert!(
            column.is_valid(51),
            "an out-of-range TIME must not be nulled"
        );
        assert_eq!(column.value(51), 359_280 * 1_000_000_000);
    }
}
