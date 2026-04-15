use crate::{
    error::{Error, Result},
    internal::{FileInner, FileSource, LayoutPlan, PageDescriptorTable},
    layout::parse_layout,
    metadata::{ColumnMeta, DatasetMetadata},
    options::{IoBackendPreference, OpenOptions},
    pages::compile_page_descriptors,
    probe::probe_header,
    projection::ProjectionBuilder,
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

        if should_try_mmap(options.io_backend) {
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
        file.rewind().map_err(|err| Error::io_error_with_path(path, &err))?;
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

    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, mapped, or its
    /// SAS7BDAT structure cannot be parsed.
    pub fn open_with(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref();
        let _meta = fs::metadata(path).map_err(|err| Error::io_error_with_path(path, &err))?;
        let file = File::open(path).map_err(|err| Error::io_error_with_path(path, &err))?;
        if should_try_mmap(options.io_backend)
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
        let bytes = Arc::<[u8]>::from(bytes);
        let mut cursor = Cursor::new(&*bytes);
        let (layout, metadata) = Self::parse_from_reader(&mut cursor)?;
        Ok(Self {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
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
        self.columns()
            .iter()
            .find(|column| column.borrowed_name() == name)
    }

    #[must_use]
    pub const fn projection(&self) -> ProjectionBuilder<'_> {
        ProjectionBuilder::new(self)
    }

    #[must_use]
    pub fn scan(&self) -> ScanBuilder<'_> {
        ScanBuilder::new(self)
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
                let mut cursor = Cursor::new(bytes.as_ref());
                compile_page_descriptors(&mut cursor, &self.layout)
            }
            FileSource::Mmap(mmap) => {
                let mut cursor = Cursor::new(&mmap[..]);
                compile_page_descriptors(&mut cursor, &self.layout)
            }
            FileSource::Path(path) => {
                let mut file =
                    File::open(path).map_err(|err| Error::io_error_with_path(path, &err))?;
                compile_page_descriptors(&mut file, &self.layout)
            }
        }
    }
}

const fn should_try_mmap(preference: IoBackendPreference) -> bool {
    matches!(
        preference,
        IoBackendPreference::Auto | IoBackendPreference::MmapPreferred
    )
}

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
}
