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
    io::{Cursor, Seek},
    path::Path,
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct Dataset {
    #[allow(dead_code)]
    pub(crate) file: Arc<FileInner>,
    pub(crate) metadata: Arc<DatasetMetadata>,
    pub(crate) layout: Arc<LayoutPlan>,
    #[allow(dead_code)]
    pub(crate) descriptors: Arc<PageDescriptorTable>,
}

impl Dataset {
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
        let _meta = fs::metadata(path).map_err(|err| {
            Error::Io(crate::error::IoError {
                path: Some(path.to_path_buf()),
                message: err.to_string(),
            })
        })?;
        let file = File::open(path).map_err(|err| {
            Error::Io(crate::error::IoError {
                path: Some(path.to_path_buf()),
                message: err.to_string(),
            })
        })?;
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
        let (header, metadata) = probe_header(&mut cursor)?;
        cursor.set_position(0);
        let (layout, metadata) = parse_layout(&mut cursor, header, metadata)?;
        cursor.set_position(0);
        let descriptors = compile_page_descriptors(&mut cursor, &layout)?;
        Ok(Self {
            file: Arc::new(FileInner {
                source: FileSource::Bytes(Arc::clone(&bytes)),
                options: OpenOptions::default(),
            }),
            metadata: Arc::new(metadata),
            layout: Arc::new(layout),
            descriptors: Arc::new(descriptors),
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
}

impl Dataset {
    fn from_buffered_file(path: &Path, mut file: File, options: OpenOptions) -> Result<Self> {
        let (header, metadata) = probe_header(&mut file)?;
        file.rewind().map_err(|err| {
            Error::Io(crate::error::IoError {
                path: Some(path.to_path_buf()),
                message: err.to_string(),
            })
        })?;
        let (layout, metadata) = parse_layout(&mut file, header, metadata)?;
        file.rewind().map_err(|err| {
            Error::Io(crate::error::IoError {
                path: Some(path.to_path_buf()),
                message: err.to_string(),
            })
        })?;
        let descriptors = compile_page_descriptors(&mut file, &layout)?;
        Ok(Self {
            file: Arc::new(FileInner {
                source: FileSource::Path(path.to_path_buf()),
                options,
            }),
            metadata: Arc::new(metadata),
            layout: Arc::new(layout),
            descriptors: Arc::new(descriptors),
        })
    }

    fn from_mmap(mmap: Mmap, options: OpenOptions) -> Result<Self> {
        let mmap = Arc::new(mmap);
        let mut cursor = Cursor::new(&mmap[..]);
        let (header, metadata) = probe_header(&mut cursor)?;
        cursor.set_position(0);
        let (layout, metadata) = parse_layout(&mut cursor, header, metadata)?;
        cursor.set_position(0);
        let descriptors = compile_page_descriptors(&mut cursor, &layout)?;
        Ok(Self {
            file: Arc::new(FileInner {
                source: FileSource::Mmap(Arc::clone(&mmap)),
                options,
            }),
            metadata: Arc::new(metadata),
            layout: Arc::new(layout),
            descriptors: Arc::new(descriptors),
        })
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
                Err(Error::Io(crate::error::IoError {
                    path: Some(path.to_path_buf()),
                    message: err.to_string(),
                }))
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
