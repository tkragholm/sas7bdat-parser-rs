use crate::{
    error::{Error, Result},
    internal::{FileInner, FileSource, LayoutPlan, PageDescriptorTable},
    layout::parse_layout,
    metadata::{ColumnMeta, DatasetMetadata},
    options::OpenOptions,
    pages::compile_page_descriptors,
    probe::probe_header,
    projection::ProjectionBuilder,
    scan::ScanBuilder,
};
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
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, OpenOptions::default())
    }

    pub fn open_with(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref();
        let _meta = fs::metadata(path).map_err(|err| {
            Error::Io(crate::error::IoError {
                path: Some(path.to_path_buf()),
                message: err.to_string(),
            })
        })?;
        let mut file = File::open(path).map_err(|err| {
            Error::Io(crate::error::IoError {
                path: Some(path.to_path_buf()),
                message: err.to_string(),
            })
        })?;
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
