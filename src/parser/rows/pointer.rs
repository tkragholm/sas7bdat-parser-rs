use std::borrow::Cow;

use crate::error::{Error, Result, Section};
use crate::dataset::Endianness;
use crate::parser::core::byteorder::{read_u32, read_u64};

use super::constants::{
    SAS_SUBHEADER_SIGNATURE_COLUMN_ATTRS, SAS_SUBHEADER_SIGNATURE_COLUMN_FORMAT,
    SAS_SUBHEADER_SIGNATURE_COLUMN_LIST, SAS_SUBHEADER_SIGNATURE_COLUMN_NAME,
    SAS_SUBHEADER_SIGNATURE_COLUMN_SIZE, SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT,
    SAS_SUBHEADER_SIGNATURE_COUNTS, SAS_SUBHEADER_SIGNATURE_ROW_SIZE,
};

pub struct PointerInfo {
    pub offset: usize,
    pub length: usize,
    pub compression: u8,
    pub is_compressed_data: bool,
}

pub fn parse_pointer(pointer: &[u8], uses_u64: bool, endian: Endianness) -> Result<PointerInfo> {
    if uses_u64 {
        if pointer.len() < 18 {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("64-bit pointer too short"),
            });
        }
        let offset =
            usize::try_from(read_u64(endian, &pointer[0..8])).map_err(|_| Error::Unsupported {
                feature: Cow::from("64-bit pointer offset exceeds platform pointer width"),
            })?;
        let length =
            usize::try_from(read_u64(endian, &pointer[8..16])).map_err(|_| Error::Unsupported {
                feature: Cow::from("64-bit pointer length exceeds platform pointer width"),
            })?;
        Ok(PointerInfo {
            offset,
            length,
            compression: pointer[16],
            is_compressed_data: pointer[17] != 0,
        })
    } else {
        if pointer.len() < 10 {
            return Err(Error::Corrupted {
                section: Section::Header,
                details: Cow::from("32-bit pointer too short"),
            });
        }
        let offset =
            usize::try_from(read_u32(endian, &pointer[0..4])).map_err(|_| Error::Unsupported {
                feature: Cow::from("32-bit pointer offset exceeds platform pointer width"),
            })?;
        let length =
            usize::try_from(read_u32(endian, &pointer[4..8])).map_err(|_| Error::Unsupported {
                feature: Cow::from("32-bit pointer length exceeds platform pointer width"),
            })?;
        Ok(PointerInfo {
            offset,
            length,
            compression: pointer[8],
            is_compressed_data: pointer[9] != 0,
        })
    }
}

pub fn read_signature(data: &[u8], endian: Endianness, uses_u64: bool) -> u32 {
    if data.len() < 4 {
        return 0;
    }
    let mut signature = read_u32(endian, &data[0..4]);
    if matches!(endian, Endianness::Big) && signature == u32::MAX && uses_u64 && data.len() >= 8 {
        signature = read_u32(endian, &data[4..8]);
    }
    signature
}

pub const fn signature_is_recognized(signature: u32) -> bool {
    matches!(
        signature,
        SAS_SUBHEADER_SIGNATURE_COLUMN_TEXT
            | SAS_SUBHEADER_SIGNATURE_COLUMN_ATTRS
            | SAS_SUBHEADER_SIGNATURE_COLUMN_FORMAT
            | SAS_SUBHEADER_SIGNATURE_COLUMN_NAME
            | SAS_SUBHEADER_SIGNATURE_COLUMN_SIZE
            | SAS_SUBHEADER_SIGNATURE_ROW_SIZE
            | SAS_SUBHEADER_SIGNATURE_COUNTS
            | SAS_SUBHEADER_SIGNATURE_COLUMN_LIST
    )
}
