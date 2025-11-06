use sas7bdat_parser_rs::parser::parse_header;
use sas7bdat_parser_rs::metadata::Endianness;
use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn read_u16(endian: Endianness, bytes: &[u8]) -> u16 {
    match endian {
        Endianness::Little => u16::from_le_bytes([bytes[0], bytes[1]]),
        Endianness::Big => u16::from_be_bytes([bytes[0], bytes[1]]),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).expect("path required");
    let mut file = File::open(&path)?;
    let header = parse_header(&mut file)?;
    let mut page = vec![0u8; header.page_size as usize];

    for page_index in 0..header.page_count {
        let offset = header.data_offset + page_index * u64::from(header.page_size);
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut page)?;
        let subheader_count = read_u16(
            header.endianness,
            &page[(header.page_header_size as usize) - 4..(header.page_header_size as usize) - 2],
        );
        let mut cursor = header.page_header_size as usize;
        for pointer_index in 0..subheader_count {
            let pointer = &page[cursor..cursor + header.subheader_pointer_size as usize];
            cursor += header.subheader_pointer_size as usize;
            let (offset, length) = if header.uses_u64 {
                let offset = match header.endianness {
                    Endianness::Little => u64::from_le_bytes(pointer[0..8].try_into().unwrap()),
                    Endianness::Big => u64::from_be_bytes(pointer[0..8].try_into().unwrap()),
                };
                let length = match header.endianness {
                    Endianness::Little => u64::from_le_bytes(pointer[8..16].try_into().unwrap()),
                    Endianness::Big => u64::from_be_bytes(pointer[8..16].try_into().unwrap()),
                };
                (offset as usize, length as usize)
            } else {
                let offset = match header.endianness {
                    Endianness::Little => u32::from_le_bytes(pointer[0..4].try_into().unwrap()),
                    Endianness::Big => u32::from_be_bytes(pointer[0..4].try_into().unwrap()),
                };
                let length = match header.endianness {
                    Endianness::Little => u32::from_le_bytes(pointer[4..8].try_into().unwrap()),
                    Endianness::Big => u32::from_be_bytes(pointer[4..8].try_into().unwrap()),
                };
                (offset as usize, length as usize)
            };
            let compression = pointer[if header.uses_u64 { 16 } else { 8 }];
            let page_ref = if header.uses_u64 {
                match header.endianness {
                    Endianness::Little => u32::from_le_bytes(pointer[18..22].try_into().unwrap()),
                    Endianness::Big => u32::from_be_bytes(pointer[18..22].try_into().unwrap()),
                }
            } else {
                match header.endianness {
                    Endianness::Little => u16::from_le_bytes(pointer[10..12].try_into().unwrap()) as u32,
                    Endianness::Big => u16::from_be_bytes(pointer[10..12].try_into().unwrap()) as u32,
                }
            };
            let extra_flag = pointer[if header.uses_u64 { 17 } else { 9 }];

            if length == 0 {
                continue;
            }
            let end = offset + length;
            if end > page.len() {
                println!(
                    "page {page_index} pointer {pointer_index}: offset={offset} length={length} end={end} > page_len={} compression={} extra_flag={} page_ref={}"
                    , page.len(), compression, extra_flag, page_ref
                );
            }
        }
    }

    Ok(())
}
