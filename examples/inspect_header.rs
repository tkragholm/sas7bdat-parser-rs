#![allow(clippy::pedantic)]
use sas7bdat::parser::parse_header;
use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).expect("path required");
    let mut file = File::open(&path)?;
    let header = parse_header(&mut file)?;
    println!("endianness: {:?}", header.endianness);
    println!("uses_u64: {}", header.uses_u64);
    println!("page_size: {}", header.page_size);
    println!("page_count: {}", header.page_count);
    println!("page_header_size: {}", header.page_header_size);
    println!("subheader_pointer_size: {}", header.subheader_pointer_size);
    println!(
        "subheader_signature_size: {}",
        header.subheader_signature_size
    );
    println!("data_offset: {}", header.data_offset);

    // read first page raw pointer info
    let mut page = vec![0u8; header.page_size as usize];
    file.seek(SeekFrom::Start(header.data_offset))?;
    file.read_exact(&mut page)?;
    let ptr_size = header.subheader_pointer_size as usize;
    let mut cursor = header.page_header_size as usize;
    let subheader_count_pos = header.page_header_size as usize - 4;
    let subheader_count = match header.endianness {
        sas7bdat::dataset::Endianness::Little => {
            u16::from_le_bytes([page[subheader_count_pos], page[subheader_count_pos + 1]])
        }
        sas7bdat::dataset::Endianness::Big => {
            u16::from_be_bytes([page[subheader_count_pos], page[subheader_count_pos + 1]])
        }
    };
    println!("subheader_count: {}", subheader_count);

    for idx in 0..subheader_count as usize {
        let pointer = &page[cursor..cursor + ptr_size];
        cursor += ptr_size;
        let (offset, length) = if header.uses_u64 {
            let offset = match header.endianness {
                sas7bdat::dataset::Endianness::Little => {
                    u64::from_le_bytes(pointer[0..8].try_into().unwrap())
                }
                sas7bdat::dataset::Endianness::Big => {
                    u64::from_be_bytes(pointer[0..8].try_into().unwrap())
                }
            };
            let length = match header.endianness {
                sas7bdat::dataset::Endianness::Little => {
                    u64::from_le_bytes(pointer[8..16].try_into().unwrap())
                }
                sas7bdat::dataset::Endianness::Big => {
                    u64::from_be_bytes(pointer[8..16].try_into().unwrap())
                }
            };
            (offset as u64, length as u64)
        } else {
            let offset = match header.endianness {
                sas7bdat::dataset::Endianness::Little => {
                    u32::from_le_bytes(pointer[0..4].try_into().unwrap())
                }
                sas7bdat::dataset::Endianness::Big => {
                    u32::from_be_bytes(pointer[0..4].try_into().unwrap())
                }
            };
            let length = match header.endianness {
                sas7bdat::dataset::Endianness::Little => {
                    u32::from_le_bytes(pointer[4..8].try_into().unwrap())
                }
                sas7bdat::dataset::Endianness::Big => {
                    u32::from_be_bytes(pointer[4..8].try_into().unwrap())
                }
            };
            (offset as u64, length as u64)
        };
        let compression = pointer[if header.uses_u64 { 16 } else { 8 }];
        let mut extra = Vec::new();
        if header.uses_u64 {
            extra.extend_from_slice(&pointer[17..24]);
        } else {
            extra.extend_from_slice(&pointer[9..12]);
        }
        println!(
            "pointer {idx}: offset={offset} length={length} compression={compression} extra={:?}",
            extra
        );
        if idx >= 10 {
            break;
        }
    }

    Ok(())
}
