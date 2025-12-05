#![allow(clippy::pedantic)]
use sas7bdat::parser::{parse_header, read_u16, read_u32, read_u64};
use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

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
                (
                    read_u64(header.endianness, &pointer[0..8]) as usize,
                    read_u64(header.endianness, &pointer[8..16]) as usize,
                )
            } else {
                (
                    read_u32(header.endianness, &pointer[0..4]) as usize,
                    read_u32(header.endianness, &pointer[4..8]) as usize,
                )
            };
            let compression = pointer[if header.uses_u64 { 16 } else { 8 }];
            let page_ref = if header.uses_u64 {
                read_u32(header.endianness, &pointer[18..22])
            } else {
                u32::from(read_u16(header.endianness, &pointer[10..12]))
            };
            let extra_flag = pointer[if header.uses_u64 { 17 } else { 9 }];

            if length == 0 {
                continue;
            }
            let end = offset + length;
            if end > page.len() {
                println!(
                    "page {page_index} pointer {pointer_index}: offset={offset} length={length} end={end} > page_len={} compression={} extra_flag={} page_ref={}",
                    page.len(),
                    compression,
                    extra_flag,
                    page_ref
                );
            }
        }
    }

    Ok(())
}
