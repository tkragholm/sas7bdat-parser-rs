use std::borrow::Cow;
use std::convert::TryInto;
use std::io::{Read, Seek, SeekFrom};

use crate::dataset::{Compression, Vendor};
use crate::error::{Error, Result, Section};
use crate::logger::log_warn;
use crate::parser::core::byteorder::read_u16;
use crate::parser::metadata::{PageKind, classify_page};

use super::buffer::RowData;
use super::compression::{decompress_rdc, decompress_rle};
use super::constants::{
    SAS_COMPRESSION_NONE, SAS_COMPRESSION_ROW, SAS_COMPRESSION_TRUNC, SAS_PAGE_TYPE_COMP,
    SAS_PAGE_TYPE_DATA, SAS_PAGE_TYPE_MASK, SAS_PAGE_TYPE_MIX, SUBHEADER_POINTER_OFFSET,
};
use super::iterator::RowIterator;
use super::pointer::{PointerInfo, parse_pointer, read_signature, signature_is_recognized};

struct PointerContext {
    page_index: u64,
    page_type: u16,
    subheader_count: u16,
    row_length: usize,
    pointer_size: usize,
    target_rows: Option<usize>,
}

impl<R: Read + Seek> RowIterator<'_, R> {
    pub(crate) fn fetch_next_page(&mut self) -> Result<()> {
        let header = &self.layout.header;
        let row_length = self.row_length;

        while self.next_page_index < header.page_count {
            let (page_index, page_type, page_row_count) = self.read_page_header()?;
            if (page_type & SAS_PAGE_TYPE_COMP) != 0 {
                continue;
            }

            let page_kind = classify_page(page_type);
            if matches!(
                page_kind,
                PageKind::Comp | PageKind::CompTable | PageKind::Unknown
            ) {
                continue;
            }
            let base_page_type = page_type & SAS_PAGE_TYPE_MASK;
            let target_rows = if page_row_count == 0 {
                None
            } else {
                Some(page_row_count as usize)
            };

            self.recycle_current_rows();

            let Some(subheader_count) = self.read_subheader_count(page_index, page_type) else {
                continue;
            };

            self.process_subheaders(
                page_index,
                page_type,
                subheader_count,
                target_rows,
                row_length,
            )?;

            if self.current_rows.is_empty() {
                self.collect_rows_from_data_area(
                    page_index,
                    base_page_type,
                    page_row_count,
                    subheader_count,
                    row_length,
                )?;
            }

            self.page_row_count
                .set(self.current_rows.len().try_into().unwrap_or(u16::MAX));
            self.row_in_page.set(0);
            if self.page_row_count.get() > 0 {
                return Ok(());
            }
        }

        self.page_row_count.set(0);
        Ok(())
    }

    fn read_page_header(&mut self) -> Result<(u64, u16, u16)> {
        let header = &self.layout.header;
        let offset = header.data_offset + self.next_page_index * u64::from(header.page_size);
        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(Error::from)?;
        self.reader
            .read_exact(&mut self.page_buffer)
            .map_err(Error::from)?;
        let page_index = self.next_page_index;
        self.next_page_index += 1;

        let page_type = read_u16(
            header.endianness,
            &self.page_buffer[(header.page_header_size as usize) - 8..],
        );
        let page_row_count = read_u16(
            header.endianness,
            &self.page_buffer[(header.page_header_size as usize) - 6..],
        );
        Ok((page_index, page_type, page_row_count))
    }

    fn read_subheader_count(&self, page_index: u64, page_type: u16) -> Option<u16> {
        let header = &self.layout.header;
        let subheader_count_pos = header.page_header_size as usize - 4;
        let Some(count_bytes) = self
            .page_buffer
            .get(subheader_count_pos..subheader_count_pos + 2)
        else {
            log_warn(&format!(
                "Skipping page {page_index} (type=0x{page_type:04X}): subheader count exceeds page bounds [page_size={}, page_header_size={}]",
                header.page_size, header.page_header_size
            ));
            return None;
        };
        let subheader_count_raw = read_u16(header.endianness, count_bytes);
        let pointer_size = header.subheader_pointer_size as usize;
        let max_subheaders = self
            .page_buffer
            .len()
            .saturating_sub(header.page_header_size as usize)
            / pointer_size;
        let (subheader_count, truncated) = if usize::from(subheader_count_raw) > max_subheaders {
            (u16::try_from(max_subheaders).unwrap_or(0), true)
        } else {
            (subheader_count_raw, false)
        };
        if truncated {
            log_warn(&format!(
                "Clamping subheader count on page {page_index} (type=0x{page_type:04X}) from {} to {} to fit page bounds [page_size={}, header_size={}, pointer_size={}]",
                subheader_count_raw,
                max_subheaders,
                header.page_size,
                header.page_header_size,
                header.subheader_pointer_size
            ));
        }
        Some(subheader_count)
    }

    fn process_subheaders(
        &mut self,
        page_index: u64,
        page_type: u16,
        subheader_count: u16,
        target_rows: Option<usize>,
        row_length: usize,
    ) -> Result<()> {
        let header = &self.layout.header;
        let pointer_size = header.subheader_pointer_size as usize;
        let mut ptr_cursor = header.page_header_size as usize;
        let ctx = PointerContext {
            page_index,
            page_type,
            subheader_count,
            row_length,
            pointer_size,
            target_rows,
        };

        for _ in 0..subheader_count {
            if let Some(target) = target_rows
                && self.current_rows.len() >= target
            {
                break;
            }
            self.process_one_pointer(&ctx, &mut ptr_cursor)?;
        }
        Ok(())
    }

    fn process_one_pointer(&mut self, ctx: &PointerContext, ptr_cursor: &mut usize) -> Result<()> {
        let header = &self.layout.header;
        let pointer_end = ptr_cursor.saturating_add(ctx.pointer_size);
        let Some(pointer) = self.page_buffer.get(*ptr_cursor..pointer_end) else {
            log_warn(&format!(
                "Skipping page {page_index} (type=0x{page_type:04X}): subheader pointer exceeds page bounds [cursor={}, pointer_size={}, page_len={}]",
                *ptr_cursor,
                ctx.pointer_size,
                self.page_buffer.len(),
                page_index = ctx.page_index,
                page_type = ctx.page_type
            ));
            return Ok(());
        };
        *ptr_cursor = pointer_end;

        let info = parse_pointer(pointer, header.uses_u64, header.endianness)?;
        let min_data_offset =
            header.page_header_size as usize + usize::from(ctx.subheader_count) * ctx.pointer_size;
        if info.offset < min_data_offset {
            log_warn(&format!(
                "Skipping page {page_index} (type=0x{page_type:04X}): subheader pointer starts before data section [offset={}, min_offset={}, pointer_size={}, subheaders={}]",
                info.offset,
                min_data_offset,
                ctx.pointer_size,
                ctx.subheader_count,
                page_index = ctx.page_index,
                page_type = ctx.page_type
            ));
            return Ok(());
        }
        if info.length == 0 {
            return Ok(());
        }
        if info.offset + info.length > self.page_buffer.len() {
            log_warn(&format!(
                "Skipping page {page_index} (type=0x{page_type:04X}): subheader pointer references data beyond page bounds [offset={}, length={}, page_len={}]",
                info.offset,
                info.length,
                self.page_buffer.len(),
                page_index = ctx.page_index,
                page_type = ctx.page_type
            ));
            return Ok(());
        }
        if info.compression == SAS_COMPRESSION_NONE {
            let sig_len = header.subheader_signature_size;
            if info.length < sig_len || info.offset + sig_len > self.page_buffer.len() {
                log_warn(&format!(
                    "Skipping page {page_index} (type=0x{page_type:04X}): subheader pointer too small for signature [offset={}, length={}, required={}, page_len={}]",
                    info.offset,
                    info.length,
                    sig_len,
                    self.page_buffer.len(),
                    page_index = ctx.page_index,
                    page_type = ctx.page_type
                ));
                return Ok(());
            }
        }

        self.handle_pointer_payload(ctx, &info)?;
        Ok(())
    }

    fn handle_pointer_payload(&mut self, ctx: &PointerContext, info: &PointerInfo) -> Result<()> {
        let header = &self.layout.header;
        let data_start = info.offset;
        let data_end = info.offset + info.length;
        match info.compression {
            SAS_COMPRESSION_NONE => {
                let data = &self.page_buffer[data_start..data_end];
                let signature = read_signature(data, header.endianness, header.uses_u64);
                if info.is_compressed_data && !signature_is_recognized(signature) {
                    let mut local_offset = info.offset;
                    let mut remaining = info.length;
                    while remaining >= ctx.row_length {
                        self.current_rows.push(RowData::Borrowed(local_offset));
                        remaining -= ctx.row_length;
                        local_offset += ctx.row_length;
                        if let Some(target) = ctx.target_rows
                            && self.current_rows.len() >= target
                        {
                            break;
                        }
                    }
                }
            }
            SAS_COMPRESSION_TRUNC => {
                // Truncated rows are continuations that reappear in the
                // next page; skip them to avoid emitting partial data.
            }
            SAS_COMPRESSION_ROW => {
                let mut buffer = self.take_row_buffer();
                let data = &self.page_buffer[data_start..data_end];
                let compression_mode = self.layout.row_info.compression;
                match compression_mode {
                    Compression::Row => decompress_rle(data, ctx.row_length, &mut buffer),
                    Compression::Binary => decompress_rdc(data, ctx.row_length, &mut buffer),
                    Compression::None => {
                        return Err(Error::Unsupported {
                            feature: Cow::from(
                                "row compression pointer seen in uncompressed dataset",
                            ),
                        });
                    }
                    Compression::Unknown(code) => {
                        return Err(Error::Unsupported {
                            feature: Cow::from(format!(
                                "row compression pointer for unsupported mode {code}",
                            )),
                        });
                    }
                }
                .map_err(|msg| Error::Corrupted {
                    section: Section::Page {
                        index: ctx.page_index,
                    },
                    details: Cow::Owned(format!(
                        "{msg} (compression={compression_mode:?}, page_type=0x{page_type:04X}, subheader_count={subheader_count}, pointer_range={data_start}..{data_end}, pointer_length={pointer_length}, row_length={row_length})",
                        page_type = ctx.page_type,
                        subheader_count = ctx.subheader_count,
                        row_length = ctx.row_length,
                        pointer_length = info.length
                    )),
                })?;
                self.current_rows.push(RowData::Owned(buffer));
            }
            other => {
                return Err(Error::Unsupported {
                    feature: Cow::from(format!("unsupported subheader compression mode {other}",)),
                });
            }
        }
        Ok(())
    }

    fn collect_rows_from_data_area(
        &mut self,
        page_index: u64,
        base_page_type: u16,
        page_row_count: u16,
        subheader_count: u16,
        row_length: usize,
    ) -> Result<()> {
        let header = &self.layout.header;
        if base_page_type != SAS_PAGE_TYPE_DATA && base_page_type != SAS_PAGE_TYPE_MIX {
            return Ok(());
        }

        let pointer_size = header.subheader_pointer_size as usize;
        let bit_offset = if header.uses_u64 { 32usize } else { 16usize };
        let pointer_section_len = (subheader_count as usize) * pointer_size;
        let base_offset = header.page_header_size as usize + pointer_section_len;
        let alignment_base = bit_offset + SUBHEADER_POINTER_OFFSET + pointer_section_len;
        let align_adjust = if alignment_base.is_multiple_of(8) {
            0
        } else {
            8 - (alignment_base % 8)
        };
        let mut data_start = base_offset.saturating_add(align_adjust);

        if base_page_type == SAS_PAGE_TYPE_MIX
            && (data_start % 8) == 4
            && data_start + 4 <= self.page_buffer.len()
        {
            let word = u32::from_le_bytes(
                self.page_buffer[data_start..data_start + 4]
                    .try_into()
                    .unwrap(),
            );
            if word == 0 || word == 0x2020_2020 || header.metadata.vendor != Vendor::StatTransfer {
                data_start = data_start.saturating_add(4);
            }
        }

        if data_start >= self.page_buffer.len() {
            return Ok(());
        }

        let available = self.page_buffer.len().saturating_sub(data_start);
        let possible_rows = available / row_length;
        if possible_rows == 0 {
            return Ok(());
        }

        let remaining_rows_u64 = self.total_rows.saturating_sub(self.emitted_rows.get());
        let remaining_rows = usize::try_from(remaining_rows_u64).map_or(usize::MAX, |value| value);

        let mut rows_to_take = if base_page_type == SAS_PAGE_TYPE_MIX {
            let mix_limit = usize::try_from(self.layout.row_info.rows_per_page)
                .map_or(usize::MAX, |value| value);
            let mix_limit = if mix_limit == 0 {
                possible_rows
            } else {
                mix_limit
            };
            mix_limit.min(possible_rows)
        } else {
            let header_limit = usize::from(page_row_count);
            let header_limit = if header_limit == 0 {
                possible_rows
            } else {
                header_limit
            };
            header_limit.min(possible_rows)
        };

        rows_to_take = rows_to_take.min(remaining_rows);
        rows_to_take = rows_to_take.min(possible_rows);

        if rows_to_take == 0 {
            return Ok(());
        }

        for idx in 0..rows_to_take {
            let offset = data_start + idx * row_length;
            if offset + row_length > self.page_buffer.len() {
                return Err(Error::Corrupted {
                    section: Section::Page { index: page_index },
                    details: Cow::from("row slice exceeds page bounds"),
                });
            }
            self.current_rows.push(RowData::Borrowed(offset));
        }

        Ok(())
    }

    pub(crate) fn recycle_current_rows(&mut self) {
        for entry in self.current_rows.drain(..) {
            if let RowData::Owned(mut buffer) = entry {
                buffer.clear();
                self.reusable_row_buffers.push(buffer);
            }
        }
    }

    pub(crate) fn recycle_owned_rows(&mut self) {
        self.columnar_owned_buffer.clear();
    }

    pub(crate) fn take_row_buffer(&mut self) -> Vec<u8> {
        self.reusable_row_buffers.pop().unwrap_or_default()
    }
}
