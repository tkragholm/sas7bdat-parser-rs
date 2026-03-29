use crate::{
    error::{CompressionError, Error, Result},
    internal::{SmallCommandBlock, SmallOp},
    metadata::CompressionKind,
};

const RLE_COMMAND_LENGTHS: [usize; 16] = [1, 1, 0, 0, 2, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0];
const COMMAND_BLOCK_CAPACITY: usize = 16;

#[derive(Debug, Clone, Copy)]
struct RleOp {
    copy_len: usize,
    insert_len: usize,
    insert_byte: u8,
}

#[derive(Debug, Clone, Copy, Default)]
struct RdcState {
    prefix: u16,
    bit_index: u8,
    has_prefix: bool,
}

pub(crate) fn decompress_row<'a>(
    compression: CompressionKind,
    input: &'a [u8],
    expected_len: usize,
    output: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    match compression {
        CompressionKind::None => Ok(input),
        CompressionKind::Row => {
            decompress_rle(input, expected_len, output)?;
            Ok(output.as_slice())
        }
        CompressionKind::Binary => {
            decompress_rdc(input, expected_len, output)?;
            Ok(output.as_slice())
        }
        CompressionKind::Unknown => Err(Error::unsupported(
            "compressed row execution for unknown compression kind",
        )),
    }
}

fn decode_rle_command(control: u8, input: &[u8], cursor: &mut usize) -> Result<RleOp> {
    let command = usize::from(control >> 4);
    if command >= RLE_COMMAND_LENGTHS.len() {
        return Err(compression_error("unknown RLE command"));
    }
    let length_nibble = usize::from(control & 0x0F);
    if cursor.saturating_add(RLE_COMMAND_LENGTHS[command]) > input.len() {
        return Err(compression_error("RLE command exceeds input length"));
    }

    let mut copy_len = 0usize;
    let mut insert_len = 0usize;
    let mut insert_byte = 0u8;

    match command {
        0 => {
            let next = usize::from(input[*cursor]);
            *cursor += 1;
            copy_len = next + 64 + length_nibble * 256;
        }
        1 => {
            let next = usize::from(input[*cursor]);
            *cursor += 1;
            copy_len = next + 64 + length_nibble * 256 + 4096;
        }
        2 => {
            copy_len = length_nibble + 96;
        }
        4 => {
            let next = usize::from(input[*cursor]);
            *cursor += 1;
            insert_len = next + 18 + length_nibble * 256;
            insert_byte = input[*cursor];
            *cursor += 1;
        }
        5 => {
            let next = usize::from(input[*cursor]);
            *cursor += 1;
            insert_len = next + 17 + length_nibble * 256;
            insert_byte = b'@';
        }
        6 => {
            let next = usize::from(input[*cursor]);
            *cursor += 1;
            insert_len = next + 17 + length_nibble * 256;
            insert_byte = b' ';
        }
        7 => {
            let next = usize::from(input[*cursor]);
            *cursor += 1;
            insert_len = next + 17 + length_nibble * 256;
            insert_byte = 0;
        }
        8 => {
            copy_len = length_nibble + 1;
        }
        9 => {
            copy_len = length_nibble + 17;
        }
        10 => {
            copy_len = length_nibble + 33;
        }
        11 => {
            copy_len = length_nibble + 49;
        }
        12 => {
            insert_byte = input[*cursor];
            *cursor += 1;
            insert_len = length_nibble + 3;
        }
        13 => {
            insert_len = length_nibble + 2;
            insert_byte = b'@';
        }
        14 => {
            insert_len = length_nibble + 2;
            insert_byte = b' ';
        }
        15 => {
            insert_len = length_nibble + 2;
            insert_byte = 0;
        }
        3 => {}
        _ => {}
    }

    Ok(RleOp {
        copy_len,
        insert_len,
        insert_byte,
    })
}

pub(crate) fn decompress_rle(
    input: &[u8],
    expected_len: usize,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let mut cursor = 0usize;
    if output.capacity() < expected_len {
        output.reserve(expected_len - output.capacity());
    }
    let mut block = SmallCommandBlock::<COMMAND_BLOCK_CAPACITY>::default();

    while cursor < input.len() {
        decode_rle_block(input, &mut cursor, &mut block)?;
        if block.len == 0 {
            break;
        }
        execute_command_block(&block, input, output)?;
    }

    if output.len() != expected_len {
        return Err(compression_error("RLE output length mismatch"));
    }

    Ok(())
}

pub(crate) fn decompress_rdc(
    input: &[u8],
    expected_len: usize,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let mut cursor = 0usize;
    if output.capacity() < expected_len {
        output.reserve(expected_len - output.capacity());
    }
    let mut state = RdcState::default();
    let mut block = SmallCommandBlock::<COMMAND_BLOCK_CAPACITY>::default();

    while cursor < input.len() || state.has_prefix {
        decode_rdc_block(input, &mut cursor, &mut state, &mut block)?;
        if block.len == 0 {
            break;
        }
        execute_command_block(&block, input, output)?;
    }

    if output.len() != expected_len {
        return Err(compression_error("RDC output length mismatch"));
    }

    Ok(())
}

fn decode_rle_block(
    input: &[u8],
    cursor: &mut usize,
    block: &mut SmallCommandBlock<COMMAND_BLOCK_CAPACITY>,
) -> Result<()> {
    clear_block(block);
    while *cursor < input.len() && usize::from(block.len) < COMMAND_BLOCK_CAPACITY {
        let control = input[*cursor];
        *cursor += 1;
        let op = decode_rle_command(control, input, cursor)?;

        if op.copy_len > 0 {
            let src_off = *cursor;
            if cursor.saturating_add(op.copy_len) > input.len() {
                return Err(compression_error("RLE copy exceeds input length"));
            }
            push_literal_segments(block, src_off, op.copy_len)?;
            *cursor += op.copy_len;
        }

        if op.insert_len > 0 {
            push_fill_segments(block, op.insert_byte, op.insert_len)?;
        }

        if usize::from(block.len) >= COMMAND_BLOCK_CAPACITY {
            break;
        }
    }
    Ok(())
}

fn decode_rdc_block(
    input: &[u8],
    cursor: &mut usize,
    state: &mut RdcState,
    block: &mut SmallCommandBlock<COMMAND_BLOCK_CAPACITY>,
) -> Result<()> {
    clear_block(block);

    while usize::from(block.len) < COMMAND_BLOCK_CAPACITY {
        if !state.has_prefix {
            if *cursor + 2 > input.len() {
                break;
            }
            state.prefix = u16::from_be_bytes([input[*cursor], input[*cursor + 1]]);
            *cursor += 2;
            state.bit_index = 0;
            state.has_prefix = true;
        }

        while state.bit_index < 16 && usize::from(block.len) < COMMAND_BLOCK_CAPACITY {
            let bit = state.bit_index;
            state.bit_index += 1;

            if (state.prefix & (1 << (15 - bit))) == 0 {
                if *cursor >= input.len() {
                    state.has_prefix = false;
                    return Ok(());
                }
                push_literal_segments(block, *cursor, 1)?;
                *cursor += 1;
                continue;
            }

            if *cursor + 2 > input.len() {
                return Err(compression_error("RDC marker exceeds input"));
            }

            let marker = input[*cursor];
            let next = input[*cursor + 1];
            *cursor += 2;

            if marker <= 0x0F {
                push_fill_segments(block, next, 3 + usize::from(marker))?;
            } else if (marker >> 4) == 1 {
                if *cursor >= input.len() {
                    return Err(compression_error("RDC insert length exceeds input"));
                }
                let insert_len = 19 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                let insert_byte = input[*cursor];
                *cursor += 1;
                push_fill_segments(block, insert_byte, insert_len)?;
            } else if (marker >> 4) == 2 {
                if *cursor >= input.len() {
                    return Err(compression_error("RDC copy length exceeds input"));
                }
                let copy_len = 16 + usize::from(input[*cursor]);
                *cursor += 1;
                let back_offset = 3 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                push_copy_segments(block, back_offset, copy_len)?;
            } else {
                let copy_len = usize::from(marker >> 4);
                let back_offset = 3 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                push_copy_segments(block, back_offset, copy_len)?;
            }
        }

        if state.bit_index >= 16 {
            state.has_prefix = false;
        }

        if usize::from(block.len) >= COMMAND_BLOCK_CAPACITY {
            break;
        }
    }

    Ok(())
}

fn execute_command_block<const N: usize>(
    block: &SmallCommandBlock<N>,
    input: &[u8],
    output: &mut Vec<u8>,
) -> Result<()> {
    for index in 0..usize::from(block.len) {
        match block.ops[index] {
            SmallOp::Literal { src_off, len } => {
                let src_off = usize::try_from(src_off)
                    .map_err(|_| compression_error("literal src offset exceeds usize"))?;
                let len = usize::from(len);
                if src_off.saturating_add(len) > input.len() {
                    return Err(compression_error("literal exceeds input length"));
                }
                output.extend_from_slice(&input[src_off..src_off + len]);
            }
            SmallOp::Fill { byte, len } => {
                let len = usize::from(len);
                output.resize(output.len().saturating_add(len), byte);
            }
            SmallOp::CopyBackref { back, len } => {
                let back = usize::from(back);
                let len = usize::from(len);
                if back == 0 || output.len() < back || len > back {
                    return Err(compression_error("copy-backref invalid"));
                }
                let start = output.len() - back;
                output.extend_from_within(start..start + len);
            }
        }
    }
    Ok(())
}

fn clear_block<const N: usize>(block: &mut SmallCommandBlock<N>) {
    block.len = 0;
}

fn push_literal_segments<const N: usize>(
    block: &mut SmallCommandBlock<N>,
    src_off: usize,
    len: usize,
) -> Result<()> {
    let mut remaining = len;
    let mut local_src = src_off;
    while remaining > 0 {
        if usize::from(block.len) >= N {
            return Err(compression_error(
                "command block overflow while decoding literal",
            ));
        }
        let chunk = remaining.min(usize::from(u16::MAX));
        block.ops[usize::from(block.len)] = SmallOp::Literal {
            src_off: u32::try_from(local_src)
                .map_err(|_| compression_error("literal src offset exceeds u32"))?,
            len: u16::try_from(chunk).unwrap_or(u16::MAX),
        };
        block.len = block.len.saturating_add(1);
        remaining -= chunk;
        local_src = local_src.saturating_add(chunk);
    }
    Ok(())
}

fn push_fill_segments<const N: usize>(
    block: &mut SmallCommandBlock<N>,
    byte: u8,
    len: usize,
) -> Result<()> {
    let mut remaining = len;
    while remaining > 0 {
        if usize::from(block.len) >= N {
            return Err(compression_error(
                "command block overflow while decoding fill",
            ));
        }
        let chunk = remaining.min(usize::from(u16::MAX));
        block.ops[usize::from(block.len)] = SmallOp::Fill {
            byte,
            len: u16::try_from(chunk).unwrap_or(u16::MAX),
        };
        block.len = block.len.saturating_add(1);
        remaining -= chunk;
    }
    Ok(())
}

fn push_copy_segments<const N: usize>(
    block: &mut SmallCommandBlock<N>,
    back: usize,
    len: usize,
) -> Result<()> {
    let back = u16::try_from(back).map_err(|_| compression_error("copy-backref exceeds u16"))?;
    let mut remaining = len;
    while remaining > 0 {
        if usize::from(block.len) >= N {
            return Err(compression_error(
                "command block overflow while decoding copy-backref",
            ));
        }
        let chunk = remaining.min(usize::from(u16::MAX));
        block.ops[usize::from(block.len)] = SmallOp::CopyBackref {
            back,
            len: u16::try_from(chunk).unwrap_or(u16::MAX),
        };
        block.len = block.len.saturating_add(1);
        remaining -= chunk;
    }
    Ok(())
}

fn compression_error(message: impl Into<String>) -> Error {
    Error::Compression(CompressionError {
        message: message.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::{decompress_rdc, decompress_rle};

    #[test]
    fn decompresses_simple_rle_literal_fill() {
        let mut output = Vec::new();
        decompress_rle(&[0xC1, b'A'], 4, &mut output).expect("rle");
        assert_eq!(output, b"AAAA");
    }

    #[test]
    fn decompresses_simple_rdc_literals() {
        let mut compressed = Vec::new();
        compressed.extend_from_slice(&0u16.to_be_bytes());
        compressed.extend_from_slice(b"BCDE");
        let mut output = Vec::new();
        decompress_rdc(&compressed, 4, &mut output).expect("rdc");
        assert_eq!(output, b"BCDE");
    }

    #[test]
    fn decompresses_rdc_copy_backref_sequence() {
        let mut compressed = Vec::new();
        compressed.extend_from_slice(&0x1000u16.to_be_bytes());
        compressed.extend_from_slice(b"ABC");
        compressed.extend_from_slice(&[0x30, 0x00]);
        let mut output = Vec::new();
        decompress_rdc(&compressed, 6, &mut output).expect("rdc backref");
        assert_eq!(output, b"ABCABC");
    }
}
