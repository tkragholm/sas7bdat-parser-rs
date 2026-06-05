use crate::{
    error::{CompressionError, Error, Result},
    metadata::CompressionKind,
};

const RLE_COMMAND_LENGTHS: [usize; 16] = [1, 1, 0, 0, 2, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0];

#[derive(Debug, Clone, Copy)]
struct RleOp {
    copy_len: usize,
    insert_len: usize,
    insert_byte: u8,
}

pub fn decompress_row<'a>(
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
        _ => {}
    }

    Ok(RleOp {
        copy_len,
        insert_len,
        insert_byte,
    })
}

pub fn decompress_rle(input: &[u8], expected_len: usize, output: &mut Vec<u8>) -> Result<()> {
    // Build the row incrementally: each output byte is written exactly once.
    // (Pre-`resize`-ing to zeros would memset the whole row and then overwrite
    // every byte during decode, doubling the writes on this per-row hot path.)
    output.clear();
    output.reserve(expected_len);
    let mut cursor = 0usize;

    while cursor < input.len() && output.len() < expected_len {
        let control = input[cursor];
        cursor += 1;
        let op = decode_rle_command(control, input, &mut cursor)?;

        if op.copy_len > 0 {
            if cursor.saturating_add(op.copy_len) > input.len() {
                return Err(compression_error("RLE copy exceeds input length"));
            }
            if output.len().saturating_add(op.copy_len) > expected_len {
                return Err(compression_error("RLE copy exceeds output length"));
            }
            output.extend_from_slice(&input[cursor..cursor + op.copy_len]);
            cursor += op.copy_len;
        }

        if op.insert_len > 0 {
            if output.len().saturating_add(op.insert_len) > expected_len {
                return Err(compression_error("RLE insert exceeds output length"));
            }
            output.resize(output.len() + op.insert_len, op.insert_byte);
        }
    }

    if output.len() != expected_len {
        return Err(compression_error("RLE output length mismatch"));
    }

    Ok(())
}

pub fn decompress_rdc(input: &[u8], expected_len: usize, output: &mut Vec<u8>) -> Result<()> {
    output.clear();
    if output.capacity() < expected_len {
        output.reserve(expected_len - output.capacity());
    }
    let mut cursor = 0usize;

    // Each 16-bit big-endian prefix word describes the next up-to-16 tokens:
    // a 0 bit is a single literal byte, a 1 bit is a fill/copy marker. Tokens
    // are applied straight to `output` as they are decoded (no staging buffer).
    'words: while cursor + 2 <= input.len() {
        let prefix = u16::from_be_bytes([input[cursor], input[cursor + 1]]);
        cursor += 2;

        for bit in 0..16u8 {
            if (prefix & (1 << (15 - bit))) == 0 {
                // Literal byte. Running out of input here ends the row.
                if cursor >= input.len() {
                    break 'words;
                }
                output.push(input[cursor]);
                cursor += 1;
                continue;
            }

            if cursor + 2 > input.len() {
                return Err(compression_error("RDC marker exceeds input"));
            }
            let marker = input[cursor];
            let next = input[cursor + 1];
            cursor += 2;

            if marker <= 0x0F {
                let fill_len = 3 + usize::from(marker);
                output.resize(output.len() + fill_len, next);
            } else if (marker >> 4) == 1 {
                if cursor >= input.len() {
                    return Err(compression_error("RDC insert length exceeds input"));
                }
                let fill_len = 19 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                let fill_byte = input[cursor];
                cursor += 1;
                output.resize(output.len() + fill_len, fill_byte);
            } else if (marker >> 4) == 2 {
                if cursor >= input.len() {
                    return Err(compression_error("RDC copy length exceeds input"));
                }
                let copy_len = 16 + usize::from(input[cursor]);
                cursor += 1;
                let back = 3 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                copy_backref(output, back, copy_len)?;
            } else {
                let copy_len = usize::from(marker >> 4);
                let back = 3 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                copy_backref(output, back, copy_len)?;
            }
        }
    }

    if output.len() != expected_len {
        return Err(compression_error("RDC output length mismatch"));
    }

    Ok(())
}

/// Copies `len` bytes from `back` bytes before the current end of `output`.
/// The encoding never references past the start nor overlaps the cursor, so a
/// back-reference that does is treated as corrupt input.
#[inline]
fn copy_backref(output: &mut Vec<u8>, back: usize, len: usize) -> Result<()> {
    if back == 0 || output.len() < back || len > back {
        return Err(compression_error("copy-backref invalid"));
    }
    let start = output.len() - back;
    output.extend_from_within(start..start + len);
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

    #[test]
    fn ignores_trailing_rle_padding_once_row_is_full() {
        let mut output = Vec::new();
        // 0xC1, 'A' -> command 12, length 4, byte 'A'.
        // 0x80 -> padding: command 8, length 1. Needs 1 literal byte.
        let input = &[0xC1, b'A', 0x80];
        decompress_rle(input, 4, &mut output).expect("rle with trailing padding");
        assert_eq!(output, b"AAAA");
    }
}
