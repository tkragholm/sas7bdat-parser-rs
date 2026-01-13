const RLE_COMMAND_LENGTHS: [usize; 16] = [1, 1, 0, 0, 2, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0];

struct RleOp {
    copy_len: usize,
    insert_len: usize,
    insert_byte: u8,
}

fn decode_rle_command(
    control: u8,
    input: &[u8],
    cursor: &mut usize,
) -> std::result::Result<RleOp, &'static str> {
    let command = (control >> 4) as usize;
    if command >= RLE_COMMAND_LENGTHS.len() {
        return Err("unknown RLE command");
    }
    let length_nibble = (control & 0x0F) as usize;
    if *cursor + RLE_COMMAND_LENGTHS[command] > input.len() {
        return Err("RLE command exceeds input length");
    }

    let mut copy_len = 0usize;
    let mut insert_len = 0usize;
    let mut insert_byte = 0u8;

    match command {
        0 => {
            let next = input[*cursor] as usize;
            *cursor += 1;
            copy_len = next + 64 + length_nibble * 256;
        }
        1 => {
            let next = input[*cursor] as usize;
            *cursor += 1;
            copy_len = next + 64 + length_nibble * 256 + 4096;
        }
        2 => {
            copy_len = length_nibble + 96;
        }
        4 => {
            let next = input[*cursor] as usize;
            *cursor += 1;
            insert_len = next + 18 + length_nibble * 256;
            insert_byte = input[*cursor];
            *cursor += 1;
        }
        5 => {
            let next = input[*cursor] as usize;
            *cursor += 1;
            insert_len = next + 17 + length_nibble * 256;
            insert_byte = b'@';
        }
        6 => {
            let next = input[*cursor] as usize;
            *cursor += 1;
            insert_len = next + 17 + length_nibble * 256;
            insert_byte = b' ';
        }
        7 => {
            let next = input[*cursor] as usize;
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

/// Decompresses RLE-compressed row data into `output`, validating bounds.
pub fn decompress_rle(
    input: &[u8],
    expected_len: usize,
    output: &mut Vec<u8>,
) -> std::result::Result<(), &'static str> {
    output.clear();
    output.resize(expected_len, 0);
    let buffer = output.as_mut_slice();
    let mut out_pos = 0usize;
    let mut i = 0usize;

    while i < input.len() {
        let control = input[i];
        i += 1;
        let op = decode_rle_command(control, input, &mut i)?;

        if op.copy_len > 0 {
            if out_pos + op.copy_len > expected_len {
                return Err("RLE copy exceeds output length");
            }
            if i + op.copy_len > input.len() {
                return Err("RLE copy exceeds input length");
            }
            buffer[out_pos..out_pos + op.copy_len].copy_from_slice(&input[i..i + op.copy_len]);
            i += op.copy_len;
            out_pos += op.copy_len;
        }

        if op.insert_len > 0 {
            if out_pos + op.insert_len > expected_len {
                return Err("RLE insert exceeds output length");
            }
            buffer[out_pos..out_pos + op.insert_len].fill(op.insert_byte);
            out_pos += op.insert_len;
        }
    }

    if out_pos != expected_len {
        return Err("RLE output length mismatch");
    }

    Ok(())
}

/// Decompresses RDC-compressed row data into `output`, validating bounds.
pub fn decompress_rdc(
    input: &[u8],
    expected_len: usize,
    output: &mut Vec<u8>,
) -> std::result::Result<(), &'static str> {
    output.clear();
    output.resize(expected_len, 0);
    let buffer = output.as_mut_slice();
    let mut out_pos = 0usize;
    let mut i = 0usize;
    while i + 2 <= input.len() {
        let prefix = u16::from_be_bytes([input[i], input[i + 1]]);
        i += 2;
        for bit in 0..16 {
            if (prefix & (1 << (15 - bit))) == 0 {
                if i >= input.len() {
                    break;
                }
                if out_pos >= expected_len {
                    return Err("RDC output overflow");
                }
                buffer[out_pos] = input[i];
                out_pos += 1;
                i += 1;
                continue;
            }

            if i + 2 > input.len() {
                return Err("RDC marker exceeds input");
            }
            let marker = input[i];
            let next = input[i + 1];
            i += 2;

            let mut insert_len = 0usize;
            let mut insert_byte = 0u8;
            let mut copy_len = 0usize;
            let mut back_offset = 0usize;

            if marker <= 0x0F {
                insert_len = 3 + marker as usize;
                insert_byte = next;
            } else if (marker >> 4) == 1 {
                if i >= input.len() {
                    return Err("RDC insert length exceeds input");
                }
                insert_len = 19 + (marker as usize & 0x0F) + (next as usize) * 16;
                insert_byte = input[i];
                i += 1;
            } else if (marker >> 4) == 2 {
                if i >= input.len() {
                    return Err("RDC copy length exceeds input");
                }
                copy_len = 16 + input[i] as usize;
                i += 1;
                back_offset = 3 + (marker as usize & 0x0F) + (next as usize) * 16;
            } else {
                copy_len = (marker >> 4) as usize;
                back_offset = 3 + (marker as usize & 0x0F) + (next as usize) * 16;
            }

            if insert_len > 0 {
                if out_pos + insert_len > expected_len {
                    return Err("RDC insert exceeds output length");
                }
                buffer[out_pos..out_pos + insert_len].fill(insert_byte);
                out_pos += insert_len;
            } else if copy_len > 0 {
                if back_offset == 0
                    || out_pos < back_offset
                    || copy_len > back_offset
                    || out_pos + copy_len > expected_len
                {
                    return Err("RDC copy invalid");
                }
                let start = out_pos - back_offset;
                for j in 0..copy_len {
                    let byte = buffer[start + j];
                    buffer[out_pos + j] = byte;
                }
                out_pos += copy_len;
            }
        }
    }

    if out_pos != expected_len {
        return Err("RDC output length mismatch");
    }
    Ok(())
}
