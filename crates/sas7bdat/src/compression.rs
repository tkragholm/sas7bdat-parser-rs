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

/// How far a short-form RLE op may write past the length it actually produces.
///
/// The short command families encode runs of at most 32 bytes — copies via commands 8
/// and 9 (1..=32 bytes), fills via 12 through 15 (2..=18). At those sizes a call into
/// `memcpy`/`memset` costs about as much as the bytes it moves, and profiling a
/// production-shaped file (14 columns, RLE) put `_platform_memset` and
/// `_platform_memmove` at 678 samples against `decompress_row`'s own 721.
///
/// So the short forms store a fixed 32 bytes regardless of the true run length and then
/// `truncate` back, which is the same trick LZ4 and zstd call "wildcopy". The long-form
/// commands carry their own length bytes and are meant for long runs, where the libc
/// call is the right tool; they keep it.
///
/// The buffer is reserved with this much slack so the overshoot always lands inside
/// already-allocated capacity, which is what keeps this safe code: `extend_from_slice`
/// with a fixed-size array cannot reallocate, and `truncate` on `u8` just moves `len`.
const WILD_OVERRUN: usize = 32;

/// Copy `len` bytes by way of one fixed-width store, or report that it does not apply.
///
/// Returns `false` when the run is too long for the fixed store, or when `src` has fewer
/// than `WILD_OVERRUN` readable bytes — near the end of the input the overshoot would
/// read past the slice, so the caller falls back to the exact copy. Both callers have
/// already bounds-checked `len` itself against the input and the row.
#[inline]
fn wild_copy(output: &mut Vec<u8>, src: &[u8], len: usize) -> bool {
    if len > WILD_OVERRUN || src.len() < WILD_OVERRUN {
        return false;
    }
    let end = output.len() + len;
    let block: [u8; WILD_OVERRUN] = src[..WILD_OVERRUN]
        .try_into()
        .expect("slice length checked above");
    output.extend_from_slice(&block);
    output.truncate(end);
    true
}

/// Fill `len` bytes by way of one fixed-width splat store, or report that it does not apply.
#[inline]
fn wild_fill(output: &mut Vec<u8>, byte: u8, len: usize) -> bool {
    if len > WILD_OVERRUN {
        return false;
    }
    let end = output.len() + len;
    output.extend_from_slice(&[byte; WILD_OVERRUN]);
    output.truncate(end);
    true
}

pub fn decompress_rle(input: &[u8], expected_len: usize, output: &mut Vec<u8>) -> Result<()> {
    // Build the row incrementally: each output byte is written exactly once.
    // (Pre-`resize`-ing to zeros would memset the whole row and then overwrite
    // every byte during decode, doubling the writes on this per-row hot path.)
    output.clear();
    reserve_row(output, expected_len)?;
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
            if !wild_copy(output, &input[cursor..], op.copy_len) {
                output.extend_from_slice(&input[cursor..cursor + op.copy_len]);
            }
            cursor += op.copy_len;
        }

        if op.insert_len > 0 {
            if output.len().saturating_add(op.insert_len) > expected_len {
                return Err(compression_error("RLE insert exceeds output length"));
            }
            if !wild_fill(output, op.insert_byte, op.insert_len) {
                output.resize(output.len() + op.insert_len, op.insert_byte);
            }
        }
    }

    if output.len() != expected_len {
        return Err(compression_error("RLE output length mismatch"));
    }

    Ok(())
}

/// Reserve room for one decompressed row, fallibly.
///
/// `expected_len` is the declared row length, so it is file-controlled. `reserve` would
/// route a failure through `handle_alloc_error` and abort the process, which is not a
/// recoverable error for the Python and R bindings. `parse_layout` separately refuses a
/// row length far larger than the page size, so this is the second line of defence
/// rather than the only one — a machine with enough RAM would happily satisfy a
/// multi-gigabyte reservation and then spend the scan filling it.
/// The `WILD_OVERRUN` slack is what makes [`wild_copy`] and [`wild_fill`] safe *and*
/// fast: with it, their fixed-size `extend_from_slice` can never hit the reallocation
/// path, so the whole short-run case stays a store into memory we already own.
#[inline]
fn reserve_row(output: &mut Vec<u8>, expected_len: usize) -> Result<()> {
    let want = expected_len.saturating_add(WILD_OVERRUN);
    let extra = want.saturating_sub(output.capacity());
    output
        .try_reserve(extra)
        .map_err(|_| compression_error("cannot allocate a buffer for the declared row length"))
}

/// Refuse an emit that would push the row past its declared length.
///
/// The RLE decoder checks this inline before every copy and insert. RDC did not: it
/// grew `output` freely and only compared lengths after the loop, so a crafted stream
/// ballooned the buffer first and failed second. One 3-byte RDC token can emit up to
/// 4114 bytes (`19 + 15 + 255 * 16`), which is roughly 1371x amplification per input
/// byte — enough for a small page to drive a multi-hundred-megabyte allocation.
#[inline]
fn ensure_room(output: &[u8], add: usize, expected_len: usize) -> Result<()> {
    if output.len().saturating_add(add) > expected_len {
        return Err(compression_error("RDC output exceeds expected row length"));
    }
    Ok(())
}

pub fn decompress_rdc(input: &[u8], expected_len: usize, output: &mut Vec<u8>) -> Result<()> {
    output.clear();
    reserve_row(output, expected_len)?;
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
                ensure_room(output, 1, expected_len)?;
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
                ensure_room(output, fill_len, expected_len)?;
                output.resize(output.len() + fill_len, next);
            } else if (marker >> 4) == 1 {
                if cursor >= input.len() {
                    return Err(compression_error("RDC insert length exceeds input"));
                }
                let fill_len = 19 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                let fill_byte = input[cursor];
                cursor += 1;
                ensure_room(output, fill_len, expected_len)?;
                output.resize(output.len() + fill_len, fill_byte);
            } else if (marker >> 4) == 2 {
                if cursor >= input.len() {
                    return Err(compression_error("RDC copy length exceeds input"));
                }
                let copy_len = 16 + usize::from(input[cursor]);
                cursor += 1;
                let back = 3 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                ensure_room(output, copy_len, expected_len)?;
                copy_backref(output, back, copy_len)?;
            } else {
                let copy_len = usize::from(marker >> 4);
                let back = 3 + usize::from(marker & 0x0F) + usize::from(next) * 16;
                ensure_room(output, copy_len, expected_len)?;
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
///
/// `len` may exceed `back`: an RDC copy command can encode `copy_len` up to 271 with `back` as
/// small as 3, which is the standard LZ77 run-length extension — bytes written during the copy
/// feed the tail of the same copy (e.g. `back = 1` repeats the last byte `len` times). Only an
/// out-of-range start (`back == 0` or `back > output.len()`) is corrupt.
#[inline]
fn copy_backref(output: &mut Vec<u8>, back: usize, len: usize) -> Result<()> {
    if back == 0 || output.len() < back {
        return Err(compression_error("copy-backref invalid"));
    }
    let start = output.len() - back;
    if len <= back {
        // Non-overlapping: the whole source range already exists; bulk-copy it.
        output.extend_from_within(start..start + len);
    } else {
        // Overlapping run: copy byte-by-byte so each freshly written byte can feed later
        // positions of this same copy.
        output
            .try_reserve(len)
            .map_err(|_| compression_error("cannot allocate for an RDC overlapping copy"))?;
        for i in 0..len {
            let byte = output[start + i];
            output.push(byte);
        }
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

    /// RDC used to grow `output` freely and only compare lengths after the loop, so a
    /// stream whose tokens emit far more than the row can hold ballooned the buffer
    /// first and failed second. One `marker >> 4 == 1` token emits
    /// `19 + (marker & 0x0F) + next * 16` bytes — 4114 at maximum — from three input
    /// bytes, so a page's worth of them reached hundreds of megabytes.
    ///
    /// The failure has to arrive without the buffer ever exceeding the row length.
    #[test]
    fn rdc_refuses_a_token_that_overflows_the_declared_row() {
        // One prefix word marking a single non-literal token, then a max-length fill:
        // marker 0x1F -> 19 + 15 + 255 * 16 = 4114 bytes of 0xAA.
        let compressed = [0x80, 0x00, 0x1F, 0xFF, 0xAA];
        let mut output = Vec::new();
        let err = decompress_rdc(&compressed, 16, &mut output)
            .expect_err("a 4114-byte fill must not be accepted into a 16-byte row");
        assert!(
            err.to_string().contains("exceeds expected row length"),
            "unexpected error: {err}"
        );
        assert!(
            output.len() <= 16,
            "buffer grew to {} bytes for a 16-byte row",
            output.len()
        );
    }

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
    fn decompresses_rdc_overlapping_short_copy() {
        // 3 literals "ABC", then a short copy of length 5 from back = 3 (overlap: len > back).
        // marker high nibble = copy_len (5); back = 3 + (marker & 0x0F) + next*16 = 3.
        // The copy re-reads its own freshly written bytes: ABC → ABCABCAB.
        let mut compressed = Vec::new();
        compressed.extend_from_slice(&0x1000u16.to_be_bytes()); // literal, literal, literal, marker
        compressed.extend_from_slice(b"ABC");
        compressed.extend_from_slice(&[0x50, 0x00]); // copy_len 5, back 3
        let mut output = Vec::new();
        decompress_rdc(&compressed, 8, &mut output).expect("rdc overlapping short copy");
        assert_eq!(output, b"ABCABCAB");
    }

    #[test]
    fn decompresses_rdc_overlapping_long_copy() {
        // 3 literals "ABC", then a long copy (marker high nibble == 2) of length 16 from
        // back = 3. copy_len = 16 + third_byte; back = 3 + (marker & 0x0F) + next*16 = 3.
        // The 3-byte pattern tiles across the 16-byte overlapping run.
        let mut compressed = Vec::new();
        compressed.extend_from_slice(&0x1000u16.to_be_bytes());
        compressed.extend_from_slice(b"ABC");
        compressed.extend_from_slice(&[0x20, 0x00, 0x00]); // long copy, back 3, copy_len 16+0
        let mut output = Vec::new();
        decompress_rdc(&compressed, 19, &mut output).expect("rdc overlapping long copy");
        assert_eq!(output, b"ABCABCABCABCABCABCA");
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
