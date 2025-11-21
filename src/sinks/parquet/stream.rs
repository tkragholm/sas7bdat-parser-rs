use parquet::errors::ParquetError;

use crate::error::{Error, Result};

pub(super) struct StreamNumericCtx<'a, T> {
    pub def_levels: &'a mut Vec<i16>,
    pub def_bitmap: &'a mut Vec<u8>,
    pub values: &'a mut Vec<T>,
    pub chunk: usize,
}

#[inline]
pub(super) fn prepare_def_bitmap(bitmap: &mut Vec<u8>, len: usize) {
    if len == 0 {
        bitmap.clear();
        return;
    }
    let bytes = (len - 1) / 8 + 1;
    if bitmap.len() < bytes {
        bitmap.resize(bytes, 0);
    } else {
        bitmap[..bytes].fill(0);
    }
}

#[inline]
pub(super) fn expand_bitmap_to_def_levels(def_levels: &mut Vec<i16>, bitmap: &[u8], len: usize) {
    def_levels.clear();
    def_levels.reserve(len);
    for idx in 0..len {
        let byte = idx >> 3;
        let bit = idx & 7;
        let level = i16::from(byte < bitmap.len() && (bitmap[byte] & (1 << bit)) != 0);
        def_levels.push(level);
    }
}

pub(super) fn stream_numeric<T, F, W, P, I>(
    ctx: &mut StreamNumericCtx<'_, T>,
    total_len: usize,
    mut iter_provider: P,
    mut map_value: F,
    mut write_chunk: W,
) -> Result<()>
where
    P: FnMut(usize, usize) -> I,
    I: Iterator<Item = Option<u64>>,
    F: FnMut(u64) -> Result<T>,
    W: FnMut(&[T], &[i16]) -> std::result::Result<usize, ParquetError>,
{
    let total = total_len;
    let mut processed = 0;
    while processed < total {
        let take = (total - processed).min(ctx.chunk);
        prepare_def_bitmap(ctx.def_bitmap, take);
        ctx.values.clear();
        ctx.values.reserve(take);
        for (idx, maybe_bits) in iter_provider(processed, take).enumerate() {
            if let Some(bits) = maybe_bits {
                let byte = idx >> 3;
                let bit = idx & 7;
                ctx.def_bitmap[byte] |= 1 << bit;
                let value = map_value(bits)?;
                ctx.values.push(value);
            }
        }
        expand_bitmap_to_def_levels(ctx.def_levels, ctx.def_bitmap, take);
        write_chunk(ctx.values, ctx.def_levels).map_err(Error::from)?;
        processed += take;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{StreamNumericCtx, stream_numeric};

    #[test]
    fn stream_numeric_builds_def_levels_from_bitmap() {
        let data: Vec<Option<u64>> = vec![Some(1), None, Some(2), None, None];
        let mut captured_defs: Vec<Vec<i16>> = Vec::new();
        let mut captured_values: Vec<Vec<u64>> = Vec::new();

        let mut ctx = StreamNumericCtx {
            def_levels: &mut Vec::new(),
            def_bitmap: &mut Vec::new(),
            values: &mut Vec::new(),
            chunk: 3,
        };

        stream_numeric(
            &mut ctx,
            data.len(),
            |start, len| data[start..start + len].iter().copied(),
            Ok,
            |vals: &[u64], defs: &[i16]| {
                captured_values.push(vals.to_vec());
                captured_defs.push(defs.to_vec());
                Ok(vals.len())
            },
        )
        .expect("streaming numeric data should succeed");

        assert_eq!(captured_defs.len(), 2);
        assert_eq!(captured_defs[0], vec![1, 0, 1]);
        assert_eq!(captured_defs[1], vec![0, 0]);
        assert_eq!(captured_values[0], vec![1, 2]);
        assert!(captured_values[1].is_empty());
    }
}
