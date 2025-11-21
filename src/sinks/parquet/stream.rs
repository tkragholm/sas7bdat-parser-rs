use parquet::errors::ParquetError;

use crate::error::{Error, Result};

pub(super) fn stream_numeric<T, F, W, P, I>(
    def_levels: &mut Vec<i16>,
    total_len: usize,
    mut iter_provider: P,
    chunk: usize,
    values: &mut Vec<T>,
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
        let take = (total - processed).min(chunk);
        def_levels.clear();
        values.clear();
        values.reserve(take);
        for maybe_bits in iter_provider(processed, take) {
            if let Some(bits) = maybe_bits {
                def_levels.push(1);
                let value = map_value(bits)?;
                values.push(value);
            } else {
                def_levels.push(0);
            }
        }
        write_chunk(values, def_levels).map_err(Error::from)?;
        processed += take;
    }
    Ok(())
}
