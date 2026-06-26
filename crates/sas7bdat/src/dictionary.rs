//! Optional string-column dictionary encoding (feature = `dictionary`).
//!
//! SAS character columns are fixed-width and often low-cardinality (category
//! codes), which makes them ideal for dictionary encoding: map each distinct
//! value to a small `u32` and store per-row codes. That feeds an Arrow
//! `DictionaryArray<u32>` → Polars `Categorical` / R `factor`, cutting memory and
//! speeding up group-by / join.
//!
//! The risk is free-text / ID columns (high cardinality), where building a
//! dictionary just wastes memory. So a cheap [`CardinalityEstimator`] probe over
//! a stride sample vetoes dictionary encoding for those columns before we commit
//! to interning ([`dictionary_encode`] returns `None` → caller keeps plain Utf8).
//!
//! Inputs are the core's already-decoded `Utf8` buffers — strings are UTF-8 and
//! right-trimmed (`TrimMode::RTrim`), so no re-normalization happens here.

use crate::columnar::OwnedColumnBuffer;
use cardinality_estimator::CardinalityEstimator;
use lasso2::{Key, Rodeo};

/// Thresholds for the Dictionary-vs-Plain decision.
#[derive(Debug, Clone, Copy)]
pub struct DictionaryPolicy {
    /// Max cells fed to the estimator in the probe pass.
    pub sample_rows: usize,
    /// Sampling stride. `0` derives it as `row_count / sample_rows`.
    pub stride: usize,
    /// distinct/sampled above this => high-cardinality (Plain).
    pub max_card_ratio: f64,
    /// Absolute distinct cap; at/above this => Plain regardless of ratio.
    pub max_abs_cardinality: usize,
}

impl Default for DictionaryPolicy {
    fn default() -> Self {
        Self {
            sample_rows: 8_192,
            stride: 0,
            max_card_ratio: 0.5,
            max_abs_cardinality: 1 << 20, // ~1M distinct
        }
    }
}

/// A dictionary-encoded string column: distinct values in key order (index ==
/// physical id) plus a per-row code (`None` == null).
#[derive(Debug, Clone)]
pub struct DictionaryColumn {
    pub dictionary: Vec<String>,
    pub codes: Vec<Option<u32>>,
}

impl DictionaryColumn {
    /// Approximate heap footprint (codes + vocabulary), for benchmarking.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        let vocab: usize = self.dictionary.iter().map(String::len).sum();
        self.codes.len() * std::mem::size_of::<u32>() // codes (Arrow uses a u32 buffer)
            + self.codes.len() / 8                     // validity bitmap
            + vocab                                    // dictionary data
            + (self.dictionary.len() + 1) * std::mem::size_of::<i32>() // dict offsets
    }
}

#[inline]
fn is_valid(valid: Option<&[u64]>, i: usize) -> bool {
    valid.is_none_or(|b| (b[i / 64] >> (i % 64)) & 1 == 1)
}

/// Number of rows in a `Utf8`/`RawBytes` buffer (0 for other variants).
fn buffer_rows(buffer: &OwnedColumnBuffer) -> usize {
    match buffer {
        OwnedColumnBuffer::Utf8 { offsets, .. } | OwnedColumnBuffer::RawBytes { offsets, .. } => {
            offsets.as_slice().len().saturating_sub(1)
        }
        _ => 0,
    }
}

/// Trusted-offset (non-negative) to `usize`.
#[inline]
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
const fn usz(offset: i64) -> usize {
    offset as usize
}

/// Return the cell at local row `i` of a `Utf8`/`RawBytes` buffer (`None` = null
/// or non-string / out-of-range buffer).
fn cell_in_buffer(buffer: &OwnedColumnBuffer, i: usize) -> Option<&str> {
    let (OwnedColumnBuffer::Utf8 {
        offsets,
        data,
        valid,
        ..
    }
    | OwnedColumnBuffer::RawBytes {
        offsets,
        data,
        valid,
    }) = buffer
    else {
        return None;
    };
    if !is_valid(valid.as_deref(), i) {
        return None;
    }
    let offs = offsets.as_slice();
    std::str::from_utf8(&data[usz(offs[i])..usz(offs[i + 1])]).ok()
}

/// Seek the cell at global row `gidx` across a column's batch buffers (`None` =
/// null or out of range; callers only seek in-range rows).
fn cell_at<'a>(
    buffers: &[&'a OwnedColumnBuffer],
    buf_rows: &[usize],
    gidx: usize,
) -> Option<&'a str> {
    let mut acc = 0usize;
    for (buffer, &rows) in buffers.iter().zip(buf_rows) {
        if gidx < acc + rows {
            return cell_in_buffer(buffer, gidx - acc);
        }
        acc += rows;
    }
    None
}

/// Decide and dictionary-encode a string column spanning one or more batch buffers.
///
/// Returns `Some` if the column is low-cardinality enough to dictionary encode,
/// `None` if the caller should keep the plain `Utf8` representation.
#[must_use]
pub fn dictionary_encode(
    buffers: &[&OwnedColumnBuffer],
    policy: &DictionaryPolicy,
) -> Option<DictionaryColumn> {
    let rows: usize = buffers.iter().copied().map(buffer_rows).sum();
    if rows == 0 {
        return Some(DictionaryColumn {
            dictionary: Vec::new(),
            codes: Vec::new(),
        });
    }

    // ── Pass 1: probe cardinality on a stride sample ────────────────────────
    // Seek directly to the sampled rows (O(sample_rows)) instead of walking every
    // cell — important for wide files where a full extra pass is costly.
    let stride = if policy.stride > 0 {
        policy.stride
    } else {
        (rows / policy.sample_rows.max(1)).max(1)
    };
    let buf_rows: Vec<usize> = buffers.iter().copied().map(buffer_rows).collect();
    let mut est = CardinalityEstimator::<str>::new();
    let mut sampled = 0usize;
    let mut gidx = 0usize;
    while sampled < policy.sample_rows && gidx < rows {
        if let Some(s) = cell_at(buffers, &buf_rows, gidx) {
            est.insert(s);
        }
        sampled += 1;
        gidx += stride;
    }
    if sampled > 0 {
        let estimate = est.estimate();
        #[allow(clippy::cast_precision_loss)]
        let ratio = estimate as f64 / sampled as f64;
        if ratio > policy.max_card_ratio || estimate >= policy.max_abs_cardinality {
            return None; // high cardinality → keep plain Utf8
        }
    }

    // ── Pass 2: build the dense dictionary ──────────────────────────────────
    let mut builder = DictBuilder::with_capacity(rows);
    for buffer in buffers {
        builder.push_buffer(buffer);
    }
    Some(builder.finish())
}

/// Index in the byte table reserved for the empty string (len 0).
const EMPTY_KEY: usize = 256;

/// Incremental dictionary builder. Interns one batch's cells at a time, so the
/// dictionary is produced *as the scan decodes* (cells cache-hot).
///
/// SAS character columns are often 1 byte wide (single-char codes). Hashing a
/// single byte is wasteful, so the builder starts in a **byte-direct** mode that
/// maps each distinct ≤1-byte value to an id through a 257-entry table with no
/// hashing at all. The first time a value longer than one byte appears it
/// promotes to a `lasso2` hash interner (migrating the existing ids so codes stay
/// stable). For the common all-single-byte column it never hashes.
pub struct DictBuilder {
    /// Byte-direct mode: id per byte value (0..256) + the empty string (256).
    /// `-1` == unseen. Active until a multi-byte value forces promotion.
    byte_ids: Box<[i32; 257]>,
    byte_mode: bool,
    rodeo: Rodeo<lasso2::Spur, ahash::RandomState>,
    dictionary: Vec<String>,
    codes: Vec<Option<u32>>,
}

impl Default for DictBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DictBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    #[must_use]
    pub fn with_capacity(rows: usize) -> Self {
        Self {
            byte_ids: Box::new([-1; 257]),
            byte_mode: true,
            rodeo: Rodeo::with_hasher(ahash::RandomState::new()),
            dictionary: Vec::new(),
            codes: Vec::with_capacity(rows),
        }
    }

    /// Leave byte-direct mode: re-intern the existing dictionary into the hash
    /// interner in id order so codes stay stable, then hash from here on.
    #[cold]
    fn promote(&mut self) {
        for s in &self.dictionary {
            self.rodeo.get_or_intern(s);
        }
        self.byte_mode = false;
    }

    #[inline]
    fn intern(&mut self, s: &str) -> u32 {
        if self.byte_mode {
            let key = match s.as_bytes() {
                [] => EMPTY_KEY,
                [b] => *b as usize,
                _ => {
                    self.promote();
                    return self.intern_hashed(s);
                }
            };
            let mut id = self.byte_ids[key];
            if id < 0 {
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let new_id = self.dictionary.len() as i32;
                self.byte_ids[key] = new_id;
                self.dictionary.push(s.to_owned());
                id = new_id;
            }
            #[allow(clippy::cast_sign_loss)]
            return id as u32;
        }
        self.intern_hashed(s)
    }

    #[inline]
    fn intern_hashed(&mut self, s: &str) -> u32 {
        let id = self.rodeo.get_or_intern(s).into_usize();
        if id == self.dictionary.len() {
            self.dictionary.push(s.to_owned());
        }
        #[allow(clippy::cast_possible_truncation)]
        {
            id as u32
        }
    }

    /// Intern every cell of one batch buffer for this column.
    pub fn push_buffer(&mut self, buffer: &OwnedColumnBuffer) {
        let (OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
            ..
        }
        | OwnedColumnBuffer::RawBytes {
            offsets,
            data,
            valid,
        }) = buffer
        else {
            return;
        };
        let offs = offsets.as_slice();
        let valid = valid.as_deref();
        for i in 0..offs.len().saturating_sub(1) {
            if is_valid(valid, i) {
                let bytes = &data[usz(offs[i])..usz(offs[i + 1])];
                let code = match std::str::from_utf8(bytes) {
                    Ok(s) => self.intern(s),
                    Err(_) => self.intern(String::from_utf8_lossy(bytes).as_ref()),
                };
                self.codes.push(Some(code));
            } else {
                self.codes.push(None);
            }
        }
    }

    /// Distinct values interned so far.
    #[must_use]
    pub const fn cardinality(&self) -> usize {
        self.dictionary.len()
    }

    #[must_use]
    pub fn finish(self) -> DictionaryColumn {
        DictionaryColumn {
            dictionary: self.dictionary,
            codes: self.codes,
        }
    }
}

/// Drive a scan and build a per-column dictionary for every string column.
///
/// Interns each batch *while the file decodes* (cache-hot). Returns one entry per
/// column: `Some` for string columns, `None` otherwise.
///
/// This measures the true added cost of dictionary-encoding during decode. It
/// interns every string column (no cardinality veto) so the cost is the upper
/// bound; a production path would apply [`DictionaryPolicy`] and could reuse the
/// decode-time interner's existing per-cell hash.
///
/// # Errors
/// Propagates scan/decode errors.
pub fn read_dictionary_columns(
    ds: &crate::Dataset,
) -> crate::Result<Vec<Option<DictionaryColumn>>> {
    let ncols = ds.columns().len();
    let mut builders: Vec<Option<DictBuilder>> = (0..ncols).map(|_| None).collect();
    ds.scan().visit_owned_batches(|batch| {
        for (ci, buffer) in batch.columns.iter().enumerate() {
            if matches!(buffer, OwnedColumnBuffer::Utf8 { .. }) {
                builders[ci]
                    .get_or_insert_with(DictBuilder::new)
                    .push_buffer(buffer);
            }
        }
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
    Ok(builders
        .into_iter()
        .map(|b| b.map(DictBuilder::finish))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::TrustedOffsets;

    fn utf8(cells: &[Option<&str>]) -> OwnedColumnBuffer {
        let mut offsets = TrustedOffsets::with_capacity_for_rows(cells.len());
        let mut data = Vec::new();
        let mut valid = vec![0u64; cells.len().div_ceil(64)];
        for (i, c) in cells.iter().enumerate() {
            if let Some(s) = c {
                data.extend_from_slice(s.as_bytes());
                valid[i / 64] |= 1 << (i % 64);
            }
            offsets.push_current_data_len(data.len()).unwrap();
        }
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid: Some(valid),
            dictionary_ids: None,
        }
    }

    /// Dictionary codes must reconstruct exactly the input strings (incl. nulls).
    #[test]
    fn dictionary_round_trips() {
        let buf = utf8(&[Some("M"), Some("K"), None, Some("M"), Some("M"), Some("K")]);
        let dict = dictionary_encode(&[&buf], &DictionaryPolicy::default()).expect("low card");
        assert_eq!(dict.dictionary, vec!["M".to_string(), "K".to_string()]);
        let rebuilt: Vec<Option<&str>> = dict
            .codes
            .iter()
            .map(|c| c.map(|id| dict.dictionary[id as usize].as_str()))
            .collect();
        assert_eq!(
            rebuilt,
            vec![Some("M"), Some("K"), None, Some("M"), Some("M"), Some("K")]
        );
    }

    /// A multi-byte value after single-byte values must promote the byte-direct
    /// builder to the hash interner while keeping codes stable.
    #[test]
    fn byte_mode_promotes_on_multibyte() {
        let buf = utf8(&[
            Some("A"),
            Some("B"),
            Some("A"),
            Some("long"),
            Some("B"),
            Some("long"),
        ]);
        let mut b = DictBuilder::new();
        b.push_buffer(&buf);
        let dict = b.finish();
        assert_eq!(
            dict.dictionary,
            vec!["A".to_string(), "B".to_string(), "long".to_string()]
        );
        assert_eq!(
            dict.codes,
            vec![Some(0), Some(1), Some(0), Some(2), Some(1), Some(2)]
        );
    }

    #[test]
    fn high_cardinality_is_vetoed() {
        let owned: Vec<String> = (0..2000).map(|i| format!("id{i}")).collect();
        let cells: Vec<Option<&str>> = owned.iter().map(|s| Some(s.as_str())).collect();
        let buf = utf8(&cells);
        let policy = DictionaryPolicy {
            sample_rows: 2000,
            ..Default::default()
        };
        assert!(dictionary_encode(&[&buf], &policy).is_none());
    }
}
