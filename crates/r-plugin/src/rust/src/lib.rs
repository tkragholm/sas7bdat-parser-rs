//! R binding for the sas7bdat parser (extendr).
//!
//! Strategy (see ../../../../docs/r-bindings/design-direct-fill.md): the row
//! count is known from metadata up front, so every column's R vector is
//! allocated at its final length and each decoded batch is written straight into
//! its slice — one write per cell, no staging buffer between the two.
//!
//! The core decodes batches across all cores and delivers them to one visitor on
//! this thread, which is what makes the R object writes (string interning
//! included) sound. Nothing accumulates: each batch is placed by its global
//! `row_base` and dropped. The one exception is `categorical`, where building an
//! R factor needs a dictionary over the whole column, so those columns' buffers
//! are moved aside as batches go by.
//!
//! `haven`-parity: SAS numerics -> double, character -> UTF-8 character, dates ->
//! `Date`, datetimes -> `POSIXct` (UTC), times -> `hms`. SAS special missings
//! (`.A`-`.Z`, `._`) -> haven `tagged_na`. Variable labels -> `label` attribute;
//! value-label catalogs -> `haven_labelled`.

use extendr_api::prelude::*;
use sas7bdat::dictionary::{dictionary_encode, DictionaryColumn, DictionaryPolicy};
use sas7bdat::{
    catalog::normalize_format_name, Dataset, IoBackendPreference, LabelSet, LogicalType,
    OpenOptions, OwnedColumnBuffer, Parallelism, SasDate, SasDateTime, TemporalDecodeOptions,
    ValueKey, ValueType,
};
use std::path::{Path, PathBuf};

/// Which R class an assembled double column carries. The epoch shift is applied
/// to the values during the fill; this governs the class/attributes attached at
/// the end.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RealClass {
    Plain,
    Date,
    DateTime,
    Time,
}

impl RealClass {
    /// SAS-epoch -> R-epoch shift applied to the raw SAS value (days for `Date`,
    /// seconds for `DateTime`). `Time` is seconds-since-midnight (no shift).
    ///
    /// Taken from the core's constants rather than spelled out here: R shares the Unix
    /// epoch with Arrow, so this is the same 1960 -> 1970 offset the Arrow and Polars
    /// conversions apply, and a literal copy would be a second place for it to drift.
    fn epoch_shift(self) -> f64 {
        match self {
            RealClass::Date => f64::from(SasDate::DAYS_SAS_TO_UNIX),
            #[allow(clippy::cast_precision_loss)]
            RealClass::DateTime => SasDateTime::SECONDS_SAS_TO_UNIX as f64,
            RealClass::Time | RealClass::Plain => 0.0,
        }
    }
}

/// The R column shape, derived from the SAS *logical type* — the semantic truth.
/// We key off `logical_type`, not the `OwnedColumnBuffer` variant: the core emits
/// a typed temporal buffer for whole-unit values but falls back to `F64` when a
/// temporal column carries fractional seconds (its `SasDateTime` is integer-only).
/// Driving the R type from `logical_type` keeps such columns as `POSIXct`/`hms`.
#[derive(Clone, Copy)]
enum ColClass {
    Real(RealClass),
    Text,
}

const fn col_class(logical: LogicalType) -> ColClass {
    match logical {
        LogicalType::Date => ColClass::Real(RealClass::Date),
        LogicalType::DateTime => ColClass::Real(RealClass::DateTime),
        LogicalType::Time => ColClass::Real(RealClass::Time),
        LogicalType::Integer | LogicalType::Float => ColClass::Real(RealClass::Plain),
        LogicalType::String | LogicalType::Bytes => ColClass::Text,
    }
}

/// Per-column metadata resolved up front: variable label and value-label set.
struct ColMeta {
    class: ColClass,
    var_label: Option<String>,
    /// The SAS format as haven's `format.sas` attribute spells it: name and width,
    /// `DATE9`, `$A`, `DATETIME23.3`.
    format_sas: Option<String>,
    value_labels: Option<LabelSet>,
}

// ── tagged-NA encoding ────────────────────────────────────────────────────────

/// R's `NA_real_` bit pattern. haven's `tagged_na(c)` is this with the tag char
/// in byte 4 (bits 32-39).
const R_NA_REAL_BITS: u64 = 0x7FF0_0000_0000_07A2;

fn haven_tagged_na(tag: u8) -> f64 {
    f64::from_bits(R_NA_REAL_BITS | (u64::from(tag) << 32))
}

/// Indicator byte -> haven tag character; `0` means "plain `.`, no tag".
///
/// SAS spells a special missing as a NaN whose byte 5 (bits 47:40) says which of
/// `.`, `._` or `.A`-`.Z` it is. Two spellings of that byte occur in the wild:
///
/// * **Ordinal** — `0xFF - n` over the sequence `_ . A .. Z`: `0xFF` is `._`,
///   `0xFE` is `.`, and `0xFD..=0xE4` are `.A..=.Z`.
/// * **Complement** — the one's complement of the character's ASCII code: `.` is
///   `0xD1`, `_` is `0xA0`, and `.A..=.Z` are `0xBE..=0xA5`.
///
/// Both spellings are decoded. Complement-spelled tags occur in real files: a census
/// of the fixture corpus found 6.3M such cells across nine files, PIAAC's
/// `.V`/`.N`/`.D`/`.R` scheme among them.
///
/// One table decodes both because the ranges cannot collide: the complement
/// family tops out at `0xD1`, below the ordinal family's `0xE4` floor. The
/// `const` builder asserts that, so an overlap would fail the build rather than
/// silently resolve one way.
const MISSING_TAG: [u8; 256] = build_missing_tag_table();

const fn build_missing_tag_table() -> [u8; 256] {
    let mut table = [0u8; 256];

    // Ordinal spelling. `0xFE` (plain `.`) is left 0 deliberately.
    table[0xFF] = b'_';
    let mut i = 0usize;
    while i < 26 {
        assert!(table[0xFD - i] == 0, "special-missing spellings overlap");
        table[0xFD - i] = b'a' + i as u8;
        i += 1;
    }

    // Complement spelling. `!'.'` == `0xD1` (plain `.`) is left 0 deliberately.
    assert!(table[0xFF - b'_' as usize] == 0, "special-missing spellings overlap");
    table[0xFF - b'_' as usize] = b'_';
    let mut c = 0u8;
    while c < 26 {
        let slot = 0xFF - (b'A' + c) as usize;
        assert!(table[slot] == 0, "special-missing spellings overlap");
        table[slot] = b'a' + c;
        c += 1;
    }

    table
}

/// Recover a SAS special-missing tag from a preserved missing cell's raw bits.
///
/// Only ever called for cells the parser already flagged missing, so the NaN test
/// here is a sanity check rather than the thing that decides missingness. It is a
/// generic "exponent all ones, mantissa non-zero" test, which also accepts the
/// quiet-NaN spelling (`0x7FF8_FF..`) that some writers use.
fn sas_special_missing_tag(bits: u64) -> Option<u8> {
    const EXPONENT: u64 = 0x7FF0_0000_0000_0000;
    const MANTISSA: u64 = 0x000F_FFFF_FFFF_FFFF;
    if bits & EXPONENT != EXPONENT || bits & MANTISSA == 0 {
        return None;
    }
    match MISSING_TAG[((bits >> 40) & 0xFF) as usize] {
        0 => None,
        tag => Some(tag),
    }
}

/// Map a catalog value-label tag key to its R numeric code (uppercase `A`-`Z` /
/// `_` from the core's catalog decode -> haven's lowercase tagged_na; `.` -> NA).
fn tagged_key_to_na(tag: char) -> Option<f64> {
    match tag {
        'A'..='Z' => Some(haven_tagged_na(tag.to_ascii_lowercase() as u8)),
        'a'..='z' => Some(haven_tagged_na(tag as u8)),
        '_' => Some(haven_tagged_na(b'_')),
        '.' => Some(f64::from_bits(R_NA_REAL_BITS)),
        _ => None,
    }
}

// ── in-place fills ────────────────────────────────────────────────────────────

/// `valid` bitmap: bit `i` set => row `i` present. `None` => all present.
#[inline]
fn is_valid(valid: Option<&[u64]>, i: usize) -> bool {
    match valid {
        None => true,
        Some(bits) => (bits[i / 64] >> (i % 64)) & 1 == 1,
    }
}

/// Fill `dst` (one batch's row range of a numeric/temporal column) from `buffer`.
/// Present cells get the epoch-shifted value; missing cells get the recovered
/// haven `tagged_na`, or plain `NA` when there is no tag to recover.
///
/// Tag recovery is not restricted to non-temporal columns: `haven` tags a missing
/// datetime just as it tags a missing number. It is limited to the `F64` arm below
/// because the typed `Date`/`DateTime`/`Time` buffers have already discarded the NaN
/// payload the tag lives in, so there is nothing left to recover from them.
fn fill_real_slice(dst: &mut [f64], class: RealClass, buffer: &OwnedColumnBuffer) {
    let shift = class.epoch_shift();
    let na = f64::from_bits(R_NA_REAL_BITS);

    macro_rules! fill {
        ($values:expr, $valid:expr, $map:expr) => {{
            let valid = $valid.as_deref();
            for (i, v) in $values.iter().enumerate() {
                dst[i] = if is_valid(valid, i) { $map(*v) } else { na };
            }
        }};
    }

    match buffer {
        OwnedColumnBuffer::F64 { values, valid } => {
            let valid = valid.as_deref();
            for (i, &v) in values.iter().enumerate() {
                dst[i] = if is_valid(valid, i) {
                    v - shift
                } else {
                    sas_special_missing_tag(v.to_bits()).map_or(na, haven_tagged_na)
                };
            }
        }
        #[allow(clippy::cast_precision_loss)]
        OwnedColumnBuffer::I64 { values, valid } => fill!(values, valid, |v: i64| v as f64 - shift),
        OwnedColumnBuffer::I32 { values, valid } => {
            fill!(values, valid, |v: i32| f64::from(v) - shift)
        }
        OwnedColumnBuffer::Date { values, valid } => {
            fill!(values, valid, |d: sas7bdat::SasDate| f64::from(d.days_since_sas_epoch) - shift)
        }
        #[allow(clippy::cast_precision_loss)]
        OwnedColumnBuffer::DateTime { values, valid } => {
            fill!(values, valid, |dt: sas7bdat::SasDateTime| dt.seconds_since_sas_epoch as f64 - shift)
        }
        OwnedColumnBuffer::Time { values, valid } => {
            fill!(values, valid, |t: sas7bdat::SasTime| f64::from(t.seconds_since_midnight) - shift)
        }
        // A numeric/temporal logical type never yields a string buffer.
        OwnedColumnBuffer::Utf8 { .. } | OwnedColumnBuffer::RawBytes { .. } => {}
    }
}

/// The part of `bytes` before the first NUL.
///
/// An R string cannot hold NUL, and making one from bytes that contain it raises an R
/// error, which would unwind through Rust frames. Text is therefore cut at the first NUL,
/// as it would be if read as a C string. SAS pads with spaces, so this only touches text
/// that holds a NUL in the middle.
fn before_nul(bytes: &[u8]) -> &[u8] {
    bytes.iter().position(|&b| b == 0).map_or(bytes, |end| &bytes[..end])
}

/// [`before_nul`] for metadata text: names, labels and value-label keys.
fn before_nul_str(text: &str) -> &str {
    text.find('\0').map_or(text, |end| &text[..end])
}

/// Intern one batch's strings into a STRSXP (`sexp`) starting at row `base`,
/// deduplicating distinct values via a per-column `dict` of cached CHARSXPs.
///
/// A wide SAS file can have thousands of low-cardinality string columns (millions
/// of cells). The dict caps `Rf_mkCharLenCE` at the column's cardinality, and we
/// use raw `SET_STRING_ELT` to skip extendr's per-call thread-guard + bounds
/// check. CHARSXPs created here live in R's global string cache, so dict entries
/// stay valid; `sexp` is kept alive by its owning `Robj` in the caller.
///
/// An empty cell is not written: R allocates a character vector with every element
/// already `""`, and in survey extracts most cells are empty (70% of the AHS file's).
///
/// # Safety
/// `sexp` is a live STRSXP of length >= `base + batch_rows`, on the main thread, and
/// its rows from `base` on have not been written since R allocated it.
unsafe fn fill_text_sexp(
    sexp: extendr_ffi::SEXP,
    base: usize,
    buffer: &OwnedColumnBuffer,
    dict: &mut TextCache,
) {
    let (offsets, data, valid) = match buffer {
        OwnedColumnBuffer::Utf8 { offsets, data, valid, .. } => (offsets, data, valid),
        OwnedColumnBuffer::RawBytes { offsets, data, valid } => (offsets, data, valid),
        _ => return,
    };
    let offs = offsets.as_slice();
    let valid = valid.as_deref();
    // Survey and register columns often repeat a value down consecutive rows, so a cell
    // equal to the one before it reuses that CHARSXP without a lookup. Short cells are
    // compared by their packed key, which avoids a `memcmp` call per cell.
    let mut previous: Option<(u64, extendr_ffi::SEXP)> = None;
    for i in 0..offs.len().saturating_sub(1) {
        let idx = (base + i) as isize;
        if is_valid(valid, i) {
            let bytes = before_nul(&data[offs[i] as usize..offs[i + 1] as usize]);
            if bytes.is_empty() {
                continue;
            }
            let charsxp = match short_key(bytes) {
                Some(key) => match previous {
                    Some((last, cs)) if last == key => cs,
                    _ => {
                        let cs = dict.short_charsxp(key, bytes);
                        previous = Some((key, cs));
                        cs
                    }
                },
                None => dict.long_charsxp(bytes),
            };
            extendr_ffi::SET_STRING_ELT(sexp, idx, charsxp);
        } else {
            extendr_ffi::SET_STRING_ELT(sexp, idx, extendr_ffi::R_NaString);
        }
    }
}

/// One string column's CHARSXPs, one per distinct value, kept across batches.
///
/// A register extract has millions of string cells over few distinct values, so this
/// caps `Rf_mkCharLenCE` at the column's cardinality. The lookup happens once per cell on
/// the main thread, which makes it the serial part of a read. A cell of up to 7 bytes is
/// keyed by its bytes and length packed into one `u64`, and both maps hash with aHash
/// rather than SipHash: it is several times faster on short keys, and it is still keyed
/// at random, so a crafted file cannot choose values that collide.
///
/// The CHARSXPs live in R's global string cache and each is stored in the column's
/// STRSXP as soon as it is made, so the vector keeps it alive while it is cached here.
#[derive(Default)]
struct TextCache {
    short: ahash::AHashMap<u64, extendr_ffi::SEXP>,
    long: ahash::AHashMap<Box<[u8]>, extendr_ffi::SEXP>,
}

/// A cell of up to 7 bytes as one `u64`: the bytes, then the length in the top byte, so
/// that cells differing only in trailing zero bytes or in length get different keys.
fn short_key(bytes: &[u8]) -> Option<u64> {
    if bytes.len() >= 8 {
        return None;
    }
    let mut key = [0u8; 8];
    key[..bytes.len()].copy_from_slice(bytes);
    #[allow(clippy::cast_possible_truncation)]
    {
        key[7] = bytes.len() as u8;
    }
    Some(u64::from_le_bytes(key))
}

impl TextCache {
    /// The CHARSXP for a short cell, whose [`short_key`] is `key`, made on first use.
    ///
    /// # Safety
    /// On R's main thread. `bytes` holds no NUL.
    unsafe fn short_charsxp(&mut self, key: u64, bytes: &[u8]) -> extendr_ffi::SEXP {
        *self.short.entry(key).or_insert_with(|| make_charsxp(bytes))
    }

    /// The CHARSXP for a cell of 8 bytes or more, made on first use.
    ///
    /// # Safety
    /// On R's main thread. `bytes` holds no NUL.
    unsafe fn long_charsxp(&mut self, bytes: &[u8]) -> extendr_ffi::SEXP {
        if let Some(&cs) = self.long.get(bytes) {
            cs
        } else {
            let cs = make_charsxp(bytes);
            self.long.insert(bytes.into(), cs);
            cs
        }
    }
}

/// A UTF-8 CHARSXP for `bytes`.
///
/// # Safety
/// On R's main thread. `bytes` holds no NUL.
unsafe fn make_charsxp(bytes: &[u8]) -> extendr_ffi::SEXP {
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let len = bytes.len() as std::os::raw::c_int;
    extendr_ffi::Rf_mkCharLenCE(
        bytes.as_ptr().cast::<std::os::raw::c_char>(),
        len,
        extendr_ffi::cetype_t::CE_UTF8,
    )
}

/// Rows in a string buffer. Offsets are one longer than the cell count, so an
/// empty buffer (no offsets at all) is zero rows rather than an underflow.
fn buffer_rows(buffer: &OwnedColumnBuffer) -> usize {
    match buffer {
        OwnedColumnBuffer::Utf8 { offsets, .. } | OwnedColumnBuffer::RawBytes { offsets, .. } => {
            offsets.as_slice().len().saturating_sub(1)
        }
        _ => 0,
    }
}

/// Build an R `factor` from a dictionary-encoded column: integer codes (1-based,
/// NA = `NA_integer_`) plus a `levels` character vector — avoiding the per-cell
/// CHARSXP interning of the `character` path entirely.
fn factor_from_dict(dict: DictionaryColumn, nrow: usize) -> Robj {
    // Codes: INTSXP, 1-based level index; null -> NA_integer_ (i32::MIN).
    let mut col: Robj = Integers::new(nrow).into();
    {
        let slice = col.as_integer_slice_mut().expect("INTSXP slice");
        for (dst, code) in slice.iter_mut().zip(dict.codes.iter()) {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            {
                *dst = code.map_or(i32::MIN, |c| (c + 1) as i32);
            }
        }
    }

    // Levels: UTF-8 character vector of the distinct values (cardinality-sized).
    let levels: Robj = Strings::new(dict.dictionary.len()).into();
    let lsexp = unsafe { levels.get() };
    for (i, s) in dict.dictionary.iter().enumerate() {
        unsafe {
            let cs = extendr_ffi::Rf_mkCharLenCE(
                s.as_ptr().cast::<std::os::raw::c_char>(),
                s.len() as std::os::raw::c_int,
                extendr_ffi::cetype_t::CE_UTF8,
            );
            extendr_ffi::SET_STRING_ELT(lsexp, i as isize, cs);
        }
    }
    col.set_attrib("levels", levels).unwrap();
    col.set_class(&["factor"]).unwrap();
    col
}

// ── value-label vectors ───────────────────────────────────────────────────────

/// Named numeric `labels` vector (names = label text, values = codes). Includes
/// tagged-missing keys as haven `tagged_na` entries. `None` if no numeric keys.
fn numeric_labels_robj(ls: &LabelSet) -> Option<Robj> {
    let mut codes: Vec<f64> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for vl in &ls.labels {
        let code = match vl.key {
            ValueKey::Numeric(v) => v,
            ValueKey::Integer(v) => f64::from(v),
            ValueKey::Tagged(tag) => match tagged_key_to_na(tag) {
                Some(code) => code,
                None => continue,
            },
            ValueKey::String(_) => continue,
        };
        codes.push(code);
        names.push(before_nul_str(&vl.label).to_owned());
    }
    if codes.is_empty() {
        return None;
    }
    let mut lab: Robj = Doubles::from_values(codes).into();
    lab.set_names(names.iter().map(String::as_str)).unwrap();
    Some(lab)
}

/// Named character `labels` vector from a string value-label set.
fn string_labels_robj(ls: &LabelSet) -> Option<Robj> {
    let mut codes: Vec<String> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for vl in &ls.labels {
        if let ValueKey::String(s) = &vl.key {
            codes.push(before_nul_str(s).to_owned());
            names.push(before_nul_str(&vl.label).to_owned());
        }
    }
    if codes.is_empty() {
        return None;
    }
    let mut s = Strings::new(codes.len());
    for (i, code) in codes.into_iter().enumerate() {
        s.set_elt(i, Rstr::from(code));
    }
    let mut lab: Robj = s.into();
    lab.set_names(names.iter().map(String::as_str)).unwrap();
    Some(lab)
}

// ── finalize ──────────────────────────────────────────────────────────────────

/// Attach class/attributes (temporal class, `haven_labelled`, variable label) to
/// a filled column.
fn finalize_column(mut col: Robj, meta: &ColMeta) -> Robj {
    match meta.class {
        ColClass::Real(RealClass::Plain) => {
            if let Some(ls) = &meta.value_labels {
                if ls.value_type == ValueType::Numeric {
                    if let Some(labels) = numeric_labels_robj(ls) {
                        col.set_attrib("labels", labels).unwrap();
                        col.set_class(&["haven_labelled", "vctrs_vctr", "double"]).unwrap();
                    }
                }
            }
        }
        ColClass::Real(RealClass::Date) => {
            col.set_class(&["Date"]).unwrap();
        }
        ColClass::Real(RealClass::DateTime) => {
            col.set_class(&["POSIXct", "POSIXt"]).unwrap();
            col.set_attrib("tzone", "UTC").unwrap();
        }
        ColClass::Real(RealClass::Time) => {
            col.set_class(&["hms", "difftime"]).unwrap();
            col.set_attrib("units", "secs").unwrap();
        }
        ColClass::Text => {
            if let Some(ls) = &meta.value_labels {
                if ls.value_type == ValueType::String {
                    if let Some(labels) = string_labels_robj(ls) {
                        col.set_attrib("labels", labels).unwrap();
                        col.set_class(&["haven_labelled", "vctrs_vctr", "character"]).unwrap();
                    }
                }
            }
        }
    }
    set_column_attributes(&mut col, meta);
    col
}

/// The attributes haven gives every column: the variable label and the SAS format.
fn set_column_attributes(col: &mut Robj, meta: &ColMeta) {
    if let Some(label) = &meta.var_label {
        col.set_attrib("label", label.as_str()).unwrap();
    }
    if let Some(format) = &meta.format_sas {
        col.set_attrib("format.sas", format.as_str()).unwrap();
    }
}

/// Assemble a bare `data.frame` from named columns.
/// The columns of a read that returns no rows, each empty with its class and labels. A
/// column that would have been a factor is a character vector here, as it is when the
/// dictionary is vetoed.
fn finish_empty_columns(cols: Vec<Option<Robj>>, metas: &[ColMeta]) -> Vec<Robj> {
    cols.into_iter()
        .zip(metas)
        .map(|(col, meta)| finalize_column(col.unwrap_or_else(|| Strings::new(0).into()), meta))
        .collect()
}

fn build_data_frame(names: &[String], cols: Vec<Robj>, nrow: usize) -> Robj {
    let mut df: Robj = List::from_values(cols).into();
    df.set_names(names.iter().map(String::as_str)).unwrap();

    // Compact row.names c(NA, -nrow), guarded against >2^31-row i32 overflow.
    if nrow <= i32::MAX as usize {
        let row_names = Integers::from_values([Rint::na(), Rint::from(-(nrow as i32))]);
        df.set_attrib("row.names", row_names).unwrap();
    } else {
        #[allow(clippy::cast_precision_loss)]
        let seq: Vec<f64> = (1..=nrow).map(|i| i as f64).collect();
        df.set_attrib("row.names", Doubles::from_values(seq)).unwrap();
    }
    df.set_class(&["data.frame"]).unwrap();
    df
}

fn resolve_catalog(path: &str, catalog: Option<&str>) -> Option<PathBuf> {
    if let Some(c) = catalog {
        return Some(PathBuf::from(c));
    }
    let sibling = Path::new(path).with_extension("sas7bcat");
    sibling.exists().then_some(sibling)
}

/// Which columns and rows to read, as `read_sas7bdat()` passes them.
struct Selection {
    /// Columns by name or by 0-based position, in output order; `None` for every column.
    columns: Option<ColumnChoice>,
    /// Rows to skip before reading, counting the rows a read returns.
    skip: u64,
    /// The most rows to read; `None` for no limit.
    n_max: Option<u64>,
    /// An encoding to read the text in instead of the one the file declares.
    encoding: Option<String>,
    /// Fail on the first cell that does not decode, rather than replace it.
    strict: bool,
}

enum ColumnChoice {
    Names(Vec<String>),
    Positions(Vec<usize>),
}

fn read_impl(
    path: &str,
    catalog: Option<&str>,
    categorical: bool,
    io_backend: &str,
    threads: Option<usize>,
    selection: &Selection,
) -> std::result::Result<Robj, String> {
    // `Auto` memory-maps local files and reads network shares sequentially, which is the
    // right default — but it can only tell the two apart on Windows, and even there a DFS
    // namespace or an unusual redirector can fool the probe. Overriding it is the whole
    // point of exposing this: mapping a file on a share turns every access into a
    // round-trip with no readahead.
    let backend: IoBackendPreference = io_backend
        .parse()
        .map_err(|e| format!("sas7bdat: io_backend: {e}"))?;
    let builder = OpenOptions::builder().io_backend(backend);
    let builder = match &selection.encoding {
        None => builder,
        Some(label) => builder
            .encoding(label)
            .map_err(|e| format!("sas7bdat: encoding: {e}"))?,
    };
    let options = builder.build();
    let mut ds =
        Dataset::open_with(path, options).map_err(|e| format!("sas7bdat: open `{path}`: {e}"))?;

    // A catalog the caller named must load. One found beside the file is used if it
    // loads, and otherwise reported as a warning, so the read is not silently unlabelled.
    let mut warning: Option<String> = None;
    if let Some(cat_path) = resolve_catalog(path, catalog) {
        match ds.attach_catalog(&cat_path) {
            Ok(()) => {}
            Err(e) if catalog.is_some() => {
                return Err(format!("sas7bdat: catalog `{}`: {e}", cat_path.display()));
            }
            Err(e) => {
                warning = Some(format!(
                    "the catalog beside the file, `{}`, could not be read, so no value labels \
                     were applied: {e}",
                    cat_path.display()
                ));
            }
        }
    }

    // The columns read, in output order: all of them, or the ones `col_select` named.
    let projection = match &selection.columns {
        None => None,
        Some(ColumnChoice::Names(names)) => Some(ds.projection().columns(names).build()),
        Some(ColumnChoice::Positions(positions)) => Some(
            positions
                .iter()
                .fold(ds.projection(), |builder, &idx| builder.column_idx(idx))
                .build(),
        ),
    }
    .transpose()
    .map_err(|e| format!("sas7bdat: col_select: {e}"))?;
    let chosen: Vec<&sas7bdat::ColumnMeta> = projection.as_ref().map_or_else(
        || ds.columns().iter().collect(),
        |projection| {
            projection
                .columns()
                .iter()
                .filter_map(|column| ds.columns().get(column.index))
                .collect()
        },
    );
    let names: Vec<String> = chosen
        .iter()
        .map(|c| before_nul_str(&c.name).to_owned())
        .collect();
    let metas: Vec<ColMeta> = {
        let label_sets = &ds.metadata().label_sets;
        chosen
            .iter()
            .map(|c| ColMeta {
                class: col_class(c.logical_type),
                var_label: c.label.as_deref().map(|label| before_nul_str(label).to_owned()),
                format_sas: c.format_spec().map(|spec| before_nul_str(&spec).to_owned()),
                value_labels: c
                    .format
                    .as_deref()
                    .map(normalize_format_name)
                    .and_then(|norm| label_sets.get(&norm).cloned()),
            })
            .collect()
    };
    // The header's row count includes rows SAS marked as deleted, which the scan skips.
    // `skip` and `n_max` count the rows a read returns, as the core's row selection does.
    let deleted = ds.metadata().deleted_row_count;
    let live = ds.metadata().row_count.saturating_sub(deleted);
    let first = selection.skip.min(live);
    let rows = selection
        .n_max
        .map_or(live - first, |n_max| n_max.min(live - first));
    let selected = first > 0 || rows < live;
    let nrow = usize::try_from(rows).map_err(|_| "sas7bdat: too many rows for R".to_owned())?;

    // Decode all batches across cores, then fill each column's R vector in place.
    //
    // Temporal decoding is turned off. The R column type comes from `logical_type`,
    // and the epoch shift is applied during the fill (see `RealClass::epoch_shift`),
    // so this binding does not need the core's typed `Date`/`DateTime`/`Time`
    // buffers. Those buffers convert the raw double to an integer count up front and
    // discard the NaN payload the SAS special-missing tag lives in, so a missing
    // datetime would reach R as a plain `NA` where `haven` gives `tagged_na`. The raw
    // `F64` keeps the payload and keeps sub-second values exact.
    //
    // `threads` is decode threads only. Read concurrency is capped separately by the
    // core (four in-flight reads), so raising it does not multiply requests against
    // a share. What it does scale is the memory held by in-flight batches.
    // `threads = NULL` defers to the core rather than taking every logical core.
    // Unconditional threads is wrong at the small end: measured across the corpus,
    // twelve of them decode `cars` 2.70x slower than one, and
    // `date_format_time_loop` 1.75x slower, because the spawn and the per-chunk setup
    // cost more than the work. `Parallelism::Auto` carries a gate calibrated on those
    // measurements, and a single worker runs the same chunk decode inline and still
    // reaches the tiled fill.
    let parallelism = threads.map_or(Parallelism::Auto, Parallelism::Threads);
    let temporal = TemporalDecodeOptions::builder()
        .decode_dates(false)
        .decode_datetimes(false)
        .decode_times(false)
        .build();
    // Every column's R vector is allocated at its final length up front, and each
    // batch is written straight into its slice and then dropped. Nothing
    // accumulates: peak memory is the finished R object plus the scan's own
    // bounded in-flight window, not the whole decoded file on top of both. On the
    // 2.15 GB / 4,041-column AHS fixture that is ~0.9 GB less and ~19% faster than
    // collecting first, the speed coming from touching each batch's bytes while
    // they are still cache-warm from decoding.
    //
    // `categorical` builds an R factor, which needs a dictionary over the whole
    // column, because a per-batch dictionary cannot number levels consistently.
    // Those columns are the one thing that cannot stream, so their buffers are moved
    // aside as batches go by (moved, not copied) and everything else still streams.
    let wants_factor: Vec<bool> = metas
        .iter()
        .map(|meta| {
            categorical && matches!(meta.class, ColClass::Text) && meta.value_labels.is_none()
        })
        .collect();
    let mut held: Vec<Vec<OwnedColumnBuffer>> = (0..metas.len()).map(|_| Vec::new()).collect();

    // A factor column gets an INTSXP of codes, not a STRSXP, so allocating one
    // here would be a whole extra vector's worth of allocate-and-discard per
    // column — 1.4 GB of it on the AHS fixture's 2,573 factor columns.
    //
    // Numeric columns are allocated before character ones. Allocating this much
    // makes R collect garbage several times, and each collection traces every
    // character vector already allocated, element by element; numeric vectors it
    // skips. Allocating the character vectors last means fewer collections see
    // them: for the AHS fixture's 2,574 character and 1,467 numeric columns this
    // takes 0.7 s in plain R, against 1.7 s with the character vectors first.
    let mut cols: Vec<Option<Robj>> = (0..metas.len()).map(|_| None).collect();
    for want_text in [false, true] {
        for (ci, meta) in metas.iter().enumerate() {
            if wants_factor[ci] || matches!(meta.class, ColClass::Text) != want_text {
                continue;
            }
            cols[ci] = Some(match meta.class {
                ColClass::Text => Strings::new(nrow).into(),
                ColClass::Real(_) => Doubles::new(nrow).into(),
            });
        }
    }

    // One CHARSXP cache per string column, alive across batches. A wide SAS file
    // has millions of string cells over few distinct values, so this caps
    // `Rf_mkCharLenCE` at each column's cardinality.
    let mut caches: Vec<TextCache> = (0..metas.len()).map(|_| TextCache::default()).collect();

    // Every batch is written at its own `row_base`, so the order batches arrive in does not
    // matter, and asking for none spares the core its reorder buffer. Two cases keep file
    // order and write each batch after the previous one instead. Factor columns collect
    // their buffers in arrival order to build one dictionary over the column. And in a file
    // with deleted rows, `row_base` counts the deleted rows before the batch, so it is not
    // the batch's position in the result. The callback runs on this thread either way.
    // A row selection also makes `row_base` count from the file's start, not the selection's.
    let positional = deleted == 0 && !selected && !wants_factor.iter().any(|w| *w);
    let ordering = if positional {
        sas7bdat::OrderingMode::Unordered
    } else {
        sas7bdat::OrderingMode::Stable
    };
    let mut next_row = 0usize;
    let mut scan = ds
        .scan()
        .with_parallelism(parallelism)
        .with_temporal_options(temporal)
        .with_ordering(ordering);
    if let Some(projection) = &projection {
        scan = scan.with_projection(projection);
    }
    if selected {
        scan = scan.select(sas7bdat::RowSelection::range(first, first + rows));
    }
    if selection.strict {
        scan = scan.with_string_options(
            sas7bdat::StringDecodeOptions::builder()
                .utf8_validation(sas7bdat::Utf8ValidationMode::Strict)
                .build(),
        );
    }
    if nrow == 0 {
        return Ok(with_warning(
            build_data_frame(&names, finish_empty_columns(cols, &metas), 0),
            warning,
        ));
    }
    scan.visit_owned_batches(|mut batch| {
            let row_count = batch.row_count;
            let base = if positional {
                usize::try_from(batch.row_base.0).unwrap_or(usize::MAX)
            } else {
                next_row
            };
            next_row = next_row.saturating_add(row_count);
            if base.checked_add(row_count).is_none_or(|end| end > nrow) {
                return Err(sas7bdat::Error::corruption(format!(
                    "the scan delivered more rows than the {nrow} the header declares"
                )));
            }
            for (ci, buffer) in std::mem::take(&mut batch.columns).into_iter().enumerate() {
                let Some(meta) = metas.get(ci) else { continue };
                if wants_factor[ci] {
                    held[ci].push(buffer);
                    continue;
                }
                let col = cols[ci].as_mut().expect("allocated above");
                match meta.class {
                    ColClass::Text => {
                        // SAFETY: `col` owns the STRSXP and keeps it alive; every
                        // write is on this (main) thread, since the visitor is called
                        // serially; `base + row_count <= nrow` was checked above; and
                        // each batch covers rows no other batch does.
                        let sexp = unsafe { col.get() };
                        unsafe { fill_text_sexp(sexp, base, &buffer, &mut caches[ci]) };
                    }
                    ColClass::Real(class) => {
                        let slice = col.as_real_slice_mut().expect("REALSXP slice");
                        let end = (base + row_count).min(slice.len());
                        if base < end {
                            fill_real_slice(&mut slice[base..end], class, &buffer);
                        }
                    }
                }
            }
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .map_err(|e| format!("sas7bdat: scan `{path}`: {e}"))?;

    for (ci, meta) in metas.iter().enumerate() {
        if wants_factor[ci] {
            // The HLL gate can still veto a genuinely high-cardinality column, in
            // which case it falls back to the `character` fill from the same
            // buffers rather than re-reading the file.
            let bufs: Vec<&OwnedColumnBuffer> = held[ci].iter().collect();
            // A level holding a NUL would be cut, and two levels could then be equal,
            // which R does not allow in a factor; such a column stays `character`.
            if let Some(dict) = dictionary_encode(&bufs, &DictionaryPolicy::default())
                .filter(|dict| !dict.dictionary.iter().any(|level| level.contains('\0')))
            {
                let mut col = factor_from_dict(dict, nrow);
                set_column_attributes(&mut col, meta);
                cols[ci] = Some(col);
                held[ci].clear();
                continue;
            }
            // Vetoed: fall back to `character`, filled from the buffers already
            // held rather than by re-reading the file.
            let col: Robj = Strings::new(nrow).into();
            let sexp = unsafe { col.get() };
            let mut base = 0usize;
            for buffer in &held[ci] {
                // SAFETY: a fresh STRSXP of `nrow`, written once per row in order.
                unsafe { fill_text_sexp(sexp, base, buffer, &mut caches[ci]) };
                base += buffer_rows(buffer);
            }
            held[ci].clear();
            cols[ci] = Some(col);
        }
        let col = cols[ci].take().expect("allocated above");
        cols[ci] = Some(finalize_column(col, meta));
    }

    let cols: Vec<Robj> = cols.into_iter().map(|c| c.unwrap_or_else(|| ().into())).collect();
    Ok(with_warning(build_data_frame(&names, cols, nrow), warning))
}

/// `df` carrying `warning` for the R wrapper to raise; Rust cannot raise an R warning
/// and carry on.
fn with_warning(mut df: Robj, warning: Option<String>) -> Robj {
    if let Some(text) = warning {
        let _ = df.set_attrib("fastsas_warning", text);
    }
    df
}

/// Read a SAS7BDAT file into a bare R `data.frame`.
///
/// `catalog` is an optional path to a `.sas7bcat` value-label catalog; when
/// absent a same-stem `.sas7bcat` sibling is attached if present.
#[extendr]
#[allow(clippy::too_many_arguments)]
fn read_sas7bdat(
    path: &str,
    catalog: Option<String>,
    categorical: bool,
    io_backend: &str,
    threads: Option<i32>,
    col_select: Robj,
    skip: f64,
    n_max: f64,
    encoding: Option<String>,
    strict: bool,
) -> Robj {
    let threads = match threads {
        None => None,
        Some(n) if n >= 1 => Some(n as usize),
        Some(n) => throw_r_error(format!("sas7bdat: threads must be >= 1, got {n}")),
    };
    // The R wrapper has already checked these; here they only change representation.
    let columns = if col_select.is_null() {
        None
    } else if let Some(names) = col_select.as_string_vector() {
        Some(ColumnChoice::Names(names))
    } else if let Some(positions) = col_select.as_real_vector() {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Some(ColumnChoice::Positions(
            positions.iter().map(|p| *p as usize - 1).collect(),
        ))
    } else {
        throw_r_error("sas7bdat: col_select must be column names or positions")
    };
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let selection = Selection {
        columns,
        skip: skip as u64,
        n_max: n_max.is_finite().then_some(n_max as u64),
        encoding,
        strict,
    };
    match read_impl(
        path,
        catalog.as_deref(),
        categorical,
        io_backend,
        threads,
        &selection,
    ) {
        Ok(df) => df,
        Err(e) => throw_r_error(e),
    }
}

/// The R class a column of this type is read as.
const fn r_class_name(logical: LogicalType) -> &'static str {
    match col_class(logical) {
        ColClass::Real(RealClass::Plain) => "double",
        ColClass::Real(RealClass::Date) => "Date",
        ColClass::Real(RealClass::DateTime) => "POSIXct",
        ColClass::Real(RealClass::Time) => "hms",
        ColClass::Text => "character",
    }
}

/// A character vector with `NA` where `values` has `None`.
fn strings_with_na<'a>(values: impl Iterator<Item = Option<&'a str>>) -> Robj {
    let values: Vec<Rstr> = values
        .map(|value| value.map_or_else(Rstr::na, Rstr::from))
        .collect();
    Strings::from_values(values).into()
}

/// A `POSIXct` in UTC, `NA` when absent.
fn posixct(time: Option<std::time::SystemTime>) -> Robj {
    let seconds = time.map_or(f64::NAN, |time| match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(after) => after.as_secs_f64(),
        Err(before) => -before.duration().as_secs_f64(),
    });
    let value = if seconds.is_nan() { Rfloat::na() } else { Rfloat::from(seconds) };
    let mut out: Robj = Doubles::from_values([value]).into();
    out.set_class(&["POSIXct", "POSIXt"]).unwrap();
    out.set_attrib("tzone", "UTC").unwrap();
    out
}

fn info_impl(
    path: &str,
    catalog: Option<&str>,
    encoding: Option<&str>,
) -> std::result::Result<Robj, String> {
    let builder = OpenOptions::builder();
    let builder = match encoding {
        None => builder,
        Some(label) => builder
            .encoding(label)
            .map_err(|e| format!("sas7bdat: encoding: {e}"))?,
    };
    let mut ds = Dataset::open_with(path, builder.build())
        .map_err(|e| format!("sas7bdat: open `{path}`: {e}"))?;
    if let Some(catalog) = catalog {
        ds.attach_catalog(catalog)
            .map_err(|e| format!("sas7bdat: catalog `{catalog}`: {e}"))?;
    }
    let meta = ds.metadata();
    let columns = ds.columns();
    let names: Vec<String> = columns.iter().map(|c| before_nul_str(&c.name).to_owned()).collect();
    let formats: Vec<Option<String>> = columns.iter().map(|c| c.format_spec()).collect();
    let column_frame = build_data_frame(
        &["name", "label", "format", "type", "width"].map(str::to_owned),
        vec![
            strings_with_na(names.iter().map(|name| Some(name.as_str()))),
            strings_with_na(columns.iter().map(|c| c.label.as_deref().map(before_nul_str))),
            strings_with_na(formats.iter().map(|f| f.as_deref().map(before_nul_str))),
            strings_with_na(columns.iter().map(|c| Some(r_class_name(c.logical_type)))),
            Doubles::from_values(columns.iter().map(|c| f64::from(c.physical_width))).into(),
        ],
        columns.len(),
    );
    #[allow(clippy::cast_precision_loss)]
    let count = |n: u64| n as f64;
    let info = List::from_names_and_values(
        [
            "n_rows",
            "n_deleted_rows",
            "n_columns",
            "encoding",
            "compression",
            "table_name",
            "file_label",
            "created",
            "modified",
            "columns",
        ],
        [
            r!(count(meta.row_count.saturating_sub(meta.deleted_row_count))),
            r!(count(meta.deleted_row_count)),
            r!(count(columns.len() as u64)),
            strings_with_na([meta.encoding.as_deref()].into_iter()),
            r!(format!("{:?}", meta.compression).to_ascii_lowercase()),
            strings_with_na([meta.table_name.as_deref()].into_iter()),
            strings_with_na([meta.file_label.as_deref()].into_iter()),
            posixct(meta.created_at),
            posixct(meta.modified_at),
            column_frame,
        ],
    )
    .map_err(|e| format!("sas7bdat: {e}"))?;
    Ok(info.into())
}

/// Header-level facts about a SAS7BDAT file, without reading its rows.
#[extendr]
fn sas7bdat_info(path: &str, catalog: Option<String>, encoding: Option<String>) -> Robj {
    match info_impl(path, catalog.as_deref(), encoding.as_deref()) {
        Ok(info) => info,
        Err(e) => throw_r_error(e),
    }
}

extendr_module! {
    mod fastsas;
    fn read_sas7bdat;
    fn sas7bdat_info;
}

#[cfg(test)]
mod tests {
    use super::{haven_tagged_na, sas_special_missing_tag, MISSING_TAG};

    /// Build the raw bits SAS writes for a missing cell with indicator byte `b`,
    /// in the signalling form (`0xFFFF_bb_00..`) both spellings use in practice.
    const fn missing_bits(indicator: u8) -> u64 {
        0xFFFF_0000_0000_0000 | ((indicator as u64) << 40)
    }

    #[test]
    fn the_ordinal_spelling_decodes() {
        assert_eq!(sas_special_missing_tag(missing_bits(0xFF)), Some(b'_'));
        assert_eq!(sas_special_missing_tag(missing_bits(0xFD)), Some(b'a'));
        assert_eq!(sas_special_missing_tag(missing_bits(0xFC)), Some(b'b'));
        assert_eq!(sas_special_missing_tag(missing_bits(0xE4)), Some(b'z'));
        // Plain `.` carries no tag.
        assert_eq!(sas_special_missing_tag(missing_bits(0xFE)), None);
    }

    #[test]
    fn the_complement_spelling_decodes() {
        // These are the bytes observed in the corpus, with the tags `haven` gives.
        for (byte, tag) in [
            (0xA9u8, b'v'),
            (0xAA, b'u'),
            (0xAD, b'r'),
            (0xB1, b'n'),
            (0xB2, b'm'),
            (0xBB, b'd'),
            (0xBD, b'b'),
            (0xBE, b'a'),
            (0xA5, b'z'),
            (0xA0, b'_'),
        ] {
            assert_eq!(sas_special_missing_tag(missing_bits(byte)), Some(tag), "byte {byte:#04x}");
        }
        // `!'.'` is a plain missing, not a tag — it is the single most common
        // indicator in the corpus, so mis-decoding it would be very loud.
        assert_eq!(sas_special_missing_tag(missing_bits(0xD1)), None);
    }

    #[test]
    fn the_two_spellings_never_collide() {
        let ordinal = (0xE4u8..=0xFF).collect::<Vec<_>>();
        let complement: Vec<u8> = (b'A'..=b'Z').chain(*b"_.").map(|c| 0xFF - c).collect();
        for b in &complement {
            assert!(!ordinal.contains(b), "byte {b:#04x} is claimed by both spellings");
        }
        // And every byte outside both families stays untagged.
        for b in 0..=u8::MAX {
            if !ordinal.contains(&b) && !complement.contains(&b) {
                assert_eq!(MISSING_TAG[b as usize], 0, "byte {b:#04x} should be untagged");
            }
        }
    }

    #[test]
    fn the_quiet_nan_spelling_is_accepted() {
        // `many_columns.sas7bdat` writes `._` as a quiet NaN rather than the
        // signalling form.
        assert_eq!(sas_special_missing_tag(0x7FF8_FF00_0000_0000), Some(b'_'));
    }

    #[test]
    fn non_nan_bits_are_never_tagged() {
        assert_eq!(sas_special_missing_tag(0f64.to_bits()), None);
        assert_eq!(sas_special_missing_tag(1.5f64.to_bits()), None);
        assert_eq!(sas_special_missing_tag(f64::INFINITY.to_bits()), None);
        assert_eq!(sas_special_missing_tag(f64::NEG_INFINITY.to_bits()), None);
    }

    #[test]
    fn tagged_na_matches_havens_bit_layout() {
        // haven::tagged_na("a") is NA_real_ with the tag char in bits 32-39.
        assert_eq!(haven_tagged_na(b'a').to_bits(), 0x7FF0_0061_0000_07A2);
        assert_eq!(haven_tagged_na(b'_').to_bits(), 0x7FF0_005F_0000_07A2);
    }
}
