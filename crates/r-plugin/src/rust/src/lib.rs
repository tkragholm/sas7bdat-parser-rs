//! R binding for the sas7bdat parser (extendr).
//!
//! Strategy (see ../../../../docs/r-bindings/design-direct-fill.md): the row
//! count is known from metadata up front, so every column's R vector is
//! allocated at its final length and the decoded batches are written *in place*
//! — one write per cell, no intermediate Rust buffer or second copy. The core
//! decodes batches across all cores; the batches are delivered to this callback
//! serially on the main thread (so R object writes, including string interning,
//! are sound), and each batch is placed by its global `row_base`.
//!
//! `haven`-parity: SAS numerics -> double, character -> UTF-8 character, dates ->
//! `Date`, datetimes -> `POSIXct` (UTC), times -> `hms`. SAS special missings
//! (`.A`-`.Z`, `._`) -> haven `tagged_na`. Variable labels -> `label` attribute;
//! value-label catalogs -> `haven_labelled`.

use extendr_api::prelude::*;
use sas7bdat::{
    catalog::normalize_format_name, Dataset, LabelSet, LogicalType, OwnedColumnBuffer, Parallelism,
    ValueKey, ValueType,
};
use std::collections::HashMap;
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
    const fn epoch_shift(self) -> f64 {
        match self {
            RealClass::Date => 3653.0,            // days 1960-01-01 -> 1970-01-01
            RealClass::DateTime => 315_619_200.0, // seconds 1960 -> 1970
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
    value_labels: Option<LabelSet>,
}

// ── tagged-NA encoding ────────────────────────────────────────────────────────

/// R's `NA_real_` bit pattern. haven's `tagged_na(c)` is this with the tag char
/// in byte 4 (bits 32-39).
const R_NA_REAL_BITS: u64 = 0x7FF0_0000_0000_07A2;

fn haven_tagged_na(tag: u8) -> f64 {
    f64::from_bits(R_NA_REAL_BITS | (u64::from(tag) << 32))
}

/// Recover a SAS special-missing tag from a preserved missing cell's raw bits.
/// SAS encodes them as a NaN `0xFFFF_TT_00..`; byte 5 is the indicator
/// (`0xFF` -> `_`, `0xFD..=0xE4` -> `a..z`). Verified byte-exact vs `haven::na_tag`.
fn sas_special_missing_tag(bits: u64) -> Option<u8> {
    if (bits >> 48) != 0xFFFF {
        return None;
    }
    match ((bits >> 40) & 0xFF) as u8 {
        0xFF => Some(b'_'),
        b @ 0xE4..=0xFD => Some(b'a' + (0xFD - b)),
        _ => None,
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
/// Present cells get the epoch-shifted value; missing cells get plain `NA`, or —
/// for plain numeric columns — the recovered haven `tagged_na`.
fn fill_real_slice(dst: &mut [f64], class: RealClass, plain: bool, buffer: &OwnedColumnBuffer) {
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
                } else if plain {
                    // Plain numerics preserve SAS special-missing tags.
                    sas_special_missing_tag(v.to_bits()).map_or(na, haven_tagged_na)
                } else {
                    na
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

/// Intern one batch's strings into a STRSXP (`sexp`) starting at row `base`,
/// deduplicating distinct values via a per-column `dict` of cached CHARSXPs.
///
/// A wide SAS file can have thousands of low-cardinality string columns (millions
/// of cells). The dict caps `Rf_mkCharLenCE` at the column's cardinality, and we
/// use raw `SET_STRING_ELT` to skip extendr's per-call thread-guard + bounds
/// check. CHARSXPs created here live in R's global string cache, so dict entries
/// stay valid; `sexp` is kept alive by its owning `Robj` in the caller.
///
/// # Safety
/// `sexp` is a live STRSXP of length >= `base + batch_rows`, on the main thread.
unsafe fn fill_text_sexp(
    sexp: libR_sys::SEXP,
    base: usize,
    buffer: &OwnedColumnBuffer,
    dict: &mut HashMap<Box<[u8]>, libR_sys::SEXP>,
) {
    let (offsets, data, valid) = match buffer {
        OwnedColumnBuffer::Utf8 { offsets, data, valid, .. } => (offsets, data, valid),
        OwnedColumnBuffer::RawBytes { offsets, data, valid } => (offsets, data, valid),
        _ => return,
    };
    let offs = offsets.as_slice();
    let valid = valid.as_deref();
    for i in 0..offs.len().saturating_sub(1) {
        let idx = (base + i) as isize;
        if is_valid(valid, i) {
            let bytes = &data[offs[i] as usize..offs[i + 1] as usize];
            let charsxp = if let Some(&cs) = dict.get(bytes) {
                cs
            } else {
                let cs = libR_sys::Rf_mkCharLenCE(
                    bytes.as_ptr().cast::<std::os::raw::c_char>(),
                    bytes.len() as std::os::raw::c_int,
                    libR_sys::cetype_t::CE_UTF8,
                );
                dict.insert(bytes.into(), cs);
                cs
            };
            libR_sys::SET_STRING_ELT(sexp, idx, charsxp);
        } else {
            libR_sys::SET_STRING_ELT(sexp, idx, libR_sys::R_NaString);
        }
    }
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
        names.push(vl.label.clone());
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
            codes.push(s.clone());
            names.push(vl.label.clone());
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
    if let Some(label) = &meta.var_label {
        col.set_attrib("label", label.as_str()).unwrap();
    }
    col
}

/// Assemble a bare `data.frame` from named columns.
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

fn read_impl(path: &str, catalog: Option<&str>) -> std::result::Result<Robj, String> {
    let mut ds = Dataset::open(path).map_err(|e| format!("sas7bdat: open `{path}`: {e}"))?;

    if let Some(cat_path) = resolve_catalog(path, catalog) {
        match ds.attach_catalog(&cat_path) {
            Ok(()) => {}
            Err(e) if catalog.is_some() => {
                return Err(format!("sas7bdat: catalog `{}`: {e}", cat_path.display()));
            }
            Err(_) => {}
        }
    }

    let names: Vec<String> = ds.columns().iter().map(|c| c.name.clone()).collect();
    let metas: Vec<ColMeta> = {
        let label_sets = &ds.metadata().label_sets;
        ds.columns()
            .iter()
            .map(|c| ColMeta {
                class: col_class(c.logical_type),
                var_label: c.label.clone(),
                value_labels: c
                    .format
                    .as_deref()
                    .map(normalize_format_name)
                    .and_then(|norm| label_sets.get(&norm).cloned()),
            })
            .collect()
    };
    let nrow = usize::try_from(ds.metadata().row_count).unwrap_or(0);

    // Decode all batches across cores, then fill each column's R vector in place.
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get());
    let batches = ds
        .scan()
        .with_parallelism(Parallelism::Threads(workers))
        .collect_batches()
        .map_err(|e| format!("sas7bdat: scan `{path}`: {e}"))?;

    let mut cols: Vec<Option<Robj>> = (0..metas.len()).map(|_| None).collect();

    // String columns FIRST: interning each value allocates a CHARSXP, which can
    // trigger R's GC — and GC cost scales with the number of live SEXPs. Filling
    // strings before allocating the (potentially thousands of) numeric vectors
    // keeps those GCs cheap. Numeric fill allocates nothing, so it triggers no GC.
    for (ci, meta) in metas.iter().enumerate() {
        if matches!(meta.class, ColClass::Text) {
            let robj: Robj = Strings::new(nrow).into();
            // SAFETY: `robj` owns the STRSXP and keeps it alive; all writes are on
            // this (main) thread; every index written is < nrow.
            let sexp = unsafe { robj.get() };
            let mut dict: HashMap<Box<[u8]>, libR_sys::SEXP> = HashMap::new();
            for batch in &batches {
                if let Some(buffer) = batch.columns.get(ci) {
                    let base = usize::try_from(batch.row_base.0).unwrap_or(0);
                    unsafe { fill_text_sexp(sexp, base, buffer, &mut dict) };
                }
            }
            cols[ci] = Some(finalize_column(robj, meta));
        }
    }

    // Numeric / temporal columns: allocate the REALSXP and fill it column-major
    // (each vector written sequentially across all batches).
    for (ci, meta) in metas.iter().enumerate() {
        if let ColClass::Real(class) = meta.class {
            let mut robj: Robj = Doubles::new(nrow).into();
            let plain = matches!(class, RealClass::Plain);
            {
                let slice = robj.as_real_slice_mut().expect("REALSXP slice");
                for batch in &batches {
                    if let Some(buffer) = batch.columns.get(ci) {
                        let base = usize::try_from(batch.row_base.0).unwrap_or(0);
                        let end = (base + batch.row_count).min(slice.len());
                        if base < end {
                            fill_real_slice(&mut slice[base..end], class, plain, buffer);
                        }
                    }
                }
            }
            cols[ci] = Some(finalize_column(robj, meta));
        }
    }

    let cols: Vec<Robj> = cols.into_iter().map(|c| c.unwrap_or_else(|| ().into())).collect();
    Ok(build_data_frame(&names, cols, nrow))
}

/// Read a SAS7BDAT file into a bare R `data.frame`.
///
/// `catalog` is an optional path to a `.sas7bcat` value-label catalog; when
/// absent a same-stem `.sas7bcat` sibling is attached if present.
#[extendr]
fn read_sas7bdat(path: &str, catalog: Option<String>) -> Robj {
    match read_impl(path, catalog.as_deref()) {
        Ok(df) => df,
        Err(e) => throw_r_error(e),
    }
}

extendr_module! {
    mod readsas;
    fn read_sas7bdat;
}
