//! R binding for the sas7bdat parser (extendr).
//!
//! v1 strategy (see ../../../../docs/r-bindings/design-direct-fill.md):
//! consume the core's existing `OwnedColumnBuffer` columns and marshal them into
//! R column vectors on the main thread. Numeric/temporal columns cost one memcpy
//! into a REALSXP (R owns its allocations); strings are interned into UTF-8
//! CHARSXPs. `haven`-parity defaults: all SAS numerics -> double, SAS missings ->
//! plain `NA`, dates -> `Date`, datetimes -> `POSIXct` (UTC), times -> `hms`.
//!
//! Metadata: SAS variable labels become the column `label` attribute, and
//! value-label formats (from an attached `.sas7bcat` catalog) become
//! `haven_labelled` columns (a `labels` named vector + the haven/vctrs class).

use extendr_api::prelude::*;
use sas7bdat::{
    catalog::normalize_format_name, Dataset, LabelSet, LogicalType, OwnedColumnBuffer, ValueKey,
    ValueType,
};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

/// Which R class the assembled double column carries. The epoch shift (for Date/
/// DateTime) is applied to the *values* during accumulation, not here — this only
/// governs the class/attributes attached at the end.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RealClass {
    Plain,
    Date,
    DateTime,
    Time,
}

impl RealClass {
    /// SAS-epoch -> R-epoch shift applied to the raw SAS value (days for `Date`,
    /// seconds for `DateTime`). `Time` is seconds-since-midnight (no shift);
    /// `Plain` is verbatim.
    const fn epoch_shift(self) -> f64 {
        match self {
            RealClass::Date => 3653.0,                 // days 1960-01-01 -> 1970-01-01
            RealClass::DateTime => 315_619_200.0,      // seconds 1960 -> 1970
            RealClass::Time | RealClass::Plain => 0.0,
        }
    }
}

/// The R column shape, derived from the SAS *logical type* — the semantic truth.
/// We deliberately key off `logical_type`, not the `OwnedColumnBuffer` variant:
/// the core emits a typed temporal buffer for whole-unit values but falls back
/// to `F64` when a temporal column carries fractional seconds (its `SasDateTime`
/// is integer-only). Driving the R type from `logical_type` keeps such columns
/// as `POSIXct`/`hms` (which hold fractional seconds), matching `haven`.
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

/// One column accumulated across all batches into a single contiguous buffer,
/// ready to become one R vector. (v1 accepts the extra Rust-side buffer; the
/// direct-fill optimization that writes the REALSXP in place is deferred — see
/// the design note.)
enum ColAccum {
    Real { values: Vec<Rfloat>, class: RealClass },
    Text { values: Vec<Option<String>> },
}

impl ColAccum {
    fn len(&self) -> usize {
        match self {
            ColAccum::Real { values, .. } => values.len(),
            ColAccum::Text { values } => values.len(),
        }
    }
}

/// Per-column metadata resolved up front (before decoding): the SAS variable
/// label and the value-label set (if the column's format matches an attached
/// catalog format).
struct ColMeta {
    class: ColClass,
    var_label: Option<String>,
    value_labels: Option<LabelSet>,
}

/// `valid` is a SAS/Arrow-style validity bitmap: bit `i` set => row `i` is
/// present. `None` => every row present.
#[inline]
fn is_valid(valid: Option<&[u64]>, i: usize) -> bool {
    match valid {
        None => true,
        Some(bits) => (bits[i / 64] >> (i % 64)) & 1 == 1,
    }
}

/// R's `NA_real_` bit pattern. haven's `tagged_na(c)` is this value with the tag
/// character placed in byte 4 (bits 32-39).
const R_NA_REAL_BITS: u64 = 0x7FF0_0000_0000_07A2;

/// Build haven's `tagged_na(tag)` value (bit-exact with `haven::tagged_na`).
fn haven_tagged_na(tag: u8) -> f64 {
    f64::from_bits(R_NA_REAL_BITS | (u64::from(tag) << 32))
}

/// Recover a SAS special-missing tag from a preserved missing cell's raw bits.
///
/// SAS encodes special missings (`.A`-`.Z`, `._`) as a NaN of the form
/// `0xFFFF_TT_00_00_00_00_00`, where the top two bytes are `0xFF` and byte 5
/// (bits 40-47) is the indicator: `0xFF` -> `_`, `0xFD..=0xE4` -> `a..z`. Plain
/// `.` missings and ordinary NaNs carry no tag. Verified byte-exact against
/// `haven::na_tag` across the fixture corpus.
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

/// Map a catalog value-label tag key to its R numeric code: a haven `tagged_na`
/// for `.A`-`.Z`/`._`, plain `NA` for `.`. The core decodes catalog tags as
/// uppercase letters (`A`-`Z`), `_`, or `.`; haven tags are lowercase.
fn tagged_key_to_na(tag: char) -> Option<f64> {
    match tag {
        'A'..='Z' => Some(haven_tagged_na(tag.to_ascii_lowercase() as u8)),
        'a'..='z' => Some(haven_tagged_na(tag as u8)),
        '_' => Some(haven_tagged_na(b'_')),
        '.' => Some(f64::from_bits(R_NA_REAL_BITS)),
        _ => None,
    }
}

/// Append a plain numeric column, emitting haven `tagged_na` for SAS special
/// missings (recovered from the preserved raw bits) and plain `NA` otherwise.
fn push_reals_with_tags(out: &mut Vec<Rfloat>, values: &[f64], valid: Option<&[u64]>) {
    out.reserve(values.len());
    for (i, &v) in values.iter().enumerate() {
        if is_valid(valid, i) {
            out.push(Rfloat::from(v));
        } else if let Some(tag) = sas_special_missing_tag(v.to_bits()) {
            out.push(Rfloat::from(haven_tagged_na(tag)));
        } else {
            out.push(Rfloat::na());
        }
    }
}

/// Append a typed numeric/temporal column slice, mapping each present cell to an
/// `f64` (epoch shift folded into `map`) and each missing cell to R's `NA`.
fn push_reals<T: Copy>(
    out: &mut Vec<Rfloat>,
    values: &[T],
    valid: Option<&[u64]>,
    map: impl Fn(T) -> f64,
) {
    out.reserve(values.len());
    for (i, &x) in values.iter().enumerate() {
        if is_valid(valid, i) {
            out.push(Rfloat::from(map(x)));
        } else {
            out.push(Rfloat::na());
        }
    }
}

/// Append a UTF-8 string column. The core already decoded `data` to UTF-8; we
/// slice per `offsets` and hand owned `String`s to R (extendr marks CHARSXPs as
/// `CE_UTF8`). `None` => `NA_character_`.
fn push_strings(
    out: &mut Vec<Option<String>>,
    offsets: &[i64],
    data: &[u8],
    valid: Option<&[u64]>,
) {
    let rows = offsets.len().saturating_sub(1);
    out.reserve(rows);
    for i in 0..rows {
        if is_valid(valid, i) {
            let lo = offsets[i] as usize;
            let hi = offsets[i + 1] as usize;
            // Core guarantees UTF-8; lossy is a cheap safety net, not a hot path.
            out.push(Some(String::from_utf8_lossy(&data[lo..hi]).into_owned()));
        } else {
            out.push(None);
        }
    }
}

/// Fold one batch's column into its (pre-typed) accumulator. The accumulator's
/// class was fixed from the column's `logical_type`; here we only extract the
/// raw SAS value out of whichever buffer variant the core produced (typed
/// temporal or `F64` fallback) and apply the class's epoch shift.
fn append_column(acc: &mut ColAccum, buffer: OwnedColumnBuffer) {
    match acc {
        ColAccum::Real { values: out, class } => {
            let shift = class.epoch_shift();
            match buffer {
                OwnedColumnBuffer::F64 { values, valid } => {
                    if *class == RealClass::Plain {
                        // Plain numerics carry SAS special-missing tags -> haven tagged_na.
                        push_reals_with_tags(out, &values, valid.as_deref());
                    } else {
                        // Temporal F64 fallback: raw SAS days/seconds; missings -> plain NA.
                        push_reals(out, &values, valid.as_deref(), |v| v - shift);
                    }
                }
                OwnedColumnBuffer::I64 { values, valid } => {
                    #[allow(clippy::cast_precision_loss)]
                    push_reals(out, &values, valid.as_deref(), |v| v as f64 - shift);
                }
                OwnedColumnBuffer::I32 { values, valid } => {
                    push_reals(out, &values, valid.as_deref(), |v| f64::from(v) - shift);
                }
                OwnedColumnBuffer::Date { values, valid } => {
                    push_reals(out, &values, valid.as_deref(), |d| {
                        f64::from(d.days_since_sas_epoch) - shift
                    });
                }
                OwnedColumnBuffer::DateTime { values, valid } => {
                    #[allow(clippy::cast_precision_loss)]
                    push_reals(out, &values, valid.as_deref(), |dt| {
                        dt.seconds_since_sas_epoch as f64 - shift
                    });
                }
                OwnedColumnBuffer::Time { values, valid } => {
                    push_reals(out, &values, valid.as_deref(), |t| {
                        f64::from(t.seconds_since_midnight) - shift
                    });
                }
                // A numeric/temporal logical type never yields a string buffer.
                OwnedColumnBuffer::Utf8 { .. } | OwnedColumnBuffer::RawBytes { .. } => {}
            }
        }
        ColAccum::Text { values: out } => match buffer {
            OwnedColumnBuffer::Utf8 { offsets, data, valid, .. } => {
                push_strings(out, offsets.as_slice(), &data, valid.as_deref());
            }
            OwnedColumnBuffer::RawBytes { offsets, data, valid } => {
                // Uninterpreted binary -> lossy UTF-8 character column (rare).
                push_strings(out, offsets.as_slice(), &data, valid.as_deref());
            }
            // A string logical type never yields a numeric buffer.
            _ => {}
        },
    }
}

/// Build a named numeric `labels` vector (names = label text, values = codes)
/// from a numeric value-label set. `None` if it carries no numeric keys.
fn numeric_labels_robj(ls: &LabelSet) -> Option<Robj> {
    let mut codes: Vec<f64> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for vl in &ls.labels {
        let code = match vl.key {
            ValueKey::Numeric(v) => v,
            ValueKey::Integer(v) => f64::from(v),
            // A value label keyed on a special missing (`.A = "Refused"`) maps to a
            // haven tagged_na entry, matching haven_labelled semantics.
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

/// Build a named character `labels` vector (names = label text, values = codes)
/// from a string value-label set. `None` if it carries no string keys.
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

/// Materialize one accumulated column into its R vector with class/attributes,
/// including the variable label and (where applicable) `haven_labelled` value
/// labels.
fn accum_to_robj(acc: ColAccum, meta: &ColMeta) -> Robj {
    let mut col = match acc {
        ColAccum::Real { values, class } => {
            let mut col: Robj = Doubles::from_values(values).into();
            match class {
                RealClass::Plain => {
                    // Value labels apply only to plain numerics (not temporals).
                    if let Some(ls) = &meta.value_labels {
                        if ls.value_type == ValueType::Numeric {
                            if let Some(labels) = numeric_labels_robj(ls) {
                                col.set_attrib("labels", labels).unwrap();
                                col.set_class(&["haven_labelled", "vctrs_vctr", "double"]).unwrap();
                            }
                        }
                    }
                }
                RealClass::Date => {
                    col.set_class(&["Date"]).unwrap();
                }
                RealClass::DateTime => {
                    col.set_class(&["POSIXct", "POSIXt"]).unwrap();
                    col.set_attrib("tzone", "UTC").unwrap();
                }
                RealClass::Time => {
                    col.set_class(&["hms", "difftime"]).unwrap();
                    col.set_attrib("units", "secs").unwrap();
                }
            }
            col
        }
        ColAccum::Text { values } => {
            let mut s = Strings::new(values.len());
            for (i, item) in values.into_iter().enumerate() {
                match item {
                    Some(text) => s.set_elt(i, Rstr::from(text)),
                    None => s.set_elt(i, Rstr::na()),
                }
            }
            let mut col: Robj = s.into();
            if let Some(ls) = &meta.value_labels {
                if ls.value_type == ValueType::String {
                    if let Some(labels) = string_labels_robj(ls) {
                        col.set_attrib("labels", labels).unwrap();
                        col.set_class(&["haven_labelled", "vctrs_vctr", "character"]).unwrap();
                    }
                }
            }
            col
        }
    };

    // Variable label applies to every column type, alongside any existing class.
    if let Some(label) = &meta.var_label {
        col.set_attrib("label", label.as_str()).unwrap();
    }
    col
}

/// Assemble a bare `data.frame` from named columns. Class/tibble reclass happens
/// in the R wrapper (O(1)).
fn build_data_frame(names: &[String], cols: Vec<Robj>, nrow: usize) -> Robj {
    let mut df: Robj = List::from_values(cols).into();
    df.set_names(names.iter().map(String::as_str)).unwrap();

    // Compact row.names form c(NA, -nrow) avoids materializing 1:nrow, but is
    // i32 — guard against >2^31-row files with the long-vector fallback.
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

/// Resolve the catalog path to attach: an explicit argument, else a same-stem
/// `.sas7bcat` sibling if one exists.
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
            // An explicitly-requested catalog that fails is an error; an
            // auto-detected sibling that fails to parse is silently ignored.
            Err(e) if catalog.is_some() => {
                return Err(format!("sas7bdat: catalog `{}`: {e}", cat_path.display()));
            }
            Err(_) => {}
        }
    }

    // Resolve per-column metadata up front. These borrows end before `scan()`.
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

    // Accumulators are typed up front from each column's logical type.
    let mut accums: Vec<ColAccum> = metas
        .iter()
        .map(|m| match m.class {
            ColClass::Real(class) => ColAccum::Real { values: Vec::new(), class },
            ColClass::Text => ColAccum::Text { values: Vec::new() },
        })
        .collect();

    ds.scan()
        .visit_owned_batches(|batch| {
            for (ci, col) in batch.columns.into_iter().enumerate() {
                if ci < accums.len() {
                    append_column(&mut accums[ci], col);
                }
            }
            Ok(ControlFlow::Continue(()))
        })
        .map_err(|e| format!("sas7bdat: scan `{path}`: {e}"))?;

    let nrow = accums.first().map_or(0, ColAccum::len);

    let cols: Vec<Robj> = accums
        .into_iter()
        .zip(metas.iter())
        .map(|(acc, meta)| accum_to_robj(acc, meta))
        .collect();

    Ok(build_data_frame(&names, cols, nrow))
}

/// Read a SAS7BDAT file into a bare R `data.frame`.
///
/// `catalog` is an optional path to a `.sas7bcat` value-label catalog. When
/// absent, a same-stem `.sas7bcat` sibling is attached if present.
///
/// Errors are converted to R conditions via `throw_r_error` (longjmp), which
/// extendr handles correctly.
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
