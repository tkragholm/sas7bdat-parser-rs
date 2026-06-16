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
    catalog::normalize_format_name, Dataset, LabelSet, OwnedColumnBuffer, ValueKey, ValueType,
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

/// Fold one batch's column into its accumulator, initializing the accumulator's
/// kind from the first batch's buffer variant.
fn append_column(slot: &mut Option<ColAccum>, col: OwnedColumnBuffer) {
    match col {
        OwnedColumnBuffer::I32 { values, valid } => {
            let acc = slot.get_or_insert_with(|| ColAccum::Real { values: Vec::new(), class: RealClass::Plain });
            if let ColAccum::Real { values: out, .. } = acc {
                push_reals(out, &values, valid.as_deref(), f64::from);
            }
        }
        OwnedColumnBuffer::I64 { values, valid } => {
            let acc = slot.get_or_insert_with(|| ColAccum::Real { values: Vec::new(), class: RealClass::Plain });
            if let ColAccum::Real { values: out, .. } = acc {
                // haven-parity: SAS numerics are doubles in R. Exact for |v| <= 2^53.
                #[allow(clippy::cast_precision_loss)]
                push_reals(out, &values, valid.as_deref(), |v| v as f64);
            }
        }
        OwnedColumnBuffer::F64 { values, valid } => {
            let acc = slot.get_or_insert_with(|| ColAccum::Real { values: Vec::new(), class: RealClass::Plain });
            if let ColAccum::Real { values: out, .. } = acc {
                push_reals(out, &values, valid.as_deref(), |v| v);
            }
        }
        OwnedColumnBuffer::Date { values, valid } => {
            let acc = slot.get_or_insert_with(|| ColAccum::Real { values: Vec::new(), class: RealClass::Date });
            if let ColAccum::Real { values: out, .. } = acc {
                // R `Date` counts days from 1970; core counts from 1960.
                push_reals(out, &values, valid.as_deref(), |d| f64::from(d.unix_days()));
            }
        }
        OwnedColumnBuffer::DateTime { values, valid } => {
            let acc = slot.get_or_insert_with(|| ColAccum::Real { values: Vec::new(), class: RealClass::DateTime });
            if let ColAccum::Real { values: out, .. } = acc {
                // R `POSIXct` counts seconds from 1970; core counts from 1960.
                #[allow(clippy::cast_precision_loss)]
                push_reals(out, &values, valid.as_deref(), |dt| dt.unix_seconds() as f64);
            }
        }
        OwnedColumnBuffer::Time { values, valid } => {
            let acc = slot.get_or_insert_with(|| ColAccum::Real { values: Vec::new(), class: RealClass::Time });
            if let ColAccum::Real { values: out, .. } = acc {
                // `hms`: seconds since midnight, no epoch shift.
                push_reals(out, &values, valid.as_deref(), |t| f64::from(t.seconds_since_midnight));
            }
        }
        OwnedColumnBuffer::Utf8 { offsets, data, valid, .. } => {
            let acc = slot.get_or_insert_with(|| ColAccum::Text { values: Vec::new() });
            if let ColAccum::Text { values: out } = acc {
                push_strings(out, offsets.as_slice(), &data, valid.as_deref());
            }
        }
        OwnedColumnBuffer::RawBytes { offsets, data, valid } => {
            // Uninterpreted binary -> lossy UTF-8 character column (rare in practice).
            let acc = slot.get_or_insert_with(|| ColAccum::Text { values: Vec::new() });
            if let ColAccum::Text { values: out } = acc {
                push_strings(out, offsets.as_slice(), &data, valid.as_deref());
            }
        }
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
            // Tagged missings would map to haven::tagged_na — deferred (v1).
            ValueKey::Tagged(_) | ValueKey::String(_) => continue,
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
                var_label: c.label.clone(),
                value_labels: c
                    .format
                    .as_deref()
                    .map(normalize_format_name)
                    .and_then(|norm| label_sets.get(&norm).cloned()),
            })
            .collect()
    };

    let ncols = names.len();
    let mut accums: Vec<Option<ColAccum>> = (0..ncols).map(|_| None).collect();

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

    let nrow = accums
        .iter()
        .find_map(|a| a.as_ref().map(ColAccum::len))
        .unwrap_or(0);

    let cols: Vec<Robj> = accums
        .into_iter()
        .zip(metas.iter())
        .map(|(a, meta)| match a {
            Some(acc) => accum_to_robj(acc, meta),
            // A column with no batches (e.g. zero-row file): empty double column.
            None => {
                let mut col: Robj = Doubles::new(nrow).into();
                if let Some(label) = &meta.var_label {
                    col.set_attrib("label", label.as_str()).unwrap();
                }
                col
            }
        })
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
