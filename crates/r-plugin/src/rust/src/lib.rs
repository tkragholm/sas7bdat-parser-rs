//! R binding for the sas7bdat parser (extendr).
//!
//! v1 strategy (see ../../../../docs/r-bindings/design-direct-fill.md):
//! consume the core's existing `OwnedColumnBuffer` columns and marshal them into
//! R column vectors on the main thread. Numeric/temporal columns cost one memcpy
//! into a REALSXP (R owns its allocations); strings are interned into UTF-8
//! CHARSXPs. `haven`-parity defaults: all SAS numerics -> double, SAS missings ->
//! plain `NA`, dates -> `Date`, datetimes -> `POSIXct` (UTC), times -> `hms`.

use extendr_api::prelude::*;
use sas7bdat::{Dataset, OwnedColumnBuffer};
use std::ops::ControlFlow;

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
                push_reals(out, &values, valid.as_deref(), |v| f64::from(v));
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

/// Materialize one accumulated column into its R vector with class/attributes.
fn accum_to_robj(acc: ColAccum) -> Robj {
    match acc {
        ColAccum::Real { values, class } => {
            let mut col: Robj = Doubles::from_values(values).into();
            match class {
                RealClass::Plain => {}
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
            s.into()
        }
    }
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

fn read_impl(path: &str) -> std::result::Result<Robj, String> {
    let ds = Dataset::open(path).map_err(|e| format!("sas7bdat: open `{path}`: {e}"))?;

    let names: Vec<String> = ds.columns().iter().map(|c| c.name.clone()).collect();
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
        .map(|a| match a {
            Some(acc) => accum_to_robj(acc),
            // A column with no batches (e.g. zero-row file): empty double column.
            None => Doubles::new(nrow).into(),
        })
        .collect();

    Ok(build_data_frame(&names, cols, nrow))
}

/// Read a SAS7BDAT file into a bare R `data.frame`.
///
/// Errors are converted to R conditions via `throw_r_error` (longjmp), which
/// extendr handles correctly.
#[extendr]
fn read_sas7bdat(path: &str) -> Robj {
    match read_impl(path) {
        Ok(df) => df,
        Err(e) => throw_r_error(e),
    }
}

extendr_module! {
    mod readsas;
    fn read_sas7bdat;
}
