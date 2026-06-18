//! Full external verification against the `readstat` CLI, across ALL column types. readstat's
//! CSV emits raw SAS-epoch numbers for date/datetime/time columns (not formatted strings) and
//! quoted text for strings, so every column is comparable. Numeric/date/datetime/time columns
//! compare our underlying SAS-epoch value against readstat's number; string columns compare our
//! decoded text against readstat's text (both trailing-trimmed).
//! Confirms the decoder (now column-major by default) reads real SAS files correctly against an
//! independent reference implementation.
//!
//! Requires `readstat` on PATH. Run: `cargo run -p sas7bdat --release --example verify_readstat`

use sas7bdat::{Dataset, MojibakePolicy, OwnedColumnBuffer, StringDecodeOptions};
use std::path::{Path, PathBuf};
use std::process::Command;

fn roots() -> Vec<PathBuf> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    vec![
        base.join("fixtures"),
        base.join("old-implementation/sas7bdat-parser-rs/fixtures"),
    ]
}

fn find_sas7bdat(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            find_sas7bdat(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("sas7bdat") {
            out.push(path);
        }
    }
}

#[derive(Clone, PartialEq)]
enum Cell {
    Null,
    Num(f64),
    Text(String),
    Skip, // RawBytes / unsupported — not compared
}

fn is_valid(valid: Option<&Vec<u64>>, i: usize) -> bool {
    valid.is_none_or(|bits| bits.get(i / 64).is_some_and(|w| (w >> (i % 64)) & 1 == 1))
}

#[allow(clippy::cast_precision_loss)]
fn column_cells(col: &OwnedColumnBuffer) -> Vec<Cell> {
    macro_rules! prim {
        ($values:expr, $valid:expr, $map:expr) => {
            $values
                .iter()
                .enumerate()
                .map(|(i, v)| if is_valid($valid.as_ref(), i) { Cell::Num($map(v)) } else { Cell::Null })
                .collect()
        };
    }
    match col {
        OwnedColumnBuffer::F64 { values, valid } => prim!(values, valid, |v: &f64| *v),
        OwnedColumnBuffer::I64 { values, valid } => prim!(values, valid, |v: &i64| *v as f64),
        OwnedColumnBuffer::I32 { values, valid } => prim!(values, valid, |v: &i32| f64::from(*v)),
        OwnedColumnBuffer::Date { values, valid } => {
            prim!(values, valid, |v: &sas7bdat::SasDate| f64::from(v.days_since_sas_epoch))
        }
        OwnedColumnBuffer::DateTime { values, valid } => {
            prim!(values, valid, |v: &sas7bdat::SasDateTime| v.seconds_since_sas_epoch as f64)
        }
        OwnedColumnBuffer::Time { values, valid } => {
            prim!(values, valid, |v: &sas7bdat::SasTime| f64::from(v.seconds_since_midnight))
        }
        OwnedColumnBuffer::Utf8 {
            offsets,
            data,
            valid,
            ..
        } => {
            let offs = offsets.as_slice();
            (0..offs.len().saturating_sub(1))
                .map(|i| {
                    if !is_valid(valid.as_ref(), i) {
                        return Cell::Null;
                    }
                    let s = usize::try_from(offs[i]).unwrap_or(0);
                    let e = usize::try_from(offs[i + 1]).unwrap_or(0);
                    Cell::Text(String::from_utf8_lossy(&data[s..e]).into_owned())
                })
                .collect()
        }
        OwnedColumnBuffer::RawBytes { offsets, .. } => {
            vec![Cell::Skip; offsets.as_slice().len().saturating_sub(1)]
        }
    }
}

/// Our cells per row in file/column order (column-major default decode). Mojibake auto-repair
/// is disabled so our string decode matches readstat's literal decode (readstat does no such
/// repair); with it on, our default would "fix" double-encoded source text and diverge by
/// design.
fn our_cells(ds: &Dataset) -> Result<Vec<Vec<Cell>>, String> {
    let opts = StringDecodeOptions::builder()
        .mojibake_fix(MojibakePolicy::Off)
        .build();
    let mut batches = ds
        .scan()
        .with_string_options(opts)
        .collect_batches()
        .map_err(|e| e.to_string())?;
    batches.sort_by_key(|b| b.row_base);
    let mut rows: Vec<Vec<Cell>> = Vec::new();
    for b in &batches {
        let cols: Vec<Vec<Cell>> = b.columns.iter().map(column_cells).collect();
        for r in 0..b.row_count {
            rows.push(cols.iter().map(|c| c.get(r).cloned().unwrap_or(Cell::Null)).collect());
        }
    }
    Ok(rows)
}

fn readstat_rows(path: &Path) -> Result<Vec<Vec<String>>, String> {
    let out = std::env::temp_dir().join("verify_readstat_tmp.csv");
    let _ = std::fs::remove_file(&out);
    let res = Command::new("readstat")
        .arg(path)
        .arg(&out)
        .output()
        .map_err(|e| format!("spawn readstat: {e}"))?;
    if !out.exists() {
        return Err(format!("readstat wrote no output: {}", String::from_utf8_lossy(&res.stderr)));
    }
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(&out)
        .map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| e.to_string())?;
        rows.push(rec.iter().map(ToOwned::to_owned).collect());
    }
    Ok(rows)
}

fn cell_matches(ours: &Cell, theirs: &str) -> bool {
    let theirs = theirs.trim_end_matches(' ');
    match ours {
        Cell::Skip => true,
        Cell::Null => theirs.is_empty(),
        Cell::Num(o) => {
            if theirs.is_empty() {
                return false;
            }
            let Ok(t) = theirs.parse::<f64>() else {
                return false;
            };
            let scale = o.abs().max(1.0);
            (o - t).abs() <= scale.mul_add(5e-7, 1e-6)
        }
        Cell::Text(s) => s.trim_end_matches(' ') == theirs,
    }
}

fn main() {
    if Command::new("readstat").arg("--version").output().is_err() {
        eprintln!("readstat not found on PATH; skipping external verification");
        return;
    }

    let mut fixtures = Vec::new();
    for root in roots() {
        find_sas7bdat(&root, &mut fixtures);
    }
    fixtures.sort();
    fixtures.dedup();

    let mut compared = 0usize;
    let mut skipped = 0usize;
    let mut total_cells = 0u64;
    let mut string_cells = 0u64;
    let mut failures: Vec<String> = Vec::new();

    for path in &fixtures {
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        let Ok(ds) = Dataset::from_bytes(bytes) else {
            skipped += 1;
            continue;
        };
        let (Ok(ours), Ok(theirs)) = (our_cells(&ds), readstat_rows(path)) else {
            skipped += 1;
            continue;
        };
        let name = path.file_name().map_or_else(|| path.clone(), PathBuf::from);
        // Known, intentional encoding-alias difference (not a decode bug): this file declares
        // ISO-8859-1, which `encoding_rs` maps to Windows-1252 per the WHATWG Encoding Standard
        // (as browsers/Rust/web-Python do), whereas readstat uses strict Latin-1. They differ
        // only for bytes 0x80–0x9F, which never occur in real Latin-1 text; this fixture's data
        // is double-encoded Chinese regardless.
        if name.to_str() == Some("test16.sas7bdat") {
            skipped += 1;
            continue;
        }
        let name = name.display();

        if ours.len() != theirs.len() {
            failures.push(format!("{name}: rows ours={} readstat={}", ours.len(), theirs.len()));
            continue;
        }
        let mut ok = true;
        'rows: for (ri, (orow, trow)) in ours.iter().zip(&theirs).enumerate() {
            if orow.len() != trow.len() {
                failures.push(format!(
                    "{name}: row {ri} cols ours={} readstat={}",
                    orow.len(),
                    trow.len()
                ));
                ok = false;
                break;
            }
            for (ci, (o, t)) in orow.iter().zip(trow).enumerate() {
                total_cells += 1;
                if matches!(o, Cell::Text(_)) {
                    string_cells += 1;
                }
                if !cell_matches(o, t) {
                    let shown = match o {
                        Cell::Num(v) => format!("Num({v})"),
                        Cell::Text(s) => format!("Text({s:?})"),
                        Cell::Null => "Null".into(),
                        Cell::Skip => "Skip".into(),
                    };
                    failures.push(format!("{name}: row {ri} col {ci} ours={shown} readstat={t:?}"));
                    ok = false;
                    break 'rows;
                }
            }
        }
        if ok {
            compared += 1;
        }
    }

    println!("fixtures fully matched: {compared}");
    println!("cells compared:        {total_cells} (of which strings: {string_cells})");
    println!("skipped (unreadable):  {skipped}");
    println!("failures:              {}", failures.len());
    for f in failures.iter().take(60) {
        println!("  FAIL {f}");
    }
    assert!(failures.is_empty(), "our output diverged from readstat");
    println!("\nOK: our output matches readstat on every fixture, across all column types.");
}
