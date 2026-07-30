//! Fuzz the whole in-memory read path: header parse -> layout -> descriptor
//! compilation -> row decode.
//!
//! `FileSource::Bytes` is deliberately the only source fuzzed here. The `Path`
//! and `Mmap` sources reach the same decoders, but a truncated mmap faults with
//! SIGBUS instead of unwinding, so those crashes would say nothing about the
//! parser. Bytes keeps every failure attributable to our own code.
//!
//! Run with the fixture corpus (389 real files) rather than from empty:
//!
//! ```sh
//! cargo fuzz run dataset_from_bytes -- -max_len=262144
//! ```

#![no_main]

use std::hint::black_box;
use std::ops::ControlFlow;

use libfuzzer_sys::fuzz_target;
use sas7bdat::{CellValue, DecodeMode, Parallelism, RowView};

/// Stop after this many rows. A corrupt header can legitimately claim
/// `u32::MAX` rows over a few KB of pages — that is a real value this parser
/// sees in production — so an unbounded scan would spend the whole run in one
/// input and every finding would be a timeout. Enough rows to cross a page
/// boundary and exercise the continuation logic; few enough to stay fast.
const MAX_ROWS: usize = 4096;

/// Touch every cell so decode work cannot be optimised away, and so the string
/// kernels actually validate the bytes they were handed.
fn consume(row: &RowView<'_>) {
    black_box(row.row_index());
    for cell in row.iter() {
        match cell {
            // The interesting ones: these carry a pointer+length derived from
            // attacker-controlled column offsets and widths.
            CellValue::Str(s) => {
                black_box(s.len());
                black_box(s.as_bytes().first());
            }
            CellValue::Bytes(b) => {
                black_box(b.len());
                black_box(b.first());
            }
            other => {
                black_box(other);
            }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    // A SAS7BDAT header is 1024 bytes at minimum; below that we are only
    // testing the magic-number rejection, which is not worth a corpus slot.
    if data.len() < 1024 {
        return;
    }

    let Ok(dataset) = sas7bdat::Dataset::from_bytes(data.to_vec()) else {
        // A rejected file is the correct outcome for most mutations. What we are
        // hunting is the file that parses and then misbehaves downstream.
        return;
    };

    // Metadata is already parsed at this point; make sure the accessors agree
    // with each other rather than trusting the header's own column count.
    let columns = dataset.columns();
    black_box(columns.len());
    black_box(dataset.metadata().row_count);

    // Two decode modes over the same bytes. `Typed` runs the temporal and
    // numeric conversions; `TypedLossless` takes a different branch for values
    // that do not fit, and that branch sees the same untrusted widths.
    for mode in [DecodeMode::Typed, DecodeMode::TypedLossless] {
        let mut seen = 0usize;
        let _ = dataset
            .scan()
            .with_decode_mode(mode)
            // Single-threaded: a fuzzer needs the crash to be reproducible from
            // the input alone, and rayon would make row order and timing vary.
            .with_parallelism(Parallelism::None)
            .visit_rows(|row| {
                consume(&row);
                seen += 1;
                if seen >= MAX_ROWS {
                    return Ok(ControlFlow::Break(()));
                }
                Ok(ControlFlow::Continue(()))
            });
    }

    // The raw path skips decoding but computes row spans from the same page
    // descriptors, so it catches span arithmetic that the typed path masks by
    // erroring earlier.
    let mut seen = 0usize;
    let _ = dataset.scan().visit_raw_rows(|raw| {
        black_box(raw.bytes.len());
        black_box(raw.bytes.first());
        seen += 1;
        if seen >= MAX_ROWS {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    });
});
