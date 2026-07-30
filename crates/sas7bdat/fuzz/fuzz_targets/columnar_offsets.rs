//! Fuzz the invariant that the Arrow bridge relies on.
//!
//! `OwnedColumnBuffer::{Utf8, RawBytes}` carry a `TrustedOffsets` whose contract is:
//! starts at zero, never decreases, ends exactly at `data.len()`. The name is the whole
//! problem — it is trusted, not checked. `polars-plugin` used to feed it straight to
//! `Offsets::new_unchecked` behind a `debug_assert`, so in release a violated invariant
//! was an out-of-bounds read rather than an error.
//!
//! That boundary now validates (see `convert.rs`), which turns a scanner bug into a
//! returned `Err` instead of UB. This target attacks the other half: rather than waiting
//! for that `Err` to surface in someone's Polars session, it hunts for an input that
//! produces one. Every variable-width column of every batch is checked against the
//! contract directly.
//!
//! It also checks what `validate_for_values_len` cannot: that the offset count matches
//! the row count, and that no offset exceeds the data buffer — the specific conditions
//! Arrow would index on.
//!
//! ```sh
//! just fuzz-seed-offsets && just fuzz 60 columnar_offsets
//! ```

#![no_main]

use std::hint::black_box;

use libfuzzer_sys::fuzz_target;
use sas7bdat::{Dataset, OwnedColumnBuffer, Parallelism, TrustedOffsets};

/// See the other targets: a corrupt header can claim `u32::MAX` rows over a few KB.
const MAX_BATCH_ROWS: usize = 4096;

/// Assert the full Arrow contract for one variable-width column.
///
/// `validate_for_values_len` is the crate's own check and is what the Polars bridge
/// calls, so a violation here is a real scanner bug. The extra assertions below cover
/// what it does not: Arrow slices `data[offsets[i]..offsets[i + 1]]` for `i` in
/// `0..row_count`, so the offset count and per-offset bounds matter independently of the
/// first/last/monotonic checks.
fn check(offsets: &TrustedOffsets, data_len: usize, row_count: usize, kind: &str) {
    offsets.validate_for_values_len(data_len).unwrap_or_else(|err| {
        panic!("{kind}: scanner produced offsets that violate the Arrow contract: {err}")
    });

    let slice = offsets.as_slice();
    assert_eq!(
        slice.len(),
        row_count + 1,
        "{kind}: {} offsets for {row_count} rows; Arrow needs exactly row_count + 1",
        slice.len()
    );
    for (i, &offset) in slice.iter().enumerate() {
        let offset = usize::try_from(offset)
            .unwrap_or_else(|_| panic!("{kind}: offset {i} is negative: {offset}"));
        assert!(
            offset <= data_len,
            "{kind}: offset {i} is {offset} but the data buffer is {data_len} bytes"
        );
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 1024 {
        return;
    }

    let Ok(dataset) = Dataset::from_bytes(data.to_vec()) else {
        return;
    };

    // Owned batches are what the Polars bridge consumes, so they are the shape whose
    // offsets matter. Bounded to keep a bogus row count from turning the run into a hang.
    let mut rows = 0usize;
    let _ = dataset
        .scan()
        .with_parallelism(Parallelism::None)
        .visit_owned_batches(|batch| {
            rows += batch.row_count;
            for column in &batch.columns {
                match column {
                    OwnedColumnBuffer::Utf8 { offsets, data, .. } => {
                        check(offsets, data.len(), batch.row_count, "Utf8");
                        // The bridge builds a `&str` over these bytes, so validity is
                        // part of the contract too, not just the offsets.
                        assert!(
                            std::str::from_utf8(data).is_ok(),
                            "Utf8 column data is not valid UTF-8"
                        );
                    }
                    OwnedColumnBuffer::RawBytes { offsets, data, .. } => {
                        check(offsets, data.len(), batch.row_count, "RawBytes");
                    }
                    other => {
                        black_box(other);
                    }
                }
            }
            if rows >= MAX_BATCH_ROWS {
                return Ok(std::ops::ControlFlow::Break(()));
            }
            Ok(std::ops::ControlFlow::Continue(()))
        });
});
