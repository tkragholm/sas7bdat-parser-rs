//! Fuzz the `.sas7bcat` value-label parser.
//!
//! A separate format from the dataset reader, reached by `Dataset::attach_catalog`
//! and by `catalog::parse_catalog` directly. It was entirely unfuzzed, and it reads
//! declared counts the same way the dataset header does — `label_count_used` is a u64
//! straight out of the file that sizes a `Vec`.
//!
//! `parse_catalog` takes any `Read + Seek`, so this needs no temp file and stays
//! deterministic. Seed from the two `.sas7bcat` fixtures:
//!
//! ```sh
//! just fuzz-seed-catalog && just fuzz 60 catalog_parse
//! ```

#![no_main]

use std::hint::black_box;
use std::io::Cursor;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(layout) = sas7bdat::catalog::parse_catalog(&mut Cursor::new(data)) else {
        return;
    };

    // Walk what came back. The parser hands out decoded strings and label sets, so
    // touching them is what catches a length that survived parsing but describes
    // something the blob cannot back.
    for set in &layout.label_sets {
        black_box(set.name.len());
        for label in &set.labels {
            black_box(&label.label);
        }
    }
});
