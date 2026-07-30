//! Fuzz `Dataset::open`, the path-backed reader.
//!
//! Not redundant with `dataset_from_bytes`. `FileSource::Path` reaches code the
//! in-memory source cannot:
//!
//! - the **fused single-pass scan**, which declines unless the source is a path and
//!   more than one worker is available — an entire pipeline (extent reads, the
//!   sequential parse stage, the reorder buffer) that byte-source fuzzing never enters;
//! - `should_try_mmap` / `try_map_file`, and so `FileSource::Mmap`;
//! - `walk_pages` over a real `Read + Seek` file rather than a `Cursor`, which is a
//!   different seek and short-read story.
//!
//! On SIGBUS, which is why the byte target avoids mmap: the hazard is a file truncated
//! *while mapped*. Here the file is written whole and never touched again for the
//! duration of the run, and `Mmap::map` maps exactly its length, so every read is an
//! ordinary bounds-checked slice. Writing the input first is what makes mmap safe to
//! fuzz.
//!
//! ```sh
//! just fuzz-seed-path && just fuzz 60 dataset_open_path
//! ```

#![no_main]

use std::hint::black_box;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::{fs, process};

use libfuzzer_sys::fuzz_target;
use sas7bdat::{Dataset, Parallelism};

/// See `dataset_from_bytes`: a corrupt header can claim `u32::MAX` rows over a few KB.
const MAX_ROWS: usize = 4096;

/// Two workers, not `Auto`. The fused path needs more than one to engage, so a
/// single-threaded scan would silently skip the subsystem this target exists for.
/// Pinning the count keeps thread scheduling as close to reproducible as a threaded
/// pipeline allows — a crash here may still need a couple of replays to land.
const WORKERS: usize = 2;

/// One path for the whole process, reused every iteration. libFuzzer runs the target
/// on a single thread, so there is no race, and rewriting one file beats creating and
/// unlinking hundreds of thousands of them.
fn scratch_path() -> &'static PathBuf {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let mut path = std::env::temp_dir();
        path.push(format!("sas7bdat-fuzz-{}.sas7bdat", process::id()));
        path
    })
}

fuzz_target!(|data: &[u8]| {
    // Below a header there is nothing to test but the magic-number rejection.
    if data.len() < 1024 {
        return;
    }

    let path = scratch_path();
    if fs::write(path, data).is_err() {
        return;
    }

    let Ok(dataset) = Dataset::open(path) else {
        return;
    };

    black_box(dataset.columns().len());
    black_box(dataset.metadata().row_count);

    let mut seen = 0usize;
    let _ = dataset
        .scan()
        .with_parallelism(Parallelism::Threads(WORKERS))
        .visit_rows(|row| {
            for cell in row.iter() {
                black_box(cell);
            }
            seen += 1;
            if seen >= MAX_ROWS {
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        });

    // Columnar batches take a different assembly path from row visiting, and on a path
    // source it is the one the fused scan feeds.
    let mut rows = 0usize;
    let _ = dataset
        .scan()
        .with_parallelism(Parallelism::Threads(WORKERS))
        .visit_batches(|batch| {
            rows += batch.row_count;
            black_box(batch.row_base);
            for column in batch.columns {
                black_box(column.is_nullable());
            }
            if rows >= MAX_ROWS {
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        });
});
