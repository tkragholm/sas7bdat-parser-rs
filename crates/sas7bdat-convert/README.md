# sas7bdat-convert

Convert SAS7BDAT files to Parquet, CSV or TSV — one file, or a directory tree
mirrored into an output root.

This is the machinery behind the `sas7bdat convert` command, published separately
so other front ends can reach it. It takes plain options and returns plain
outcomes; argument parsing, progress reporting and terminal styling belong to the
caller.

```rust
use sas7bdat_convert::{OutputLayout, RecursionMode, SinkKind};
use sas7bdat_convert::paths::{compute_output_path, discover_inputs};

let inputs = discover_inputs(&["data".into()], RecursionMode::Recursive)?;
let layout = OutputLayout {
    out_dir: Some("parquet".into()),
    flatten: false,
    sink: SinkKind::Parquet,
};

for (root, input) in &inputs {
    let output = compute_output_path(root, input, &layout);
    // -> parquet/<the path below `root`>.parquet
}
# Ok::<(), anyhow::Error>(())
```

Conversion never materialises a whole file: Parquet row groups are encoded across
cores and appended as they fill, so memory tracks the row group rather than the
dataset. That is what makes it usable from an R or Python binding against inputs
far larger than the host's RAM.

## Relationship to the other crates

| Crate | Role |
|-------|------|
| [`sas7bdat`](https://crates.io/crates/sas7bdat) | Reads SAS7BDAT files. No writing, no Parquet. |
| `sas7bdat-convert` | This crate: discovery, output-tree mirroring, and the Parquet/CSV writers. |
| `sas7bdat-cli` | The `sas7bdat` command. Argument parsing and reporting over this crate. |

## License

MIT.
