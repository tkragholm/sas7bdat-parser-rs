use sas7bdat::{DecodePolicy, OrderingMode, SasReader, Shape, dataset::VariableKind};
use std::{fs::File, path::Path, time::Instant};

#[derive(Clone, Copy, Debug)]
enum BenchMode {
    Full,
    QueryFull,
    Projection,
    QueryProjection,
    TypedNumeric,
    Raw,
    RawBatched,
    RawStreaming,
    QueryRaw,
    QueryRawBatches,
    QueryFrame,
    QueryFrameArrow,
    Metadata,
}

impl BenchMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "full" => Some(Self::Full),
            "query_full" => Some(Self::QueryFull),
            "projection" => Some(Self::Projection),
            "query_projection" => Some(Self::QueryProjection),
            "typed_numeric" => Some(Self::TypedNumeric),
            "raw" => Some(Self::Raw),
            "raw_batched" => Some(Self::RawBatched),
            "raw_streaming" => Some(Self::RawStreaming),
            "query_raw" => Some(Self::QueryRaw),
            "query_raw_batches" => Some(Self::QueryRawBatches),
            "query_frame" => Some(Self::QueryFrame),
            "query_frame_arrow" => Some(Self::QueryFrameArrow),
            "metadata" => Some(Self::Metadata),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::QueryFull => "query_full",
            Self::Projection => "projection",
            Self::QueryProjection => "query_projection",
            Self::TypedNumeric => "typed_numeric",
            Self::Raw => "raw",
            Self::RawBatched => "raw_batched",
            Self::RawStreaming => "raw_streaming",
            Self::QueryRaw => "query_raw",
            Self::QueryRawBatches => "query_raw_batches",
            Self::QueryFrame => "query_frame",
            Self::QueryFrameArrow => "query_frame_arrow",
            Self::Metadata => "metadata",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum DecodePolicyArg {
    Compat,
    Fast,
}

impl DecodePolicyArg {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "compat" => Some(Self::Compat),
            "fast" => Some(Self::Fast),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Compat => "compat",
            Self::Fast => "fast",
        }
    }
}

#[derive(Debug)]
struct Config {
    path: String,
    mode: BenchMode,
    decode_policy: DecodePolicyArg,
    projection_cols: usize,
    parse_threads: usize,
    raw_batch_rows: usize,
}

#[derive(Debug, Clone, Copy)]
struct BenchStats {
    row_count: u64,
    column_count: usize,
    raw_bytes: u64,
    elapsed_ms: f64,
}

fn use_unordered_parallel_callbacks() -> bool {
    std::env::var("SAS7BDAT_BENCH_PARALLEL_MODE")
        .map(|value| !value.eq_ignore_ascii_case("ordered"))
        .unwrap_or(true)
}

fn print_help() {
    println!("Usage: sas7bdat-rustbench <path-to-sas7bdat> [options]");
    println!();
    println!("Options:");
    println!(
        "  --mode <full|query_full|projection|query_projection|typed_numeric|raw|raw_batched|raw_streaming|query_raw|query_raw_batches|query_frame|query_frame_arrow|metadata>   Benchmark mode (default: full)"
    );
    println!("  --decode-policy <compat|fast>           Decode policy (default: compat)");
    println!(
        "  --projection-cols <N>                   Projected columns for projection mode (default: 8)"
    );
    println!("  --parse-threads <N>                     Parser worker threads (default: 1)");
    println!(
        "  --raw-batch-rows <N>                    Rows per raw_batched callback (default: 1024)"
    );
    println!("  -h, --help                              Show this help");
}

fn parse_args() -> Config {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("Usage: sas7bdat-rustbench <path-to-sas7bdat>");
        std::process::exit(1);
    };

    if path == "-h" || path == "--help" {
        print_help();
        std::process::exit(0);
    }

    let mut mode = BenchMode::Full;
    let mut decode_policy = DecodePolicyArg::Compat;
    let mut projection_cols = 8usize;
    let mut parse_threads = 1usize;
    let mut raw_batch_rows = 1024usize;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "--mode" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --mode");
                    std::process::exit(1);
                };
                mode = BenchMode::parse(&value).unwrap_or_else(|| {
                    eprintln!("invalid --mode value: {value}");
                    std::process::exit(1);
                });
            }
            "--decode-policy" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --decode-policy");
                    std::process::exit(1);
                };
                decode_policy = DecodePolicyArg::parse(&value).unwrap_or_else(|| {
                    eprintln!("invalid --decode-policy value: {value}");
                    std::process::exit(1);
                });
            }
            "--projection-cols" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --projection-cols");
                    std::process::exit(1);
                };
                projection_cols = value.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("invalid --projection-cols value: {value}");
                    std::process::exit(1);
                });
                if projection_cols == 0 {
                    eprintln!("--projection-cols must be > 0");
                    std::process::exit(1);
                }
            }
            "--parse-threads" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --parse-threads");
                    std::process::exit(1);
                };
                parse_threads = value.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("invalid --parse-threads value: {value}");
                    std::process::exit(1);
                });
                if parse_threads == 0 {
                    eprintln!("--parse-threads must be > 0");
                    std::process::exit(1);
                }
            }
            "--raw-batch-rows" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --raw-batch-rows");
                    std::process::exit(1);
                };
                raw_batch_rows = value.parse::<usize>().unwrap_or_else(|_| {
                    eprintln!("invalid --raw-batch-rows value: {value}");
                    std::process::exit(1);
                });
                if raw_batch_rows == 0 {
                    eprintln!("--raw-batch-rows must be > 0");
                    std::process::exit(1);
                }
            }
            unknown => {
                eprintln!("unknown argument: {unknown}");
                std::process::exit(1);
            }
        }
    }

    Config {
        path,
        mode,
        decode_policy,
        projection_cols,
        parse_threads,
        raw_batch_rows,
    }
}

fn open_reader(path: &str) -> SasReader<File> {
    SasReader::open(path).unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {err}");
        std::process::exit(1);
    })
}

const fn decode_policy_from_arg(arg: DecodePolicyArg) -> DecodePolicy {
    match arg {
        DecodePolicyArg::Compat => DecodePolicy::COMPAT,
        DecodePolicyArg::Fast => DecodePolicy::FAST_SCAN,
    }
}

fn benchmark_metadata(path: &str) -> BenchStats {
    let start = Instant::now();
    let reader = open_reader(path);
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });
    BenchStats {
        row_count: 0,
        column_count,
        raw_bytes: 0,
        elapsed_ms,
    }
}

fn benchmark_full(path: &str, decode_policy: DecodePolicyArg, parse_threads: usize) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });

    let start = Instant::now();
    let policy = decode_policy_from_arg(decode_policy);
    let unordered_parallel = parse_threads > 1 && use_unordered_parallel_callbacks();
    let row_count = if unordered_parallel {
        reader
            .scan_rows_parallel_unordered_with_decode_policy(parse_threads, policy, |row| {
                let _ = row;
                Ok(())
            })
            .unwrap_or_else(|err| {
                eprintln!("sas7bdat error: {err}");
                std::process::exit(1);
            })
    } else {
        reader
            .scan_rows_parallel_ordered_with_decode_policy(parse_threads, policy, |row| {
                let _ = row;
                Ok(())
            })
            .unwrap_or_else(|err| {
                eprintln!("sas7bdat error: {err}");
                std::process::exit(1);
            })
    };
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count,
        raw_bytes: 0,
        elapsed_ms,
    }
}

fn benchmark_projection(
    path: &str,
    decode_policy: DecodePolicyArg,
    projection_cols: usize,
    parse_threads: usize,
) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });
    if column_count == 0 {
        eprintln!("dataset has zero columns");
        std::process::exit(1);
    }

    let projection_cols = projection_cols.min(column_count);
    let projection_indices: Vec<usize> = (0..projection_cols).collect();
    let decode_policy = decode_policy_from_arg(decode_policy);

    let start = Instant::now();
    let unordered_parallel = parse_threads > 1 && use_unordered_parallel_callbacks();
    let row_count = if unordered_parallel {
        reader
            .scan_projected_columns_parallel_unordered_with_decode_policy(
                &projection_indices,
                parse_threads,
                decode_policy,
                |_| Ok(()),
            )
            .unwrap_or_else(|err| {
                eprintln!("sas7bdat error: {err}");
                std::process::exit(1);
            })
    } else {
        reader
            .scan_projected_columns_parallel_ordered_with_decode_policy(
                &projection_indices,
                parse_threads,
                decode_policy,
                |_| Ok(()),
            )
            .unwrap_or_else(|err| {
                eprintln!("sas7bdat error: {err}");
                std::process::exit(1);
            })
    };
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count: projection_cols,
        raw_bytes: 0,
        elapsed_ms,
    }
}

fn benchmark_query_full(
    path: &str,
    decode_policy: DecodePolicyArg,
    parse_threads: usize,
) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });
    let start = Instant::now();
    let policy = decode_policy_from_arg(decode_policy);
    let unordered_parallel = parse_threads > 1 && use_unordered_parallel_callbacks();
    let mut query = reader
        .query()
        .shape(Shape::Rows)
        .decode(policy)
        .parallel(parse_threads);
    query = if unordered_parallel {
        query.ordering(OrderingMode::Unordered)
    } else {
        query.ordering(OrderingMode::Ordered)
    };
    let row_count = if unordered_parallel {
        query.scan_unordered(|_| Ok(()))
    } else {
        query.scan_ordered(|_| Ok(()))
    }
    .unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {err}");
        std::process::exit(1);
    });
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count,
        raw_bytes: 0,
        elapsed_ms,
    }
}

fn benchmark_query_projection(
    path: &str,
    decode_policy: DecodePolicyArg,
    projection_cols: usize,
    parse_threads: usize,
) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });
    if column_count == 0 {
        eprintln!("dataset has zero columns");
        std::process::exit(1);
    }
    let projection_cols = projection_cols.min(column_count);
    let projection_indices: Vec<usize> = (0..projection_cols).collect();
    let start = Instant::now();
    let policy = decode_policy_from_arg(decode_policy);
    let unordered_parallel = parse_threads > 1 && use_unordered_parallel_callbacks();
    let mut query = reader
        .query()
        .shape(Shape::Projection)
        .projection(&projection_indices)
        .decode(policy)
        .parallel(parse_threads);
    query = if unordered_parallel {
        query.ordering(OrderingMode::Unordered)
    } else {
        query.ordering(OrderingMode::Ordered)
    };
    let row_count = if unordered_parallel {
        query.scan_unordered(|_| Ok(()))
    } else {
        query.scan_ordered(|_| Ok(()))
    }
    .unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {err}");
        std::process::exit(1);
    });
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count: projection_cols,
        raw_bytes: 0,
        elapsed_ms,
    }
}

fn benchmark_raw(path: &str, decode_policy: DecodePolicyArg, parse_threads: usize) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });

    let start = Instant::now();
    let unordered_parallel = parse_threads > 1 && use_unordered_parallel_callbacks();
    let (row_count, raw_bytes) = if unordered_parallel {
        let stats = match decode_policy {
            DecodePolicyArg::Compat | DecodePolicyArg::Fast => {
                reader.scan_raw_rows_parallel_unordered_with_stats(parse_threads, |_| Ok(()))
            }
        }
        .unwrap_or_else(|err| {
            eprintln!("sas7bdat error: {err}");
            std::process::exit(1);
        });
        (stats.rows, stats.raw_bytes)
    } else {
        let mut raw_bytes = 0u64;
        let row_count = match decode_policy {
            DecodePolicyArg::Compat | DecodePolicyArg::Fast => reader
                .scan_raw_rows_parallel_ordered(parse_threads, |row| {
                    raw_bytes = raw_bytes.saturating_add(row.len() as u64);
                    Ok(())
                }),
        }
        .unwrap_or_else(|err| {
            eprintln!("sas7bdat error: {err}");
            std::process::exit(1);
        });
        (row_count, raw_bytes)
    };
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count,
        raw_bytes,
        elapsed_ms,
    }
}

fn benchmark_raw_batched(
    path: &str,
    decode_policy: DecodePolicyArg,
    parse_threads: usize,
    raw_batch_rows: usize,
) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });

    let start = Instant::now();
    let unordered_parallel = parse_threads > 1 && use_unordered_parallel_callbacks();
    let stats = if unordered_parallel {
        match decode_policy {
            DecodePolicyArg::Compat | DecodePolicyArg::Fast => reader
                .scan_raw_rows_parallel_unordered_batched_with_stats(
                    parse_threads,
                    raw_batch_rows,
                    |_batch| Ok(()),
                ),
        }
    } else {
        match decode_policy {
            DecodePolicyArg::Compat | DecodePolicyArg::Fast => {
                reader.scan_raw_rows_batched_with_stats(raw_batch_rows, |_batch| Ok(()))
            }
        }
    }
    .unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {err}");
        std::process::exit(1);
    });
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count: stats.rows,
        column_count,
        raw_bytes: stats.raw_bytes,
        elapsed_ms,
    }
}

fn benchmark_raw_streaming(path: &str, decode_policy: DecodePolicyArg) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });

    let start = Instant::now();
    let mut row_count = 0u64;
    let mut raw_bytes = 0u64;
    let mut rows = match decode_policy {
        DecodePolicyArg::Compat => reader.stream_rows(),
        DecodePolicyArg::Fast => reader.stream_rows_fast(),
    }
    .unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {err}");
        std::process::exit(1);
    });
    while let Some(row) = rows.try_next().unwrap_or_else(|err| {
        eprintln!("sas7bdat error: {err}");
        std::process::exit(1);
    }) {
        row_count = row_count.saturating_add(1);
        for cell in row.streaming_row() {
            let cell = cell.unwrap_or_else(|err| {
                eprintln!("sas7bdat error: {err}");
                std::process::exit(1);
            });
            raw_bytes =
                raw_bytes.saturating_add(u64::try_from(cell.raw_slice().len()).unwrap_or(0));
        }
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count,
        raw_bytes,
        elapsed_ms,
    }
}

fn benchmark_query_raw(path: &str, parse_threads: usize) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });
    let start = Instant::now();
    let unordered_parallel = parse_threads > 1 && use_unordered_parallel_callbacks();
    let (row_count, raw_bytes) = if unordered_parallel {
        let stats = reader
            .query()
            .shape(Shape::Raw)
            .parallel(parse_threads)
            .ordering(OrderingMode::Unordered)
            .scan_raw_unordered(|_| Ok(()))
            .unwrap_or_else(|err| {
                eprintln!("sas7bdat error: {err}");
                std::process::exit(1);
            });
        (stats.rows, stats.raw_bytes)
    } else {
        let mut raw_bytes = 0u64;
        let row_count = reader
            .query()
            .shape(Shape::Raw)
            .parallel(parse_threads)
            .ordering(OrderingMode::Ordered)
            .scan_raw_ordered(|row| {
                raw_bytes = raw_bytes.saturating_add(row.len() as u64);
                Ok(())
            })
            .unwrap_or_else(|err| {
                eprintln!("sas7bdat error: {err}");
                std::process::exit(1);
            });
        (row_count, raw_bytes)
    };
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count,
        raw_bytes,
        elapsed_ms,
    }
}

fn benchmark_query_raw_batches(
    path: &str,
    parse_threads: usize,
    raw_batch_rows: usize,
) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });
    let start = Instant::now();
    let unordered_parallel = parse_threads > 1 && use_unordered_parallel_callbacks();
    let mut query = reader
        .query()
        .shape(Shape::Raw)
        .parallel(parse_threads)
        .batch_rows(raw_batch_rows);
    query = if unordered_parallel {
        query.ordering(OrderingMode::Unordered)
    } else {
        query.ordering(OrderingMode::Ordered)
    };
    let batches = query
        .collect_raw_batches(raw_batch_rows)
        .unwrap_or_else(|err| {
            eprintln!("sas7bdat error: {err}");
            std::process::exit(1);
        });
    let row_count: u64 = batches
        .iter()
        .map(|batch| u64::try_from(batch.row_count()).unwrap_or(0))
        .sum();
    let raw_bytes: u64 = batches
        .iter()
        .map(|batch| u64::try_from(batch.raw_bytes()).unwrap_or(0))
        .sum();
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count,
        raw_bytes,
        elapsed_ms,
    }
}

fn benchmark_query_frame(
    path: &str,
    decode_policy: DecodePolicyArg,
    parse_threads: usize,
) -> BenchStats {
    let mut reader = open_reader(path);
    let start = Instant::now();
    let frame = reader
        .query()
        .shape(Shape::Frame)
        .decode(decode_policy_from_arg(decode_policy))
        .parallel(parse_threads)
        .ordering(OrderingMode::Ordered)
        .collect_frame()
        .unwrap_or_else(|err| {
            eprintln!("sas7bdat error: {err}");
            std::process::exit(1);
        });
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count: u64::try_from(frame.row_count).unwrap_or(0),
        column_count: frame.columns.len(),
        raw_bytes: 0,
        elapsed_ms,
    }
}

fn benchmark_query_frame_arrow(
    path: &str,
    decode_policy: DecodePolicyArg,
    parse_threads: usize,
) -> BenchStats {
    let mut reader = open_reader(path);
    let start = Instant::now();
    let batch = reader
        .query()
        .shape(Shape::Frame)
        .decode(decode_policy_from_arg(decode_policy))
        .parallel(parse_threads)
        .ordering(OrderingMode::Ordered)
        .collect_frame()
        .and_then(sas7bdat::FrameBatch::into_arrow_record_batch)
        .unwrap_or_else(|err| {
            eprintln!("sas7bdat error: {err}");
            std::process::exit(1);
        });
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count: u64::try_from(batch.num_rows()).unwrap_or(0),
        column_count: batch.num_columns(),
        raw_bytes: 0,
        elapsed_ms,
    }
}

fn benchmark_typed_numeric(path: &str, projection_cols: usize) -> BenchStats {
    let mut reader = open_reader(path);
    let column_count = usize::try_from(reader.metadata().column_count).unwrap_or_else(|_| {
        eprintln!("column count exceeds usize");
        std::process::exit(1);
    });
    if column_count == 0 {
        eprintln!("dataset has zero columns");
        std::process::exit(1);
    }

    let projection_indices: Vec<usize> = reader
        .metadata()
        .variables
        .iter()
        .enumerate()
        .filter_map(|(idx, variable)| match variable.kind {
            VariableKind::Numeric => Some(idx),
            VariableKind::Character => None,
        })
        .take(projection_cols)
        .collect();
    if projection_indices.is_empty() {
        eprintln!("dataset has no numeric columns for typed numeric benchmark");
        std::process::exit(1);
    }

    let start = Instant::now();
    let mut row_count = 0u64;
    let projected_numeric_cols = projection_indices.len();
    let mut raw_bytes = 0u64;
    reader
        .scan_numeric_columns(&projection_indices, |values| {
            row_count = row_count.saturating_add(1);
            raw_bytes = raw_bytes.saturating_add(u64::try_from(values.len() * 8).unwrap_or(0));
            Ok(())
        })
        .unwrap_or_else(|err| {
            eprintln!("sas7bdat error: {err}");
            std::process::exit(1);
        });
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    BenchStats {
        row_count,
        column_count: projected_numeric_cols,
        raw_bytes,
        elapsed_ms,
    }
}

#[allow(clippy::cast_precision_loss)]
fn print_stats(
    path: &str,
    mode: BenchMode,
    decode_policy: DecodePolicyArg,
    projection_cols: usize,
    parse_threads: usize,
    raw_batch_rows: usize,
    stats: BenchStats,
) {
    let elapsed_sec = stats.elapsed_ms / 1000.0;
    let rows_per_sec = if elapsed_sec > 0.0 {
        stats.row_count as f64 / elapsed_sec
    } else {
        0.0
    };
    let raw_mb_per_sec = if stats.raw_bytes > 0 && elapsed_sec > 0.0 {
        (stats.raw_bytes as f64 / 1_048_576.0) / elapsed_sec
    } else {
        0.0
    };
    let input_size_bytes = std::fs::metadata(Path::new(path))
        .map(|meta| meta.len())
        .unwrap_or(0);
    let input_mb_per_sec = if input_size_bytes > 0 && elapsed_sec > 0.0 {
        (input_size_bytes as f64 / 1_048_576.0) / elapsed_sec
    } else {
        0.0
    };

    println!("File           : {path}");
    println!("Mode           : {}", mode.as_str());
    println!("Decode policy  : {}", decode_policy.as_str());
    println!("Parse threads  : {parse_threads}");
    if matches!(mode, BenchMode::RawBatched | BenchMode::QueryRawBatches) {
        println!("Raw batch rows : {raw_batch_rows}");
    }
    if matches!(mode, BenchMode::Projection) {
        println!("Projection cols: {projection_cols}");
    }
    println!("Rows processed : {}", stats.row_count);
    println!("Columns        : {}", stats.column_count);
    if stats.raw_bytes > 0 {
        println!("Raw bytes      : {}", stats.raw_bytes);
        println!("Raw MB/s       : {raw_mb_per_sec:.2}");
    }
    println!("Rows/s         : {rows_per_sec:.2}");
    println!("Input MB/s     : {input_mb_per_sec:.2}");
    println!("Elapsed (ms)   : {:.2}", stats.elapsed_ms);
}

fn main() {
    let config = parse_args();
    let stats = match config.mode {
        BenchMode::Full => benchmark_full(&config.path, config.decode_policy, config.parse_threads),
        BenchMode::QueryFull => {
            benchmark_query_full(&config.path, config.decode_policy, config.parse_threads)
        }
        BenchMode::Projection => benchmark_projection(
            &config.path,
            config.decode_policy,
            config.projection_cols,
            config.parse_threads,
        ),
        BenchMode::QueryProjection => benchmark_query_projection(
            &config.path,
            config.decode_policy,
            config.projection_cols,
            config.parse_threads,
        ),
        BenchMode::TypedNumeric => benchmark_typed_numeric(&config.path, config.projection_cols),
        BenchMode::Raw => benchmark_raw(&config.path, config.decode_policy, config.parse_threads),
        BenchMode::RawBatched => benchmark_raw_batched(
            &config.path,
            config.decode_policy,
            config.parse_threads,
            config.raw_batch_rows,
        ),
        BenchMode::RawStreaming => benchmark_raw_streaming(&config.path, config.decode_policy),
        BenchMode::QueryRaw => benchmark_query_raw(&config.path, config.parse_threads),
        BenchMode::QueryRawBatches => {
            benchmark_query_raw_batches(&config.path, config.parse_threads, config.raw_batch_rows)
        }
        BenchMode::QueryFrame => {
            benchmark_query_frame(&config.path, config.decode_policy, config.parse_threads)
        }
        BenchMode::QueryFrameArrow => {
            benchmark_query_frame_arrow(&config.path, config.decode_policy, config.parse_threads)
        }
        BenchMode::Metadata => benchmark_metadata(&config.path),
    };
    print_stats(
        &config.path,
        config.mode,
        config.decode_policy,
        config.projection_cols,
        config.parse_threads,
        config.raw_batch_rows,
        stats,
    );
}
