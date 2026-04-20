#!/usr/bin/env python3
"""Compare current, GitHub snapshot, and dev parser checkouts.

This script benchmarks two aligned paths on the same fixtures:

* `raw`: raw row byte callbacks
* `decoded`: decoded batch materialization

The goal is to make sure we compare the same semantic work across all three
checkouts:

* current `sas7bdat-simd`
* nested GitHub snapshot checkout in this repo
* newer dev checkout in `../sas7bdat-parser-rs`

The script builds tiny throwaway runners in `/tmp` so the benchmark code is
explicit and the result parsing is stable.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from statistics import median
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import Iterable, Literal


Mode = Literal["raw", "decoded"]

DEFAULT_FIXTURES = [
    Path("fixtures/raw_data/ahs2013/topical.sas7bdat"),
    Path("fixtures/raw_data/other/cars.sas7bdat"),
    Path("fixtures/raw_data/csharp/mixed_data_one.sas7bdat"),
]


@dataclass(frozen=True)
class Result:
    rows_per_second: float
    rows_per_second_low: float
    rows_per_second_high: float
    elapsed_ms: float
    rows: int
    batches: int
    samples: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare the current SIMD parser against the GitHub snapshot and newer dev checkout."
    )
    parser.add_argument(
        "--github-repo",
        type=Path,
        default=Path("sas7bdat-parser-rs"),
        help="Path to the GitHub snapshot checkout (default: nested sas7bdat-parser-rs).",
    )
    parser.add_argument(
        "--dev-repo",
        type=Path,
        default=Path("sas7bdat-parser-rs"),
        help="Path to the newer dev checkout (default: sibling ../sas7bdat-parser-rs).",
    )
    parser.add_argument(
        "--fixture",
        action="append",
        type=Path,
        help="Fixture path relative to the current repo root. Can be passed multiple times.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Number of timed runs to average for each parser/fixture pair.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=5,
        help="Number of benchmark invocations to summarize for each parser/fixture pair.",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=4096,
        help="Target batch size for the decoded-batch comparison.",
    )
    parser.add_argument(
        "--mode",
        choices=["raw", "decoded", "both"],
        default="both",
        help="Which aligned benchmark path(s) to run.",
    )
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def fixture_paths(args: argparse.Namespace) -> list[Path]:
    fixtures = args.fixture or DEFAULT_FIXTURES
    resolved: list[Path] = []
    for fixture in fixtures:
        absolute = fixture if fixture.is_absolute() else (repo_root() / fixture).resolve()
        if not absolute.exists():
            raise FileNotFoundError(f"fixture not found: {absolute}")
        resolved.append(absolute)
    return resolved


def build_current_runner(root: Path) -> Path:
    return build_runner(
        label="current",
        dependency_path=root / "crates" / "sas7bdat-simd",
        package_name="sas7bdat-simd",
        crate_name="sas7bdat_simd",
        use_current_api=True,
    )


def build_legacy_runner(label: str, repo_root: Path) -> Path:
    return build_runner(
        label=label,
        dependency_path=repo_root / "crates" / "sas7bdat",
        package_name="sas7bdat",
        crate_name="sas7bdat",
        use_current_api=False,
    )


def build_runner(
    *,
    label: str,
    dependency_path: Path,
    package_name: str,
    crate_name: str,
    use_current_api: bool,
) -> Path:
    project_dir = Path("/tmp") / f"sas7bdat-compare-{label}"
    src_dir = project_dir / "src"
    src_dir.mkdir(parents=True, exist_ok=True)

    cargo_toml = dedent(
        f"""\
        [package]
        name = "sas7bdat-compare-{label}"
        version = "0.1.0"
        edition = "2024"

        [dependencies]
        {crate_name} = {{ package = "{package_name}", path = "{dependency_path.as_posix()}" }}
        serde_json = "1"
        """
    )
    main_rs = build_runner_source(crate_name, use_current_api)

    (project_dir / "Cargo.toml").write_text(cargo_toml)
    src_dir.joinpath("main.rs").write_text(main_rs)

    subprocess.run(
        [
            "cargo",
            "build",
            "--quiet",
            "--release",
            "--manifest-path",
            str(project_dir / "Cargo.toml"),
            "--target-dir",
            str(project_dir / "target"),
        ],
        cwd=project_dir,
        env=cargo_env(),
        check=True,
    )
    return project_dir / "target" / "release" / f"sas7bdat-compare-{label}"


def cargo_env() -> dict[str, str]:
    env = dict(os.environ)
    env["RUSTUP_TOOLCHAIN"] = "nightly"
    return env


def build_runner_source(crate_name: str, use_current_api: bool) -> str:
    if use_current_api:
        return dedent(
            f"""\
            use {crate_name}::{{
                BatchHint,
                Dataset,
            }};
            use std::{{
                env,
                process::ExitCode,
                ops::ControlFlow,
                time::Instant,
            }};

            fn main() -> ExitCode {{
                match run() {{
                    Ok(()) => ExitCode::SUCCESS,
                    Err(message) => {{
                        eprintln!("{{message}}");
                        ExitCode::FAILURE
                    }}
                }}
            }}

            fn run() -> Result<(), String> {{
                let mut args = env::args().skip(1);
                let fixture = args
                    .next()
                    .ok_or_else(|| "missing fixture path".to_owned())?;
                let mode = args.next().ok_or_else(|| "missing mode".to_owned())?;
                let repeat = parse_usize(args.next(), "repeat")?.max(1);
                let batch_rows = parse_usize(args.next(), "batch_rows")?.max(1);
                let ds = Dataset::open(&fixture).map_err(|err| err.to_string())?;
                let expected_rows = ds.metadata().row_count;

                let mut elapsed_total = 0u128;

                let prime_start = Instant::now();
                let mut last = match mode.as_str() {{
                    "raw" => run_raw_once(&ds).map(|rows| (rows, 0usize))?,
                    "decoded" => run_decoded_once(&ds, batch_rows)?,
                    other => return Err(format!("invalid mode: {{other}}")),
                }};
                let _priming_ns = prime_start.elapsed().as_nanos();

                for _ in 0..repeat {{
                    let start = Instant::now();
                    last = match mode.as_str() {{
                        "raw" => run_raw_once(&ds).map(|rows| (rows, 0usize))?,
                        "decoded" => run_decoded_once(&ds, batch_rows)?,
                        other => return Err(format!("invalid mode: {{other}}")),
                    }};
                    elapsed_total += start.elapsed().as_nanos();
                }}

                let (rows_last, batches_last) = last;

                if rows_last as u64 != expected_rows {{
                    return Err(format!(
                        "row count mismatch: expected {{expected_rows}} got {{rows_last}}"
                    ));
                }}

                let elapsed_avg = elapsed_total / repeat as u128;
                let rows_per_second = if elapsed_avg > 0 {{
                    rows_last as f64 / (elapsed_avg as f64 / 1_000_000_000.0)
                }} else {{
                    0.0
                }};

                println!(
                    "{{}}",
                    serde_json::json!({{
                        "mode": mode,
                        "rows_per_second": rows_per_second,
                        "elapsed_ns_avg": elapsed_avg,
                        "rows_last": rows_last,
                        "rows_expected": expected_rows,
                        "batches": batches_last,
                    }})
                );
                Ok(())
            }}

            fn run_raw_once(ds: &Dataset) -> Result<usize, String> {{
                let mut rows = 0usize;
                let mut bytes = 0usize;
                ds.scan()
                    .visit_raw_rows(|row| {{
                        rows += 1;
                        bytes += row.bytes.len();
                        std::hint::black_box(row.row_index);
                        std::hint::black_box(row.bytes.len());
                        Ok(ControlFlow::Continue(()))
                    }})
                    .map_err(|err| err.to_string())?;
                std::hint::black_box(bytes);
                Ok(rows)
            }}

            fn run_decoded_once(ds: &Dataset, batch_rows: usize) -> Result<(usize, usize), String> {{
                let mut rows = 0usize;
                let mut batches = 0usize;
                ds.scan()
                    .with_batch_hint(BatchHint::Rows(batch_rows))
                    .collect_batches()
                    .map_err(|err| err.to_string())?
                    .into_iter()
                    .for_each(|batch| {{
                        rows += batch.row_count;
                        batches += 1;
                        std::hint::black_box(batch.row_base);
                        std::hint::black_box(batch.row_count);
                        std::hint::black_box(batch.columns.len());
                    }});
                std::hint::black_box(batches);
                Ok((rows, batches))
            }}

            fn parse_usize(value: Option<String>, name: &str) -> Result<usize, String> {{
                let value = value.ok_or_else(|| format!("missing {{name}}"))?;
                value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid {{name}}: {{value}}"))
            }}
            """
        )

    return dedent(
        f"""\
        use {crate_name}::SasReader;
        use std::{{
            env,
            process::ExitCode,
            time::Instant,
        }};

        fn main() -> ExitCode {{
            match run() {{
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {{
                    eprintln!("{{message}}");
                    ExitCode::FAILURE
                }}
            }}
        }}

        fn run() -> Result<(), String> {{
            let mut args = env::args().skip(1);
            let fixture = args
                .next()
                .ok_or_else(|| "missing fixture path".to_owned())?;
            let mode = args.next().ok_or_else(|| "missing mode".to_owned())?;
            let repeat = parse_usize(args.next(), "repeat")?.max(1);
            let batch_rows = parse_usize(args.next(), "batch_rows")?.max(1);
            let mut reader = SasReader::open(&fixture).map_err(|err| err.to_string())?;
            let expected_rows = usize::try_from(reader.metadata().row_count)
                .map_err(|_| "row count exceeds usize".to_owned())?;

            let mut elapsed_total = 0u128;

            let prime_start = Instant::now();
                let mut last = match mode.as_str() {{
                    "raw" => run_raw_once(&mut reader).map(|rows| (rows, 0usize))?,
                    "decoded" => run_decoded_once(&mut reader, batch_rows)?,
                    other => return Err(format!("invalid mode: {{other}}")),
                }};
            let _priming_ns = prime_start.elapsed().as_nanos();

            for _ in 0..repeat {{
                let start = Instant::now();
                last = match mode.as_str() {{
                    "raw" => run_raw_once(&mut reader).map(|rows| (rows, 0usize))?,
                    "decoded" => run_decoded_once(&mut reader, batch_rows)?,
                    other => return Err(format!("invalid mode: {{other}}")),
                }};
                elapsed_total += start.elapsed().as_nanos();
            }}

            let (rows_last, batches_last) = last;

            if rows_last != expected_rows {{
                return Err(format!(
                    "row count mismatch: expected {{expected_rows}} got {{rows_last}}"
                ));
            }}

            let elapsed_avg = elapsed_total / repeat as u128;
            let rows_per_second = if elapsed_avg > 0 {{
                rows_last as f64 / (elapsed_avg as f64 / 1_000_000_000.0)
            }} else {{
                0.0
            }};

            println!(
                "{{}}",
                serde_json::json!({{
                    "mode": mode,
                    "rows_per_second": rows_per_second,
                    "elapsed_ns_avg": elapsed_avg,
                    "rows_last": rows_last,
                    "rows_expected": expected_rows,
                    "batches": batches_last,
                    }})
                );
                Ok(())
            }}

        fn run_raw_once(reader: &mut SasReader<std::fs::File>) -> Result<usize, String> {{
            let mut rows = 0usize;
            let mut bytes = 0usize;
            reader
                .scan_raw_rows_with_stats(|row| {{
                    rows += 1;
                    bytes += row.len();
                    std::hint::black_box(row.len());
                    Ok(())
                }})
                .map_err(|err| err.to_string())?;
            std::hint::black_box(bytes);
            Ok(rows)
        }}

        fn run_decoded_once(reader: &mut SasReader<std::fs::File>, batch_rows: usize) -> Result<(usize, usize), String> {{
            let query = reader
                .collect_frame_batches(batch_rows)
                .map_err(|err| err.to_string())?;
            let mut rows = 0usize;
            let mut batches = 0usize;
            query.into_iter().for_each(|batch| {{
                rows += batch.row_count;
                batches += 1;
                std::hint::black_box(batch.row_count);
            }});
            std::hint::black_box(batches);
            Ok((rows, batches))
        }}

        fn parse_usize(value: Option<String>, name: &str) -> Result<usize, String> {{
            let value = value.ok_or_else(|| format!("missing {{name}}"))?;
            value
                .parse::<usize>()
                .map_err(|_| format!("invalid {{name}}: {{value}}"))
        }}
        """
    )


def run_benchmark(
    binary: Path, fixture: Path, mode: Mode, repeat: int, batch_rows: int, samples: int
) -> Result:
    if samples <= 0:
        raise ValueError("samples must be greater than zero")

    payloads: list[dict[str, object]] = []
    for _ in range(samples):
        completed = subprocess.run(
            [
                str(binary),
                str(fixture),
                mode,
                str(repeat),
                str(batch_rows),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        payloads.append(json.loads(completed.stdout.strip()))

    rows_per_second_values = [float(payload["rows_per_second"]) for payload in payloads]
    elapsed_values = [float(payload["elapsed_ns_avg"]) / 1_000_000.0 for payload in payloads]
    payload = payloads[0]
    return Result(
        rows_per_second=float(median(rows_per_second_values)),
        rows_per_second_low=min(rows_per_second_values),
        rows_per_second_high=max(rows_per_second_values),
        elapsed_ms=float(median(elapsed_values)),
        rows=int(payload["rows_last"]),
        batches=int(payload["batches"]),
        samples=samples,
    )


def average(results: Iterable[Result]) -> Result:
    items = list(results)
    if not items:
        raise ValueError("cannot average zero results")
    rows = items[0].rows
    batches = items[0].batches
    samples = items[0].samples
    return Result(
        rows_per_second=sum(item.rows_per_second for item in items) / len(items),
        rows_per_second_low=min(item.rows_per_second_low for item in items),
        rows_per_second_high=max(item.rows_per_second_high for item in items),
        elapsed_ms=sum(item.elapsed_ms for item in items) / len(items),
        rows=rows,
        batches=batches,
        samples=samples,
    )


def mode_header(mode: Mode) -> str:
    return "raw rows" if mode == "raw" else "decoded batches"


def format_rows_per_second(value: float) -> str:
    return f"{value:,.2f}"


def format_rows_per_second_summary(value: float, low: float, high: float) -> str:
    return f"{value:,.2f} [{low:,.2f}..{high:,.2f}]"


def format_batches(mode: Mode, batches: int) -> str:
    return "-" if mode == "raw" else f"{batches}"


def main() -> int:
    args = parse_args()
    root = repo_root()
    github_root = (
        args.github_repo.expanduser().resolve()
        if args.github_repo.is_absolute()
        else (root / args.github_repo).resolve()
    )
    dev_root = (
        args.dev_repo.expanduser().resolve()
        if args.dev_repo.is_absolute()
        else (root.parent / args.dev_repo).resolve()
    )

    current_bin = build_current_runner(root)
    github_bin = build_legacy_runner("github", github_root)
    dev_bin = build_legacy_runner("dev", dev_root)

    fixtures = fixture_paths(args)
    modes: list[Mode] = ["raw", "decoded"] if args.mode == "both" else [args.mode]

    print(f"current repo: {root}")
    print(f"github repo  : {github_root}")
    print(f"dev repo     : {dev_root}")
    print(f"current bin  : {current_bin}")
    print(f"github bin   : {github_bin}")
    print(f"dev bin      : {dev_bin}")
    print(f"repeat       : {args.repeat}")
    print(f"samples      : {args.samples}")
    print(f"batch rows   : {args.batch_rows}")
    print()

    for mode in modes:
        print(f"== {mode_header(mode)} ==")
        print(
            f"{'fixture':48} {'current rows/s':>24} {'github rows/s':>24} "
            f"{'dev rows/s':>24} {'rows':>8} {'batches':>8} {'samples':>8}"
        )
        print("-" * 150)

        for fixture in fixtures:
            current_runs = [
                run_benchmark(current_bin, fixture, mode, args.repeat, args.batch_rows, args.samples)
                for _ in range(1)
            ]
            github_runs = [
                run_benchmark(github_bin, fixture, mode, args.repeat, args.batch_rows, args.samples)
                for _ in range(1)
            ]
            dev_runs = [
                run_benchmark(dev_bin, fixture, mode, args.repeat, args.batch_rows, args.samples)
                for _ in range(1)
            ]

            current_avg = average(current_runs)
            github_avg = average(github_runs)
            dev_avg = average(dev_runs)

            if not (current_avg.rows == github_avg.rows == dev_avg.rows):
                raise RuntimeError(
                    "row count mismatch across targets for "
                    f"{fixture}: current={current_avg.rows} github={github_avg.rows} dev={dev_avg.rows}"
                )

            try:
                relative = fixture.relative_to(root).as_posix()
            except ValueError:
                relative = str(fixture)

            print(
                f"{relative:48} "
                f"{format_rows_per_second_summary(current_avg.rows_per_second, current_avg.rows_per_second_low, current_avg.rows_per_second_high):>24} "
                f"{format_rows_per_second_summary(github_avg.rows_per_second, github_avg.rows_per_second_low, github_avg.rows_per_second_high):>24} "
                f"{format_rows_per_second_summary(dev_avg.rows_per_second, dev_avg.rows_per_second_low, dev_avg.rows_per_second_high):>24} "
                f"{current_avg.rows:8} {format_batches(mode, current_avg.batches):>8} {current_avg.samples:8}"
            )
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
