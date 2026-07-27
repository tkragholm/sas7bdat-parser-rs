set shell := ["/bin/sh", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

catalog sample_rows="256" out="fixtures/fixture_catalog.local.json":
    @cargo run --release -p sas7bdat-cli --bin sas7bdat-corpus-catalog -- fixtures/raw_data fixtures/ahs2013n.sas7bdat --sample-rows {{sample_rows}} --out {{out}}

catalog-stdout sample_rows="64":
    @cargo run --release -p sas7bdat-cli --bin sas7bdat-corpus-catalog -- fixtures/raw_data fixtures/ahs2013n.sas7bdat --sample-rows {{sample_rows}}

correctness-all:
    @cargo test -q -p sas7bdat --test fixture_smoke

profile fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @cargo run --release -p sas7bdat-cli --bin sas7bdat-fixture-profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}}

string-profile fixture sample_rows="2048" top="12":
    @cargo run --release -p sas7bdat-cli --bin sas7bdat-fixture-string-profile -- --fixture {{fixture}} --sample-rows {{sample_rows}} --top {{top}}

profile-rss fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @/bin/zsh -lc 'set +e; tmp="$(mktemp)"; /usr/bin/time -l cargo run --release -p sas7bdat-cli --bin sas7bdat-fixture-profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}} 2>"$tmp"; code=$?; grep -v "^time: sysctl kern.clockrate: Operation not permitted$" "$tmp" >&2 || true; rm -f "$tmp"; if [[ "$code" -eq 1 ]]; then exit 0; fi; exit "$code"'

profile-sample fixture mode projection="full" repeat="50" limit="0" batch_rows="256" seconds="5" out="tmp/profile.sample.txt" io_backend="auto":
    @mkdir -p "$(dirname {{out}})"
    @/bin/zsh -lc 'cargo run --release -p sas7bdat-cli --bin sas7bdat-fixture-profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}} >/tmp/sas7bdat-fixture-profile.json & pid=$!; sleep 0.5; /usr/bin/sample "$pid" {{seconds}} -file {{out}} >/dev/null 2>&1 || true; wait "$pid"; cat /tmp/sas7bdat-fixture-profile.json'

profile-leaks fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @leaks --atExit -- cargo run --release -p sas7bdat-cli --bin sas7bdat-fixture-profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}}

build-polars-plugin:
    @uvx maturin build --release --manifest-path crates/polars-plugin/Cargo.toml

# Binary wheel for PyPI (`sas7bdat-cli`): ships only the `sas7bdat` command.
build-cli-wheel:
    @uvx maturin build --release --manifest-path crates/sas7bdat-cli/Cargo.toml

check-polars-plugin:
    @cargo check --manifest-path crates/polars-plugin/Cargo.toml

test-polars-plugin:
    @VIRTUAL_ENV="$(pwd)/.venv" uvx maturin develop --release --manifest-path crates/polars-plugin/Cargo.toml --features arrow,extension-module
    @.venv/bin/python -m pytest crates/polars-plugin/tests

test-polars-plugin-rust:
    @cargo test -p sas7bdat-polars --no-default-features --features arrow --lib

test-core:
    @cargo nextest run --release -p sas7bdat -p sas7bdat-cli

test: test-core test-polars-plugin-rust test-polars-plugin

install-polars-reader-baselines:
    @.venv/bin/python -m pip install polars-readstat polars_io

bench-plugin-vs-raw fixture="fixtures/ahs2013n.sas7bdat" columns="CONTROL,DEGREE,LMED" repeat="5" batch_rows="4096" limit="0":
    @VIRTUAL_ENV="$(pwd)/.venv" uvx maturin develop --release --manifest-path crates/polars-plugin/Cargo.toml
    @.venv/bin/python scripts/compare_plugin_vs_raw.py --fixture {{fixture}} --columns {{columns}} --repeat {{repeat}} --batch-rows {{batch_rows}} --limit {{limit}}

bench-plugin-vs-raw-string-heavy fixture="fixtures/raw_data/ahs2013/owner.sas7bdat" columns="__ALL__" repeat="10" batch_rows="4096" limit="0":
    @just bench-plugin-vs-raw {{fixture}} "{{columns}}" {{repeat}} {{batch_rows}} {{limit}}

bench-plugin-vs-raw-corpus repeat="5" batch_rows="4096" limit="0" catalog="fixtures/fixture_catalog.local.json":
    @VIRTUAL_ENV="$(pwd)/.venv" uvx maturin develop --release --manifest-path crates/polars-plugin/Cargo.toml
    @.venv/bin/python scripts/compare_plugin_vs_raw.py --suite corpus-local --catalog {{catalog}} --repeat {{repeat}} --batch-rows {{batch_rows}} --limit {{limit}}

bench-plugin-vs-polars-readers-corpus repeat="5" batch_rows="4096" limit="0" catalog="fixtures/fixture_catalog.local.json":
    @VIRTUAL_ENV="$(pwd)/.venv" uvx maturin develop --release --manifest-path crates/polars-plugin/Cargo.toml
    @.venv/bin/python scripts/compare_plugin_vs_raw.py --suite corpus-local --catalog {{catalog}} --repeat {{repeat}} --batch-rows {{batch_rows}} --limit {{limit}} --external-readers polars-native

bench-plugins-corpus repeat="5" batch_rows="4096" limit="0" catalog="fixtures/fixture_catalog.local.json":
    @VIRTUAL_ENV="$(pwd)/.venv" uvx maturin develop --release --manifest-path crates/polars-plugin/Cargo.toml
    @scripts/bench_plugins.py --suite corpus-local --catalog {{catalog}} --repeat {{repeat}} --batch-rows {{batch_rows}} --limit {{limit}}

bench-plugins-single fixture="fixtures/ahs2013n.sas7bdat" columns="CONTROL,DEGREE,LMED" repeat="5" batch_rows="4096" limit="0":
    @VIRTUAL_ENV="$(pwd)/.venv" uvx maturin develop --release --manifest-path crates/polars-plugin/Cargo.toml
    @scripts/bench_plugins.py --fixture {{fixture}} --columns {{columns}} --repeat {{repeat}} --batch-rows {{batch_rows}} --limit {{limit}}

bench-compare fixture="fixtures/raw_data/other/cars.sas7bdat" mode="both" repeat="3" batch_rows="4096":
    @python3 scripts/compare_simd_vs_old.py --fixture {{fixture}} --mode {{mode}} --repeat {{repeat}} --batch-rows {{batch_rows}}

bench-tags tags projection="full" max_fixtures="999" catalog="fixtures/fixture_catalog.local.json":
    @/bin/zsh -lc 'args=(${=CRITERION_ARGS:-}); BENCH_TAGS={{tags}} BENCH_PROJECTION={{projection}} BENCH_CATALOG={{catalog}} BENCH_MAX_FIXTURES={{max_fixtures}} cargo bench -p sas7bdat --bench scan_hotpaths -- "${args[@]}"'

bench-standard projection="full" max_fixtures="999" catalog="fixtures/fixture_catalog.local.json":
    @just bench-tags benchmark-standard {{projection}} {{max_fixtures}} {{catalog}}

bench-compressed projection="full" max_fixtures="999" catalog="fixtures/fixture_catalog.local.json":
    @just bench-tags compressed {{projection}} {{max_fixtures}} {{catalog}}

bench-string-heavy projection="strings" max_fixtures="999" catalog="fixtures/fixture_catalog.local.json":
    @just bench-tags string-heavy {{projection}} {{max_fixtures}} {{catalog}}

bench-numeric-heavy projection="numeric" max_fixtures="999" catalog="fixtures/fixture_catalog.local.json":
    @just bench-tags numeric-heavy {{projection}} {{max_fixtures}} {{catalog}}

bench-macro projection="full" max_fixtures="999" catalog="fixtures/fixture_catalog.local.json":
    @just bench-tags benchmark-macro {{projection}} {{max_fixtures}} {{catalog}}

update-top3-bench-readme:
    @python3 scripts/update_top3_bench_table.py

bench-top3-readme:
    @cargo bench -p sas7bdat --bench compression_matrix -- 'top3_target/'
    @python3 scripts/update_top3_bench_table.py

batch-family-stats fixture batch_rows="256":
    @BATCH_ROWS={{batch_rows}} cargo run --release -p sas7bdat --example batch_family_stats -- {{fixture}}

batch-family-stats-target batch_rows="256" max_files="999999" top="10":
    @BATCH_ROWS={{batch_rows}} MAX_FILES={{max_files}} TOP={{top}} cargo run --release -p sas7bdat --example batch_family_stats_target

hotpath-typed-batches-target:
    @/bin/zsh -lc 'out="${HOTPATH_OUTPUT_PATH:-target/criterion/hotpath/typed_batches_target.json}"; mkdir -p "$(dirname "$out")"; BATCH_ROWS="${BATCH_ROWS:-256}" MAX_FILES="${MAX_FILES:-999999}" HOTPATH_OUTPUT_PATH="$out" cargo run --release -p sas7bdat --features hotpath-profile --example hotpath_typed_batches_target'

hotpath-typed-batches-top3:
    @/bin/zsh -lc 'out="${HOTPATH_OUTPUT_PATH:-target/criterion/hotpath/typed_batches_top3_target.json}"; mkdir -p "$(dirname "$out")"; BATCH_ROWS="${BATCH_ROWS:-256}" MAX_FILES=3 HOTPATH_OUTPUT_PATH="$out" cargo run --release -p sas7bdat --features hotpath-profile --example hotpath_typed_batches_target'

convert *args:
    @cargo run --manifest-path crates/sas7bdat-cli/Cargo.toml --bin sas7bdat-convert -- {{args}}

inspect *args:
    @cargo run --manifest-path crates/sas7bdat-cli/Cargo.toml --bin sas7bdat-inspect -- {{args}}
