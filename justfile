set shell := ["/bin/sh", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

catalog sample_rows="256" out="fixtures/fixture_catalog.local.json":
    @cargo run --release -p sas7bdat-profiler --bin fixture_catalog -- fixtures/raw_data fixtures/ahs2013n.sas7bdat --sample-rows {{sample_rows}} --out {{out}}

catalog-stdout sample_rows="64":
    @cargo run --release -p sas7bdat-profiler --bin fixture_catalog -- fixtures/raw_data fixtures/ahs2013n.sas7bdat --sample-rows {{sample_rows}}

correctness-all:
    @cargo test -q --test fixture_smoke

profile fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @cargo run --release -p sas7bdat-profiler --bin fixture_profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}}

string-profile fixture sample_rows="2048" top="12":
    @cargo run --release -p sas7bdat-profiler --bin fixture_string_profile -- --fixture {{fixture}} --sample-rows {{sample_rows}} --top {{top}}

profile-rss fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @/bin/zsh -lc 'set +e; tmp="$(mktemp)"; /usr/bin/time -l cargo run --release -p sas7bdat-profiler --bin fixture_profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}} 2>"$tmp"; code=$?; grep -v "^time: sysctl kern.clockrate: Operation not permitted$" "$tmp" >&2 || true; rm -f "$tmp"; if [[ "$code" -eq 1 ]]; then exit 0; fi; exit "$code"'

profile-sample fixture mode projection="full" repeat="50" limit="0" batch_rows="256" seconds="5" out="tmp/profile.sample.txt" io_backend="auto":
    @mkdir -p "$(dirname {{out}})"
    @/bin/zsh -lc 'cargo run --release -p sas7bdat-profiler --bin fixture_profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}} >/tmp/sas7bdat-fixture-profile.json & pid=$!; sleep 0.5; /usr/bin/sample "$pid" {{seconds}} -file {{out}} >/dev/null 2>&1 || true; wait "$pid"; cat /tmp/sas7bdat-fixture-profile.json'

profile-leaks fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @leaks --atExit -- cargo run --release -p sas7bdat-profiler --bin fixture_profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}}

bench-tags tags projection="full" max_fixtures="999" catalog="fixtures/fixture_catalog.local.json":
    @/bin/zsh -lc 'args=(${=CRITERION_ARGS:-}); BENCH_TAGS={{tags}} BENCH_PROJECTION={{projection}} BENCH_CATALOG={{catalog}} BENCH_MAX_FIXTURES={{max_fixtures}} cargo bench --bench scan_hotpaths -- "${args[@]}"'

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
    @cargo bench --bench compression_matrix -- 'top3_target/'
    @python3 scripts/update_top3_bench_table.py

batch-family-stats fixture batch_rows="256":
    @BATCH_ROWS={{batch_rows}} cargo run --release --example batch_family_stats -- {{fixture}}

batch-family-stats-target batch_rows="256" max_files="999999" top="10":
    @BATCH_ROWS={{batch_rows}} MAX_FILES={{max_files}} TOP={{top}} cargo run --release --example batch_family_stats_target

hotpath-typed-batches-target:
    @/bin/zsh -lc 'out="${HOTPATH_OUTPUT_PATH:-target/criterion/hotpath/typed_batches_target.json}"; mkdir -p "$(dirname "$out")"; BATCH_ROWS="${BATCH_ROWS:-256}" MAX_FILES="${MAX_FILES:-999999}" HOTPATH_OUTPUT_PATH="$out" cargo run --release --features hotpath-profile --example hotpath_typed_batches_target'

hotpath-typed-batches-top3:
    @/bin/zsh -lc 'out="${HOTPATH_OUTPUT_PATH:-target/criterion/hotpath/typed_batches_top3_target.json}"; mkdir -p "$(dirname "$out")"; BATCH_ROWS="${BATCH_ROWS:-256}" MAX_FILES=3 HOTPATH_OUTPUT_PATH="$out" cargo run --release --features hotpath-profile --example hotpath_typed_batches_target'
