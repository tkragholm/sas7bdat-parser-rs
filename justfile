set shell := ["/bin/zsh", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

catalog:
    @cargo run --release --bin fixture_catalog -- fixtures/raw_data fixtures/ahs2013n.sas7bdat --sample-rows "${SAMPLE_ROWS:-256}" --out "${OUT:-fixtures/fixture_catalog.local.json}"

catalog-stdout:
    @cargo run --release --bin fixture_catalog -- fixtures/raw_data fixtures/ahs2013n.sas7bdat --sample-rows "${SAMPLE_ROWS:-64}"

correctness-all:
    @cargo test -q --test fixture_smoke

profile fixture mode:
    @cargo run --release --bin fixture_profile -- --fixture {{fixture}} --mode {{mode}} --projection "${PROJECTION:-full}" --repeat "${REPEAT:-1}" --limit "${LIMIT:-0}" --batch-rows "${BATCH_ROWS:-256}"

profile-rss fixture mode:
    @/bin/zsh -lc 'set +e; tmp="$(mktemp)"; /usr/bin/time -l cargo run --release --bin fixture_profile -- --fixture {{fixture}} --mode {{mode}} --projection "${PROJECTION:-full}" --repeat "${REPEAT:-1}" --limit "${LIMIT:-0}" --batch-rows "${BATCH_ROWS:-256}" 2>"$tmp"; code=$?; grep -v "^time: sysctl kern.clockrate: Operation not permitted$" "$tmp" >&2 || true; rm -f "$tmp"; if [[ "$code" -eq 1 ]]; then exit 0; fi; exit "$code"'

profile-sample fixture mode:
    @mkdir -p "$(dirname "${OUT:-tmp/profile.sample.txt}")"
    @/bin/zsh -lc 'cargo run --release --bin fixture_profile -- --fixture {{fixture}} --mode {{mode}} --projection "${PROJECTION:-full}" --repeat "${REPEAT:-50}" --limit "${LIMIT:-0}" --batch-rows "${BATCH_ROWS:-256}" >/tmp/sas7bdat-fixture-profile.json & pid=$!; sleep 0.5; /usr/bin/sample "$pid" "${SECONDS:-5}" -file "${OUT:-tmp/profile.sample.txt}" >/dev/null 2>&1 || true; wait "$pid"; cat /tmp/sas7bdat-fixture-profile.json'

profile-leaks fixture mode:
    @leaks --atExit -- cargo run --release --bin fixture_profile -- --fixture {{fixture}} --mode {{mode}} --projection "${PROJECTION:-full}" --repeat "${REPEAT:-1}" --limit "${LIMIT:-0}" --batch-rows "${BATCH_ROWS:-256}"

bench-tags tags:
    @/bin/zsh -lc 'args=(${=CRITERION_ARGS:-}); BENCH_TAGS={{tags}} BENCH_PROJECTION="${PROJECTION:-full}" BENCH_CATALOG="${BENCH_CATALOG:-fixtures/fixture_catalog.local.json}" BENCH_MAX_FIXTURES="${BENCH_MAX_FIXTURES:-999}" cargo bench --bench scan_hotpaths -- "${args[@]}"'
