set shell := ["/bin/sh", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

catalog sample_rows="256" out="fixtures/fixture_catalog.local.json":
    @cargo run --release -p sas7bdat-cli --features dev-tools --bin sas7bdat-corpus-catalog -- fixtures/raw_data fixtures/ahs2013n.sas7bdat --sample-rows {{sample_rows}} --out {{out}}

catalog-stdout sample_rows="64":
    @cargo run --release -p sas7bdat-cli --features dev-tools --bin sas7bdat-corpus-catalog -- fixtures/raw_data fixtures/ahs2013n.sas7bdat --sample-rows {{sample_rows}}

correctness-all:
    @cargo test -q -p sas7bdat --test fixture_smoke

# cargo-fuzz resolves `./fuzz` relative to the cwd, so it only works from the
# crate dir — hence the `cd` in these three wrappers.
# Fuzz the in-memory read path (header -> layout -> descriptors -> row decode).
fuzz seconds="60" target="dataset_from_bytes":
    @cd crates/sas7bdat && cargo fuzz run {{target}} -- -max_len=262144 -max_total_time={{seconds}}

# Starting from real headers is the difference between finding parser bugs and
# burning the run on inputs the magic-number check rejects. Skips files over
# 2 MB: libFuzzer slows badly on large units and the small fixtures already
# cover every layout variant.
# Seed the corpus from fixtures/ (idempotent).
fuzz-seed target="dataset_from_bytes":
    @mkdir -p crates/sas7bdat/fuzz/corpus/{{target}}
    @find fixtures -name '*.sas7bdat' -type f -size -2M -exec sh -c 'cp -n "$1" crates/sas7bdat/fuzz/corpus/{{target}}/"$(shasum -a 256 "$1" | cut -c1-16)"' _ {} \;
    @printf 'corpus units: %s\n' "$(ls crates/sas7bdat/fuzz/corpus/{{target}} | wc -l | tr -d ' ')"

# Seed the path-source target from the same .sas7bdat corpus. That target reaches the
# fused single-pass scan, which declines on in-memory sources, so it explores a
# pipeline `dataset_from_bytes` never enters.
fuzz-seed-path:
    @just fuzz-seed dataset_open_path

# Seed the columnar-offsets target from the same .sas7bdat corpus.
fuzz-seed-offsets:
    @just fuzz-seed columnar_offsets

# Seed the catalog target from the .sas7bcat fixtures. Only two exist, so this corpus
# is thin -- expect it to grow mostly by mutation.
fuzz-seed-catalog:
    @mkdir -p crates/sas7bdat/fuzz/corpus/catalog_parse
    @find fixtures crates/r-plugin/inst/extdata -iname '*.sas7bcat' -type f -exec sh -c 'cp -n "$1" crates/sas7bdat/fuzz/corpus/catalog_parse/"$(shasum -a 256 "$1" | cut -c1-16)"' _ {} \;
    @printf 'corpus units: %s\n' "$(ls crates/sas7bdat/fuzz/corpus/catalog_parse | wc -l | tr -d ' ')"

# The path is relative to the crate dir and must keep the `fuzz/` prefix, e.g.
# fuzz/artifacts/dataset_from_bytes/oom-abc123.
# Reproduce one saved crash artifact.
fuzz-repro artifact target="dataset_from_bytes":
    @cd crates/sas7bdat && cargo fuzz run {{target}} {{artifact}}

profile fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @cargo run --release -p sas7bdat-cli --features dev-tools --bin sas7bdat-fixture-profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}}

string-profile fixture sample_rows="2048" top="12":
    @cargo run --release -p sas7bdat-cli --features dev-tools --bin sas7bdat-fixture-string-profile -- --fixture {{fixture}} --sample-rows {{sample_rows}} --top {{top}}

profile-rss fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @/bin/zsh -lc 'set +e; tmp="$(mktemp)"; /usr/bin/time -l cargo run --release -p sas7bdat-cli --features dev-tools --bin sas7bdat-fixture-profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}} 2>"$tmp"; code=$?; grep -v "^time: sysctl kern.clockrate: Operation not permitted$" "$tmp" >&2 || true; rm -f "$tmp"; if [[ "$code" -eq 1 ]]; then exit 0; fi; exit "$code"'

profile-sample fixture mode projection="full" repeat="50" limit="0" batch_rows="256" seconds="5" out="tmp/profile.sample.txt" io_backend="auto":
    @mkdir -p "$(dirname {{out}})"
    @/bin/zsh -lc 'cargo run --release -p sas7bdat-cli --features dev-tools --bin sas7bdat-fixture-profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}} >/tmp/sas7bdat-fixture-profile.json & pid=$!; sleep 0.5; /usr/bin/sample "$pid" {{seconds}} -file {{out}} >/dev/null 2>&1 || true; wait "$pid"; cat /tmp/sas7bdat-fixture-profile.json'

profile-leaks fixture mode projection="full" repeat="1" limit="0" batch_rows="256" io_backend="auto":
    @leaks --atExit -- cargo run --release -p sas7bdat-cli --features dev-tools --bin sas7bdat-fixture-profile -- --fixture {{fixture}} --mode {{mode}} --projection {{projection}} --repeat {{repeat}} --limit {{limit}} --batch-rows {{batch_rows}} --io-backend {{io_backend}}

build-polars-plugin:
    @uvx maturin build --release --manifest-path crates/polars-plugin/Cargo.toml

# Windows CLI tuned for the deployment server (96-core EPYC, Zen 5), cross-built from
# macOS. `RUSTFLAGS` overrides `[target.x86_64-pc-windows-msvc]` in .cargo/config.toml
# rather than merging with it, so this replaces the v3 baseline with znver5.
#
# Deliberately NOT the default in .cargo/config.toml: that file also governs the PyPI
# wheel builds in wheels.yml, and an AVX-512 binary dies with an illegal instruction on
# any CPU without it — which is every Intel consumer part since Alder Lake. The published
# wheels stay on x86-64-v3 (AVX2).
#
# Verified by `cargo asm`: this puts the string kernel's Simd<u8, 64> and the numeric
# gather's 8-lane u64 tile in one zmm register each, where v3 splits both into two
# 256-bit halves. Zen 5 runs AVX-512 on a full 512-bit datapath with no clock penalty.
build-cli-server:
    @RUSTFLAGS="-C target-cpu=znver5" cargo xwin build --profile dist --target x86_64-pc-windows-msvc -p sas7bdat-cli --bin sas7bdat
    @printf 'built: target/x86_64-pc-windows-msvc/dist/sas7bdat.exe\n'

# Binary wheel for PyPI (`sas7bdat-cli`): ships only the `sas7bdat` command.
build-cli-wheel:
    @uvx maturin build --release --manifest-path crates/sas7bdat-cli/Cargo.toml

check-polars-plugin:
    @cargo check --manifest-path crates/polars-plugin/Cargo.toml

# The venv `test-polars-plugin` builds into. A fresh clone has none, and maturin's
# failure for a missing one ("could not determine version from interpreter name")
# points nowhere near the cause. Idempotent, so `test-polars-plugin` can just call it.
#
# The polars pin is read from the package rather than written here: the extension
# links polars-rust through pyo3-polars, so the installed Python polars has to match
# the version it was built against, and two copies of that number would drift.
# pytest-xdist is not optional -- pytest resolves the *parent* directory's
# pyproject.toml as its config when this repo sits inside another, and that one
# passes `-n`.
setup-python:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -x .venv/bin/python ]; then
      uv venv --python 3.12 .venv
    fi
    pin=$(awk -F'"' '/^dependencies = \[/{print $2}' crates/polars-plugin/pyproject.toml)
    uv pip install --quiet --python .venv/bin/python "$pin" pytest pytest-xdist
    printf 'python env ready: %s\n' "$(.venv/bin/python --version)"

test-polars-plugin: setup-python
    @VIRTUAL_ENV="$(pwd)/.venv" uvx maturin develop --release --manifest-path crates/polars-plugin/Cargo.toml --features arrow,extension-module
    @.venv/bin/python -m pytest crates/polars-plugin/tests

test-polars-plugin-rust:
    @cargo test -p sas7bdat-polars --no-default-features --features arrow --lib

# The R suites, which used to live only in ci.yml. They are the strongest evidence
# the core still behaves, because several of their assertions compare against
# `haven`, and their bundled fixtures are small enough to resolve to a single worker
# -- so they exercise the inline scan that `cargo test` alone barely reaches.
#
# `R CMD INSTALL` compiles the binding against this working tree, through the
# `[patch.crates-io]` in each package's `src/.cargo/config.toml`. It also installs
# into your user R library, which is what CI does and is the point.
test-r:
    #!/usr/bin/env bash
    set -euo pipefail
    for pkg in r-plugin:fastsas r-convert-plugin:fastsasconvert; do
      dir="crates/${pkg%%:*}"
      name="${pkg##*:}"
      printf '==> %s\n' "$name"
      R CMD INSTALL "$dir" > /dev/null
      ( cd "$dir/tests" && Rscript testthat.R )
    done

test-core:
    @cargo nextest run --release -p sas7bdat -p sas7bdat-cli

test: test-core test-polars-plugin-rust test-polars-plugin test-r

# Install the repository's git hooks. Currently one: a commit-msg check that
# rejects a subject git-cliff would silently drop from the changelog.
install-hooks:
    #!/usr/bin/env bash
    set -euo pipefail
    hooks="$(git rev-parse --git-path hooks)"
    ln -sf "$(pwd)/scripts/commit-msg" "$hooks/commit-msg"
    printf 'installed: %s/commit-msg -> scripts/commit-msg\n' "$hooks"

# What is missing on this machine before anything else will make sense.
doctor:
    @python3 scripts/doctor.py

# Plan or run a release, including everything it forces. Prints the plan and stops
# unless --execute is passed; safe to re-run, since it works out where it got to.
#   just release sas7bdat 0.9.0
#   just release sas7bdat 0.9.0 --execute
release crate version *flags:
    @python3 scripts/release.py {{crate}} {{version}} {{flags}}

# Bump one crate's version and everything coupled to it: the Python package's
# pyproject.toml, the lockfiles that pin it -- two of which live outside the
# workspace and only re-resolve when cargo runs from inside their `src/` -- and the
# changelog heading. It does not touch the R bindings' version *requirements*, which
# have to be sequenced against the publish by hand; see the script's docstring.
#   just bump sas7bdat 0.8.1
#   just bump polars-plugin 0.9.1
bump crate version:
    @python3 scripts/bump-version.py {{crate}} {{version}}

# Everything `release-crate.yml` runs before it uploads, against this working tree.
# The workflow re-runs all of it on the tagged commit, but by then the tag exists and
# an irreversible crates.io upload is one green job away — so the useful place to
# find out is here.
#
# Takes a package name, not a directory: `just release-preflight sas7bdat-convert`.
release-preflight crate="sas7bdat":
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> version couplings"
    bash scripts/check-versions.sh
    echo "==> rustfmt"
    cargo fmt --all --check
    echo "==> clippy (workspace)"
    cargo clippy --workspace --all-targets -- -D warnings
    echo "==> clippy (sas7bdat, no default features)"
    cargo clippy -p sas7bdat --all-targets --no-default-features -- -D warnings
    echo "==> tests (workspace)"
    cargo test --workspace
    # Outside the workspace, and `--locked` is the point: it fails on a stale binding
    # lockfile instead of silently re-resolving one.
    echo "==> clippy --locked (R bindings)"
    for dir in crates/r-plugin/src crates/r-convert-plugin/src; do
      ( cd "$dir" && cargo clippy --locked --manifest-path rust/Cargo.toml --all-targets -- -D warnings )
    done
    publishable=$(cargo metadata --format-version 1 --no-deps \
      | python3 -c "import json,sys;n=sys.argv[1];p=next(p for p in json.load(sys.stdin)['packages'] if p['name']==n);print('no' if p.get('publish')==[] else 'yes')" "{{crate}}")
    if [ "$publishable" = "yes" ]; then
      echo "==> package and verify"
      # `--dry-run` refuses a dirty tree, which is right for the real release and
      # wrong for a preflight, whose whole point is to run before the commit.
      if [ -n "$(git status --porcelain)" ]; then
        echo "    tree is dirty, packaging with --allow-dirty (the release will not)"
        cargo publish --dry-run --locked --allow-dirty -p "{{crate}}"
      else
        cargo publish --dry-run --locked -p "{{crate}}"
      fi
    else
      echo "==> {{crate}} is publish = false; it ships as a wheel"
      echo "    build it with: just build-polars-plugin / just build-cli-wheel"
    fi
    echo
    echo "preflight clean for {{crate}}"

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

# Build the network-storage lab image (see test-lab/README.md).
lab-build:
    @test-lab/lab.sh --build

# Run a command against storage that reads like a slow share.
#   just lab 5 340 sas7bdat convert /mnt/in/f.sas7bdat --out-dir /mnt/out --io-backend buffered
lab latency="5" rate="340" *args:
    @LATENCY={{latency}} RATE={{rate}} test-lab/lab.sh {{args}}
