# Contributing

Bug reports, fixture donations, benchmarks, and pull requests are welcome.

## Reporting issues

Open a [GitHub issue](https://github.com/tkragholm/sas7bdat-parser-rs/issues)
with what you did, what you expected, and what happened.

For a file that does not parse, include the output of `sas7bdat info <file>`
and, if you can, a small `.sas7bdat` file that reproduces the problem. If the
data is confidential, the `info` output and the SAS version and compression
setting that wrote the file are often enough.

Questions about usage are welcome in the issue tracker too.

## Development setup

The workspace builds on stable Rust. The exact toolchain is pinned in
`rust-toolchain.toml`, so with [rustup](https://rustup.rs) installed, `cargo`
picks it up. Only the opt-in `nightly-simd` feature needs a nightly toolchain.

```sh
git clone https://github.com/tkragholm/sas7bdat-parser-rs
cd sas7bdat-parser-rs
cargo test -p sas7bdat
```

Common tasks are in the root [`justfile`](justfile): `just test`,
`just catalog`, `just bench-standard`, and others.

- Test fixtures: a few small files are tracked in git. The larger corpus under
  `fixtures/` is not, see `fixtures/README.md`. Tests that need it skip when
  the files are absent, so a fresh clone tests green without downloads.
- Python plugin: built with maturin, see `crates/polars-plugin/README.md`.
- R package: in `crates/r-plugin`. Install with
  `R CMD INSTALL --no-staged-install crates/r-plugin` and test with testthat.

`just doctor` lists what a fresh clone is missing for the test suites (the
Python venv, the fixture corpus, the R packages) and the recipe that fixes
each. Then:

```sh
just test-core   # the Rust core and the CLI
just test        # also the Polars plugin
just test-all    # also both R packages; installs them into your R library
```

Run `just install-hooks` once. It adds a `commit-msg` check, because a commit
subject that git-cliff cannot parse is left out of the changelog without
warning.

## Pull requests

- Keep `cargo clippy` clean. The workspace enables `clippy::pedantic`, and CI
  treats warnings as errors.
- New `unsafe` code needs a `SAFETY:` comment and an explicit
  `#[allow(unsafe_code)]` at the site. Without both, the workspace lint fails
  the build.
- Add a test with any change in behaviour: a unit test, or a pinned fixture
  regression if it concerns the file format.
- Performance claims need numbers. See the benchmark decision policy in
  `fixtures/README.md`.
- Do not add third-party code or test files without clear licence provenance.
  Redistributed fixtures are listed in `THIRD_PARTY_NOTICES.md`.
- Code written with AI assistance is welcome if you have read, tested, and
  understood it. Say in the pull request which tools you used.

## Releases

```sh
just release sas7bdat 0.10.0                      # print the plan; changes nothing
just release sas7bdat 0.10.0 --execute            # do the local steps and push main
just release sas7bdat 0.10.0 --execute --publish  # also push the tags, which publishes
```

A release touches several packages: `sas7bdat-convert` and both R packages
depend on the core by version, and the R packages' `inst/AUTHORS` can only be
regenerated once the new crates are on crates.io. `just release` works out the
order from the manifests, shows which steps are done, and can be rerun after
an interruption.

By contributing, you agree that your contributions are licensed under the
project's [MIT licence](LICENSE).
