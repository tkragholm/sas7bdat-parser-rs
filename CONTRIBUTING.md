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

## How the code is organised

[DESIGN.md](DESIGN.md) describes the layers, the scan pipelines, and the
measurements behind their limits. Read it before changing how pages are
decoded, scheduled or read.

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
just release            # what is owed, and at what version; changes nothing
just release --execute  # write the versions, lockfiles and changelog drafts
just release --publish  # commit, push, tag and upload; publishes for good
just release-status     # where every package stands
```

The repository ships six packages: the `sas7bdat` and `sas7bdat-convert`
crates, the `sas7bdat-polars` and `sas7bdat-cli` wheels, and the `fastsas` and
`fastsasconvert` R packages. Each has its own version and its own
`<name>-v<version>` tag. A package owes a release when the code it ships has
changed since its tag, which for the wheels includes the crates they compile
in, and for the R packages the crates they vendor.

`--execute` drafts a changelog entry for each package from the commit
subjects. Edit it and delete its marker line; `--publish` will not run while
one is left. `--publish` then uploads the crates and the wheels, refreshes the
R packages' vendored set once the crates are on crates.io, and publishes a
GitHub release for each tag. Every step checks whether it has already
happened, so an interrupted run resumes when run again.

## Support and maintenance

The project is maintained by Tobias Kragholm (Mary Elizabeth's Hospital,
Copenhagen). It was written for register research on Statistics Denmark's
research servers and is used there, so a file that fails to parse is treated
as a bug and fixed first. Maintenance continues after the maintainer's PhD
ends in June 2027.

Issues and pull requests get a first answer within two weeks. Report all
problems as public issues, including a file that crashes the reader. If the
file is confidential, the `sas7bdat info` output described above is enough
to start from.

Each crate and package is versioned on its own and follows semantic
versioning. Breaking changes in the core are listed in
[`crates/sas7bdat/CHANGELOG.md`](crates/sas7bdat/CHANGELOG.md) under the
release that makes them. The minimum supported Rust version is 1.89, checked
in CI, and is raised only in a minor release.

Decisions about format support and the public APIs are made by the
maintainer, in the open on the issue tracker. Regular contributors can be
given commit access.

By contributing, you agree that your contributions are licensed under the
project's [MIT licence](LICENSE).
