# Build this Rust crate as usual
win_target := env('WIN_TARGET', 'x86_64-pc-windows-msvc')
py_bin := env('PYTHON_BIN', 'python3.12')
r_bin := env('R_BIN', 'R')
rust_flags := env('RUSTFLAGS', '-C target-cpu=native')
macosx_deployment_target := env('MACOSX_DEPLOYMENT_TARGET', '15.5')

cargo-build:
    cargo build

cargo-build-release:
    cargo build --release

# R package helpers
r-dev-update:
    Rscript -e "source('scripts/R/00_update_package.R')"

r-install-dev:
    Rscript -e "source('scripts/R/01_install_package.R')"

# Build and install the R package with Rust in release mode
r-install-release:
    MACOSX_DEPLOYMENT_TARGET="{{macosx_deployment_target}}" RUSTFLAGS="{{rust_flags}}" SAVVY_PROFILE=release {{r_bin}} CMD INSTALL --clean R-package

# Build and install the R package with the dist-release profile (LTO, 1 codegen unit)
r-install-dist-release:
    MACOSX_DEPLOYMENT_TARGET="{{macosx_deployment_target}}" RUSTFLAGS="{{rust_flags}}" SAVVY_PROFILE=dist-release {{r_bin}} CMD INSTALL --clean R-package

# Cross-compile Windows wheel with maturin + cargo-xwin (requires target installed)
win-wheel:
    maturin build --release --target {{win_target}} --compatibility off -i {{py_bin}}
