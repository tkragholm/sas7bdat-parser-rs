# Simple dev flow for Rust core + R savvy wrapper

# Usage:
# - Override R_PKG_DIR at runtime if needed:
#   just R_PKG_DIR=/path/to/SASreaderRUST r-install
# - Or export once in your shell:
#   export R_PKG_DIR=/path/to/SASreaderRUST

# # Resolve project locations relative to this justfile (overridable via env vars)
# project_root := justfile_directory()
# r_pkg_dir := env_var('R_PKG_DIR', project_root + "/R-package")
# core_dir := env_var('RUST_CORE_DIR', project_root)
# r_bin := env_var('R_BIN', 'R')

# set shell := ["bash", "-cu"]

# _echo-env:
#     @echo "R package dir: {{r_pkg_dir}}"
#     @echo "R binary     : {{r_bin}}"
#     @echo "Core crate   : {{core_dir}}"

# help: _echo-env
#     @echo
#     @echo "Targets:"
#     @echo "  r-init      Initialize savvy in R pkg (once)"
#     @echo "  r-update    Update savvy wrappers + roxygen docs"
#     @echo "  r-build     Build R package (source tarball)"
#     @echo "  r-install   Install R package from source"
#     @echo "  r-load      devtools::load_all() for interactive dev"
#     @echo "  r-test      Run R tests"
#     @echo "  r-check     Run devtools::check()"
#     @echo "  r-link      Add path dep to this Rust crate in wrapper Cargo.toml"
#     @echo "  cargo-build Build core Rust crate (debug)"
#     @echo "  cargo-build-release Build core Rust crate (release)"

# # # One-time initialization if you created an empty R pkg
# # r-init: _echo-env
# #     {{r_bin}} -q -e 'setwd("{{r_pkg_dir}}"); if (requireNamespace("savvy", quietly=TRUE)) { savvy::savvy_init() } else { system("savvy-cli init .") }; if (!requireNamespace("devtools", quietly=TRUE)) message("Install R pkg `devtools` for docs/check/install"); if (requireNamespace("devtools", quietly=TRUE)) devtools::document()'

# # Update C/R wrappers after editing Rust code, then update docs
# r-update: _echo-env
#     {{r_bin}} -q -e 'setwd("{{r_pkg_dir}}"); if (requireNamespace("savvy", quietly=TRUE)) { savvy::savvy_update() } else { system("savvy-cli update .") }; if (requireNamespace("devtools", quietly=TRUE)) devtools::document()'

# # Build (source tarball under current working dir)
# r-build: _echo-env
#     R CMD build "{{r_pkg_dir}}"

# # Install the package from source
# r-install: _echo-env r-update
#     {{r_bin}} -q -e 'setwd("{{r_pkg_dir}}"); libdir <- file.path(getwd(), ".r-lib"); dir.create(libdir, showWarnings = FALSE, recursive = TRUE); system(sprintf("R CMD INSTALL --library=%s .", shQuote(libdir)))'

# # Load package for interactive dev (no install)
# r-load: _echo-env r-update
#     {{r_bin}} -q -e 'setwd("{{r_pkg_dir}}"); if (!requireNamespace("devtools", quietly=TRUE)) stop("Install R pkg `devtools`"), devtools::load_all(quiet = TRUE)'

# # Run tests via devtools if available
# r-test: _echo-env r-update
#     {{r_bin}} -q -e 'setwd("{{r_pkg_dir}}"); if (!requireNamespace("devtools", quietly=TRUE)) stop("Install R pkg `devtools`"), devtools::test()'

# # Run devtools::check()
# r-check: _echo-env r-update
#     {{r_bin}} -q -e 'setwd("{{r_pkg_dir}}"); if (!requireNamespace("devtools", quietly=TRUE)) stop("Install R pkg `devtools`"), devtools::check(error_on = "warning")'


# Build this Rust crate as usual
win_target := env_var('WIN_TARGET', 'x86_64-pc-windows-msvc')
py_bin := env_var('PYTHON_BIN', 'python3.12')

cargo-build:
    cargo build

cargo-build-release:
    cargo build --release

# Cross-compile Windows wheel with maturin + cargo-xwin (requires target installed)
win-wheel:
    maturin build --release --target {{win_target}} --compatibility off -i {{py_bin}}
