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
