// Emit the platform-specific linker arguments a Python extension module needs.
//
// On macOS this adds `-undefined dynamic_lookup` (scoped to this crate's cdylib via
// `cargo:rustc-cdylib-link-arg`), so the `_PyExc_*`/`Py*` symbols are resolved by the
// interpreter at import time instead of failing to link. Without it, building the cdylib
// standalone (e.g. `cargo build --all-features`, which enables `pyo3/extension-module`)
// fails on macOS, since macOS — unlike Linux — rejects undefined symbols in a cdylib.
//
// It's a no-op on Linux/Windows and only affects this crate's cdylib, so the rest of the
// workspace keeps its normal undefined-symbol link checking. Maturin still drives the
// real wheel build; this just lets plain `cargo` commands link the plugin too.
fn main() {
    pyo3_build_config::add_extension_module_link_args();
}
