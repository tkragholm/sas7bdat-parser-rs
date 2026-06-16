// Forwards R's package init to the Rust-side routine registration emitted by
// `extendr_module!`. Keeping a C translation unit in `src/` ensures R links the
// static library and does not garbage-collect the registration symbols.

// Suppress a known clang warning about an empty translation unit on some
// toolchains when the macro below is the only content.
void R_init_readsas_extendr(void *dll);

void R_init_readsas(void *dll) {
    R_init_readsas_extendr(dll);
}
