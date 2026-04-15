use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[inline]
pub const fn init_profiler_runtime() {}
