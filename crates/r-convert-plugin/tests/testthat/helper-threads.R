# CRAN's check machines allow a package at most two cores. `convert_sas()` uses
# every core when `threads` is not given, so the tests call it through this
# wrapper, which passes two unless a test sets `threads` itself.
convert_sas <- function(..., threads = 2L) {
  fastsasconvert::convert_sas(..., threads = threads)
}
