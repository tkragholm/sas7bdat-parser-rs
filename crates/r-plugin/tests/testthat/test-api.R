test_that("read_sas is an alias for read_sas7bdat", {
  # `haven` also exports `read_sas`, so the primary name is the one that does not
  # collide; the alias exists only for callers who already wrote `read_sas`.
  expect_identical(read_sas, read_sas7bdat)
  expect_true(all(c("read_sas", "read_sas7bdat") %in% getNamespaceExports("fastsas")))
})

test_that("bad arguments are rejected by name", {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  # These used to be silently accepted: `isTRUE()` turned every non-TRUE value
  # into FALSE, so `categorical = "yes"` quietly did nothing.
  expect_error(read_sas7bdat(f, categorical = "yes"), "`categorical`")
  expect_error(read_sas7bdat(f, categorical = NA), "`categorical`")
  expect_error(read_sas7bdat(f, categorical = c(TRUE, FALSE)), "`categorical`")

  # And a bad `catalog` used to be reported as an invalid 'path'.
  expect_error(read_sas7bdat(f, catalog = 42), "`catalog`")
  expect_error(read_sas7bdat(42), "`path`")
  expect_error(read_sas7bdat(c(f, f)), "`path`")
  expect_error(read_sas7bdat(NA_character_), "`path`")
})

test_that("valid arguments still work", {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")
  expect_s3_class(read_sas7bdat(f, categorical = TRUE), "data.frame")
  expect_s3_class(read_sas7bdat(f, categorical = FALSE), "data.frame")
})

test_that("special missings are recovered in both SAS spellings", {
  skip_if_not_installed("haven")
  f <- system.file("extdata", "missing_test.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  # missing_test uses the ordinal spelling (indicator 0xFF-n over `_ . A..Z`).
  # The complement spelling (indicator = !ASCII) has no bundled fixture, so it is
  # covered by the Rust unit tests in src/rust/src/lib.rs; this asserts the
  # end-to-end path still agrees with haven byte for byte.
  df <- read_sas7bdat(f)
  ref <- haven::read_sas(f)
  for (nm in names(df)) {
    expect_identical(haven::na_tag(df[[nm]]), haven::na_tag(ref[[nm]]), info = nm)
  }
})

test_that("variable labels keep the whitespace haven keeps", {
  skip_if_not_installed("haven")
  f <- system.file("extdata", "test_data_win.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  df <- read_sas7bdat(f)
  ref <- haven::read_sas(f)
  for (nm in names(df)) {
    expect_identical(attr(df[[nm]], "label"), attr(ref[[nm]], "label"), info = nm)
  }
})

test_that("io_backend selects a reader and rejects unknown names", {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  # All three must produce identical output -- the backend changes how bytes are
  # fetched, never what they decode to.
  auto <- read_sas7bdat(f)
  for (backend in c("auto", "mmap", "buffered")) {
    expect_identical(read_sas7bdat(f, io_backend = backend), auto, info = backend)
  }

  expect_error(read_sas7bdat(f, io_backend = "nfs"), "arg")
  expect_error(read_sas7bdat(f, io_backend = 1), "arg")
})

test_that("threads bounds decode concurrency without changing results", {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  expect_identical(read_sas7bdat(f, threads = 1), read_sas7bdat(f))
  expect_identical(read_sas7bdat(f, threads = 4), read_sas7bdat(f))

  # Values R would otherwise coerce into something meaningless.
  expect_error(read_sas7bdat(f, threads = 0), "`threads`")
  expect_error(read_sas7bdat(f, threads = -1), "`threads`")
  expect_error(read_sas7bdat(f, threads = 2.5), "`threads`")
  expect_error(read_sas7bdat(f, threads = NA), "`threads`")
  expect_error(read_sas7bdat(f, threads = c(1, 2)), "`threads`")
})
