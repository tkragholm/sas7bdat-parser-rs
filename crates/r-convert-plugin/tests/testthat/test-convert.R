fixture <- function() system.file("extdata", "people.sas7bdat", package = "fastsasconvert")

test_that("a file converts and reports what it produced", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  out <- file.path(tempdir(), "one")
  on.exit(unlink(out, recursive = TRUE), add = TRUE)

  res <- convert_sas(f, out)

  expect_s3_class(res, "data.frame")
  expect_equal(nrow(res), 1L)
  expect_equal(res$status, "ok")
  expect_true(is.na(res$error))
  expect_equal(res$rows, 5)
  expect_equal(res$columns, 6L)
  expect_true(file.exists(res$output))
  expect_true(res$output_bytes > 0)
  # The measure that matters for a bulk run: nothing was read into R.
  expect_true(res$input_bytes > 0)
})

test_that("the input tree is mirrored under the output root", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  src <- file.path(tempdir(), "tree-in")
  out <- file.path(tempdir(), "tree-out")
  on.exit(unlink(c(src, out), recursive = TRUE), add = TRUE)
  dir.create(file.path(src, "a", "b"), recursive = TRUE, showWarnings = FALSE)
  file.copy(f, file.path(src, "a", "b", "people.sas7bdat"))

  res <- convert_sas(src, out)

  expect_equal(nrow(res), 1L)
  expect_true(file.exists(file.path(out, "a", "b", "people.parquet")))
})

test_that("flatten discards the tree", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  src <- file.path(tempdir(), "flat-in")
  out <- file.path(tempdir(), "flat-out")
  on.exit(unlink(c(src, out), recursive = TRUE), add = TRUE)
  dir.create(file.path(src, "deep"), recursive = TRUE, showWarnings = FALSE)
  file.copy(f, file.path(src, "deep", "people.sas7bdat"))

  convert_sas(src, out, flatten = TRUE)

  expect_true(file.exists(file.path(out, "people.parquet")))
})

test_that("a bad file is a row, not an error", {
  # The property the whole design rests on: one unreadable file must not lose a
  # run over a large tree.
  src <- file.path(tempdir(), "bad-in")
  out <- file.path(tempdir(), "bad-out")
  on.exit(unlink(c(src, out), recursive = TRUE), add = TRUE)
  dir.create(src, showWarnings = FALSE)
  writeLines("not a sas file", file.path(src, "broken.sas7bdat"))
  file.copy(fixture(), file.path(src, "people.sas7bdat"))

  res <- convert_sas(src, out)

  expect_equal(nrow(res), 2L)
  expect_setequal(res$status, c("ok", "error"))
  expect_true(any(!is.na(res$error)))
  expect_equal(sum(res$status == "ok"), 1L)
})

test_that("an existing output is refused unless overwrite is set", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  out <- file.path(tempdir(), "twice")
  on.exit(unlink(out, recursive = TRUE), add = TRUE)

  first <- convert_sas(f, out)
  expect_equal(first$status, "ok")

  again <- convert_sas(f, out)
  expect_equal(again$status, "error")
  expect_match(again$error, "already exists")

  forced <- convert_sas(f, out, overwrite = TRUE)
  expect_equal(forced$status, "ok")
})

test_that("csv and tsv sinks pick the right extension", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  out <- file.path(tempdir(), "delim")
  on.exit(unlink(out, recursive = TRUE), add = TRUE)

  expect_match(convert_sas(f, out, sink = "csv")$output, "\\.csv$")
  expect_match(convert_sas(f, out, sink = "tsv")$output, "\\.tsv$")
})

test_that("the run carries its own summary attributes", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  out <- file.path(tempdir(), "attrs")
  on.exit(unlink(out, recursive = TRUE), add = TRUE)

  res <- convert_sas(f, out)

  expect_false(attr(res, "interrupted"))
  # `discovered` is what tells a caller a run stopped early: it exceeds nrow()
  # only when files were found but not reached.
  expect_equal(attr(res, "discovered"), nrow(res))
})

test_that("bad arguments are rejected by name", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")

  expect_error(convert_sas(character(0)), "`input`")
  expect_error(convert_sas(NA_character_), "`input`")
  expect_error(convert_sas(f, 42), "`output`")
  expect_error(convert_sas(f, tempdir(), recursive = "yes"), "`recursive`")
  expect_error(convert_sas(f, tempdir(), overwrite = NA), "`overwrite`")
  expect_error(convert_sas(f, tempdir(), threads = 0), "`threads`")
  expect_error(convert_sas(f, tempdir(), threads = 2.5), "`threads`")
  expect_error(convert_sas(f, tempdir(), sink = "orc"), "arg")
  expect_error(convert_sas(f, tempdir(), compression = "gzip"), "arg")
  expect_error(convert_sas(f, tempdir(), io_backend = "nfs"), "arg")
})

test_that("finding nothing returns an empty frame rather than erroring", {
  # "Nothing to do" is a legitimate outcome for a bulk operation, and the resume
  # idiom in the docs depends on it: filtering out everything already converted
  # leaves an empty vector, which must not throw.
  empty <- file.path(tempdir(), "empty-dir")
  dir.create(empty, showWarnings = FALSE)
  on.exit(unlink(empty, recursive = TRUE), add = TRUE)

  res <- convert_sas(empty, tempdir())

  expect_s3_class(res, "data.frame")
  expect_equal(nrow(res), 0L)
  expect_equal(names(res)[1:2], c("input", "output"))
  expect_equal(attr(res, "discovered"), 0L)
})
