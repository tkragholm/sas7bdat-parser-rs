# The column plan is checked through CSV output, which base R reads. The Parquet
# types it sets are covered by the Rust tests of sas7bdat-convert.

fixture <- function() system.file("extdata", "mix_and_missing.sas7bdat", package = "fastsasconvert")

read_out <- function(res) utils::read.csv(res$output, colClasses = "character")

test_that("names, renames and constant columns reach the output", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  out <- file.path(tempdir(), "columns-csv")
  on.exit(unlink(out, recursive = TRUE), add = TRUE)

  res <- convert_sas(f, out, sink = "csv", columns = sas_columns(
    names = "upper", rename = c(x1 = "row_id"),
    add = c(register = "mixed"), source_column = "source_file"
  ))

  expect_equal(res$status, "ok")
  csv <- read_out(res)
  expect_named(csv, c("row_id", "X2", "X3", "X4", "X5", "register", "source_file"))
  expect_equal(nrow(csv), 48L)
  expect_true(all(csv$register == "mixed"))
  expect_true(all(csv$source_file == f))
})

test_that("output_files names each destination in input order", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  dir <- file.path(tempdir(), "columns-files")
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  copy <- file.path(dir, "in", "zzz.sas7bdat")
  dir.create(dirname(copy), recursive = TRUE, showWarnings = FALSE)
  file.copy(f, copy)
  targets <- file.path(dir, "out", c("first", "second"), "part-a.parquet")

  res <- convert_sas(c(copy, f), output_files = targets)

  expect_equal(res$status, c("ok", "ok"))
  expect_equal(normalizePath(res$output), normalizePath(targets))
  expect_equal(res$input, c(copy, f))
})

test_that("output_files is checked against the inputs", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  expect_error(convert_sas(f, output_files = c("a.parquet", "b.parquet")), "`output_files`")
  expect_error(convert_sas(f, tempdir(), output_files = "a.parquet"), "`output`")
  expect_error(convert_sas(dirname(f), output_files = "a.parquet"), "must be a file")
})

test_that("a rule that fails is a row with the reason", {
  f <- fixture()
  skip_if(f == "", "bundled fixture not installed")
  out <- file.path(tempdir(), "columns-bad")
  on.exit(unlink(out, recursive = TRUE), add = TRUE)

  unknown <- convert_sas(f, out, columns = sas_columns(rename = c(nope = "x")))
  expect_equal(unknown$status, "error")
  expect_match(unknown$error, "`nope`")

  clash <- convert_sas(f, out, overwrite = TRUE,
                       columns = sas_columns(rename = c(x1 = "x2")))
  expect_equal(clash$status, "error")
  expect_match(clash$error, "both be written")

  typed <- convert_sas(f, out, sink = "csv", columns = sas_columns(strings = "large_string"))
  expect_equal(typed$status, "error")
  expect_match(typed$error, "only apply to Parquet")
})

test_that("sas_columns validates its arguments", {
  expect_s3_class(sas_columns(), "fastsasconvert_columns")
  expect_error(sas_columns(names = "title"), "arg")
  expect_error(sas_columns(rename = "x"), "`rename`")
  expect_error(sas_columns(add = c(a = "1", a = "2")), "`add`")
  expect_error(sas_columns(source_column = ""), "`source_column`")
  expect_error(sas_columns(timestamps = "h"), "arg")
  expect_error(sas_columns(times = "clock"), "arg")
  expect_equal(sas_columns()$times, "time_of_day")
  expect_error(convert_sas(fixture(), tempdir(), columns = list()), "`columns`")
  expect_output(print(sas_columns(names = "lower", times = "duration")), "duration")
})
