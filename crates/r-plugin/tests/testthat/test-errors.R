people <- function() {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  if (f == "") skip("bundled fixture not installed")
  f
}

test_that("errors carry a class saying whose problem they are", {
  missing <- file.path(tempdir(), "no-such-file.sas7bdat")
  expect_error(read_sas7bdat(missing), class = "fastsas_error_io")
  expect_error(read_sas7bdat(missing), class = "fastsas_error")

  junk <- tempfile(fileext = ".sas7bdat")
  on.exit(unlink(junk))
  writeBin(as.raw(rep(0x41, 4096)), junk)
  expect_error(read_sas7bdat(junk), class = "fastsas_error_format")

  expect_error(read_sas7bdat(people(), col_select = "NOPE"),
               class = "fastsas_error_argument")
  expect_error(read_sas7bdat(people(), skip = -1), class = "fastsas_error_argument")
  expect_error(read_sas7bdat(people(), encoding = "nope"),
               class = "fastsas_error_argument")
  expect_error(sas_info(missing), class = "fastsas_error_io")
})

test_that("a classed error keeps its message, without the class tag", {
  err <- tryCatch(read_sas7bdat(people(), col_select = "NOPE"), error = identity)
  expect_match(conditionMessage(err), "col_select")
  expect_false(grepl("\x1f", conditionMessage(err), fixed = TRUE))
})

test_that("a loop can skip one kind of failure", {
  junk <- tempfile(fileext = ".sas7bdat")
  on.exit(unlink(junk))
  writeBin(as.raw(rep(0x41, 4096)), junk)
  read_or_null <- function(f) {
    tryCatch(read_sas7bdat(f), fastsas_error_format = function(e) NULL)
  }
  results <- lapply(c(people(), junk), read_or_null)
  expect_identical(nrow(results[[1]]), 5L)
  expect_null(results[[2]])
})
