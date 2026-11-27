fixture <- function() {
  f <- system.file("extdata", "mix_and_missing.sas7bdat", package = "fastsas")
  if (f == "") skip("bundled fixture not installed")
  f
}

test_that("col_select picks columns by name, in the order given", {
  df <- read_sas7bdat(fixture(), col_select = c("x3", "x1"))
  expect_identical(names(df), c("x3", "x1"))
  expect_identical(df$x1, read_sas7bdat(fixture())$x1)
})

test_that("col_select takes 1-based positions", {
  all <- read_sas7bdat(fixture())
  df <- read_sas7bdat(fixture(), col_select = c(2, 1))
  expect_identical(names(df), names(all)[c(2, 1)])
})

test_that("an unknown column is an error that names the argument", {
  expect_error(read_sas7bdat(fixture(), col_select = "NOPE"), "col_select")
  expect_error(read_sas7bdat(fixture(), col_select = 99), "col_select")
})

test_that("skip and n_max select rows", {
  all <- read_sas7bdat(fixture())
  expect_identical(read_sas7bdat(fixture(), skip = 1, n_max = 2), all[2:3, ])
  expect_identical(read_sas7bdat(fixture(), skip = 45), all[46:48, ])
  expect_identical(nrow(read_sas7bdat(fixture(), skip = 50)), 0L)
})

test_that("n_max = 0 returns the columns with no rows", {
  df <- read_sas7bdat(fixture(), n_max = 0)
  expect_identical(nrow(df), 0L)
  expect_identical(names(df), names(read_sas7bdat(fixture())))
  expect_type(df$x1, "double")
  expect_type(df$x3, "character")
})

test_that("bad row counts are rejected by name", {
  expect_error(read_sas7bdat(fixture(), skip = -1), "skip")
  expect_error(read_sas7bdat(fixture(), skip = Inf), "skip")
  expect_error(read_sas7bdat(fixture(), n_max = 1.5), "n_max")
})

test_that("skip and n_max count the rows a read returns, past deleted rows", {
  path <- file.path("..", "..", "..", "sas7bdat", "tests", "fixtures", "corpus", "parso",
                    "all_rand_normal_with_deleted2.sas7bdat")
  if (!file.exists(path)) skip("tracked test file not present")
  all <- read_sas7bdat(path)
  expect_identical(read_sas7bdat(path, skip = 8, n_max = 4), all[9:12, ])
})
