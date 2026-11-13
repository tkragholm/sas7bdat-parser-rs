people <- function() {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  if (f == "") skip("bundled fixture not installed")
  f
}

test_that("col_select picks columns by name, in the order given", {
  df <- read_sas7bdat(people(), col_select = c("GENDER", "ID"))
  expect_identical(names(df), c("GENDER", "ID"))
  expect_identical(df$ID, read_sas7bdat(people())$ID)
})

test_that("col_select takes 1-based positions", {
  all <- read_sas7bdat(people())
  df <- read_sas7bdat(people(), col_select = c(2, 1))
  expect_identical(names(df), names(all)[c(2, 1)])
})

test_that("an unknown column is an error that names the argument", {
  expect_error(read_sas7bdat(people(), col_select = "NOPE"), "col_select")
  expect_error(read_sas7bdat(people(), col_select = 99), "col_select")
})

test_that("skip and n_max select rows", {
  all <- read_sas7bdat(people())
  expect_identical(read_sas7bdat(people(), skip = 1, n_max = 2), all[2:3, ])
  expect_identical(read_sas7bdat(people(), skip = 3), all[4:5, ])
  expect_identical(nrow(read_sas7bdat(people(), skip = 10)), 0L)
})

test_that("n_max = 0 returns the columns with no rows", {
  df <- read_sas7bdat(people(), n_max = 0)
  expect_identical(nrow(df), 0L)
  expect_identical(names(df), names(read_sas7bdat(people())))
  expect_type(df$SBP, "double")
  expect_type(df$GENDER, "character")
})

test_that("bad row counts are rejected by name", {
  expect_error(read_sas7bdat(people(), skip = -1), "skip")
  expect_error(read_sas7bdat(people(), skip = Inf), "skip")
  expect_error(read_sas7bdat(people(), n_max = 1.5), "n_max")
})

test_that("skip and n_max count the rows a read returns, past deleted rows", {
  path <- file.path("..", "..", "..", "sas7bdat", "tests", "fixtures", "corpus", "parso",
                    "all_rand_normal_with_deleted2.sas7bdat")
  if (!file.exists(path)) skip("tracked test file not present")
  all <- read_sas7bdat(path)
  expect_identical(read_sas7bdat(path, skip = 8, n_max = 4), all[9:12, ])
})
