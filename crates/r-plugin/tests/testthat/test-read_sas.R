test_that("read_sas returns a typed data frame", {
  f <- system.file("extdata", "mix_and_missing.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  df <- read_sas(f)

  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 48L)
  expect_equal(ncol(df), 5L)
  expect_equal(names(df), c("x1", "x2", "x3", "x4", "x5"))

  # haven-parity column types
  expect_type(df$x1, "double")
  expect_type(df$x2, "double")
  expect_type(df$x3, "character")
  expect_type(df$x5, "character")

  # Missing numbers are NA; missing strings are empty, as in haven
  expect_false(anyNA(df$x1))
  expect_equal(sum(is.na(df$x2)), 16L)
  expect_false(anyNA(df$x3))
  expect_equal(sum(df$x3 == ""), 32L)
  expect_equal(Encoding(df$x5[1]), "unknown") # ascii -> unknown is fine
})

test_that("missing path errors cleanly", {
  expect_error(read_sas("/no/such/file.sas7bdat"), "sas7bdat")
})
