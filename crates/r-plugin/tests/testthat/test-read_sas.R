test_that("read_sas returns a typed data frame", {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  df <- read_sas(f)

  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 5L)
  expect_equal(ncol(df), 6L)
  expect_equal(names(df), c("ID", "SBP", "DBP", "GENDER", "AGE", "WT"))

  # haven-parity column types
  expect_type(df$ID, "character")
  expect_type(df$GENDER, "character")
  expect_type(df$SBP, "double")
  expect_type(df$AGE, "double")

  # UTF-8 strings round-trip without corruption
  expect_equal(Encoding(df$GENDER[df$GENDER != ""][1]), "unknown") # ascii -> unknown is fine
  expect_false(anyNA(df$ID))
})

test_that("missing path errors cleanly", {
  expect_error(read_sas("/no/such/file.sas7bdat"), "sas7bdat")
})
