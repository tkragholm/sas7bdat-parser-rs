test_that("temporal columns are typed by logical type, not buffer variant", {
  f <- system.file("extdata", "dtdate.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled temporal fixture not installed")

  df <- read_sas(f)

  # Every column carries a SAS DATETIME format. Even though several rows hold
  # fractional seconds (which the core surfaces as an F64 fallback buffer), the
  # binding must still type these as POSIXct (haven parity) rather than double.
  expect_s3_class(df[[1]], "POSIXct")
  expect_equal(attr(df[[1]], "tzone"), "UTC")

  # Fractional seconds survive the round-trip (POSIXct is a double).
  secs_since_1960 <- as.numeric(df[[1]]) + 315619200
  expect_true(any(abs(secs_since_1960 - round(secs_since_1960)) > 1e-6, na.rm = TRUE))

  # Known boundary values decode correctly (1582 / 1960 / 1970 test rows).
  yrs <- as.integer(format(df[[1]], "%Y"))
  expect_true(all(c(1582L, 1959L, 1960L, 1969L, 1970L) %in% yrs))
})
