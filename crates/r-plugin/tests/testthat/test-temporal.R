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

# `temporal_as_numbers = TRUE` gives the numbers SAS stores, counted from 1 January
# 1960, in place of Date, POSIXct and hms columns.

corpus_file <- function(name) {
  path <- file.path("..", "..", "..", "sas7bdat", "tests", "fixtures", "corpus", name)
  if (!file.exists(path)) skip(paste("tracked test file not present:", name))
  path
}

test_that("dates and datetimes come back as SAS's numbers", {
  path <- corpus_file(file.path("pandas", "datetime.sas7bdat"))
  decoded <- read_sas7bdat(path)
  raw <- read_sas7bdat(path, temporal_as_numbers = TRUE)
  expect_s3_class(decoded$Date1, "Date")
  expect_type(raw$Date1, "double")
  expect_null(attr(raw$Date1, "class"))
  expect_equal(raw$Date1, as.numeric(decoded$Date1 - as.Date("1960-01-01")),
               ignore_attr = TRUE)
  # The SAS format stays, so the column still says what its numbers mean.
  expect_identical(attr(raw$DateTime, "format.sas"), "DATETIME19")
  epoch <- as.POSIXct("1960-01-01", tz = "UTC")
  expect_null(attr(raw$DateTime, "class"))
  expect_equal(raw$DateTime, as.numeric(decoded$DateTime) - as.numeric(epoch),
               ignore_attr = TRUE)
})

test_that("times come back as seconds since midnight", {
  raw <- read_sas7bdat(corpus_file(file.path("parso", "time_formats.sas7bdat")),
                       temporal_as_numbers = TRUE)
  expect_identical(as.vector(raw$TIME), 36610)
})

test_that("temporal_as_numbers must be TRUE or FALSE", {
  path <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  expect_error(read_sas7bdat(path, temporal_as_numbers = NA),
               class = "fastsas_error_argument")
})
