test_that("value-label catalog produces haven_labelled columns", {
  data <- system.file("extdata", "test_data_win.sas7bdat", package = "fastsas")
  cat <- system.file("extdata", "test_formats_win.sas7bcat", package = "fastsas")
  skip_if(data == "" || cat == "", "bundled label fixtures not installed")

  df <- read_sas(data, catalog = cat)

  # SEXA / SEXB carry a $A / $B string format defined in the catalog.
  expect_s3_class(df$SEXA, "haven_labelled")
  expect_s3_class(df$SEXB, "haven_labelled")

  # Underlying codes are preserved (not replaced by the label text).
  expect_type(unclass(df$SEXA), "character")

  labs <- attr(df$SEXA, "labels")
  expect_false(is.null(labs))
  # names = human labels, values = codes
  expect_true(all(c("Male", "Female") %in% names(labs)))
})

test_that("without a catalog, formatted columns are plain (no labels attr)", {
  data <- system.file("extdata", "test_data_win.sas7bdat", package = "fastsas")
  skip_if(data == "", "bundled fixture not installed")

  df <- read_sas(data) # no catalog, no same-stem sibling
  expect_null(attr(df$SEXA, "labels"))
  expect_type(df$SEXA, "character")
})

test_that("a missing explicit catalog errors", {
  data <- system.file("extdata", "test_data_win.sas7bdat", package = "fastsas")
  skip_if(data == "", "bundled fixture not installed")
  expect_error(read_sas(data, catalog = "/no/such/catalog.sas7bcat"), "catalog")
})

test_that("a catalog beside the file that cannot be read is a warning", {
  people <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  skip_if(people == "", "bundled fixture not installed")
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))
  file.copy(people, file.path(dir, "people.sas7bdat"))
  writeLines("not a catalog", file.path(dir, "people.sas7bcat"))
  expect_warning(df <- read_sas7bdat(file.path(dir, "people.sas7bdat")), "catalog")
  expect_null(attr(df, "fastsas_warning"))
  expect_identical(nrow(df), 5L)
})
