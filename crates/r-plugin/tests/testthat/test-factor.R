test_that("categorical = TRUE round-trips; low-card cols become factors", {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  ch <- read_sas(f, categorical = FALSE)
  fa <- read_sas(f, categorical = TRUE)

  any_factor <- FALSE
  for (nm in names(ch)) {
    if (is.character(ch[[nm]])) {
      # Either a factor (low cardinality) or still character (high-cardinality
      # vetoed by the HLL gate) — both must reconstruct the original strings.
      expect_identical(as.character(fa[[nm]]), as.character(ch[[nm]]))
      if (is.factor(fa[[nm]])) any_factor <- TRUE
    } else {
      # non-string columns are unaffected
      expect_identical(fa[[nm]], ch[[nm]])
    }
  }
  expect_true(any_factor) # at least one low-cardinality column became a factor
})

test_that("value-labelled columns are not turned into plain factors", {
  data <- system.file("extdata", "test_data_win.sas7bdat", package = "fastsas")
  cat <- system.file("extdata", "test_formats_win.sas7bcat", package = "fastsas")
  skip_if(data == "" || cat == "", "bundled fixtures not installed")

  fa <- read_sas(data, catalog = cat, categorical = TRUE)
  # SEXA/SEXB are value-labelled -> keep haven_labelled, not factor
  expect_s3_class(fa$SEXA, "haven_labelled")
  expect_false(is.factor(fa$SEXA))
})
