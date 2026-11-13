# Extracted from test-api.R:21

# test -------------------------------------------------------------------------
f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
skip_if(f == "", "bundled fixture not installed")
expect_error(read_sas7bdat(f, categorical = "yes"), "`categorical`")
expect_error(read_sas7bdat(f, categorical = NA), "`categorical`")
expect_error(read_sas7bdat(f, categorical = c(TRUE, FALSE)), "`categorical`")
expect_error(read_sas7bdat(f, catalog = 42), "`catalog`")
expect_error(read_sas7bdat(42), "`path`")
expect_error(read_sas7bdat(c(f, f)), "`path`")
