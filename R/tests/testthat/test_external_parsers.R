test_that("haven comparison", {
  if (Sys.getenv("SAS7BDAT_VERIFY_HAVEN") == "") {
    skip("SAS7BDAT_VERIFY_HAVEN not set")
  }
  if (!requireNamespace("haven", quietly = TRUE)) {
    skip("haven not installed")
  }
  if (!exists("sas7bdat_snapshot_fixture", where = asNamespace("SASreaderRUST"), inherits = FALSE)) {
    skip("R bindings snapshot helper not implemented")
  }

  repo_root <- Sys.getenv("SAS7BDAT_REPO_ROOT")
  if (repo_root == "") {
    repo_root <- normalizePath(file.path(getwd(), "..", ".."), winslash = "/", mustWork = FALSE)
  }
  fixture <- file.path(repo_root, "fixtures", "raw_data", "pandas", "airline.sas7bdat")
  if (!file.exists(fixture)) {
    skip("fixture not available in this checkout")
  }

  snapshot_fun <- get("sas7bdat_snapshot_fixture", envir = asNamespace("SASreaderRUST"))
  actual <- snapshot_fun(fixture)
  reference <- haven::read_sas(fixture)

  expect_true(length(actual$rows) == nrow(reference))
})
