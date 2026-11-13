test_that("sas_info reports the file and its columns without reading rows", {
  f <- system.file("extdata", "dtdate.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")
  info <- sas_info(f)
  df <- read_sas7bdat(f)
  expect_identical(info$n_rows, as.double(nrow(df)))
  expect_identical(info$n_columns, as.double(ncol(df)))
  expect_identical(info$columns$name, names(df))
  expect_identical(info$columns$type, rep("POSIXct", 6))
  expect_identical(info$columns$format,
                   unname(vapply(df, function(x) attr(x, "format.sas"), "")))
  expect_s3_class(info$created, "POSIXct")
  expect_true(all(c("encoding", "compression", "table_name", "file_label",
                    "modified", "n_deleted_rows") %in% names(info)))
})

test_that("sas_info counts deleted rows apart", {
  path <- file.path("..", "..", "..", "sas7bdat", "tests", "fixtures", "corpus", "parso",
                    "all_rand_normal_with_deleted2.sas7bdat")
  if (!file.exists(path)) skip("tracked test file not present")
  info <- sas_info(path)
  expect_identical(info$n_rows, 32)
  expect_identical(info$n_deleted_rows, 5)
})
