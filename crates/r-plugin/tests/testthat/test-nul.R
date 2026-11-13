# An R string cannot hold NUL. A cell with a NUL in the middle is cut at it rather
# than raising R's "embedded nul in string" error from inside the reader.
test_that("a string cell holding a NUL is cut at the NUL", {
  f <- system.file("extdata", "test_data_win.sas7bdat", package = "fastsas")
  skip_if(f == "", "bundled fixture not installed")

  bytes <- readBin(f, "raw", file.info(f)$size)
  at <- grepRaw("ID1", bytes, fixed = TRUE)
  expect_length(at, 1)
  bytes[at + 1] <- as.raw(0)
  patched <- tempfile(fileext = ".sas7bdat")
  on.exit(unlink(patched))
  writeBin(bytes, patched)

  df <- read_sas7bdat(patched)
  expect_identical(df$ID, c("I", "ID2", "ID3"))
})
