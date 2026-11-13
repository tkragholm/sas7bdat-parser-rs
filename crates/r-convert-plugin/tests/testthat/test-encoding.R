# The core's tracked people_nonascii.sas7bdat declares windows-1252 and holds the
# byte 0xE9 in its first GENDER cell: "é" there, "ι" in ISO-8859-7, not UTF-8.
nonascii <- function() {
  path <- file.path("..", "..", "..", "sas7bdat", "tests", "fixtures",
                    "people_nonascii.sas7bdat")
  if (!file.exists(path)) skip("tracked test file not present")
  normalizePath(path)
}

test_that("encoding and strict reach the conversion", {
  out <- tempfile()
  on.exit(unlink(out, recursive = TRUE))

  greek <- convert_sas(nonascii(), out, sink = "csv", encoding = "iso-8859-7")
  expect_identical(greek$status, "ok")
  csv <- readLines(greek$output, encoding = "UTF-8")
  expect_match(csv[2], "ι", fixed = TRUE)

  strict <- convert_sas(nonascii(), out, sink = "csv", encoding = "utf-8",
                        strict = TRUE, overwrite = TRUE)
  expect_identical(strict$status, "error")
  expect_match(strict$error, "decode")

  expect_error(convert_sas(nonascii(), out, encoding = "not-an-encoding"), "encoding")
  expect_error(convert_sas(nonascii(), out, strict = NA), "strict")
})
