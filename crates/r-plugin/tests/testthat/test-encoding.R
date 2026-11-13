# The core's tracked people_nonascii.sas7bdat declares windows-1252 and holds the
# byte 0xE9 in its first GENDER cell: "é" there, "ι" in ISO-8859-7, not UTF-8.
nonascii <- function() {
  path <- file.path("..", "..", "..", "sas7bdat", "tests", "fixtures",
                    "people_nonascii.sas7bdat")
  if (!file.exists(path)) skip("tracked test file not present")
  path
}

test_that("encoding overrides the declared one", {
  expect_identical(read_sas7bdat(nonascii())$GENDER[1], "é")
  expect_identical(read_sas7bdat(nonascii(), encoding = "iso-8859-7")$GENDER[1],
                   "ι")
  expect_identical(read_sas7bdat(nonascii(), encoding = "wlatin1")$GENDER[1], "é")
  expect_error(read_sas7bdat(nonascii(), encoding = "not-an-encoding"), "encoding")
  expect_error(read_sas7bdat(nonascii(), encoding = 1), "encoding")
})

test_that("strict fails where the default replaces", {
  expect_identical(read_sas7bdat(nonascii(), encoding = "utf-8")$GENDER[1], "�")
  expect_error(read_sas7bdat(nonascii(), encoding = "utf-8", strict = TRUE))
  expect_error(read_sas7bdat(nonascii(), strict = NA), "strict")
})
