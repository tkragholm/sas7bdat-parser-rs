two_deliveries <- function() {
  f <- system.file("extdata", "people.sas7bdat", package = "fastsas")
  if (f == "") skip("bundled fixture not installed")
  dir <- tempfile()
  dir.create(dir)
  files <- file.path(dir, c("people_2020.sas7bdat", "people_2021.sas7bdat"))
  file.copy(f, files)
  files
}

test_that("several files are read in order and stacked", {
  skip_if_not_installed("vctrs")
  files <- two_deliveries()
  one <- read_sas7bdat(files[1])
  both <- read_sas7bdat(files)
  expect_identical(nrow(both), 10L)
  expect_identical(both[6:10, ], one, ignore_attr = TRUE)
  expect_equal(as.data.frame(both[1:5, ]), as.data.frame(one))
})

test_that("id adds the file of each row", {
  skip_if_not_installed("vctrs")
  files <- two_deliveries()
  both <- read_sas7bdat(files, id = "file")
  expect_identical(names(both)[1], "file")
  expect_identical(both$file, rep(files, each = 5))
  single <- read_sas7bdat(files[1], id = "file")
  expect_identical(unique(single$file), files[1])
})

test_that("skip and n_max count across the files", {
  skip_if_not_installed("vctrs")
  files <- two_deliveries()
  both <- read_sas7bdat(files)
  window <- read_sas7bdat(files, skip = 3, n_max = 4)
  expect_equal(as.data.frame(window), as.data.frame(both[4:7, ]), ignore_attr = TRUE)
  expect_identical(nrow(read_sas7bdat(files, skip = 6)), 4L)
  expect_identical(nrow(read_sas7bdat(files, n_max = 0)), 0L)
})
