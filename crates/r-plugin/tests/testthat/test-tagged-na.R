test_that("SAS special missings become haven tagged_na", {
  skip_if_not_installed("haven")
  f <- system.file("extdata", "missing_test.sas7bdat", package = "readsas")
  skip_if(f == "", "bundled fixture not installed")

  df <- read_sas(f)

  # missing_test holds .A .B .C .X .Y .Z . ._ and a regular value (1), one per col.
  tags <- vapply(df, function(x) {
    t <- haven::na_tag(x)[1]
    if (is.na(t)) NA_character_ else t
  }, character(1), USE.NAMES = FALSE)

  # var1..6 -> a b c x y z ; var7 -> _ ; var8 -> plain NA ; var9 -> regular value
  expect_equal(tags[1:6], c("a", "b", "c", "x", "y", "z"))
  expect_equal(tags[7], "_")
  expect_true(is.na(tags[8]))           # plain '.' missing, no tag
  expect_false(is.na(df[[9]][1]))       # regular value (not missing)

  # Bit-exact with haven::read_sas.
  ref <- haven::read_sas(f)
  for (nm in names(df)) {
    expect_identical(haven::na_tag(df[[nm]]), haven::na_tag(ref[[nm]]))
  }
})

test_that("value labels keyed on special missings map to tagged_na", {
  skip_if_not_installed("haven")
  data <- system.file("extdata", "missing_test.sas7bdat", package = "readsas")
  cat <- system.file("extdata", "missing_formats.sas7bcat", package = "readsas")
  skip_if(data == "" || cat == "", "bundled fixtures not installed")

  df <- read_sas(data, catalog = cat)
  ref <- haven::read_sas(data, catalog_file = cat)

  # var1 holds .A and its format labels .A as "missing".
  labs <- attr(df$var1, "labels")
  expect_false(is.null(labs))
  expect_equal(names(labs), "missing")
  expect_equal(haven::na_tag(labs), "a")

  # Bit-exact labels vs haven, every column.
  for (nm in names(df)) {
    expect_identical(attr(df[[nm]], "labels"), attr(ref[[nm]], "labels"))
  }
})
