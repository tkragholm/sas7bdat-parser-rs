# SAS deletes a row by marking it, and the header's row count still includes it.
# The result must hold the surviving rows only, and the right ones. The first column
# of these files is a ladder (0..10, then by tens, hundreds and thousands), so the
# values that survive name the deleted rows. Golden values are ReadStat's, as in the
# core's tests/deleted_rows.rs. The files are in the repository's local corpus.

corpus_file <- function(name) {
  path <- file.path("..", "..", "..", "..", "fixtures", "raw_data", "csharp", name)
  if (!file.exists(path)) skip(paste("corpus file not present:", name))
  path
}

cases <- list(
  "all_rand_normal_with_deleted.sas7bdat" = c(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 60, 70, 80, 90, 100, 200, 300, 400,
    500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000
  ),
  "all_rand_normal_with_deleted2.sas7bdat" = c(
    1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 40, 60, 70, 80, 90, 100, 200, 300, 400, 600,
    700, 800, 900, 1000, 2000, 4000, 5000, 6000, 7000, 8000, 9000
  )
)

for (name in names(cases)) {
  test_that(paste("deleted rows are left out of", name), {
    df <- read_sas7bdat(corpus_file(name))
    expect_equal(nrow(df), length(cases[[name]]))
    expect_equal(as.numeric(df[[1]]), cases[[name]])
  })
}
