# Batches reach the R vectors in whatever order the decode threads finish them, and
# each is written at its own row offset. A multi-threaded read must therefore give
# exactly the result of a single-threaded one. The bundled files are too small to
# split into several batches, so this uses larger files from the repository's local
# corpus when they are there.

corpus_file <- function(name) {
  path <- file.path("..", "..", "..", "..", "fixtures", "benchmark", name)
  if (!file.exists(path)) skip(paste("corpus file not present:", name))
  path
}

for (name in c("ahs2013_person.sas7bdat", "ahs2013_topical.sas7bdat")) {
  test_that(paste("eight threads read", name, "exactly as one does"), {
    path <- corpus_file(name)
    one <- read_sas7bdat(path, threads = 1)
    many <- read_sas7bdat(path, threads = 8)
    expect_identical(many, one)
  })

  test_that(paste("eight threads read", name, "as factors exactly as one does"), {
    path <- corpus_file(name)
    one <- read_sas7bdat(path, threads = 1, categorical = TRUE)
    many <- read_sas7bdat(path, threads = 8, categorical = TRUE)
    expect_identical(many, one)
  })
}
