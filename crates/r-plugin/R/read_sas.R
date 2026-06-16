#' Read a SAS7BDAT file
#'
#' Reads a `.sas7bdat` file into a data frame using the Rust parsing core.
#' Column types follow `haven` conventions: SAS numerics become `double`,
#' character columns become UTF-8 `character`, SAS dates become `Date`,
#' datetimes become `POSIXct` (UTC), and times become `hms`.
#'
#' SAS variable labels are attached as the column `label` attribute. When a
#' value-label catalog is available, labelled columns are returned as
#' `haven_labelled` vectors (a `labels` named vector plus the haven/vctrs class).
#'
#' @param path Path to a `.sas7bdat` file.
#' @param catalog Optional path to a `.sas7bcat` value-label catalog. If `NULL`
#'   (the default), a same-stem `.sas7bcat` sibling next to `path` is used when
#'   present.
#' @return A tibble (or a base `data.frame` if the `tibble` package is not
#'   installed).
#' @export
#' @examples
#' \dontrun{
#' df <- read_sas("path/to/file.sas7bdat")
#' df <- read_sas("data.sas7bdat", catalog = "formats.sas7bcat")
#' }
read_sas <- function(path, catalog = NULL) {
  path <- path.expand(path)
  if (!is.null(catalog)) {
    catalog <- path.expand(catalog)
  }
  df <- read_sas7bdat(path, catalog)
  if (requireNamespace("tibble", quietly = TRUE)) {
    tibble::new_tibble(df, nrow = nrow(df))
  } else {
    df
  }
}
