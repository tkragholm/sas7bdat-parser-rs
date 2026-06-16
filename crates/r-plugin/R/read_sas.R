#' Read a SAS7BDAT file
#'
#' Reads a `.sas7bdat` file into a data frame using the Rust parsing core.
#' Column types follow `haven` conventions: SAS numerics become `double`,
#' character columns become UTF-8 `character`, SAS dates become `Date`,
#' datetimes become `POSIXct` (UTC), and times become `hms`.
#'
#' @param path Path to a `.sas7bdat` file.
#' @return A tibble (or a base `data.frame` if the `tibble` package is not
#'   installed).
#' @export
#' @examples
#' \dontrun{
#' df <- read_sas("path/to/file.sas7bdat")
#' }
read_sas <- function(path) {
  path <- path.expand(path)
  df <- read_sas7bdat(path)
  if (requireNamespace("tibble", quietly = TRUE)) {
    tibble::new_tibble(df, nrow = nrow(df))
  } else {
    df
  }
}
