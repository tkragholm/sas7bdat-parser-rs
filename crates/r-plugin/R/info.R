#' Facts about a SAS7BDAT file, without reading its rows
#'
#' Reads the file header and metadata only, which is fast on any file size.
#' The `columns` element lists each variable with its label and SAS format, as
#' `PROC CONTENTS` does.
#'
#' @param path Path to a `.sas7bdat` file.
#' @param catalog Optional path to a `.sas7bcat` value-label catalog.
#' @param encoding Read the file's text in this encoding instead of the one its
#'   header declares; see [read_sas7bdat()].
#' @return A list with `n_rows` (the rows [read_sas7bdat()] returns, rows SAS
#'   marked as deleted excluded), `n_deleted_rows`, `n_columns`, `encoding`,
#'   `compression`, `table_name`, `file_label`, `created` and `modified` (UTC
#'   date-times from the header), and `columns`: a data frame with one row per
#'   column and the columns `name`, `label`, `format` (as haven's `format.sas`
#'   attribute spells it), `type` (the R class it is read as) and `width` in
#'   bytes.
#' @export
#' @examples
#' info <- sas_info(system.file("extdata", "dtdate.sas7bdat", package = "fastsas"))
#' info$n_rows
#' info$columns
sas_info <- function(path, catalog = NULL, encoding = NULL) {
  path <- check_path(path, "path")
  if (!is.null(catalog)) {
    catalog <- check_path(catalog, "catalog")
  }
  if (!is.null(encoding) &&
      (!is.character(encoding) || length(encoding) != 1L || is.na(encoding))) {
    abort_argument("`encoding` must be NULL or a single encoding name, not ",
                   deparse1(encoding))
  }
  raise_classed(sas7bdat_info_impl(path, catalog, encoding))
}
