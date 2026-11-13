# Errors carry a class saying whose problem they are, all under `fastsas_error`:
#
#   fastsas_error_argument     a bad argument, or a column that does not exist
#   fastsas_error_io           the file could not be read: missing, permissions, network
#   fastsas_error_format       not a readable SAS7BDAT file, or a corrupt one
#   fastsas_error_decode       text that does not decode, with `strict = TRUE`
#   fastsas_error_unsupported  a part of the format this reader does not handle
#   fastsas_error_internal     a bug in this package
#
# so a loop over many files can catch one kind and let the others stop it:
#
#   tryCatch(read_sas7bdat(f), fastsas_error_format = function(e) NULL)

fastsas_condition <- function(message, class) {
  structure(
    class = c(class, "fastsas_error", "error", "condition"),
    list(message = message, call = NULL)
  )
}

abort_argument <- function(...) {
  stop(fastsas_condition(paste0(...), "fastsas_error_argument"))
}

# The Rust side cannot build an R condition, so its messages carry their class before
# a unit separator. Raise them as classed conditions; any other error passes through.
raise_classed <- function(expr) {
  withCallingHandlers(
    expr,
    error = function(e) {
      if (inherits(e, "fastsas_error")) {
        return()
      }
      parts <- strsplit(conditionMessage(e), "\x1f", fixed = TRUE)[[1]]
      if (length(parts) == 2L && startsWith(parts[[1]], "fastsas_error_")) {
        stop(fastsas_condition(parts[[2]], parts[[1]]))
      }
    }
  )
}
