#!/usr/bin/env Rscript
# Read one SAS7BDAT file once with one R reader and print one JSON line.
#
# Usage: read_r.R <tool> <path>    tool: fastsas | haven
#
# The timed region is the read call alone; the package is loaded first and the
# checksum is computed after the clock stops, and only when BENCH_CHECKSUM=1. The checksum matches read_py.py:
# numeric columns give their sum and missing count, character columns the total
# UTF-8 length after removing trailing spaces. Date, datetime and time columns
# are left out, as in read_py.py.
args <- commandArgs(trailingOnly = TRUE)
tool <- args[[1]]
path <- args[[2]]

reader <- switch(tool,
  fastsas = {
    loadNamespace("fastsas")
    function(p) fastsas::read_sas7bdat(p)
  },
  haven = {
    loadNamespace("haven")
    function(p) haven::read_sas(p)
  },
  stop("unknown tool: ", tool)
)

start <- proc.time()[["elapsed"]]
df <- reader(path)
seconds <- proc.time()[["elapsed"]] - start

entries <- character()
want_checksum <- identical(Sys.getenv("BENCH_CHECKSUM"), "1")
for (name in if (want_checksum) names(df) else character()) {
  x <- df[[name]]
  if (inherits(x, c("Date", "POSIXct", "hms", "difftime"))) next
  x <- unclass(x)
  if (is.double(x) || is.integer(x)) {
    total <- sum(as.double(x), na.rm = TRUE)
    entries <- c(entries, sprintf('"%s":["num",%.17g,%d]', name, total, sum(is.na(x))))
  } else if (is.character(x)) {
    lens <- nchar(sub(" +$", "", x), type = "bytes")
    lens[is.na(lens)] <- 0L
    entries <- c(entries, sprintf('"%s":["str",%.0f]', name, sum(as.double(lens))))
  }
}
checksum <- if (want_checksum) sprintf("{%s}", paste(entries, collapse = ",")) else "null"
cat(sprintf(
  '{"tool":"%s","seconds":%.6f,"rows":%d,"cols":%d,"checksum":%s}\n',
  tool, seconds, nrow(df), ncol(df), checksum
))
