#!/usr/bin/env Rscript
# Time in-memory SAS7BDAT reads for readsas and haven.
# Usage: bench_r.R <path> <iters> [tools]   tools: comma list {readsas,haven}
suppressWarnings(suppressMessages({
  ok_readsas <- requireNamespace("readsas", quietly = TRUE)
  ok_haven <- requireNamespace("haven", quietly = TRUE)
}))

args <- commandArgs(trailingOnly = TRUE)
path <- args[[1]]
iters <- if (length(args) >= 2) as.integer(args[[2]]) else 5L
tools <- if (length(args) >= 3) strsplit(args[[3]], ",")[[1]] else c("readsas", "haven")

bench <- function(label, fn) {
  rows <- tryCatch(nrow(fn(path)), error = function(e) {
    cat(sprintf("RESULT tool=%s file=%s ERROR=%s\n", label, basename(path), conditionMessage(e)))
    return(NA)
  })
  if (is.na(rows)) return(invisible())
  ts <- numeric(iters)
  for (i in seq_len(iters)) {
    ts[i] <- as.numeric(system.time(fn(path))["elapsed"])
  }
  ts <- sort(ts)
  mb <- file.info(path)$size / 1e6
  med <- ts[ceiling(length(ts) / 2)]
  cat(sprintf(
    "RESULT tool=%s file=%s min=%.3f med=%.3f mbps=%.1f rows=%d\n",
    label, basename(path), ts[1], med, mb / ts[1], rows
  ))
}

readers <- list(
  readsas = function(p) readsas::read_sas(p),
  haven = function(p) haven::read_sas(p)
)
for (t in tools) {
  if (t %in% names(readers)) bench(t, readers[[t]])
}
