# BEF Snapshot Compression Plan (SCD2)

Goal: index and compress annual/quarterly BEF snapshots into a change-based
table that preserves full schema, supports point-in-time lookups, and records
schema coverage over time.

## Inputs and assumptions
- BEF files are Parquet snapshots with full or partial schema.
- `referencetid` is not present in files; derive it from filenames.
- Filename patterns: `befYYYY`, `YYYY`, `befYYYYMM`, `YYYYMM`.
- End-of-period reference dates:
  - `YYYY` -> `YYYY-12-31`
  - `YYYYMM` -> last day of month
- Files may omit/discontinue variables by year.

## Outputs
1) `bef_file_index.parquet`
   - `file_id`: integer
   - `path`: utf8
   - `ref_date`: date32 (derived)
   - `period_kind`: utf8 (`year` | `month`)
   - `schema_hash`: utf8 (stable hash of column names + data types)
   - `columns_present`: utf8 list (comma-separated or JSON)
   - `row_count`: int64

2) `bef_schema_coverage.parquet`
   - `file_id`: int
   - `column_name`: utf8
   - `present`: boolean

3) `bef_scd.parquet`
   - `pnr`: utf8
   - `valid_from`: date32 (derived from file ref date)
   - `valid_to`: date32 (next change ref date, last uses `datacens`)
   - Full BEF schema columns (as available)
   - `source_file_id`: int

## Ingestion and indexing
- Enumerate all BEF files under a path.
- For each file:
  - Parse `ref_date` from filename (error if not ASCII or no date token).
  - Read schema and record `columns_present`.
  - Compute a deterministic `schema_hash` (column name + data type).
  - Count rows (post-read).
  - Append a row to `bef_file_index`.
- Write `bef_schema_coverage` for every `(file_id, column)` pair.

## Snapshot normalization
- Normalize columns to canonical names where available.
- Coerce column types where needed:
  - Date fields -> Date32
  - Categorical fields -> Utf8
- Add derived `ref_date` column to every row for downstream sorting.
- Do not drop columns absent in a file; leave them missing in that snapshot.

## SCD2 construction (full schema)
For each `pnr`, ordered by `ref_date`:
1) Build a stable row signature for the full schema.
   - Only compare columns that are present in both the current and previous file.
   - This avoids false changes when a column is not collected.
2) Start a new SCD row when the signature changes.
3) Set `valid_from = ref_date` for the first row in a run.
4) Set `valid_to = next ref_date` when a change occurs, else at the end use
   `datacens` (e.g., 9999-12-31).
5) Store `source_file_id` for traceability.

## Query patterns
- Point-in-time:
  - `valid_from <= date AND date < valid_to`
- Longitudinal (change points):
  - scan `bef_scd` by `pnr`.
- Distinguish "missing value" vs "not collected" using `bef_schema_coverage`.

## Data quality checks
- Verify monotonic `ref_date` per `pnr`.
- Ensure `valid_to >= valid_from`.
- Track counts: files, rows, distinct `pnr`, change rows per `pnr`.
- Log columns that disappear or appear by year.

## Performance notes
- Partition outputs by year of `valid_from` (or hash bucket of `pnr`).
- Dictionary encode low-cardinality columns.
- Optional: produce a thin snapshot table with `(pnr, ref_date, source_file_id)`
  for auditing and reconstruction.

## CLI / binary shape (proposed)
- New subcommand: `bef-scd`
  - `--bef-path <dir>`
  - `--output-dir <dir>`
  - `--datacens <YYYY-MM-DD>` (default `9999-12-31`)
  - `--limit <N>` (optional, for testing)
