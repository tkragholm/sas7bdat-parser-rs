"""Type stubs for the sas7bdat_polars Polars IO plugin."""

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import polars as pl

__version__: str
__core_version__: str
PLUGIN_CONTRACT_VERSION: str

def read_sas(
    path: str,
    columns: Sequence[str] | None = ...,
    n_rows: int | None = ...,
    predicate: pl.Expr | None = ...,
    catalog_path: str | None = ...,
    schema_overrides: Mapping[str, Any] | None = ...,
) -> pl.DataFrame:
    """Read a SAS7BDAT file eagerly into a DataFrame. Pass ``columns`` to project."""

def scan_sas(
    path: str,
    catalog_path: str | None = ...,
    schema_overrides: Mapping[str, Any] | None = ...,
    categorical: bool = ...,
    columns: Sequence[str] | None = ...,
    n_rows: int | None = ...,
) -> pl.LazyFrame:
    """Lazily scan a SAS7BDAT file into a LazyFrame. Pass ``columns`` to project."""

def sas_info(path: str, catalog_path: str | None = ...) -> dict[str, Any]:
    """Header-only metadata: path, n_rows, n_columns, row_length_bytes, page_count,
    encoding, size_bytes. Does not decode the body."""

def schema_for_file(path: str) -> pl.Schema:
    """Return the Polars schema of a SAS7BDAT file without decoding the body."""

def batch_reader(
    path: str,
    with_columns: Sequence[str] | None = ...,
    predicate: pl.Expr | None = ...,
    n_rows: int | None = ...,
    batch_size: int | None = ...,
    catalog_path: str | None = ...,
    schema_overrides: Mapping[str, Any] | None = ...,
) -> BatchReader:
    """Iterate a SAS7BDAT file as DataFrame batches (low-level; prefer read_sas)."""

def scan_threads() -> int:
    """Number of decode threads the reader will use (env override, else all cores)."""

def set_scan_threads(n: int) -> None:
    """Cap the reader's decode-thread pool (``0`` resets to all cores)."""

class SasDataset:
    """A SAS7BDAT file opened once, reusable for schema/scan/batch reads."""

    def __init__(
        self,
        path: str,
        catalog_path: str | None = ...,
        schema_overrides: Mapping[str, Any] | None = ...,
    ) -> None: ...
    def schema(self) -> pl.Schema: ...
    def scan_sas(self) -> pl.LazyFrame: ...
    def batch_reader(
        self,
        with_columns: Sequence[str] | None = ...,
        predicate: pl.Expr | None = ...,
        n_rows: int | None = ...,
        batch_size: int | None = ...,
    ) -> BatchReader: ...

class BatchReader(Iterator[pl.DataFrame]):
    def __iter__(self) -> BatchReader: ...
    def __next__(self) -> pl.DataFrame: ...

class SasIoSource: ...
