"""Type stubs for sas7bdat_polars."""

import os
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import polars as pl

__version__: str
__core_version__: str
PLUGIN_CONTRACT_VERSION: str

class ArrowStream:
    """An Arrow C stream, importable once through ``__arrow_c_stream__``."""

    def __arrow_c_stream__(self, requested_schema: object = ...) -> object: ...

class BatchIterator(Iterator[ArrowStream]):
    def __iter__(self) -> BatchIterator: ...
    def __next__(self) -> ArrowStream: ...

class BatchReader(Iterator[pl.DataFrame]):
    def __init__(self, batches: BatchIterator, predicate: pl.Expr | None) -> None: ...
    def __iter__(self) -> BatchReader: ...
    def __next__(self) -> pl.DataFrame: ...

class SasDataset:
    path: str
    def __init__(
        self,
        path: str | os.PathLike[str],
        catalog_path: str | os.PathLike[str] | None = ...,
        schema_overrides: Mapping[str, Any] | None = ...,
        io_backend: str | None = ...,
    ) -> None: ...
    @property
    def column_names(self) -> list[str]: ...
    def info(self) -> dict[str, Any]: ...
    def schema(self, columns: Sequence[str] | None = ...) -> pl.Schema: ...
    def stream(
        self,
        columns: Sequence[str] | None = ...,
        n_rows: int | None = ...,
        batch_size: int | None = ...,
    ) -> ArrowStream: ...
    def __arrow_c_stream__(self, requested_schema: object = ...) -> object: ...
    def read(
        self,
        columns: Sequence[str] | None = ...,
        n_rows: int | None = ...,
        predicate: pl.Expr | None = ...,
    ) -> pl.DataFrame: ...
    def batch_reader(
        self,
        with_columns: Sequence[str] | None = ...,
        predicate: pl.Expr | None = ...,
        n_rows: int | None = ...,
        batch_size: int | None = ...,
    ) -> BatchReader: ...
    def scan_sas(
        self,
        columns: Sequence[str] | None = ...,
        n_rows: int | None = ...,
        predicate: pl.Expr | None = ...,
        categorical: bool = ...,
    ) -> pl.LazyFrame: ...

class SasIoSource:
    def __init__(
        self, dataset: SasDataset, columns: list[str] | None, n_rows: int | None
    ) -> None: ...
    def __call__(
        self,
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]: ...

def sas_info(
    path: str | os.PathLike[str],
    catalog_path: str | os.PathLike[str] | None = ...,
    io_backend: str | None = ...,
) -> dict[str, Any]:
    """Header-level facts about a file; no rows are decoded."""

def schema_for_file(
    path: str | os.PathLike[str], catalog_path: str | os.PathLike[str] | None = ...
) -> pl.Schema:
    """A file's polars schema from its header."""

def scan_sas(
    path: str | os.PathLike[str],
    catalog_path: str | os.PathLike[str] | None = ...,
    schema_overrides: Mapping[str, Any] | None = ...,
    categorical: bool = ...,
    columns: Sequence[str] | None = ...,
    n_rows: int | None = ...,
    predicate: pl.Expr | None = ...,
    io_backend: str | None = ...,
) -> pl.LazyFrame:
    """Lazily scan a SAS7BDAT file. Pass ``columns`` to project at the reader."""

def read_sas(
    path: str | os.PathLike[str],
    columns: Sequence[str] | None = ...,
    n_rows: int | None = ...,
    predicate: pl.Expr | None = ...,
    catalog_path: str | os.PathLike[str] | None = ...,
    schema_overrides: Mapping[str, Any] | None = ...,
    io_backend: str | None = ...,
) -> pl.DataFrame:
    """Read a SAS7BDAT file eagerly into a DataFrame."""

def batch_reader(
    path: str | os.PathLike[str],
    with_columns: Sequence[str] | None = ...,
    predicate: pl.Expr | None = ...,
    n_rows: int | None = ...,
    batch_size: int | None = ...,
    catalog_path: str | os.PathLike[str] | None = ...,
    schema_overrides: Mapping[str, Any] | None = ...,
    io_backend: str | None = ...,
) -> BatchReader:
    """Iterate a file one DataFrame per decoded batch."""

def scan_threads() -> int:
    """Decode threads the reader will use."""

def set_scan_threads(n: int) -> None:
    """Cap the reader's decode-thread pool; ``0`` resets."""

def polars_requirement() -> str | None:
    """The polars specifier this installed wheel declares, read from its metadata."""

def check_polars_version(
    installed: str | None = ..., requirement: str | None = ...
) -> None:
    """Raise ``ImportError`` when the installed polars is outside the tested range."""
