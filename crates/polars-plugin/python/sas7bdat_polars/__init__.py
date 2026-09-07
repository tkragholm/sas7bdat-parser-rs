"""SAS7BDAT files as Arrow streams, with a Polars API on top.

Quick start
-----------
>>> import sas7bdat_polars as sp
>>> df = sp.read_sas("data.sas7bdat", columns=["ID", "DATE"])   # eager, projected
>>> lf = sp.scan_sas("data.sas7bdat", columns=["ID"])           # lazy
>>> sp.sas_info("data.sas7bdat")                                # header-only metadata

Any Arrow consumer works too, not only polars: a ``SasDataset`` and every
stream it hands out expose ``__arrow_c_stream__``, so ``pyarrow.table(ds)``,
``pandas.api.interchange`` and ``duckdb`` read the same file without polars.

Performance cookbook
--------------------
SAS7BDAT is wide and row-oriented, so **projecting the columns you need is the
single biggest speed-up**: decode one column instead of all of them.

>>> sp.read_sas(path, columns=["D_INDDTO"])              # ~50x faster than a full read
>>> sp.read_sas(path, columns=["c"], n_rows=1_000_000)   # bound I/O on a huge file
>>> sp.sas_info(path)["n_rows"]                           # exact row count, no body decode

Threading
---------
The reader parallelises its own SIMD page decode across all cores. Tune it with
``set_scan_threads(n)`` (or the ``SAS7BDAT_SCAN_THREADS`` environment variable).
Do **not** throttle Polars' own thread pool (``POLARS_MAX_THREADS``): it does not
control the decoder and only starves the pipeline.
"""

from __future__ import annotations

import os
import re
import warnings
from collections.abc import Iterator, Mapping, Sequence
from importlib.metadata import PackageNotFoundError, requires
from typing import Any

_SKIP_VERSION_CHECK_ENV = "SAS7BDAT_POLARS_SKIP_VERSION_CHECK"


def _version_key(text: str) -> tuple[int, ...]:
    """``"1.41.2"`` to ``(1, 41, 2)``; a non-numeric tail such as ``rc1`` is dropped."""
    parts: list[int] = []
    for piece in text.split("."):
        digits = ""
        for ch in piece:
            if not ch.isdigit():
                break
            digits += ch
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def _clause_holds(installed: tuple[int, ...], clause: str) -> bool:
    """One PEP 440 clause of the kinds this package's metadata uses."""
    clause = clause.strip()
    for op in ("===", "==", "!=", "<=", ">=", "<", ">", "~="):
        if clause.startswith(op):
            wanted = clause[len(op) :].strip()
            break
    else:
        return True
    if op in ("==", "!=") and wanted.endswith(".*"):
        prefix = _version_key(wanted[:-2])
        matches = installed[: len(prefix)] == prefix
        return matches if op == "==" else not matches
    target = _version_key(wanted)
    if op == "~=":
        # `~=1.41.2` is `>=1.41.2, ==1.41.*`; `~=1.41` is `>=1.41, ==1.*`.
        if len(target) < 2:
            return installed >= target
        prefix = target[:-1]
        return installed >= target and installed[: len(prefix)] == prefix
    width = max(len(installed), len(target))
    left = installed + (0,) * (width - len(installed))
    right = target + (0,) * (width - len(target))
    return {
        "===": left == right,
        "==": left == right,
        "!=": left != right,
        "<=": left <= right,
        ">=": left >= right,
        "<": left < right,
        ">": left > right,
    }[op]


def polars_requirement() -> str | None:
    """The polars specifier this installed wheel declares, e.g. ``">=1.41"``.

    Read from the distribution metadata, so the one copy in ``pyproject.toml`` is
    the only copy. ``None`` when the package is not installed as a distribution
    (an editable or in-tree import), where there is nothing to check against.
    """
    try:
        declared = requires("sas7bdat-polars") or []
    except PackageNotFoundError:
        return None
    for requirement in declared:
        head, _, marker = requirement.partition(";")
        if marker.strip():
            continue  # an extra's dependency, not a base one
        match = re.fullmatch(r"\s*polars\s*(?P<spec>[<>=!~].*)?", head, re.IGNORECASE)
        if match is None:
            continue
        return (match.group("spec") or "").replace(" ", "")
    return None


def check_polars_version(
    installed: str | None = None, requirement: str | None = None
) -> None:
    """Raise ``ImportError`` when the installed polars is outside the tested range.

    The extension itself does not depend on polars; the layer above it uses
    polars' public constructors, which the ``compat`` job exercises from the floor
    declared in the metadata upward. Below that floor the constructors this
    package needs may not exist, so the message names the versions instead of a
    stack trace from inside polars. ``SAS7BDAT_POLARS_SKIP_VERSION_CHECK=1``
    proceeds anyway.
    """
    if os.environ.get(_SKIP_VERSION_CHECK_ENV):
        return
    if requirement is None:
        requirement = polars_requirement()
    if not requirement:
        return
    if installed is None:
        import polars

        installed = str(polars.__version__)
    key = _version_key(installed)
    if all(_clause_holds(key, clause) for clause in requirement.split(",")):
        return
    raise ImportError(
        f"sas7bdat_polars is built and tested against polars{requirement}; "
        f"polars {installed} is installed. Install a polars in that range, or a "
        f"sas7bdat-polars built for this one. Set {_SKIP_VERSION_CHECK_ENV}=1 to "
        "import anyway."
    )


check_polars_version()

import polars as pl  # noqa: E402
from polars.io.plugins import register_io_source  # noqa: E402

# The compiled extension is a submodule of this package (mixed maturin layout).
from . import sas7bdat_polars as _native  # noqa: E402
from .sas7bdat_polars import (  # noqa: E402
    PLUGIN_CONTRACT_VERSION,
    ArrowStream,
    BatchIterator,
)
from .sas7bdat_polars import SasDataset as _NativeDataset  # noqa: E402
from .sas7bdat_polars import sas_info as _native_sas_info  # noqa: E402

try:
    __version__ = _native.__version__
except AttributeError:  # pragma: no cover
    __version__ = "unknown"

# The version of the Rust `sas7bdat` crate compiled into this wheel. It moves on its
# own line: the 0.8.0 wheel carried core 0.6.0 and the 0.9.0 wheel carried core 0.8.0,
# so `__version__` alone does not tell you which reader you have.
try:
    __core_version__ = _native.__core_version__
except AttributeError:  # pragma: no cover
    __core_version__ = "unknown"

_SCAN_THREADS_ENV = "SAS7BDAT_SCAN_THREADS"


def scan_threads() -> int:
    """Return the number of decode threads the reader will use.

    This is the ``SAS7BDAT_SCAN_THREADS`` override if set, otherwise all logical
    cores, NOT ``POLARS_MAX_THREADS``, which does not control the decoder.
    """
    val = os.environ.get(_SCAN_THREADS_ENV)
    if val and val.isdigit() and int(val) > 0:
        return int(val)
    return os.cpu_count() or 1


def set_scan_threads(n: int) -> None:
    """Cap the reader's decode-thread pool (``0`` resets to all cores).

    Sets ``SAS7BDAT_SCAN_THREADS``, which the reader consults on every scan. This
    is the knob for decode parallelism; ``POLARS_MAX_THREADS`` is not.
    """
    if not n:
        os.environ.pop(_SCAN_THREADS_ENV, None)
        return
    if int(n) < 1:
        raise ValueError("n must be a positive integer, or 0 to reset")
    os.environ[_SCAN_THREADS_ENV] = str(int(n))


def _warn_on_thread_mismatch() -> None:
    """Nudge users who throttled Polars expecting it to bound the SAS decoder."""
    pmt = os.environ.get("POLARS_MAX_THREADS")
    if pmt and pmt.isdigit() and int(pmt) <= 2 and _SCAN_THREADS_ENV not in os.environ:
        warnings.warn(
            f"POLARS_MAX_THREADS={pmt} does not control the sas7bdat_polars decoder, "
            "which has its own thread pool. To limit decode threads use "
            "set_scan_threads(n) / SAS7BDAT_SCAN_THREADS; throttling Polars only "
            "starves the pipeline.",
            RuntimeWarning,
            stacklevel=2,
        )


_warn_on_thread_mismatch()


# ─── schema overrides ─────────────────────────────────────────────────────────

# What the extension accepts for a schema override, by the polars dtype's base
# name. Written here rather than in Rust so the compiled half never has to look
# at a polars object.
_OVERRIDE_TYPES: dict[str, str] = {
    "Int64": "int64",
    "Float64": "float64",
    "Date": "date",
    "Datetime": "datetime",
    "Time": "time",
    "Duration": "time",
    "String": "string",
    "Utf8": "string",
    "Binary": "binary",
}


def _override_names(
    schema_overrides: Mapping[str, Any] | None,
) -> dict[str, str] | None:
    """``{column: pl.Int64}`` (a dtype, a dtype class, or a type name) to the
    ``{column: "int64"}`` the extension takes."""
    if not schema_overrides:
        return None
    out: dict[str, str] = {}
    for name, dtype in schema_overrides.items():
        if isinstance(dtype, str):
            out[name] = dtype
            continue
        base = dtype.base_type() if hasattr(dtype, "base_type") else dtype
        key = getattr(base, "__name__", None) or type(base).__name__
        try:
            out[name] = _OVERRIDE_TYPES[key]
        except KeyError:
            raise ValueError(
                f"unsupported dtype {dtype!r} in schema_overrides for {name!r}; "
                f"supported: {', '.join(sorted(set(_OVERRIDE_TYPES)))}"
            ) from None
    return out


# ─── the dataset and its readers ──────────────────────────────────────────────


class BatchReader(Iterator[pl.DataFrame]):
    """Iterates a scan one ``pl.DataFrame`` per decoded batch.

    A ``predicate`` is applied per batch with polars' own ``filter``; batches it
    empties are skipped rather than yielded.
    """

    def __init__(self, batches: BatchIterator, predicate: pl.Expr | None) -> None:
        self._batches = batches
        self._predicate = predicate

    def __iter__(self) -> BatchReader:
        return self

    def __next__(self) -> pl.DataFrame:
        while True:
            batch = next(self._batches)  # raises StopIteration at the end
            df = pl.DataFrame(batch)
            if self._predicate is not None:
                df = df.filter(self._predicate)
                if df.is_empty():
                    continue
            return df


class SasDataset:
    """An opened SAS7BDAT file, reused by every read taken from it.

    Opening parses the header and the optional format catalog once and applies
    ``schema_overrides`` (``{column: pl.Int64}`` and the like) at schema time,
    so every scan and every batch agree on the dtypes.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        catalog_path: str | os.PathLike[str] | None = None,
        schema_overrides: Mapping[str, Any] | None = None,
    ) -> None:
        self.path = str(path)
        self._native = _NativeDataset(
            self.path,
            None if catalog_path is None else str(catalog_path),
            _override_names(schema_overrides),
        )

    @property
    def column_names(self) -> list[str]:
        return list(self._native.column_names)

    def info(self) -> dict[str, Any]:
        """Header-level facts: row and column counts, encoding, compression."""
        return dict(self._native.info())

    def schema(self, columns: Sequence[str] | None = None) -> pl.Schema:
        """The polars schema of ``columns`` (or every column), from the header alone."""
        stream = self._native.schema_stream(list(columns) if columns else None)
        return pl.DataFrame(stream).schema

    def stream(
        self,
        columns: Sequence[str] | None = None,
        n_rows: int | None = None,
        batch_size: int | None = None,
    ) -> ArrowStream:
        """An Arrow C stream of the rows: give it to anything that imports
        ``__arrow_c_stream__``."""
        return self._native.stream(
            list(columns) if columns else None, n_rows, batch_size
        )

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return self._native.stream(None, None, None).__arrow_c_stream__(
            requested_schema
        )

    def read(
        self,
        columns: Sequence[str] | None = None,
        n_rows: int | None = None,
        predicate: pl.Expr | None = None,
    ) -> pl.DataFrame:
        df = pl.DataFrame(self.stream(columns, n_rows))
        return df if predicate is None else df.filter(predicate)

    def batch_reader(
        self,
        with_columns: Sequence[str] | None = None,
        predicate: pl.Expr | None = None,
        n_rows: int | None = None,
        batch_size: int | None = None,
    ) -> BatchReader:
        batches = self._native.batches(
            list(with_columns) if with_columns else None, n_rows, batch_size
        )
        return BatchReader(batches, predicate)

    def scan_sas(
        self,
        columns: Sequence[str] | None = None,
        n_rows: int | None = None,
        predicate: pl.Expr | None = None,
        categorical: bool = False,
    ) -> pl.LazyFrame:
        """A lazy frame over the file. Projection and a row limit reach the
        decoder; a filter is applied per batch by polars."""
        columns = list(columns) if columns else None
        lf = register_io_source(
            io_source=SasIoSource(self, columns, n_rows),
            schema=self.schema(columns),
            validate_schema=False,
            is_pure=True,
        )
        if predicate is not None:
            lf = lf.filter(predicate)
        if categorical:
            lf = lf.with_columns(pl.col(pl.String).cast(pl.Categorical))
        return lf


class SasIoSource:
    """The callable ``polars.io.plugins.register_io_source`` drives.

    Polars calls it with what it could push down: the columns the plan needs,
    a filter, a row limit, and a batch size. Columns and the limit go to the
    decoder; the filter is applied to each batch with polars' own ``filter``.
    """

    def __init__(
        self, dataset: SasDataset, columns: list[str] | None, n_rows: int | None
    ) -> None:
        self._dataset = dataset
        self._columns = columns
        self._n_rows = n_rows

    def __call__(
        self,
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        columns = with_columns if with_columns is not None else self._columns
        limits = [n for n in (n_rows, self._n_rows) if n is not None]
        limit = min(limits) if limits else None
        return self._dataset.batch_reader(columns, predicate, limit, batch_size)


# ─── module-level API ─────────────────────────────────────────────────────────


def sas_info(
    path: str | os.PathLike[str], catalog_path: str | os.PathLike[str] | None = None
) -> dict[str, Any]:
    """Header-level facts about a file: ``n_rows``, ``n_columns``, ``encoding``,
    ``compression``, ``rows_per_page``, ``size_bytes``. No rows are decoded."""
    return dict(
        _native_sas_info(str(path), None if catalog_path is None else str(catalog_path))
    )


def schema_for_file(
    path: str | os.PathLike[str], catalog_path: str | os.PathLike[str] | None = None
) -> pl.Schema:
    """A file's polars schema from its header, without decoding rows."""
    return SasDataset(path, catalog_path).schema()


def scan_sas(
    path: str | os.PathLike[str],
    catalog_path: str | os.PathLike[str] | None = None,
    schema_overrides: Mapping[str, Any] | None = None,
    categorical: bool = False,
    columns: Sequence[str] | None = None,
    n_rows: int | None = None,
    predicate: pl.Expr | None = None,
) -> pl.LazyFrame:
    """Lazily scan a SAS7BDAT file into a ``pl.LazyFrame``.

    Pass ``columns`` to decode only those (the dominant cost on a wide file) and
    ``n_rows`` to bound the read. ``schema_overrides`` re-types columns at schema
    time, e.g. ``{"ID": pl.Int64}`` for an integer-coded numeric column.
    """
    return SasDataset(path, catalog_path, schema_overrides).scan_sas(
        columns, n_rows, predicate, categorical
    )


def read_sas(
    path: str | os.PathLike[str],
    columns: Sequence[str] | None = None,
    n_rows: int | None = None,
    predicate: pl.Expr | None = None,
    catalog_path: str | os.PathLike[str] | None = None,
    schema_overrides: Mapping[str, Any] | None = None,
) -> pl.DataFrame:
    """Read a SAS7BDAT file eagerly into a ``pl.DataFrame``."""
    return SasDataset(path, catalog_path, schema_overrides).read(
        columns, n_rows, predicate
    )


def batch_reader(
    path: str | os.PathLike[str],
    with_columns: Sequence[str] | None = None,
    predicate: pl.Expr | None = None,
    n_rows: int | None = None,
    batch_size: int | None = None,
    catalog_path: str | os.PathLike[str] | None = None,
    schema_overrides: Mapping[str, Any] | None = None,
) -> BatchReader:
    """Iterate a file one ``pl.DataFrame`` per decoded batch."""
    return SasDataset(path, catalog_path, schema_overrides).batch_reader(
        with_columns, predicate, n_rows, batch_size
    )


__all__ = [
    "PLUGIN_CONTRACT_VERSION",
    "ArrowStream",
    "BatchIterator",
    "BatchReader",
    "SasDataset",
    "SasIoSource",
    "batch_reader",
    "check_polars_version",
    "polars_requirement",
    "read_sas",
    "sas_info",
    "scan_sas",
    "scan_threads",
    "schema_for_file",
    "set_scan_threads",
]
