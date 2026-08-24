"""Thin, fast Polars IO plugin for SAS7BDAT files.

Quick start
-----------
>>> import sas7bdat_polars as sp
>>> df = sp.read_sas("data.sas7bdat", columns=["ID", "DATE"])   # eager, projected
>>> lf = sp.scan_sas("data.sas7bdat", columns=["ID"])           # lazy
>>> sp.sas_info("data.sas7bdat")                                # header-only metadata

Performance cookbook
--------------------
SAS7BDAT is wide and row-oriented, so **projecting the columns you need is the
single biggest speed-up** — decode one column instead of all of them:

>>> sp.read_sas(path, columns=["D_INDDTO"])              # ~50x faster than a full read
>>> sp.read_sas(path, columns=["c"], n_rows=1_000_000)   # bound I/O on a huge file
>>> sp.sas_info(path)["n_rows"]                           # exact row count, no body decode

Prefer ``read_sas`` (eager, projection baked in) over ``scan_sas(...).collect()``.

Threading
---------
The reader parallelises its own SIMD page decode across all cores. Tune it with
``set_scan_threads(n)`` (or the ``SAS7BDAT_SCAN_THREADS`` environment variable).
Do **not** throttle Polars' own thread pool (``POLARS_MAX_THREADS``) — it does not
control the decoder and only starves the pipeline. On this build the reader is
``Send + Sync``, so Polars' streaming engine works too.
"""

from __future__ import annotations

import os
import warnings

# The compiled extension is a submodule of this package (mixed maturin layout).
from . import sas7bdat_polars as _native
from .sas7bdat_polars import (  # noqa: F401  (re-export the native symbols)
    PLUGIN_CONTRACT_VERSION,
    BatchReader,
    SasDataset,
    SasIoSource,
    batch_reader,
    read_sas,
    sas_info,
    scan_sas,
    schema_for_file,
)

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
    cores — NOT ``POLARS_MAX_THREADS``, which does not control the decoder.
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

__all__ = [
    "PLUGIN_CONTRACT_VERSION",
    "BatchReader",
    "SasDataset",
    "SasIoSource",
    "batch_reader",
    "read_sas",
    "sas_info",
    "scan_sas",
    "scan_threads",
    "schema_for_file",
    "set_scan_threads",
]
