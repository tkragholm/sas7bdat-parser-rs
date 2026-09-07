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
import re
import warnings
from importlib.metadata import PackageNotFoundError, requires

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
    """The polars specifier this installed wheel declares, e.g. ``">=1.41,<1.45"``.

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

    The extension talks to polars through private hooks, so a version it was not
    built and tested against can fault rather than fail. This turns that into a
    message naming both versions. Set ``SAS7BDAT_POLARS_SKIP_VERSION_CHECK=1`` to
    proceed anyway.
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
        "import anyway, at the risk of a crash instead of an error."
    )


check_polars_version()

# The compiled extension is a submodule of this package (mixed maturin layout).
from . import sas7bdat_polars as _native  # noqa: E402
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
    "check_polars_version",
    "polars_requirement",
    "read_sas",
    "sas_info",
    "scan_sas",
    "scan_threads",
    "schema_for_file",
    "set_scan_threads",
]
