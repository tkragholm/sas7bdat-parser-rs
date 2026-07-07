"""Ergonomic API + streaming-regression tests for sas7bdat_polars."""

import os
from pathlib import Path

import polars as pl
import pytest

import sas7bdat_polars as sp

FIXTURE = Path(__file__).resolve().parents[3] / "fixtures" / "ahs2013n.sas7bdat"


@pytest.fixture(scope="module")
def some_columns() -> list[str]:
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"
    return sp.scan_sas(str(FIXTURE)).collect_schema().names()[:3]


def test_read_sas_projects_columns(some_columns):
    df = sp.read_sas(str(FIXTURE), columns=some_columns)
    assert isinstance(df, pl.DataFrame)
    assert df.columns == some_columns
    assert df.height > 0


def test_read_sas_n_rows(some_columns):
    df = sp.read_sas(str(FIXTURE), columns=some_columns, n_rows=10)
    assert df.height == 10


def test_read_sas_predicate(some_columns):
    col = some_columns[0]
    df = sp.read_sas(str(FIXTURE), columns=some_columns, predicate=pl.col(col).is_not_null())
    assert df.height >= 0
    assert df[col].null_count() == 0


def test_scan_sas_columns_and_n_rows(some_columns):
    lf = sp.scan_sas(str(FIXTURE), columns=some_columns, n_rows=5)
    df = lf.collect()
    assert df.columns == some_columns
    assert df.height == 5


def test_scan_sas_backward_compatible():
    # The pre-existing positional call must still return a full LazyFrame.
    lf = sp.scan_sas(str(FIXTURE))
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect_schema().len() > 3


def test_sas_info_is_header_only():
    info = sp.sas_info(str(FIXTURE))
    assert info["n_rows"] > 0
    assert info["n_columns"] > 0
    assert info["size_bytes"] == FIXTURE.stat().st_size
    assert "encoding" in info
    # n_rows must match an actual read.
    assert info["n_rows"] == sp.read_sas(str(FIXTURE), columns=sp.scan_sas(str(FIXTURE)).collect_schema().names()[:1]).height


def test_streaming_engine_does_not_panic(some_columns):
    # Regression: the reader's BatchReader must be Send+Sync so Polars' streaming
    # engine can drive it without the "unsendable, but sent to another thread" panic.
    df = sp.scan_sas(str(FIXTURE), columns=some_columns).collect(engine="streaming")
    assert df.height > 0
    assert df.columns == some_columns


def test_set_and_get_scan_threads():
    original = os.environ.get("SAS7BDAT_SCAN_THREADS")
    try:
        sp.set_scan_threads(3)
        assert os.environ["SAS7BDAT_SCAN_THREADS"] == "3"
        assert sp.scan_threads() == 3
        # a scan still works with the cap applied
        assert sp.read_sas(str(FIXTURE), n_rows=1).height == 1
        sp.set_scan_threads(0)  # reset
        assert "SAS7BDAT_SCAN_THREADS" not in os.environ
        assert sp.scan_threads() == (os.cpu_count() or 1)
        with pytest.raises(ValueError):
            sp.set_scan_threads(-1)
    finally:
        if original is None:
            os.environ.pop("SAS7BDAT_SCAN_THREADS", None)
        else:
            os.environ["SAS7BDAT_SCAN_THREADS"] = original


def test_public_api_surface():
    for name in ("read_sas", "scan_sas", "sas_info", "schema_for_file", "batch_reader",
                 "scan_threads", "set_scan_threads", "SasDataset", "BatchReader"):
        assert hasattr(sp, name), name
    assert sp.__doc__ and "cookbook" in sp.__doc__.lower()
