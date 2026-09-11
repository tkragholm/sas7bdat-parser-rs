"""A round trip through the extension on a real file, under whichever polars is installed.

This is what the `compat` job in `.github/workflows/wheels.yml` runs against the
lowest and the highest polars the package declares, on CPython 3.12 and 3.13.
Every batch reaches polars as an Arrow C stream through the PyCapsule
interface, an ABI that tracks Arrow rather than polars, so a polars outside the
declared range fails here first, as a crash or a wrong value, rather than on a
register file.

The fixture is `airline.sas7bdat` from the pandas SAS test corpus (BSD-3-Clause),
5 KB, 32 rows, six numeric columns: `fixtures/raw_data/pandas/airline.sas7bdat`
where the corpus is populated locally, or the path in `SAS7BDAT_COMPAT_FIXTURE`.
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path

import polars as pl
import pytest
import sas7bdat_polars as sp

_DEFAULT = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "raw_data"
    / "pandas"
    / "airline.sas7bdat"
)
FIXTURE = Path(os.environ.get("SAS7BDAT_COMPAT_FIXTURE", _DEFAULT))

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(), reason=f"missing fixture: {FIXTURE}"
)


def test_eager_read_hands_polars_every_column():
    df = sp.read_sas(str(FIXTURE))
    assert df.height == 32
    assert df.columns == ["YEAR", "Y", "W", "R", "L", "K"]
    assert all(dtype == pl.Float64 for dtype in df.dtypes)
    assert df["YEAR"].to_list()[:3] == [1948.0, 1949.0, 1950.0]
    assert df["YEAR"].sum() == pytest.approx(sum(range(1948, 1980)))


def test_lazy_scan_with_projection_and_pushed_filter():
    with warnings.catch_warnings():
        # A filter the plugin cannot read warns; under a tested polars it must
        # read it, so the warning is a failure here.
        warnings.simplefilter("error", RuntimeWarning)
        df = (
            sp.scan_sas(str(FIXTURE))
            .filter(pl.col("YEAR") >= 1970)
            .select(["YEAR", "Y"])
            .collect()
        )
    assert df.columns == ["YEAR", "Y"]
    assert df.height == 10
    assert df["YEAR"].min() == 1970.0


def test_batches_stack_into_the_eager_frame():
    frames = list(sp.batch_reader(str(FIXTURE), None, None, None, 8))
    assert len(frames) >= 2
    stacked = pl.concat(frames)
    assert stacked.equals(sp.read_sas(str(FIXTURE)))


def test_the_header_schema_matches_the_data():
    schema = sp.schema_for_file(str(FIXTURE))
    assert schema == sp.read_sas(str(FIXTURE)).schema
    info = sp.sas_info(str(FIXTURE))
    assert info["n_rows"] == 32
