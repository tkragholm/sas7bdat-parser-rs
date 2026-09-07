"""The Arrow C stream is the whole boundary, and these pin what crosses it.

Fixtures are the small files the R package ships (tracked in git, so these run
everywhere): `people.sas7bdat` (5 rows, ID and GENDER), `dtdate.sas7bdat`
(sub-second datetimes), `test_data_win.sas7bdat` with its format catalog
`test_formats_win.sas7bcat` (value labels), and the pandas corpus's
`airline.sas7bdat` where it is present.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import sas7bdat_polars as sp

EXTDATA = (
    Path(__file__).resolve().parents[3] / "crates" / "r-plugin" / "inst" / "extdata"
)
PEOPLE = EXTDATA / "people.sas7bdat"
DTDATE = EXTDATA / "dtdate.sas7bdat"
LABELLED = EXTDATA / "test_data_win.sas7bdat"
CATALOG = EXTDATA / "test_formats_win.sas7bcat"


def test_a_dataset_is_an_arrow_stream_and_polars_imports_it_whole():
    ds = sp.SasDataset(PEOPLE)
    df = pl.DataFrame(ds)  # via __arrow_c_stream__ on the dataset itself
    assert df.height == 5
    assert df.columns == ds.column_names
    assert df.equals(sp.read_sas(PEOPLE))


def test_the_schema_stream_carries_the_schema_without_a_decode():
    ds = sp.SasDataset(PEOPLE)
    assert ds.schema() == sp.read_sas(PEOPLE).schema
    assert ds.schema(["GENDER"]) == pl.Schema({"GENDER": pl.String})


def test_a_stream_is_consumed_once():
    stream = sp.SasDataset(PEOPLE).stream()
    stream.__arrow_c_stream__()
    with pytest.raises(RuntimeError, match="already consumed"):
        stream.__arrow_c_stream__()


def test_projection_and_limit_reach_the_decoder():
    df = pl.DataFrame(sp.SasDataset(PEOPLE).stream(columns=["GENDER"], n_rows=2))
    assert df.columns == ["GENDER"]
    assert df.height == 2


def test_an_unknown_column_fails_at_the_call():
    with pytest.raises(ValueError):
        sp.SasDataset(PEOPLE).stream(columns=["NOPE"])


def test_batches_stack_into_the_whole():
    ds = sp.SasDataset(PEOPLE)
    frames = list(ds.batch_reader(None, None, None, 2))
    assert len(frames) >= 2
    assert pl.concat(frames).equals(pl.DataFrame(ds))


def test_a_predicate_filters_per_batch_and_skips_empties():
    ds = sp.SasDataset(PEOPLE)
    whole = pl.DataFrame(ds)
    value = whole["GENDER"][0]
    frames = list(ds.batch_reader(None, pl.col("GENDER") == value, None, 2))
    assert all(frame.height > 0 for frame in frames)
    assert pl.concat(frames).equals(whole.filter(pl.col("GENDER") == value))


def test_scan_pushes_projection_and_applies_the_filter():
    lf = sp.scan_sas(PEOPLE)
    whole = pl.DataFrame(sp.SasDataset(PEOPLE))
    value = whole["GENDER"][0]
    df = lf.filter(pl.col("GENDER") == value).select("ID").collect()
    assert df.columns == ["ID"]
    assert df.height == whole.filter(pl.col("GENDER") == value).height
    assert sp.scan_sas(PEOPLE, n_rows=3).collect().height == 3
    assert sp.scan_sas(PEOPLE).head(2).collect().height == 2


def test_sub_second_datetimes_survive_as_microseconds():
    import datetime as dt

    df = sp.read_sas(DTDATE)
    assert all(dtype == pl.Datetime("us") for dtype in df.dtypes)
    # The fixture brackets the SAS datetime range at millisecond precision.
    assert df["DTDATE"][0] == dt.datetime(1582, 1, 1, 0, 0, 0, 1000)
    assert df["DTDATE"][1] == dt.datetime(1582, 12, 31, 23, 59, 59, 999000)


def test_value_labels_from_a_catalog_replace_the_codes():
    plain = sp.read_sas(LABELLED)
    labelled = sp.read_sas(LABELLED, catalog_path=CATALOG)
    assert labelled.schema == plain.schema
    changed = [name for name in plain.columns if not plain[name].equals(labelled[name])]
    assert changed, "the catalog should relabel at least one column"
    for name in changed:
        assert labelled[name].dtype == pl.String
    assert "Male" in labelled[changed[0]].to_list()
    assert sp.schema_for_file(LABELLED, CATALOG) == labelled.schema


AIRLINE = Path(__file__).resolve().parents[3] / "fixtures" / "raw_data" / "pandas" / "airline.sas7bdat"


@pytest.mark.skipif(not AIRLINE.exists(), reason=f"missing fixture: {AIRLINE}")
def test_schema_overrides_accept_a_polars_dtype_or_a_name():
    # YEAR is a SAS numeric holding whole years: the case an Int64 override is for.
    with_dtype = sp.read_sas(AIRLINE, columns=["YEAR"], schema_overrides={"YEAR": pl.Int64})
    with_name = sp.read_sas(AIRLINE, columns=["YEAR"], schema_overrides={"YEAR": "int64"})
    assert with_dtype["YEAR"].dtype == pl.Int64
    assert with_dtype["YEAR"][0] == 1948
    assert with_dtype.equals(with_name)
    assert (
        sp.scan_sas(AIRLINE, schema_overrides={"YEAR": pl.Int64}).collect_schema()["YEAR"]
        == pl.Int64
    )
    with pytest.raises(ValueError, match="unsupported"):
        sp.read_sas(AIRLINE, schema_overrides={"YEAR": pl.List(pl.Int64)})


def test_an_override_across_the_character_boundary_is_refused():
    with pytest.raises(ValueError, match="String column"):
        sp.SasDataset(PEOPLE, schema_overrides={"ID": pl.Int64})


def test_pyarrow_reads_the_same_stream_when_present():
    pa = pytest.importorskip("pyarrow")
    table = pa.table(sp.SasDataset(PEOPLE))
    assert table.num_rows == 5
    assert table.column_names == sp.SasDataset(PEOPLE).column_names
