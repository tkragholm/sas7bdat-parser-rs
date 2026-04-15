from pathlib import Path

import polars as pl
import sas7bdat_polars as sp

FIXTURE = Path(__file__).resolve().parents[3] / "fixtures" / "ahs2013n.sas7bdat"


def test_scan_sas_head_collects():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    lf = sp.scan_sas(str(FIXTURE))
    assert isinstance(lf, pl.LazyFrame)

    df = lf.head(1).collect()
    assert df.height == 1
    assert df.width > 0


def test_schema_for_file_returns_schema():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    schema = sp.schema_for_file(str(FIXTURE))
    assert len(schema) > 0


def test_sasdataset_reuses_open_dataset_for_schema_and_scan():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    ds = sp.SasDataset(str(FIXTURE))

    schema = ds.schema()
    assert len(schema) > 0

    lf = ds.scan_sas()
    assert isinstance(lf, pl.LazyFrame)

    df = lf.select(["CONTROL", "DEGREE"]).head(2).collect()

    assert df.columns == ["CONTROL", "DEGREE"]
    assert df.height == 2


def test_scan_sas_projection_collects_requested_columns():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    df = sp.scan_sas(str(FIXTURE)).select(["CONTROL", "DEGREE"]).head(3).collect()

    assert df.columns == ["CONTROL", "DEGREE"]
    assert df.height == 3


def test_scan_sas_filter_collects_matching_rows():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    df = (
        sp.scan_sas(str(FIXTURE))
        .filter(pl.col("DEGREE") == "2")
        .select(["CONTROL", "DEGREE"])
        .head(5)
        .collect()
    )

    assert df.columns == ["CONTROL", "DEGREE"]
    assert df.height == 5
    assert df["DEGREE"].drop_nulls().unique().to_list() == ["2"]


def test_scan_sas_numeric_filter_and_projection():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    df = (
        sp.scan_sas(str(FIXTURE))
        .filter((pl.col("LMED") > 60000) & (pl.col("DEGREE") == "2"))
        .select(["CONTROL", "DEGREE", "LMED"])
        .head(5)
        .collect()
    )

    assert df.columns == ["CONTROL", "DEGREE", "LMED"]
    assert df.height == 5
    assert df["DEGREE"].drop_nulls().unique().to_list() == ["2"]
    assert df["LMED"].min() > 60000


def test_scan_sas_filter_with_head_limits_rows():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    df = (
        sp.scan_sas(str(FIXTURE))
        .filter(pl.col("DEGREE") == "2")
        .select(["CONTROL", "DEGREE"])
        .head(2)
        .collect()
    )

    assert df.columns == ["CONTROL", "DEGREE"]
    assert df.height == 2
    assert df["DEGREE"].drop_nulls().unique().to_list() == ["2"]


def test_scan_sas_empty_filter_returns_no_rows():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    df = (
        sp.scan_sas(str(FIXTURE))
        .filter(pl.col("DEGREE") == "__nope__")
        .select(["CONTROL", "DEGREE"])
        .collect()
    )

    assert df.columns == ["CONTROL", "DEGREE"]
    assert df.height == 0


def test_batch_reader_respects_row_limit():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    reader = sp.batch_reader(
        str(FIXTURE),
        ["CONTROL", "DEGREE", "LMED"],
        None,
        3,
        64,
    )
    frames = list(reader)

    assert frames, "expected at least one batch"
    assert all(frame.columns == ["CONTROL", "DEGREE", "LMED"] for frame in frames)

    df = pl.concat(frames)

    assert df.height == 3
    assert df.columns == ["CONTROL", "DEGREE", "LMED"]


def test_batch_reader_applies_predicate_and_projection():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    reader = sp.batch_reader(
        str(FIXTURE),
        ["CONTROL", "DEGREE"],
        pl.col("DEGREE") == "2",
        None,
        64,
    )
    frames = list(reader)

    assert frames, "expected at least one batch"
    assert all(frame.columns == ["CONTROL", "DEGREE"] for frame in frames)

    df = pl.concat(frames)
    values = df["DEGREE"].drop_nulls().unique().to_list()

    assert df.height > 0
    assert values == ["2"]


def test_batch_reader_filters_across_multiple_batches():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    reader = sp.batch_reader(
        str(FIXTURE),
        ["CONTROL", "DEGREE", "LMED"],
        pl.col("LMED") > 60000,
        None,
        32,
    )
    frames = list(reader)

    assert len(frames) > 1, "expected multiple batches"
    assert all(frame.columns == ["CONTROL", "DEGREE", "LMED"] for frame in frames)

    df = pl.concat(frames)

    assert df.height > 0
    assert df["LMED"].min() > 60000


def test_batch_reader_empty_filter_returns_no_rows():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    reader = sp.batch_reader(
        str(FIXTURE),
        ["CONTROL", "DEGREE"],
        pl.col("DEGREE") == "__nope__",
        None,
        64,
    )
    frames = list(reader)

    assert frames == []


def test_sasdataset_repeated_scans_and_batch_readers_reuse_cached_schema():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    ds = sp.SasDataset(str(FIXTURE))

    first = ds.scan_sas().select(["CONTROL", "DEGREE"]).head(2).collect()
    second = ds.scan_sas().select(["CONTROL", "DEGREE"]).head(2).collect()

    assert first.to_dict(as_series=False) == second.to_dict(as_series=False)

    first_batches = list(ds.batch_reader(["CONTROL", "DEGREE"], None, 2, 64))
    second_batches = list(ds.batch_reader(["CONTROL", "DEGREE"], None, 2, 64))

    assert pl.concat(first_batches).to_dict(as_series=False) == pl.concat(
        second_batches
    ).to_dict(as_series=False)
