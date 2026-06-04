from pathlib import Path

import polars as pl
import sas7bdat_polars as sp

FIXTURE = Path(__file__).resolve().parents[3] / "fixtures" / "ahs2013n.sas7bdat"


def test_public_api_contract_is_exposed():
    # __version__ comes from CARGO_PKG_VERSION at build time; assert the
    # SemVer shape rather than a literal so the test doesn't rot on every bump.
    import re

    assert re.fullmatch(r"\d+\.\d+\.\d+", sp.__version__), sp.__version__
    assert sp.PLUGIN_CONTRACT_VERSION == "sas7bdat_polars.v1"
    assert callable(sp.scan_sas)
    assert callable(sp.schema_for_file)
    assert callable(sp.batch_reader)
    assert hasattr(sp, "SasDataset")


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


def test_projected_scan_schema_matches_collected_frame():
    assert FIXTURE.exists(), f"missing fixture: {FIXTURE}"

    lf = sp.scan_sas(str(FIXTURE)).select(["CONTROL", "DEGREE", "LMED"])
    schema = lf.collect_schema()
    df = lf.head(3).collect()

    assert schema.names() == ["CONTROL", "DEGREE", "LMED"]
    assert df.schema == schema


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


DATETIME_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "raw_data"
    / "csharp"
    / "date_format_datetime.sas7bdat"
)


def test_datetime_columns_are_microsecond_and_stack_across_batches(tmp_path):
    # Regression: datetime columns were declared Datetime('us') but materialized as
    # Datetime('ms') (Timestamp(Second) -> ms), so the declared and actual units
    # disagreed and Polars refused to stack batches with a SchemaError. Sub-second
    # values were additionally widened to a raw-seconds F64 and decoded as garbage.
    import datetime as dt

    if not DATETIME_FIXTURE.exists():
        import pytest

        pytest.skip(f"missing fixture: {DATETIME_FIXTURE}")

    lf = sp.scan_sas(str(DATETIME_FIXTURE))
    declared = lf.collect_schema()
    df = lf.collect()

    dt_cols = [n for n, t in df.schema.items() if isinstance(t, pl.Datetime)]
    assert dt_cols, "fixture should expose datetime columns"
    for name in dt_cols:
        # us, and declared schema == materialized schema
        assert df.schema[name] == pl.Datetime("us"), name
        assert declared[name] == df.schema[name], name

    # Exact value (cross-checked against pyreadstat / ReadStat), incl. sub-second.
    assert df["DATETIME"][0] == dt.datetime(2013, 3, 17, 19, 53, 1, 321000)

    # The exact pipeline path that used to raise SchemaError("ms" != "us").
    out = tmp_path / "concat.parquet"
    pl.concat(
        [sp.scan_sas(str(DATETIME_FIXTURE)), sp.scan_sas(str(DATETIME_FIXTURE))],
        how="diagonal_relaxed",
    ).sink_parquet(out)
    assert pl.read_parquet(out).height == 2 * df.height
