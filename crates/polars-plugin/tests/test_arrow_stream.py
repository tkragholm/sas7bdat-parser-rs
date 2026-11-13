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


def test_a_buffered_read_is_the_mapped_read():
    mapped = sp.read_sas(LABELLED, catalog_path=CATALOG, io_backend="mmap")
    buffered = sp.read_sas(LABELLED, catalog_path=CATALOG, io_backend="buffered")
    assert buffered.equals(mapped)
    buffered_info = sp.sas_info(PEOPLE, io_backend="buffered")
    mapped_info = sp.sas_info(PEOPLE, io_backend="mmap")
    assert buffered_info.pop("io_backend") == "buffered"
    assert mapped_info.pop("io_backend") == "mmap"
    assert buffered_info == mapped_info
    # `auto` maps a local file; a network share would answer "buffered" here.
    assert sp.sas_info(PEOPLE)["io_backend"] == "mmap"
    with pytest.raises(ValueError, match="io_backend"):
        sp.SasDataset(PEOPLE, io_backend="tape")


def test_the_backend_can_be_set_for_the_process(monkeypatch):
    monkeypatch.setenv("SAS7BDAT_IO_BACKEND", "buffered")
    assert sp.read_sas(PEOPLE).equals(sp.read_sas(PEOPLE, io_backend="mmap"))
    monkeypatch.setenv("SAS7BDAT_IO_BACKEND", "tape")
    with pytest.raises(ValueError, match="io_backend"):
        sp.SasDataset(PEOPLE)


def test_categorical_columns_cross_as_dictionaries():
    plain = sp.read_sas(PEOPLE)
    cat = sp.read_sas(PEOPLE, categorical=["GENDER"])
    assert cat["GENDER"].dtype == pl.Categorical
    assert cat["GENDER"].cast(pl.String).equals(plain["GENDER"])
    assert cat.drop("GENDER").equals(plain.drop("GENDER"))
    assert sp.SasDataset(PEOPLE).schema(categorical=["GENDER"])["GENDER"] == pl.Categorical
    every = sp.read_sas(PEOPLE, categorical=True)
    assert [name for name, dtype in every.schema.items() if dtype == pl.Categorical] == [
        name for name, dtype in plain.schema.items() if dtype == pl.String
    ]


def test_categorical_batches_with_their_own_dictionaries_concatenate():
    ds = sp.SasDataset(PEOPLE)
    frames = list(ds.batch_reader(None, None, None, 2, ["GENDER"]))
    assert len(frames) >= 2
    together = pl.concat(frames, rechunk=False)
    assert together["GENDER"].dtype == pl.Categorical
    assert together["GENDER"].cast(pl.String).equals(sp.read_sas(PEOPLE)["GENDER"])


def test_a_categorical_scan_projects_and_filters():
    lf = sp.scan_sas(PEOPLE, categorical=["GENDER"])
    assert lf.collect_schema()["GENDER"] == pl.Categorical
    value = sp.read_sas(PEOPLE)["GENDER"][0]
    df = lf.filter(pl.col("GENDER").cast(pl.String) == value).select("ID").collect()
    assert df.columns == ["ID"]
    assert df.height == sp.read_sas(PEOPLE).filter(pl.col("GENDER") == value).height


def test_labels_can_be_categorical_too():
    df = sp.read_sas(LABELLED, catalog_path=CATALOG, categorical=["SEXA"])
    assert df["SEXA"].dtype == pl.Categorical
    assert "Male" in df["SEXA"].cast(pl.String).to_list()


def test_categorical_refuses_what_it_cannot_encode():
    with pytest.raises(ValueError, match="does not have"):
        sp.read_sas(PEOPLE, categorical=["NOPE"])
    with pytest.raises(ValueError, match="only string"):
        sp.read_sas(PEOPLE, categorical=["AGE"])


def test_a_missing_file_raises_file_not_found(tmp_path):
    missing = tmp_path / "absent.sas7bdat"
    with pytest.raises(FileNotFoundError, match="absent.sas7bdat"):
        sp.SasDataset(str(missing))
    with pytest.raises(FileNotFoundError):
        sp.SasDataset(str(PEOPLE), catalog_path=str(tmp_path / "absent.sas7bcat"))


def test_a_file_that_is_not_sas7bdat_raises_value_error(tmp_path):
    junk = tmp_path / "junk.sas7bdat"
    junk.write_bytes(b"not a SAS file" * 100)
    with pytest.raises(ValueError):
        sp.SasDataset(str(junk))


DELETED = (
    Path(__file__).resolve().parents[2]
    / "sas7bdat"
    / "tests"
    / "fixtures"
    / "corpus"
    / "parso"
    / "all_rand_normal_with_deleted2.sas7bdat"
)


@pytest.mark.skipif(not DELETED.exists(), reason=f"tracked test file not present: {DELETED}")
def test_info_counts_the_rows_a_read_returns():
    # The header counts five rows SAS marked as deleted; a read skips them.
    info = sp.sas_info(str(DELETED))
    assert info["n_deleted_rows"] == 5
    assert info["n_rows"] == sp.read_sas(str(DELETED)).height == 32


def test_info_reports_file_and_column_metadata():
    info = sp.sas_info(str(EXTDATA / "test_data_win.sas7bdat"))
    assert info["n_columns"] == len(info["columns"]) == 3
    assert {"table_name", "file_label", "created", "modified"} <= info.keys()
    first = info["columns"][0]
    assert first["name"] == "ID"
    assert first["type"] == "string"
    assert set(first) == {"name", "label", "format", "type", "width"}


def test_column_info_is_a_frame_of_names_labels_and_formats():
    frame = sp.column_info(str(EXTDATA / "dtdate.sas7bdat"))
    assert frame.columns == ["name", "label", "format", "type", "width"]
    assert frame.height == sp.sas_info(str(EXTDATA / "dtdate.sas7bdat"))["n_columns"]
    # Its DTDATEw. columns are datetimes and carry their SAS format.
    assert frame["type"].to_list() == ["datetime"] * 6
    assert frame["format"].to_list()[:2] == ["DTDATE", "DTDATE5"]


def test_row_offset_skips_rows_that_a_read_returns():
    every = sp.read_sas(str(PEOPLE))
    assert sp.read_sas(str(PEOPLE), row_offset=1, n_rows=2).equals(every.slice(1, 2))
    assert sp.read_sas(str(PEOPLE), row_offset=3).equals(every.slice(3))
    assert sp.read_sas(str(PEOPLE), row_offset=10).height == 0
    lazy = sp.scan_sas(str(PEOPLE), row_offset=2).collect()
    assert lazy.equals(every.slice(2))
    batches = list(sp.batch_reader(str(PEOPLE), row_offset=4))
    assert sum(b.height for b in batches) == 1


@pytest.mark.skipif(not DELETED.exists(), reason=f"tracked test file not present: {DELETED}")
def test_row_offset_counts_past_deleted_rows():
    every = sp.read_sas(str(DELETED))
    assert sp.read_sas(str(DELETED), row_offset=8, n_rows=4).equals(every.slice(8, 4))


def test_a_negative_row_offset_is_rejected():
    with pytest.raises(ValueError, match="row_offset"):
        sp.read_sas(str(PEOPLE), row_offset=-1)


NONASCII = (
    Path(__file__).resolve().parents[2]
    / "sas7bdat"
    / "tests"
    / "fixtures"
    / "people_nonascii.sas7bdat"
)


def test_encoding_overrides_the_declared_one():
    # The file declares windows-1252, where its byte 0xE9 is "é"; in ISO-8859-7 it is "ι".
    assert sp.read_sas(str(NONASCII))["GENDER"][0] == "é"
    greek = sp.read_sas(str(NONASCII), encoding="iso-8859-7")
    assert greek["GENDER"][0] == "ι"
    assert sp.sas_info(str(NONASCII), encoding="wlatin1")["encoding"] == "windows-1252"
    with pytest.raises(ValueError, match="encoding"):
        sp.read_sas(str(NONASCII), encoding="not-an-encoding")


def test_strict_fails_where_the_default_replaces():
    # 0xE9 alone is not UTF-8: replaced by default, an error under strict.
    lenient = sp.read_sas(str(NONASCII), encoding="utf-8")
    assert lenient["GENDER"][0] == "�"
    with pytest.raises(Exception, match="(?i)decode|utf-8|invalid"):
        sp.read_sas(str(NONASCII), encoding="utf-8", strict=True)
    assert sp.read_sas(str(PEOPLE), strict=True).height == 5


def test_apply_labels_false_keeps_the_codes():
    plain = sp.read_sas(LABELLED)
    codes = sp.SasDataset(LABELLED, CATALOG, apply_labels=False).read()
    assert codes.equals(plain)


def test_value_labels_map_each_code_to_its_label():
    labels = sp.SasDataset(LABELLED, CATALOG).value_labels()
    assert labels, "the catalog labels at least one column"
    assert any("Male" in mapping.values() for mapping in labels.values())
    assert sp.value_labels(LABELLED, CATALOG) == labels
    assert sp.SasDataset(LABELLED).value_labels() == {}


def test_value_labels_finds_a_catalog_beside_the_file(tmp_path):
    import shutil

    data = tmp_path / "survey.sas7bdat"
    shutil.copy(LABELLED, data)
    shutil.copy(CATALOG, tmp_path / "survey.sas7bcat")
    assert sp.value_labels(data) == sp.value_labels(LABELLED, CATALOG)


def _two_deliveries(tmp_path):
    import shutil

    first, second = tmp_path / "people_2020.sas7bdat", tmp_path / "people_2021.sas7bdat"
    shutil.copy(PEOPLE, first)
    shutil.copy(PEOPLE, second)
    return first, second


def test_several_files_read_as_one_frame(tmp_path):
    first, second = _two_deliveries(tmp_path)
    one = sp.read_sas(str(PEOPLE))
    both = sp.read_sas([first, second])
    assert both.equals(pl.concat([one, one]))
    by_pattern = sp.read_sas(str(tmp_path / "people_*.sas7bdat"))
    assert by_pattern.equals(both)
    lazy = sp.scan_sas([first, second], columns=["ID"]).collect()
    assert lazy.columns == ["ID"] and lazy.height == 10


def test_file_paths_column_and_ranges_across_files(tmp_path):
    first, second = _two_deliveries(tmp_path)
    both = sp.read_sas([first, second], include_file_paths="source")
    assert both["source"].to_list() == [str(first)] * 5 + [str(second)] * 5
    window = sp.read_sas([first, second], row_offset=3, n_rows=4)
    assert window.equals(both.drop("source").slice(3, 4))
    single = sp.read_sas(str(PEOPLE), include_file_paths="source")
    assert single["source"].unique().to_list() == [str(PEOPLE)]


def test_a_pattern_that_matches_nothing_is_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        sp.read_sas(str(tmp_path / "none_*.sas7bdat"))


MISSING = EXTDATA / "missing_test.sas7bdat"
# haven's na_tag() for each column of missing_test.sas7bdat, upper-cased.
HAVEN_TAGS = [".A", ".B", ".C", ".X", ".Y", ".Z", "._", ".", None]


def test_missing_tags_give_each_special_missing_value_its_tag():
    columns = sp.SasDataset(MISSING).column_names
    df = sp.SasDataset(MISSING, missing_tags=columns).read()
    assert df.columns[:2] == ["var1", "var1_missing"]
    assert [df[f"{c}_missing"][0] for c in columns] == HAVEN_TAGS
    # The values themselves are unchanged: missing is still null.
    assert df.select(columns).equals(sp.read_sas(MISSING))


def test_missing_tags_follow_projection_and_lazy_scans():
    ds = sp.SasDataset(MISSING, missing_tags=["var2", "var9"])
    assert ds.read(columns=["var2_missing"]).to_dict(as_series=False) == {
        "var2_missing": [".B"]
    }
    lazy = ds.scan_sas().select("var9", "var9_missing").collect()
    assert lazy.to_dict(as_series=False) == {"var9": [1.0], "var9_missing": [None]}
    assert ds.schema()["var2_missing"] == pl.String


def test_missing_tags_on_a_bad_column_is_an_error():
    with pytest.raises(ValueError, match="no column"):
        sp.SasDataset(MISSING, missing_tags=["nope"])
    with pytest.raises(ValueError, match="numeric"):
        sp.SasDataset(PEOPLE, missing_tags=["GENDER"])


def test_module_readers_take_missing_tags():
    df = sp.read_sas(MISSING, missing_tags=["var1"])
    assert df["var1_missing"].to_list() == [".A"]
    assert sp.scan_sas(MISSING, missing_tags=["var1"]).collect().equals(df)


TRACKED = Path(__file__).resolve().parents[2] / "sas7bdat" / "tests" / "fixtures" / "corpus"


def test_temporal_as_numbers_gives_the_stored_numbers():
    import datetime as dt

    epoch = dt.datetime(1960, 1, 1)
    path = TRACKED / "pandas" / "datetime.sas7bdat"
    decoded = sp.read_sas(path)
    raw = sp.read_sas(path, temporal_as_numbers=True)
    assert sp.scan_sas(path, temporal_as_numbers=True).collect_schema() == raw.schema
    assert all(dtype == pl.Float64 for dtype in raw.dtypes)
    for name in ("Date1", "Date2", "Taiw"):
        days = [None if d is None else (d - epoch.date()).days for d in decoded[name]]
        assert raw[name].to_list() == days, name
    seconds = [None if d is None else (d - epoch).total_seconds() for d in decoded["DateTime"]]
    assert raw["DateTime"].to_list() == pytest.approx(seconds, abs=1e-6)

    times = sp.read_sas(TRACKED / "parso" / "time_formats.sas7bdat", temporal_as_numbers=True)
    assert times["TIME"].to_list() == [10 * 3600 + 10 * 60 + 10]
