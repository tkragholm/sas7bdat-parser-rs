import json
import os
import subprocess
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _bindings_snapshot(path: Path):
    try:
        import sas7bdat  # type: ignore
    except Exception as exc:  # pragma: no cover - bindings optional
        pytest.skip(f"sas7bdat python bindings not importable: {exc}")

    snapshot = getattr(sas7bdat, "_test_snapshot_fixture", None)
    if snapshot is None:
        pytest.skip("python bindings snapshot helper not implemented")
    return snapshot(str(path))


def _run_pandas_generator(output_dir: Path) -> None:
    script = _repo_root() / "scripts" / "snapshots" / "generate_pandas_dumps.py"
    subprocess.run(
        [
            os.environ.get("PYTHON_BIN", "python3"),
            str(script),
            "--output-dir",
            str(output_dir),
            "--parsers",
            "pandas",
            "pyreadstat",
        ],
        check=True,
        cwd=_repo_root(),
    )


def _load_reference(output_dir: Path, parser: str, fixture: Path) -> dict:
    relative = fixture.relative_to(_repo_root())
    reference = output_dir / parser / relative.with_suffix(".json")
    return json.loads(reference.read_text(encoding="utf-8"))


@pytest.mark.skipif(
    os.environ.get("SAS7BDAT_VERIFY_PANDAS") is None,
    reason="SAS7BDAT_VERIFY_PANDAS not set",
)
def test_pandas_vs_bindings(tmp_path: Path) -> None:
    _run_pandas_generator(tmp_path)
    fixture = _repo_root() / "fixtures" / "raw_data" / "pandas" / "airline.sas7bdat"
    reference = _load_reference(tmp_path, "pandas", fixture)
    actual = _bindings_snapshot(fixture)
    assert actual == reference


@pytest.mark.skipif(
    os.environ.get("SAS7BDAT_VERIFY_PYREADSTAT") is None,
    reason="SAS7BDAT_VERIFY_PYREADSTAT not set",
)
def test_pyreadstat_vs_bindings(tmp_path: Path) -> None:
    _run_pandas_generator(tmp_path)
    fixture = _repo_root() / "fixtures" / "raw_data" / "pandas" / "airline.sas7bdat"
    reference = _load_reference(tmp_path, "pyreadstat", fixture)
    actual = _bindings_snapshot(fixture)
    assert actual == reference
