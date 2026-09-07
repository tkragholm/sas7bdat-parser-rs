"""The import-time polars check: a clear error where the export hooks would fault.

The extension hands Series to polars through private hooks, so a polars outside
the range the wheel was tested against can crash rather than raise. These tests
pin the check that turns that into an ``ImportError`` naming both versions, and
the parser of the specifier it reads from the package's own metadata.
"""

from __future__ import annotations

import pytest
import sas7bdat_polars as sp


@pytest.mark.parametrize(
    "installed,requirement,ok",
    [
        ("1.41.2", ">=1.41,<1.45", True),
        ("1.41.0", ">=1.41,<1.45", True),
        ("1.44.1", ">=1.41,<1.45", True),
        ("1.45.0", ">=1.41,<1.45", False),
        ("1.40.9", ">=1.41,<1.45", False),
        ("1.41.2", "==1.41.*", True),
        ("1.42.0", "==1.41.*", False),
        ("1.41.7", "~=1.41.2", True),
        ("1.42.0", "~=1.41.2", False),
        ("1.99.0", "~=1.41", True),
        ("2.0.0", "~=1.41", False),
        ("1.45.0rc1", "<1.45", False),
        ("1.44.9", "!=1.44.9", False),
        ("1.44.9", "", True),
    ],
)
def test_the_specifier_is_read_the_way_pip_reads_it(installed, requirement, ok):
    if ok:
        sp.check_polars_version(installed, requirement)
    else:
        with pytest.raises(ImportError, match=installed.split("rc")[0]):
            sp.check_polars_version(installed, requirement)


def test_the_error_names_both_versions_and_the_override():
    with pytest.raises(ImportError) as excinfo:
        sp.check_polars_version("1.50.0", ">=1.41,<1.45")
    message = str(excinfo.value)
    assert "polars>=1.41,<1.45" in message
    assert "1.50.0" in message
    assert "SAS7BDAT_POLARS_SKIP_VERSION_CHECK" in message


def test_the_override_lets_an_untested_polars_through(monkeypatch):
    monkeypatch.setenv("SAS7BDAT_POLARS_SKIP_VERSION_CHECK", "1")
    sp.check_polars_version("1.50.0", ">=1.41,<1.45")


def test_the_requirement_comes_from_the_installed_metadata():
    """Installed as a distribution, the range is the one pyproject.toml declares;
    in-tree, there is none and the check is a no-op rather than a guess."""
    requirement = sp.polars_requirement()
    if requirement is None:
        pytest.skip("not installed as a distribution")
    assert requirement.startswith(">=") or requirement.startswith("==")
    # Whatever it is, the polars this suite runs under satisfies it, or the
    # package could not have been imported above.
    import polars

    sp.check_polars_version(polars.__version__, requirement)
