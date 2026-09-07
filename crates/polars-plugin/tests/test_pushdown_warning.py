"""A filter the plugin cannot read is reported once, not swallowed.

The plugin deserialises the expression polars serialises for it; a polars whose
format this build does not know makes that fail, and the scan falls back to
decoding every row for polars to filter afterwards. Correct, and silently slow,
which is why it warns. The warning names the polars-rust the wheel was built
against, since that is the number the reader needs.

One test only: the warning is issued once per process by design.
"""

from __future__ import annotations

import os
from pathlib import Path

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


class _UnreadableExpr:
    """Quacks like `pl.Expr` as far as `.meta.serialize()` and returns bytes no
    decoder accepts, which is what a foreign polars' format looks like here.
    A plausible-looking byte string is not enough: a leading zero deserialises
    as variant 0 of `Expr`, a unit variant, and the plugin reads it fine."""

    class meta:  # noqa: N801 - mirrors the attribute name on pl.Expr
        @staticmethod
        def serialize() -> bytes:
            # 0xC1 is the one byte MessagePack reserves and never emits, so no
            # decoder of any version reads it as anything.
            return b"\xc1"


@pytest.mark.skipif(not FIXTURE.exists(), reason=f"missing fixture: {FIXTURE}")
def test_an_unreadable_filter_warns_once_and_names_the_built_polars():
    with pytest.warns(
        RuntimeWarning, match="could not read the filter expression"
    ) as caught:
        sp.batch_reader(str(FIXTURE), None, _UnreadableExpr(), None, 8)
        sp.batch_reader(str(FIXTURE), None, _UnreadableExpr(), None, 8)
    ours = [
        w for w in caught if "could not read the filter expression" in str(w.message)
    ]
    assert len(ours) == 1
    assert "built against polars-rust 0." in str(ours[0].message)
