"""Make a fixture-shaped hole in this suite impossible to mistake for a pass.

Most tests here open with `assert FIXTURE.exists()` against a corpus file that is
not in git (see `fixtures/README.md`). Without it they do not fail for any reason
to do with the plugin, and 21 of 24 failing on a missing path reads, at a glance,
like the plugin is broken. Skipping them silently is worse: `3 passed` looks green.

So: skip them with the reason attached, and print a banner naming the file and the
count. `just doctor` reports the same gap before you get here.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest


def _missing_module_fixtures(item: pytest.Item) -> dict[str, Path]:
    """Module-level `*_FIXTURE` paths that do not exist, by variable name."""
    module = getattr(item, "module", None)
    if module is None:
        return {}
    return {
        name: value
        for name, value in vars(module).items()
        if name.endswith("FIXTURE") and isinstance(value, Path) and not value.exists()
    }


def _uses(item: pytest.Item, name: str) -> bool:
    """Whether the test's own body names this fixture.

    Per test rather than per module: a module that declares a missing fixture still
    holds tests that never touch it, and skipping those would hide real failures
    behind an environment problem.
    """
    function = getattr(item, "function", None)
    if function is None:
        return False
    try:
        source = inspect.getsource(function)
    except (OSError, TypeError):
        return False
    return re.search(rf"\b{re.escape(name)}\b", source) is not None


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    missing: dict[str, int] = {}
    for item in items:
        for name, path in _missing_module_fixtures(item).items():
            if not _uses(item, name):
                continue
            item.add_marker(pytest.mark.skip(reason=f"missing fixture: {path}"))
            missing[str(path)] = missing.get(str(path), 0) + 1
            break
    config.stash[_MISSING] = missing


_MISSING = pytest.StashKey[dict]()


def pytest_terminal_summary(terminalreporter, exitstatus, config: pytest.Config) -> None:
    missing = config.stash.get(_MISSING, {})
    if not missing:
        return
    write = terminalreporter.write_line
    write("")
    write("NOT RUN, and not because anything is wrong with the plugin:", bold=True, yellow=True)
    for path, count in sorted(missing.items()):
        write(f"  {count} test(s) need {path}, which is not on this machine", yellow=True)
    write("  See fixtures/README.md. `just doctor` reports this too.", yellow=True)
    write("")
