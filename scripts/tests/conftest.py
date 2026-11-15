"""Fixtures that build a small git repository shaped like this one and point
scripts/release.py at it. Nothing here reaches the network, cargo or the real
checkout."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]

CRATE_CHANGELOG = """# Changelog

## [Unreleased]

## [{version}] - 2026-01-01

- Earlier work.
"""

SAS7BDAT_CARGO = """[package]
name = "sas7bdat"
version = "0.10.3"
edition = "2021"

[dependencies]
serde = "1"

[features]
arrow = []
"""

CONVERT_CARGO = """[package]
name = "sas7bdat-convert"
version = "0.4.2"
edition = "2021"

[dependencies]
sas7bdat = { path = "../sas7bdat", version = "0.10.2", features = ["arrow"] }
"""

POLARS_CARGO = """[package]
name = "sas7bdat-polars"
version = "0.10.3"
edition = "2021"

[dependencies]
sas7bdat = { path = "../sas7bdat", features = ["arrow"] }
"""

CLI_CARGO = """[package]
name = "sas7bdat-cli"
version = "0.9.6"
edition = "2021"

[dependencies]
anyhow = { workspace = true }
sas7bdat = { path = "../sas7bdat", features = ["arrow"] }
sas7bdat-convert = { path = "../sas7bdat-convert" }
"""

PYPROJECT = """[project]
name = "{name}"
version = "{version}"
"""

R_PLUGIN_CARGO = """[package]
name = "fastsas"
version = "0.1.0"

[dependencies]
sas7bdat = { version = "0.10", features = ["dictionary"] }

[workspace]
"""

R_CONVERT_CARGO = """[package]
name = "fastsasconvert"
version = "0.1.0"

[dependencies]
sas7bdat = { version = "0.10" }
sas7bdat-convert = { version = "0.4" }

[workspace]
"""

DESCRIPTION = """Package: {name}
Title: Something
Version: {version}
License: MIT
"""

NEWS = """# {name} {version}

- Earlier work.
"""

CITATION = """cff-version: 1.2.0
title: "sas7bdat"
version: 0.10.3
date-released: 2026-01-01
"""


def lock(**pins: str) -> str:
    blocks = [
        f'[[package]]\nname = "{name.replace("_", "-")}"\nversion = "{version}"\n'
        for name, version in pins.items()
    ]
    return "version = 4\n\n" + "\n".join(blocks)


TAGS = [
    "sas7bdat-v0.10.3",
    "sas7bdat-convert-v0.4.2",
    "sas7bdat-polars-v0.10.3",
    "sas7bdat-cli-v0.9.6",
    "fastsas-v0.3.0",
    "fastsasconvert-v0.2.0",
]


class Repo:
    def __init__(self, root: Path):
        self.root = root

    def path(self, rel: str) -> Path:
        return self.root / rel

    def read(self, rel: str) -> str:
        return self.path(rel).read_text()

    def write(self, rel: str, text: str) -> None:
        target = self.path(rel)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def edit(self, rel: str, old: str, new: str) -> None:
        text = self.read(rel)
        assert old in text, f"{old!r} not in {rel}"
        self.write(rel, text.replace(old, new, 1))

    def git(self, *args: str) -> str:
        done = subprocess.run(
            ["git", *args], cwd=self.root, capture_output=True, text=True, check=True
        )
        return done.stdout

    def commit(self, message: str) -> None:
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)

    def change(self, rel: str, message: str) -> None:
        """Commit a change to a file under `rel`."""
        target = self.path(rel)
        old = target.read_text() if target.is_file() else ""
        self.write(rel, old + f"// {message}\n")
        self.commit(message)

    def retag(self, old: str, new: str) -> None:
        self.git("tag", "-d", old)
        self.git("tag", new)


def populate(root: Path) -> None:
    def put(rel: str, text: str) -> None:
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    put("crates/sas7bdat/Cargo.toml", SAS7BDAT_CARGO)
    put("crates/sas7bdat/src/lib.rs", "pub fn read() {}\n")
    put("crates/sas7bdat/examples/demo.rs", "fn main() {}\n")
    put("crates/sas7bdat/benches/bench.rs", "fn main() {}\n")
    put("crates/sas7bdat/CHANGELOG.md", CRATE_CHANGELOG.format(version="0.10.3"))

    put("crates/sas7bdat-convert/Cargo.toml", CONVERT_CARGO)
    put("crates/sas7bdat-convert/src/lib.rs", "pub fn convert() {}\n")
    put("crates/sas7bdat-convert/CHANGELOG.md", CRATE_CHANGELOG.format(version="0.4.2"))

    put("crates/polars-plugin/Cargo.toml", POLARS_CARGO)
    put(
        "crates/polars-plugin/pyproject.toml",
        PYPROJECT.format(name="sas7bdat-polars", version="0.10.3"),
    )
    put("crates/polars-plugin/src/lib.rs", "pub fn plugin() {}\n")
    put("crates/polars-plugin/CHANGELOG.md", CRATE_CHANGELOG.format(version="0.10.3"))

    put("crates/sas7bdat-cli/Cargo.toml", CLI_CARGO)
    put(
        "crates/sas7bdat-cli/pyproject.toml",
        PYPROJECT.format(name="sas7bdat-cli", version="0.9.6"),
    )
    put("crates/sas7bdat-cli/src/main.rs", "fn main() {}\n")
    put("crates/sas7bdat-cli/CHANGELOG.md", CRATE_CHANGELOG.format(version="0.9.6"))

    put(
        "crates/r-plugin/DESCRIPTION",
        DESCRIPTION.format(name="fastsas", version="0.3.0"),
    )
    put("crates/r-plugin/NEWS.md", NEWS.format(name="fastsas", version="0.3.0"))
    put("crates/r-plugin/R/read.R", "read <- function() NULL\n")
    put("crates/r-plugin/src/rust/Cargo.toml", R_PLUGIN_CARGO)
    put("crates/r-plugin/src/rust/vendor.lock", lock(sas7bdat="0.10.3"))
    put("crates/r-plugin/src/rust/Cargo.lock", lock(sas7bdat="0.10.3"))

    put(
        "crates/r-convert-plugin/DESCRIPTION",
        DESCRIPTION.format(name="fastsasconvert", version="0.2.0"),
    )
    put(
        "crates/r-convert-plugin/NEWS.md",
        NEWS.format(name="fastsasconvert", version="0.2.0"),
    )
    put("crates/r-convert-plugin/R/convert.R", "convert <- function() NULL\n")
    put("crates/r-convert-plugin/src/rust/Cargo.toml", R_CONVERT_CARGO)
    vendored = lock(sas7bdat="0.10.3", sas7bdat_convert="0.4.2")
    put("crates/r-convert-plugin/src/rust/vendor.lock", vendored)
    put("crates/r-convert-plugin/src/rust/Cargo.lock", vendored)

    put("CITATION.cff", CITATION)
    put(
        "Cargo.lock",
        lock(
            sas7bdat="0.10.3",
            sas7bdat_convert="0.4.2",
            sas7bdat_polars="0.10.3",
            sas7bdat_cli="0.9.6",
        ),
    )


@pytest.fixture(scope="session")
def template(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("template")
    env = {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        populate(root)
        repo = Repo(root)
        repo.git("init", "-q", "-b", "main")
        repo.commit("initial")
        for tag in TAGS:
            repo.git("tag", tag)
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    return root


@pytest.fixture(scope="session")
def release_module():
    spec = importlib.util.spec_from_file_location("release", SCRIPTS / "release.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _forbidden(name: str):
    def fail(*_args, **_kwargs):
        raise AssertionError(f"{name} would leave the sandbox")

    return fail


@pytest.fixture(autouse=True)
def sandbox(monkeypatch, release_module, tmp_path):
    """Git works with a fixed identity and no user config; the registries and
    cargo are unreachable; the module's root is a directory that does not exist
    until `repo` replaces it."""
    for key, value in {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }.items():
        monkeypatch.setenv(key, value)
    for name in (
        "_get",
        "crates_io_versions",
        "pypi_versions",
        "r_universe",
        "remote_tags",
        "regenerate_lockfiles",
    ):
        monkeypatch.setattr(release_module, name, _forbidden(name))
    monkeypatch.setattr(release_module, "ROOT", tmp_path / "unset")


@pytest.fixture
def repo(template, tmp_path, monkeypatch, release_module) -> Repo:
    root = tmp_path / "repo"
    shutil.copytree(template, root)
    monkeypatch.setattr(release_module, "ROOT", root)
    return Repo(root)


@pytest.fixture
def release(release_module, repo):
    return release_module
