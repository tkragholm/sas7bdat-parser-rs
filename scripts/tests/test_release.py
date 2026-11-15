"""Tests for scripts/release.py against a throwaway repository."""

from __future__ import annotations

import shutil
import subprocess

import pytest

from conftest import SCRIPTS

# ------------------------------------------------------------- version rules


@pytest.mark.parametrize(
    ("requirement", "version", "expected"),
    [
        ("0.10", "0.10.3", True),
        ("0.10.2", "0.10.3", True),
        ("0.10.3", "0.10.3", True),
        ("0.10.3", "0.10.2", False),
        ("0.10.10", "0.10.3", False),
        ("0.10", "0.11.0", False),
        ("0.10.2", "0.11.0", False),
        ("0.11", "0.10.3", False),
        ("0.1", "0.10.3", False),
        ("0.100", "0.10.3", False),
    ],
)
def test_compatible(release_module, requirement, version, expected):
    assert release_module.compatible(requirement, version) is expected


def test_nothing_accepts_the_next_minor(release_module):
    for requirement in ("0.10", "0.10.0", "0.10.9"):
        assert not release_module.compatible(requirement, "0.11.0")


def test_series(release_module):
    assert release_module.series("0.10.3") == "0.10"
    assert release_module.series("0.4.0") == "0.4"


def test_bumps(release_module):
    assert release_module.bump_patch("0.10.3") == "0.10.4"
    assert release_module.bump_minor("0.10.3") == "0.11.0"


# --------------------------------------------------------------------- owed


def owed(release):
    return {m.pkg.name: m for m in release.owed()}


def test_nothing_owed_right_after_tags(release):
    assert release.owed() == []


def test_change_in_a_crate_owes_a_patch(release, repo):
    repo.change("crates/sas7bdat-convert/src/lib.rs", "fix: handle empty input")
    moves = owed(release)
    assert moves["sas7bdat-convert"].target == "0.4.3"
    assert moves["sas7bdat-convert"].current == "0.4.2"
    assert moves["sas7bdat-convert"].because == "crates/sas7bdat-convert changed"
    assert "sas7bdat" not in moves
    assert "sas7bdat-polars" not in moves


@pytest.mark.parametrize(
    "path",
    [
        "crates/sas7bdat/examples/demo.rs",
        "crates/sas7bdat/benches/bench.rs",
        "crates/sas7bdat/CHANGELOG.md",
    ],
)
def test_files_that_do_not_ship_owe_nothing(release, repo, path):
    repo.change(path, "docs: note something")
    assert release.owed() == []


def test_reader_source_owes_both_wheels(release, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "fix: decode padding")
    moves = owed(release)
    assert set(moves) == {"sas7bdat", "sas7bdat-polars", "sas7bdat-cli"}
    assert "sas7bdat's source changed" in moves["sas7bdat-polars"].because
    assert "sas7bdat's source changed" in moves["sas7bdat-cli"].because
    assert moves["sas7bdat-polars"].target == "0.10.4"


def test_converter_source_owes_the_cli_but_not_polars(release, repo):
    repo.change("crates/sas7bdat-convert/src/lib.rs", "fix: quoting")
    assert set(owed(release)) == {"sas7bdat-convert", "sas7bdat-cli"}


def test_wheel_own_directory_owes_only_itself(release, repo):
    repo.change("crates/polars-plugin/src/lib.rs", "fix: lazy scan")
    assert set(owed(release)) == {"sas7bdat-polars"}


@pytest.mark.parametrize("committed", [True, False])
def test_crate_version_line_alone_owes_the_wheels_nothing(release, repo, committed):
    repo.edit("crates/sas7bdat/Cargo.toml", 'version = "0.10.3"', 'version = "0.10.4"')
    if committed:
        repo.commit("chore: bump")
    moves = owed(release)
    assert set(moves) == {"sas7bdat"}
    assert moves["sas7bdat"].because == "bumped, not yet tagged"


def test_requirement_on_a_path_dependency_owes_the_wheels_nothing(release, repo):
    repo.edit(
        "crates/sas7bdat-convert/Cargo.toml",
        'version = "0.10.2"',
        'version = "0.10.3"',
    )
    repo.commit("chore: raise requirement")
    moves = owed(release)
    assert set(moves) == {"sas7bdat-convert"}


def test_real_manifest_change_of_the_reader_owes_the_wheels(release, repo):
    repo.edit("crates/sas7bdat/Cargo.toml", "arrow = []", "arrow = []\nsimd = []")
    repo.commit("feat: simd feature")
    moves = owed(release)
    assert "sas7bdat's manifest changed" in moves["sas7bdat-polars"].because
    assert "sas7bdat's manifest changed" in moves["sas7bdat-cli"].because


def test_untagged_bump_is_reported(release, repo):
    repo.edit(
        "crates/sas7bdat-convert/Cargo.toml", 'version = "0.4.2"', 'version = "0.4.3"'
    )
    repo.commit("chore: bump")
    move = owed(release)["sas7bdat-convert"]
    assert (move.current, move.target) == ("0.4.2", "0.4.3")
    assert move.because == "bumped, not yet tagged"


def test_untagged_with_no_tag_at_all(release, repo):
    repo.git("tag", "-d", "sas7bdat-convert-v0.4.2")
    move = owed(release)["sas7bdat-convert"]
    assert (move.current, move.target) == ("-", "0.4.2")
    assert move.because == "bumped, not yet tagged"


def test_uncommitted_edits_are_seen(release, repo):
    repo.edit("crates/sas7bdat/src/lib.rs", "read()", "read_all()")
    assert "sas7bdat" in owed(release)


def test_breaking_commit_proposes_a_minor(release, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "feat!: drop the old reader")
    move = owed(release)["sas7bdat"]
    assert move.target == "0.11.0"
    assert "breaking" in move.because


def test_scoped_breaking_commit_proposes_a_minor(release, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "refactor(api)!: rename")
    assert owed(release)["sas7bdat"].target == "0.11.0"


def test_breaking_change_footer_proposes_a_minor(release, repo):
    repo.write("crates/sas7bdat/src/lib.rs", "pub fn other() {}\n")
    repo.git("add", "-A")
    repo.git("commit", "-q", "-m", "fix: rename", "-m", "BREAKING CHANGE: renamed")
    assert owed(release)["sas7bdat"].target == "0.11.0"


def test_feat_is_a_patch_for_a_crate(release, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "feat: read more")
    assert owed(release)["sas7bdat"].target == "0.10.4"


def test_feat_is_a_minor_for_an_r_package(release, repo):
    repo.change("crates/r-plugin/R/read.R", "feat: col_select")
    move = owed(release)["fastsas"]
    assert move.current == "0.3.0"
    assert move.target == "0.4.0"
    assert "new features" in move.because


def test_fix_is_a_patch_for_an_r_package(release, repo):
    repo.change("crates/r-plugin/R/read.R", "fix: dates")
    assert owed(release)["fastsas"].target == "0.3.1"


def test_feat_in_vendored_source_is_not_a_feature_of_the_r_package(release, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "feat: read more")
    repo.change("crates/r-plugin/R/read.R", "fix: dates")
    moves = owed(release)
    assert moves["sas7bdat"].target == "0.10.4"
    assert moves["fastsas"].target == "0.3.1"


def test_breaking_commit_in_vendored_source_is_not_breaking_for_the_wheel(
    release, repo
):
    repo.change("crates/sas7bdat/src/lib.rs", "feat!: rename")
    moves = owed(release)
    assert moves["sas7bdat"].target == "0.11.0"
    assert moves["sas7bdat-polars"].target == "0.10.4"


def test_r_package_changes_do_not_owe_the_other_packages(release, repo):
    repo.change("crates/r-plugin/R/read.R", "fix: dates")
    assert set(owed(release)) == {"fastsas"}


def test_commits_before_the_tag_are_not_counted(release, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "feat!: old break")
    repo.git("tag", "-d", "sas7bdat-v0.10.3")
    repo.git("tag", "sas7bdat-v0.10.3")
    repo.change("crates/sas7bdat/src/lib.rs", "fix: later")
    assert owed(release)["sas7bdat"].target == "0.10.4"


@pytest.mark.parametrize(
    ("wheel", "legacy", "path"),
    [
        ("sas7bdat-polars", "v0.10.3", "crates/polars-plugin/src/lib.rs"),
        ("sas7bdat-cli", "v0.9.6", "crates/sas7bdat-cli/src/main.rs"),
    ],
)
def test_legacy_wheel_tags_are_honoured(release, repo, wheel, legacy, path):
    current = {"sas7bdat-polars": "0.10.3", "sas7bdat-cli": "0.9.6"}[wheel]
    repo.retag(f"{wheel}-v{current}", legacy)
    assert release.owed() == []
    repo.change(path, "fix: something")
    assert wheel in owed(release)


def test_legacy_tags_are_not_honoured_for_crates(release, repo):
    repo.retag("sas7bdat-v0.10.3", "v0.10.3")
    move = owed(release)["sas7bdat"]
    assert move.because == "bumped, not yet tagged"


def test_last_tag_picks_the_newest(release, repo):
    repo.git("tag", "sas7bdat-v0.9.10")
    repo.git("tag", "sas7bdat-v0.10.0")
    assert release.last_tag(release.BY_NAME["sas7bdat"]) == "sas7bdat-v0.10.3"


# -------------------------------------------------------------------- plan


def moves_of(plan):
    return {m.pkg.name: m for m in plan.moves}


def edits_of(plan, repo):
    return {
        (str(path.relative_to(repo.root)), dep, old, new)
        for path, dep, old, new in plan.requirement_edits
    }


def test_nothing_owed_gives_an_empty_plan(release):
    plan = release.build_plan({})
    assert plan.moves == []
    assert plan.requirement_edits == []


def test_minor_override_moves_the_converter_and_the_r_packages(release, repo):
    plan = release.build_plan({"sas7bdat": "0.11.0"})
    moves = moves_of(plan)
    assert list(moves) == ["sas7bdat", "sas7bdat-convert", "fastsas", "fastsasconvert"]
    assert moves["sas7bdat"].target == "0.11.0"
    assert moves["sas7bdat"].because == "asked for"
    assert moves["sas7bdat-convert"].target == "0.5.0"
    assert 'requires sas7bdat "0.10.2"' in moves["sas7bdat-convert"].because
    assert moves["fastsas"].target == "0.3.1"
    assert moves["fastsasconvert"].target == "0.2.1"
    assert edits_of(plan, repo) == {
        ("crates/sas7bdat-convert/Cargo.toml", "sas7bdat", "0.10.2", "0.11"),
        ("crates/r-plugin/src/rust/Cargo.toml", "sas7bdat", "0.10", "0.11"),
        ("crates/r-convert-plugin/src/rust/Cargo.toml", "sas7bdat", "0.10", "0.11"),
        (
            "crates/r-convert-plugin/src/rust/Cargo.toml",
            "sas7bdat-convert",
            "0.4",
            "0.5",
        ),
    }


def test_plan_is_in_publish_order(release):
    plan = release.build_plan({"sas7bdat": "0.11.0"})
    names = [m.pkg.name for m in plan.moves]
    assert names == [p.name for p in release.PACKAGES if p.name in names]


def test_minor_override_does_not_force_the_wheels(release):
    plan = release.build_plan({"sas7bdat": "0.11.0"})
    assert "sas7bdat-polars" not in moves_of(plan)
    assert "sas7bdat-cli" not in moves_of(plan)


def test_same_minor_override_moves_only_the_vendoring_r_packages(release, repo):
    plan = release.build_plan({"sas7bdat": "0.10.5"})
    moves = moves_of(plan)
    assert set(moves) == {"sas7bdat", "fastsas", "fastsasconvert"}
    assert plan.requirement_edits == []
    assert moves["fastsas"].because == "vendors sas7bdat"
    assert moves["fastsasconvert"].because == "vendors sas7bdat"


def test_converter_release_moves_its_own_r_package_only(release):
    plan = release.build_plan({"sas7bdat-convert": "0.4.3"})
    moves = moves_of(plan)
    assert set(moves) == {"sas7bdat-convert", "fastsasconvert"}
    assert moves["fastsasconvert"].because == "vendors sas7bdat-convert"


def test_owed_patch_is_raised_to_a_minor_when_the_requirement_breaks(release, repo):
    repo.change("crates/sas7bdat-convert/src/lib.rs", "fix: quoting")
    plan = release.build_plan({"sas7bdat": "0.11.0"})
    assert moves_of(plan)["sas7bdat-convert"].target == "0.5.0"


def test_owed_minor_is_kept_when_the_requirement_breaks(release, repo):
    repo.change("crates/sas7bdat-convert/src/lib.rs", "feat!: new output")
    plan = release.build_plan({"sas7bdat": "0.11.0"})
    assert moves_of(plan)["sas7bdat-convert"].target == "0.5.0"
    assert (
        'requires sas7bdat "0.10.2"' not in moves_of(plan)["sas7bdat-convert"].because
    )


def test_override_of_an_owed_package_replaces_its_target(release, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "fix: padding")
    plan = release.build_plan({"sas7bdat": "0.10.9"})
    assert moves_of(plan)["sas7bdat"].target == "0.10.9"
    assert "crates/sas7bdat changed" in moves_of(plan)["sas7bdat"].because


def test_override_accepts_the_directory_name(release):
    plan = release.build_plan({"r-plugin": "0.3.5"})
    assert moves_of(plan)["fastsas"].target == "0.3.5"


# ------------------------------------------------------------ vendor.lock


def test_vendor_lock_current(release):
    for name in ("fastsas", "fastsasconvert"):
        assert release.vendor_stale(release.BY_NAME[name]) is False


def test_vendor_stale_when_a_crate_moves_past_the_lock(release, repo):
    repo.edit("crates/sas7bdat/Cargo.toml", 'version = "0.10.3"', 'version = "0.10.4"')
    assert release.vendor_stale(release.BY_NAME["fastsas"]) is True
    assert release.vendor_stale(release.BY_NAME["fastsasconvert"]) is True


def test_vendor_stale_only_for_the_package_that_vendors_the_crate(release, repo):
    repo.edit(
        "crates/sas7bdat-convert/Cargo.toml", 'version = "0.4.2"', 'version = "0.4.3"'
    )
    assert release.vendor_stale(release.BY_NAME["fastsas"]) is False
    assert release.vendor_stale(release.BY_NAME["fastsasconvert"]) is True


def test_vendor_stale_without_a_lock(release, repo):
    repo.path("crates/r-plugin/src/rust/vendor.lock").unlink()
    assert release.vendor_stale(release.BY_NAME["fastsas"]) is True


def test_vendor_stale_when_the_lock_lacks_the_crate(release, repo):
    repo.write("crates/r-plugin/src/rust/vendor.lock", "version = 4\n")
    assert release.vendor_stale(release.BY_NAME["fastsas"]) is True


def test_locked_version_reads_the_right_block(release, repo):
    repo.write(
        "crates/r-plugin/src/rust/vendor.lock",
        'version = 4\n\n[[package]]\nname = "sas7bdat-convert"\nversion = "9.9.9"\n\n'
        '[[package]]\nname = "sas7bdat"\nversion = "0.10.3"\n',
    )
    lock = repo.path("crates/r-plugin/src/rust/vendor.lock")
    assert release.locked_version(lock, "sas7bdat") == "0.10.3"
    assert release.locked_version(lock, "sas7bdat-convert") == "9.9.9"
    assert release.locked_version(lock, "absent") is None


# --------------------------------------------------------------- changelogs

TODAY = "2030-01-02"


@pytest.fixture
def dated(monkeypatch, release_module):
    monkeypatch.setattr(release_module.time, "strftime", lambda _fmt: TODAY)


def move(release, name, target):
    pkg = release.BY_NAME[name]
    return release.Move(pkg, release.version_of(pkg), target, "test")


def test_section_extracts_exactly_one_version(release, repo):
    repo.write(
        "crates/sas7bdat/CHANGELOG.md",
        "# Changelog\n\n## [Unreleased]\n\nPending.\n\n"
        "## [0.10.3] - 2026-02-01\n\nThree.\n\n### Fixed\n\n- x\n\n"
        "## [0.10.2] - 2026-01-01\n\nTwo.\n\n"
        "## [0.10.30] - 2026-01-01\n\nThirty.\n",
    )
    pkg = release.BY_NAME["sas7bdat"]
    assert release.section(pkg, "0.10.3") == "Three.\n\n### Fixed\n\n- x"
    assert release.section(pkg, "0.10.2") == "Two."
    assert release.section(pkg, "0.10.30") == "Thirty."
    assert release.section(pkg, "0.10.4") is None
    assert release.section(pkg, "Unreleased") == "Pending."


def test_section_of_a_missing_file(release, repo):
    repo.path("crates/sas7bdat/CHANGELOG.md").unlink()
    assert release.section(release.BY_NAME["sas7bdat"], "0.10.3") is None


def test_section_of_an_r_news_file(release, repo):
    repo.write(
        "crates/r-plugin/NEWS.md",
        "# fastsas 0.4.0\n\n- Four.\n\n# fastsas 0.3.0\n\n- Three.\n",
    )
    pkg = release.BY_NAME["fastsas"]
    assert release.section(pkg, "0.4.0") == "- Four."
    assert release.section(pkg, "0.3.0") == "- Three."
    assert release.section(pkg, "0.3") is None


def test_write_draft_creates_a_missing_crate_changelog(release, repo, dated):
    repo.path("crates/sas7bdat-convert/CHANGELOG.md").unlink()
    repo.change("crates/sas7bdat-convert/src/lib.rs", "fix: quoting")
    m = move(release, "sas7bdat-convert", "0.4.3")
    assert release.write_draft(m, {}) is True
    text = repo.read("crates/sas7bdat-convert/CHANGELOG.md")
    assert text.startswith("# Changelog\n")
    assert "`sas7bdat-convert` crate" in text
    assert text.index("## [Unreleased]") < text.index(f"## [0.4.3] - {TODAY}")
    assert release.DRAFT in release.section(m.pkg, "0.4.3")
    assert "- Quoting" in text


def test_write_draft_inserts_under_unreleased(release, repo, dated):
    repo.change("crates/sas7bdat/src/lib.rs", "fix: padding")
    m = move(release, "sas7bdat", "0.10.4")
    assert release.write_draft(m, {}) is True
    text = repo.read("crates/sas7bdat/CHANGELOG.md")
    assert (
        text.index("## [Unreleased]")
        < text.index(f"## [0.10.4] - {TODAY}")
        < text.index("## [0.10.3]")
    )
    assert text.count("## [Unreleased]") == 1
    assert "- Earlier work." in text


def test_write_draft_is_idempotent(release, repo, dated):
    repo.change("crates/sas7bdat/src/lib.rs", "fix: padding")
    m = move(release, "sas7bdat", "0.10.4")
    assert release.write_draft(m, {}) is True
    first = repo.read("crates/sas7bdat/CHANGELOG.md")
    assert release.write_draft(m, {}) is False
    assert repo.read("crates/sas7bdat/CHANGELOG.md") == first


def test_write_draft_requires_the_unreleased_heading(release, repo):
    repo.write("crates/sas7bdat/CHANGELOG.md", "# Changelog\n\n## [0.10.3] - x\n")
    with pytest.raises(SystemExit, match="Unreleased"):
        release.write_draft(move(release, "sas7bdat", "0.10.4"), {})


def test_write_draft_for_an_r_package(release, repo):
    repo.change("crates/r-plugin/R/read.R", "feat: col_select")
    m = move(release, "fastsas", "0.4.0")
    assert release.write_draft(m, {}) is True
    text = repo.read("crates/r-plugin/NEWS.md")
    assert text.startswith("# fastsas 0.4.0\n\n")
    assert text.index("# fastsas 0.4.0") < text.index("# fastsas 0.3.0")
    assert "- Col_select." in text
    assert "- Built with sas7bdat 0.10.3." in text
    assert text.endswith("\n") and not text.endswith("\n\n")


def test_r_draft_names_the_targeted_crate_versions(release, repo):
    m = move(release, "fastsasconvert", "0.2.1")
    entry = release.draft_entry(m, {"sas7bdat": "0.11.0", "sas7bdat-convert": "0.5.0"})
    assert "- Built with sas7bdat 0.11.0, sas7bdat-convert 0.5.0." in entry


def test_wheel_draft_names_what_it_is_built_with(release, repo):
    m = move(release, "sas7bdat-cli", "0.9.7")
    entry = release.draft_entry(m, {"sas7bdat": "0.10.4"})
    assert "Built with sas7bdat 0.10.4, sas7bdat-convert 0.4.2." in entry


def test_draft_entry_groups_subjects(release, repo):
    for message in (
        "feat(reader): add datetimes",
        "fix: padding",
        "perf: faster scan",
        "chore: tidy",
        "ci: cache",
        "docs: readme",
        "test: more",
        "feat!: drop the old api",
        "refactor: split module",
        "not a conventional subject",
    ):
        repo.change("crates/sas7bdat/src/lib.rs", message)
    entry = release.draft_entry(move(release, "sas7bdat", "0.11.0"), {})
    assert entry.startswith(release.DRAFT)
    added, changed, fixed = (
        entry.index("### Added"),
        entry.index("### Changed"),
        entry.index("### Fixed"),
    )
    assert added < changed < fixed
    assert "- Add datetimes" in entry[added:changed]
    assert "- Faster scan" in entry[changed:fixed]
    assert "- Split module" in entry[changed:fixed]
    assert "- Drop the old api" in entry[changed:fixed]
    assert "- Padding" in entry[fixed:]
    for skipped in ("tidy", "cache", "readme", "more", "conventional"):
        assert skipped not in entry


def test_draft_entry_without_subjects_asks_for_a_description(release, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "chore: tidy")
    entry = release.draft_entry(move(release, "sas7bdat", "0.10.4"), {})
    assert "Describe this release" in entry


def test_drafts_left(release, repo, dated):
    repo.change("crates/sas7bdat/src/lib.rs", "fix: padding")
    repo.change("crates/sas7bdat-convert/src/lib.rs", "fix: quoting")
    moves = [
        move(release, "sas7bdat", "0.10.4"),
        move(release, "sas7bdat-convert", "0.4.3"),
    ]
    plan = release.Plan(moves, [])
    assert release.drafts_left(plan) == []
    for m in moves:
        release.write_draft(m, {})
    left = release.drafts_left(plan)
    assert [str(p) for p in left] == [
        "crates/sas7bdat/CHANGELOG.md",
        "crates/sas7bdat-convert/CHANGELOG.md",
    ]
    repo.edit("crates/sas7bdat/CHANGELOG.md", release.DRAFT + "\n", "")
    assert [str(p) for p in release.drafts_left(plan)] == [
        "crates/sas7bdat-convert/CHANGELOG.md"
    ]


def test_drafts_left_ignores_the_marker_in_another_version(release, repo):
    repo.write(
        "crates/sas7bdat/CHANGELOG.md",
        f"## [Unreleased]\n\n## [0.10.4] - x\n\nDone.\n\n## [0.10.3] - x\n\n{release.DRAFT}\n",
    )
    plan = release.Plan([move(release, "sas7bdat", "0.10.4")], [])
    assert release.drafts_left(plan) == []


# ------------------------------------------------------------ writing files


def test_write_version_for_a_crate_touches_only_the_package_version(release, repo):
    release.write_version(release.BY_NAME["sas7bdat-convert"], "0.5.0")
    text = repo.read("crates/sas7bdat-convert/Cargo.toml")
    assert 'version = "0.5.0"' in text
    assert 'name = "sas7bdat-convert"\nversion = "0.5.0"' in text
    assert 'version = "0.10.2"' in text


def test_write_version_for_a_wheel_updates_both_manifests(release, repo):
    release.write_version(release.BY_NAME["sas7bdat-polars"], "0.10.4")
    assert 'version = "0.10.4"' in repo.read("crates/polars-plugin/Cargo.toml")
    assert 'version = "0.10.4"' in repo.read("crates/polars-plugin/pyproject.toml")
    assert release.version_of(release.BY_NAME["sas7bdat-polars"]) == "0.10.4"


def test_write_version_for_a_crate_leaves_pyproject_alone(release, repo):
    before = repo.read("crates/polars-plugin/pyproject.toml")
    release.write_version(release.BY_NAME["sas7bdat"], "0.10.4")
    assert repo.read("crates/polars-plugin/pyproject.toml") == before


def test_write_version_for_an_r_package(release, repo):
    release.write_version(release.BY_NAME["fastsas"], "0.4.0")
    text = repo.read("crates/r-plugin/DESCRIPTION")
    assert "Version: 0.4.0\n" in text
    assert "Package: fastsas\n" in text
    assert 'version = "0.1.0"' in repo.read("crates/r-plugin/src/rust/Cargo.toml")
    assert release.version_of(release.BY_NAME["fastsas"]) == "0.4.0"


def test_write_requirement_edits_one_dependency(release, repo):
    path = repo.path("crates/sas7bdat-convert/Cargo.toml")
    release.write_requirement(path, "sas7bdat", "0.10.2", "0.11")
    text = path.read_text()
    assert '{ path = "../sas7bdat", version = "0.11", features = ["arrow"] }' in text
    assert 'version = "0.4.2"' in text


def test_write_requirement_leaves_a_similarly_named_dependency(release, repo):
    path = repo.path("crates/r-convert-plugin/src/rust/Cargo.toml")
    release.write_requirement(path, "sas7bdat", "0.10", "0.11")
    text = path.read_text()
    assert 'sas7bdat = { version = "0.11" }' in text
    assert 'sas7bdat-convert = { version = "0.4" }' in text
    release.write_requirement(path, "sas7bdat-convert", "0.4", "0.5")
    assert 'sas7bdat-convert = { version = "0.5" }' in path.read_text()


def test_write_requirement_only_matches_the_old_value(release, repo):
    path = repo.path("crates/r-plugin/src/rust/Cargo.toml")
    before = path.read_text()
    release.write_requirement(path, "sas7bdat", "0.9", "0.11")
    assert path.read_text() == before


def test_requirement_reads_the_version_half(release, repo):
    convert = repo.path("crates/sas7bdat-convert/Cargo.toml")
    assert release.requirement(convert, "sas7bdat") == "0.10.2"
    cli = repo.path("crates/sas7bdat-cli/Cargo.toml")
    assert release.requirement(cli, "sas7bdat") is None
    assert release.requirement(cli, "absent") is None


def test_write_citation(release, repo, dated):
    assert release.write_citation("0.10.4") is True
    text = repo.read("CITATION.cff")
    assert "\nversion: 0.10.4\n" in text
    assert f"date-released: {TODAY}\n" in text
    assert "cff-version: 1.2.0\n" in text


def test_write_citation_is_a_no_op_for_the_current_version(release, repo, dated):
    before = repo.read("CITATION.cff")
    assert release.write_citation("0.10.3") is False
    assert repo.read("CITATION.cff") == before


# ----------------------------------------------------------------- prepare


def test_prepare_writes_the_whole_release(release, repo, dated, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(release, "regenerate_lockfiles", lambda: calls.append(1))
    repo.change("crates/sas7bdat/src/lib.rs", "fix: padding")
    plan = release.build_plan({"sas7bdat": "0.11.0"})
    release.prepare(plan)
    assert calls == [1]
    assert release.version_of(release.BY_NAME["sas7bdat"]) == "0.11.0"
    assert release.version_of(release.BY_NAME["sas7bdat-convert"]) == "0.5.0"
    assert (
        release.requirement(repo.path("crates/sas7bdat-convert/Cargo.toml"), "sas7bdat")
        == "0.11"
    )
    assert (
        release.requirement(
            repo.path("crates/r-plugin/src/rust/Cargo.toml"), "sas7bdat"
        )
        == "0.11"
    )
    assert "\nversion: 0.11.0\n" in repo.read("CITATION.cff")
    assert len(release.drafts_left(plan)) == len(plan.moves)
    # A second run finds everything done.
    before = repo.git("diff")
    release.prepare(plan)
    assert repo.git("diff") == before


# ------------------------------------------------------- check-versions.sh


@pytest.fixture
def checked(repo):
    """The repository with the real scripts copied in, so the shell script's
    `cd` to its own parent lands in the throwaway tree."""
    target = repo.path("scripts")
    target.mkdir()
    for name in ("check-versions.sh", "release.py"):
        shutil.copy(SCRIPTS / name, target / name)

    def run():
        return subprocess.run(
            ["bash", "scripts/check-versions.sh"],
            cwd=repo.root,
            capture_output=True,
            text=True,
        )

    return run


def test_check_versions_passes_on_a_consistent_tree(checked):
    result = checked()
    assert result.returncode == 0, result.stderr
    assert "ok every package matches its last release" in result.stdout


@pytest.mark.parametrize(
    ("path", "old", "new"),
    [
        ("crates/r-plugin/src/rust/Cargo.toml", '"0.10"', '"0.9"'),
        ("crates/r-plugin/src/rust/Cargo.toml", '"0.10"', '"0.10.9"'),
        (
            "crates/r-convert-plugin/src/rust/Cargo.toml",
            'version = "0.4"',
            'version = "0.3"',
        ),
        ("crates/sas7bdat-convert/Cargo.toml", '"0.10.2"', '"0.11"'),
        ("crates/r-plugin/src/rust/Cargo.lock", '"0.10.3"', '"0.10.2"'),
        ("crates/polars-plugin/pyproject.toml", '"0.10.3"', '"0.10.4"'),
    ],
)
def test_check_versions_fails_on_drift(checked, repo, path, old, new):
    repo.edit(path, old, new)
    assert checked().returncode == 1


@pytest.mark.parametrize("requirement", ["0.10", "0.10.0", "0.10.3"])
def test_check_versions_accepts_requirements_in_range(checked, repo, requirement):
    repo.edit("crates/r-plugin/src/rust/Cargo.toml", '"0.10"', f'"{requirement}"')
    assert checked().returncode == 0


def test_check_versions_reports_what_is_owed(checked, repo):
    repo.change("crates/sas7bdat/src/lib.rs", "fix: padding")
    result = checked()
    assert result.returncode == 0
    assert "owed  sas7bdat 0.10.3 -> 0.10.4" in result.stdout
    assert "owed  sas7bdat-polars" in result.stdout


def test_last_tag_orders_by_version_not_by_name(release, repo):
    repo.git("tag", "-a", "v0.10.1", "-m", "legacy")
    pkg = release.BY_NAME["sas7bdat-polars"]
    assert release.last_tag(pkg) == "sas7bdat-polars-v0.10.3"


def test_draft_skips_a_commit_that_only_touched_the_changelog(release, repo):
    repo.change("crates/sas7bdat/CHANGELOG.md", "fix(convert): a header line")
    repo.change("crates/sas7bdat/src/lib.rs", "fix: a real fix")
    move = release.Move(release.BY_NAME["sas7bdat"], "0.10.3", "0.10.4", "")
    entry = release.draft_entry(move, {})
    assert "A real fix" in entry
    assert "A header line" not in entry


def test_a_legacy_tag_of_the_other_wheel_is_not_this_wheels_last(release, repo):
    repo.git("tag", "-a", "v0.10.1", "-m", "the plugin, under the shared prefix")
    pkg = release.BY_NAME["sas7bdat-cli"]
    assert release.last_tag(pkg).startswith("sas7bdat-cli-v")
