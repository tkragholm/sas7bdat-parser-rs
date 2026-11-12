#!/usr/bin/env python3
"""Download the benchmark files into fixtures/benchmark/ and check them.

Usage: fetch.py [--dest DIR]

Every file is public US federal data: the American Housing Survey from the US
Census Bureau and the Medical Expenditure Panel Survey from AHRQ. Each archive
is downloaded once, the one member needed is extracted under the name run.py
expects, and its SHA-256 is checked against run.py's table.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run import DATA, FILES

AHS = "https://www2.census.gov/programs-surveys/ahs/2013"
SOURCES = {
    # name in FILES: (archive URL, member inside the archive)
    "ahs2013n": (
        f"{AHS}/AHS%202013%20National%20PUF%20v2.0%20Flat%20SAS.zip",
        "ahs2013n.sas7bdat",
    ),
    "ahs2013_household": (
        f"{AHS}/AHS%202013%20National%20PUF%20v2.0%20SAS.zip",
        "household.sas7bdat",
    ),
    "ahs2013_person": (
        f"{AHS}/AHS%202013%20National%20PUF%20v2.0%20SAS.zip",
        "person.sas7bdat",
    ),
    "meps2021_h233": (
        "https://meps.ahrq.gov/mepsweb/data_files/pufs/h233/h233v9.zip",
        "h233.sas7bdat",
    ),
    "ahs2013_topical": (None, "topical.sas7bdat"),
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest", type=Path, default=DATA)
    args = ap.parse_args()
    args.dest.mkdir(parents=True, exist_ok=True)
    archives: dict[str, Path] = {}
    failed = False
    with tempfile.TemporaryDirectory() as tmp:
        for key, (url, member) in SOURCES.items():
            name, expected = FILES[key]
            target = args.dest / name
            if target.exists() and sha256(target) == expected:
                print(f"ok      {name}")
                continue
            if url is None:
                print(
                    f"missing {name}: no public download is known; see benchmarks/README.md"
                )
                failed = True
                continue
            if url not in archives:
                archives[url] = Path(tmp) / f"archive{len(archives)}.zip"
                print(f"fetch   {url}")
                with urllib.request.urlopen(url) as r, archives[url].open("wb") as f:
                    shutil.copyfileobj(r, f)
            with zipfile.ZipFile(archives[url]) as z:
                entry = next(
                    i for i in z.infolist() if Path(i.filename).name.lower() == member
                )
                with z.open(entry) as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            if sha256(target) != expected:
                print(f"BAD     {name}: checksum differs from run.py")
                failed = True
            else:
                print(f"ok      {name}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
