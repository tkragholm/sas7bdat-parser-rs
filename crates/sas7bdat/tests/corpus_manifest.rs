//! The tracked test files are the ones `MANIFEST.tsv` lists, and each still reads as the
//! manifest says: byte order, compression, encoding, columns, the rows its header counts,
//! how many of those are deleted, and that a full read returns the rest.
//!
//! The facts are read from each file's header, so this checks that every format feature the
//! set covers (big-endian, RLE and RDC compression, deleted rows, multi-byte encodings, an
//! empty file) still opens and decodes, in CI and without the untracked corpus.

mod common;

use sas7bdat::{CompressionKind, Dataset, Endianness};
use std::collections::BTreeSet;

struct Entry {
    path: String,
    bytes: u64,
    byte_order: String,
    compression: String,
    rows: u64,
    deleted: u64,
    encoding: String,
    columns: usize,
}

fn entries() -> Vec<Entry> {
    let text = std::fs::read_to_string(common::tracked_root().join("MANIFEST.tsv"))
        .expect("read MANIFEST.tsv");
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("path\t") && !line.is_empty())
        .map(|line| {
            let f: Vec<&str> = line.split('\t').collect();
            assert_eq!(
                f.len(),
                14,
                "MANIFEST.tsv: wrong number of fields in {line}"
            );
            Entry {
                path: f[0].to_owned(),
                bytes: f[1].parse().expect("bytes"),
                byte_order: f[8].to_owned(),
                compression: f[9].to_owned(),
                rows: f[10].parse().expect("rows"),
                deleted: f[11].parse().expect("deleted"),
                encoding: f[12].to_owned(),
                columns: f[13].parse().expect("columns"),
            }
        })
        .collect()
}

#[test]
fn the_manifest_lists_exactly_the_tracked_files() {
    let root = common::tracked_root();
    let mut on_disk = BTreeSet::new();
    for dir in std::fs::read_dir(&root).expect("read corpus dir") {
        let dir = dir.expect("dir entry").path();
        if !dir.is_dir() {
            continue;
        }
        for file in std::fs::read_dir(&dir).expect("read source dir") {
            let file = file.expect("file entry").path();
            let rel = file.strip_prefix(&root).expect("under root");
            on_disk.insert(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    let listed: BTreeSet<String> = entries().into_iter().map(|e| e.path).collect();
    assert_eq!(on_disk, listed);
}

#[test]
fn every_tracked_file_reads_as_its_manifest_says() {
    for entry in entries() {
        let path = common::tracked_root().join(&entry.path);
        let size = std::fs::metadata(&path).expect("stat").len();
        assert_eq!(size, entry.bytes, "{}: size", entry.path);

        let ds = Dataset::open(&path).unwrap_or_else(|e| panic!("{}: open: {e}", entry.path));
        let meta = ds.metadata();
        let byte_order = match meta.endianness {
            Endianness::Little => "little",
            Endianness::Big => "big",
        };
        let compression = match meta.compression {
            CompressionKind::None => "none",
            CompressionKind::Row => "row",
            CompressionKind::Binary => "binary",
            CompressionKind::Unknown => "unknown",
        };
        assert_eq!(byte_order, entry.byte_order, "{}: byte order", entry.path);
        assert_eq!(
            compression, entry.compression,
            "{}: compression",
            entry.path
        );
        assert_eq!(
            meta.encoding.as_deref().unwrap_or("-"),
            entry.encoding,
            "{}: encoding",
            entry.path
        );
        assert_eq!(ds.columns().len(), entry.columns, "{}: columns", entry.path);
        assert_eq!(
            meta.row_count, entry.rows,
            "{}: rows in the header",
            entry.path
        );
        assert_eq!(
            meta.deleted_row_count, entry.deleted,
            "{}: deleted rows",
            entry.path
        );

        let read = ds
            .collect_rows()
            .unwrap_or_else(|e| panic!("{}: read: {e}", entry.path));
        assert_eq!(
            read.len() as u64,
            entry.rows - entry.deleted,
            "{}: rows read",
            entry.path
        );
    }
}
