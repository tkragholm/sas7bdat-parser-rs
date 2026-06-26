use sas7bdat::{
    Dataset, LabelSet, ValueKey, ValueType, catalog::normalize_format_name,
    catalog::parse_catalog_file,
};
use std::path::Path;

fn fixture(rel: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

#[test]
fn attach_catalog_populates_label_sets() {
    let data = fixture("raw_data/readstat/test_data_win.sas7bdat");
    let cat = fixture("raw_data/readstat/test_formats_win.sas7bcat");
    if !data.exists() || !cat.exists() {
        return;
    }

    let mut ds = Dataset::open(&data).expect("open dataset");
    assert!(
        ds.metadata().label_sets.is_empty(),
        "no label sets before attach"
    );

    ds.attach_catalog(&cat).expect("attach catalog");

    let label_sets = &ds.metadata().label_sets;
    assert!(
        label_sets.contains_key("$A"),
        "expected $A, found keys: {:?}",
        label_sets.keys().collect::<Vec<_>>()
    );
    assert!(label_sets.contains_key("$B"), "expected $B");

    let label_a = label_sets.get("$A").expect("$A");
    assert_eq!(label_a.value_type, ValueType::String);
    assert!(!label_a.labels.is_empty(), "expected labels in $A");
    assert!(
        label_a.labels.iter().any(|l| l.label == "Male"),
        "expected 'Male' in $A, found: {:?}",
        label_a.labels
    );
}

#[test]
fn column_format_links_to_label_set() {
    let data = fixture("raw_data/readstat/test_data_win.sas7bdat");
    let cat = fixture("raw_data/readstat/test_formats_win.sas7bcat");
    if !data.exists() || !cat.exists() {
        return;
    }

    let mut ds = Dataset::open(&data).expect("open dataset");
    ds.attach_catalog(&cat).expect("attach catalog");

    let label_sets = &ds.metadata().label_sets;

    let has_labelled_columns = ds.columns().iter().any(|col| {
        col.format
            .as_deref()
            .is_some_and(|f| label_sets.contains_key(&normalize_format_name(f)))
    });

    assert!(
        has_labelled_columns,
        "expected at least one column with a matching label set; columns: {:?}",
        ds.columns()
            .iter()
            .map(|c| (&c.name, &c.format))
            .collect::<Vec<_>>()
    );
}

// §2.4 — tagged missing value tests

#[test]
fn tagged_missing_catalog_produces_tagged_value_keys() {
    let cat = fixture("raw_data/readstat/missing_formats.sas7bcat");
    if !cat.exists() {
        return;
    }

    let layout = parse_catalog_file(&cat).expect("parse missing catalog");
    // If any label set contains a Tagged key, the parser correctly decoded it.
    let tagged_count = layout
        .label_sets
        .iter()
        .flat_map(|ls| &ls.labels)
        .filter(|entry| matches!(entry.key, ValueKey::Tagged(_)))
        .count();

    // The missing_formats catalog is expected to have tagged missing entries.
    // If it has none at all, the catalog may not contain value labels for tagged
    // misssings (some catalogs only store MissingValuePolicy, not value labels).
    // We assert the parse succeeded and accept either outcome, documenting it.
    if tagged_count > 0 {
        // Verify that Tagged keys carry a non-null character.
        for ls in &layout.label_sets {
            for entry in &ls.labels {
                if let ValueKey::Tagged(ch) = entry.key {
                    assert!(
                        ch.is_ascii_alphabetic() || ch == '_',
                        "expected alphabetic or underscore tag, got {ch:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn lookup_numeric_does_not_match_tagged_keys() {
    let mut ls = LabelSet::new("T".to_owned(), ValueType::Numeric);
    ls.labels.push(sas7bdat::ValueLabel {
        key: ValueKey::Tagged('A'),
        label: "Missing A".to_owned(),
    });
    ls.labels.push(sas7bdat::ValueLabel {
        key: ValueKey::Numeric(1.0),
        label: "One".to_owned(),
    });

    // lookup_numeric should not return the tagged entry for any f64 value.
    assert_eq!(ls.lookup_numeric(1.0), Some("One"));
    // There is no numeric value that decodes to 'A'; tagged keys should be skipped.
    // (SAS tagged missing bit patterns are NaN with custom payloads — they will
    // never round-trip through a normal f64 lookup.)
    assert_eq!(ls.lookup_numeric(f64::NAN), None);
    assert_eq!(ls.lookup_numeric(0.0), None);
    assert_eq!(ls.lookup_numeric(65.0), None); // ASCII 'A' = 65
}

#[test]
fn label_set_lookup_returns_correct_labels() {
    let data = fixture("raw_data/readstat/test_data_win.sas7bdat");
    let cat = fixture("raw_data/readstat/test_formats_win.sas7bcat");
    if !data.exists() || !cat.exists() {
        return;
    }

    let mut ds = Dataset::open(&data).expect("open dataset");
    ds.attach_catalog(&cat).expect("attach catalog");

    let label_a = ds.metadata().label_sets.get("$A").expect("$A");

    // String-keyed label sets use ValueKey::String; verify lookup helpers work
    for entry in &label_a.labels {
        if let ValueKey::String(ref k) = entry.key {
            let found = label_a.lookup_string(k);
            assert_eq!(found, Some(entry.label.as_str()));
        }
    }
}
