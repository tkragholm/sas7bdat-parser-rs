use sas7bdat::SasReader;
use sas7bdat_test_support::common;

#[test]
fn attach_catalog_assigns_value_labels() {
    let data_path = common::fixture_path("fixtures/raw_data/readstat/test_data_win.sas7bdat");
    let catalog_path = common::fixture_path("fixtures/raw_data/readstat/test_formats_win.sas7bcat");

    let mut sas = SasReader::open(data_path).expect("open dataset");
    let metadata = sas.metadata();
    assert!(metadata.label_sets.is_empty());

    sas.attach_catalog(catalog_path).expect("load catalog");

    let metadata = sas.metadata();
    assert!(metadata.label_sets.contains_key("$A"));
    assert!(metadata.label_sets.contains_key("$B"));

    let label_a = metadata.label_sets.get("$A").expect("label $A");
    assert_eq!(label_a.labels.len(), 2);
    assert!(
        label_a.labels.iter().any(|label| label.label == "Male"),
        "labels: {:?}",
        label_a.labels
    );

    let sex_a = metadata
        .variables
        .iter()
        .find(|var| var.name == "SEXA")
        .expect("variable SEXA");
    assert_eq!(sex_a.value_labels.as_deref(), Some("$A"));

    let sex_b = metadata
        .variables
        .iter()
        .find(|var| var.name == "SEXB")
        .expect("variable SEXB");
    assert_eq!(sex_b.value_labels.as_deref(), Some("$B"));
}
