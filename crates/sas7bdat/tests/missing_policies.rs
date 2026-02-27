use sas7bdat::{CatalogScanPolicy, OrderingMode, SasReader, Shape};
use sas7bdat_test_support::common;

fn tagged_tags(policy: &sas7bdat::dataset::MissingValuePolicy) -> Vec<char> {
    policy
        .tagged_missing
        .iter()
        .filter_map(|missing| missing.tag)
        .collect()
}

#[test]
fn scan_missing_policies_records_numeric_tags() {
    let data = common::fixture_path("fixtures/raw_data/readstat/missing_test.sas7bdat");
    let catalog = common::fixture_path("fixtures/raw_data/readstat/missing_formats.sas7bcat");

    let mut sas = SasReader::open(data).expect("open dataset");
    sas.attach_catalog(catalog).expect("load catalog");

    let metadata = sas.metadata();
    let variables = &metadata.variables;

    let var1 = variables.iter().find(|var| var.name == "var1").unwrap();
    assert!(tagged_tags(&var1.missing).contains(&'A'));

    let var2 = variables.iter().find(|var| var.name == "var2").unwrap();
    assert!(tagged_tags(&var2.missing).contains(&'B'));

    let var3 = variables.iter().find(|var| var.name == "var3").unwrap();
    assert!(tagged_tags(&var3.missing).contains(&'C'));

    let var4 = variables.iter().find(|var| var.name == "var4").unwrap();
    assert!(tagged_tags(&var4.missing).contains(&'X'));

    let var5 = variables.iter().find(|var| var.name == "var5").unwrap();
    assert!(tagged_tags(&var5.missing).contains(&'Y'));

    let var6 = variables.iter().find(|var| var.name == "var6").unwrap();
    assert!(tagged_tags(&var6.missing).contains(&'Z'));

    let var7 = variables.iter().find(|var| var.name == "var7").unwrap();
    assert!(var7.missing.system_missing);
}

fn policy_signature(
    policy: &sas7bdat::dataset::MissingValuePolicy,
) -> (bool, Vec<Option<char>>, usize) {
    let tags = policy.tagged_missing.iter().map(|item| item.tag).collect();
    (policy.system_missing, tags, policy.ranges.len())
}

#[test]
fn deferred_catalog_scan_matches_eager_after_row_access() {
    let data = common::fixture_path("fixtures/raw_data/readstat/missing_test.sas7bdat");
    let catalog = common::fixture_path("fixtures/raw_data/readstat/missing_formats.sas7bcat");

    let mut eager = SasReader::open(&data).expect("open eager dataset");
    eager
        .attach_catalog_with_policy(&catalog, CatalogScanPolicy::Eager)
        .expect("load catalog eagerly");
    let eager_policies: Vec<_> = eager
        .metadata()
        .variables
        .iter()
        .map(|var| policy_signature(&var.missing))
        .collect();

    let mut deferred = SasReader::open(&data).expect("open deferred dataset");
    deferred
        .attach_catalog_with_policy(&catalog, CatalogScanPolicy::Deferred)
        .expect("load catalog deferred");

    let mut query = deferred
        .query()
        .shape(Shape::Rows)
        .ordering(OrderingMode::Ordered);
    let mut rows = query
        .stream_ordered()
        .expect("rows should trigger deferred scan");
    let _ = rows.try_next().expect("iterate first row");

    let deferred_policies: Vec<_> = deferred
        .metadata()
        .variables
        .iter()
        .map(|var| policy_signature(&var.missing))
        .collect();

    assert_eq!(eager_policies, deferred_policies);
}
