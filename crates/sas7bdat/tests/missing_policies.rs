use sas7bdat::SasReader;
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
