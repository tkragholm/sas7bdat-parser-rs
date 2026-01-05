#![allow(clippy::pedantic)]
use std::path::Path;

use sas7bdat::SasReader;

fn tagged_tags(policy: &sas7bdat::dataset::MissingValuePolicy) -> Vec<char> {
    policy
        .tagged_missing
        .iter()
        .filter_map(|missing| missing.tag)
        .collect()
}

#[test]
fn scan_missing_policies_records_numeric_tags() {
    let data = Path::new("fixtures/raw_data/readstat/missing_test.sas7bdat");
    let catalog = Path::new("fixtures/raw_data/readstat/missing_formats.sas7bcat");

    let mut sas = SasReader::open(data).expect("open dataset");
    sas.attach_catalog(catalog).expect("load catalog");

    let metadata = sas.metadata();
    let vars = &metadata.variables;

    let var1 = vars.iter().find(|var| var.name == "var1").unwrap();
    assert!(tagged_tags(&var1.missing).contains(&'A'));

    let var2 = vars.iter().find(|var| var.name == "var2").unwrap();
    assert!(tagged_tags(&var2.missing).contains(&'B'));

    let var3 = vars.iter().find(|var| var.name == "var3").unwrap();
    assert!(tagged_tags(&var3.missing).contains(&'C'));

    let var4 = vars.iter().find(|var| var.name == "var4").unwrap();
    assert!(tagged_tags(&var4.missing).contains(&'X'));

    let var5 = vars.iter().find(|var| var.name == "var5").unwrap();
    assert!(tagged_tags(&var5.missing).contains(&'Y'));

    let var6 = vars.iter().find(|var| var.name == "var6").unwrap();
    assert!(tagged_tags(&var6.missing).contains(&'Z'));

    let var7 = vars.iter().find(|var| var.name == "var7").unwrap();
    assert!(var7.missing.system_missing);
}
