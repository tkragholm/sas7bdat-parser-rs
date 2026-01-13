use crate::dataset::LabelSet;
use std::collections::HashMap;

pub(super) fn build_label_lookup(
    label_sets: &HashMap<String, LabelSet>,
) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for name in label_sets.keys() {
        let normalized = normalize_label_name(name);
        map.entry(normalized.clone())
            .or_insert_with(|| name.clone());
        if !normalized.starts_with('$') {
            let prefixed = format!("${normalized}");
            map.entry(prefixed).or_insert_with(|| name.clone());
        }
    }
    map
}

pub(super) fn normalize_label_name(name: &str) -> String {
    name.trim()
        .trim_end_matches('.')
        .trim()
        .to_ascii_uppercase()
}
