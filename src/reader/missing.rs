use std::collections::HashSet;

use crate::cell::MissingValue;
use crate::dataset::{
    LabelSet, MissingLiteral, MissingRange, MissingValuePolicy, TaggedMissing, ValueKey, ValueType,
};

pub(super) fn merge_label_set_missing(policy: &mut MissingValuePolicy, set: &LabelSet) {
    if matches!(set.value_type, ValueType::Numeric) {
        policy.system_missing = true;
        for value_label in &set.labels {
            if let ValueKey::Tagged(tag) = value_label.key {
                if tag == '_' {
                    policy.system_missing = true;
                } else if !policy
                    .tagged_missing
                    .iter()
                    .any(|item| item.tag == Some(tag))
                {
                    policy.tagged_missing.push(TaggedMissing {
                        tag: Some(tag),
                        literal: MissingLiteral::Numeric(f64::NAN),
                    });
                }
            }
        }
    }
}

pub(super) fn record_missing_observation(policy: &mut MissingValuePolicy, missing: &MissingValue) {
    match missing {
        MissingValue::System => {
            policy.system_missing = true;
        }
        MissingValue::Tagged(tagged) => {
            if let Some(tag) = tagged.tag {
                if tag == '_' {
                    policy.system_missing = true;
                }
            } else {
                policy.system_missing = true;
            }
            if !policy.tagged_missing.iter().any(|item| item == tagged) {
                policy.tagged_missing.push(tagged.clone());
            }
        }
        MissingValue::Range { lower, upper } => {
            let range = match (lower, upper) {
                (MissingLiteral::Numeric(start), MissingLiteral::Numeric(end)) => {
                    MissingRange::Numeric {
                        start: *start,
                        end: *end,
                    }
                }
                (MissingLiteral::String(start), MissingLiteral::String(end)) => {
                    MissingRange::String {
                        start: start.clone(),
                        end: end.clone(),
                    }
                }
                _ => return,
            };
            if !policy.ranges.iter().any(|item| item == &range) {
                policy.ranges.push(range);
            }
        }
    }
}

pub(super) fn dedup_tagged_missing(entries: &mut Vec<TaggedMissing>) {
    let mut seen = HashSet::with_capacity(entries.len());
    entries.retain(|entry| seen.insert(TaggedMissingKey::from(entry)));
}

pub(super) fn dedup_missing_ranges(entries: &mut Vec<MissingRange>) {
    let mut seen = HashSet::with_capacity(entries.len());
    entries.retain(|entry| seen.insert(MissingRangeKey::from(entry)));
}

#[derive(Hash, PartialEq, Eq)]
struct TaggedMissingKey {
    tag: Option<char>,
    literal: MissingLiteralKey,
}

impl From<&TaggedMissing> for TaggedMissingKey {
    fn from(value: &TaggedMissing) -> Self {
        Self {
            tag: value.tag,
            literal: MissingLiteralKey::from(&value.literal),
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
enum MissingLiteralKey {
    Numeric(u64),
    String(String),
}

impl From<&MissingLiteral> for MissingLiteralKey {
    fn from(value: &MissingLiteral) -> Self {
        match value {
            MissingLiteral::Numeric(number) => Self::Numeric(number.to_bits()),
            MissingLiteral::String(text) => Self::String(text.clone()),
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
enum MissingRangeKey {
    Numeric { start: u64, end: u64 },
    String { start: String, end: String },
}

impl From<&MissingRange> for MissingRangeKey {
    fn from(value: &MissingRange) -> Self {
        match value {
            MissingRange::Numeric { start, end } => Self::Numeric {
                start: start.to_bits(),
                end: end.to_bits(),
            },
            MissingRange::String { start, end } => Self::String {
                start: start.clone(),
                end: end.clone(),
            },
        }
    }
}
