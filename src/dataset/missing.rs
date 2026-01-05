#[derive(Debug, Clone, Default)]
pub struct MissingValuePolicy {
    pub system_missing: bool,
    pub tagged_missing: Vec<TaggedMissing>,
    pub ranges: Vec<MissingRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedMissing {
    pub tag: Option<char>,
    pub literal: MissingLiteral,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MissingRange {
    Numeric { start: f64, end: f64 },
    String { start: String, end: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MissingLiteral {
    Numeric(f64),
    String(String),
}
