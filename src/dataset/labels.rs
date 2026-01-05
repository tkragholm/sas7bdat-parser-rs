#[derive(Debug, Clone, PartialEq)]
pub struct LabelSet {
    pub name: String,
    pub value_type: ValueType,
    pub labels: Vec<ValueLabel>,
}

impl LabelSet {
    #[must_use]
    pub const fn new(name: String, value_type: ValueType) -> Self {
        Self {
            name,
            value_type,
            labels: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueLabel {
    pub key: ValueKey,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueKey {
    Numeric(f64),
    Integer(i32),
    Tagged(char),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Numeric,
    String,
}
