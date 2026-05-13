#[derive(Debug, Clone, PartialEq)]
pub struct LabelSet {
    pub name: String,
    pub value_type: ValueType,
    pub labels: Vec<ValueLabel>,
}

impl LabelSet {
    #[must_use]
    pub fn new(name: String, value_type: ValueType) -> Self {
        Self {
            name,
            value_type,
            labels: Vec::new(),
        }
    }

    /// Returns the label string for a numeric key, if one exists.
    #[must_use]
    pub fn lookup_numeric(&self, value: f64) -> Option<&str> {
        for label in &self.labels {
            match &label.key {
                ValueKey::Numeric(v) if *v == value => return Some(&label.label),
                ValueKey::Integer(v) if f64::from(*v) == value => return Some(&label.label),
                _ => {}
            }
        }
        None
    }

    /// Returns the label string for a string key, if one exists.
    #[must_use]
    pub fn lookup_string(&self, value: &str) -> Option<&str> {
        for label in &self.labels {
            if let ValueKey::String(k) = &label.key {
                if k.trim_end() == value.trim_end() {
                    return Some(&label.label);
                }
            }
        }
        None
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
