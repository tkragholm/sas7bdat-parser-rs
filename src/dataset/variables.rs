use super::missing::MissingValuePolicy;

/// Variable metadata mirroring the SAS column descriptor.
#[derive(Debug, Clone)]
pub struct Variable {
    pub index: u32,
    pub name: String,
    pub label: Option<String>,
    pub format: Option<Format>,
    pub kind: VariableKind,
    pub storage_width: usize,
    pub user_width: Option<usize>,
    pub missing: MissingValuePolicy,
    pub measure: Measure,
    pub alignment: Alignment,
    pub display_width: Option<u16>,
    pub decimals: Option<u16>,
    pub value_labels: Option<String>,
}

impl Variable {
    #[must_use]
    pub fn new(index: u32, name: String, kind: VariableKind, storage_width: usize) -> Self {
        Self {
            index,
            name,
            label: None,
            format: None,
            kind,
            storage_width,
            user_width: None,
            missing: MissingValuePolicy::default(),
            measure: Measure::Unknown,
            alignment: Alignment::Unknown,
            display_width: None,
            decimals: None,
            value_labels: None,
        }
    }
}

impl Default for Variable {
    fn default() -> Self {
        Self::new(0, String::new(), VariableKind::Numeric, 0)
    }
}

#[derive(Debug, Clone)]
pub enum VariableKind {
    Numeric,
    Character,
}

#[derive(Debug, Clone)]
pub struct Format {
    pub name: String,
    pub width: Option<u16>,
    pub decimals: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Measure {
    Unknown,
    Nominal,
    Ordinal,
    Scale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alignment {
    Unknown,
    Left,
    Center,
    Right,
}
