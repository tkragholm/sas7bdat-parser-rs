use crate::parser::metadata::ColumnKind;

#[derive(Clone, Copy)]
pub struct RuntimeColumn {
    pub index: u32,
    pub offset: usize,
    pub width: usize,
    pub end: usize,
    pub raw_width: u32,
    pub kind: ColumnKind,
}

impl RuntimeColumn {
    #[must_use]
    pub const fn as_ref(&self) -> RuntimeColumnRef {
        RuntimeColumnRef {
            index: self.index,
            offset: self.offset,
            width: self.width,
            end: self.end,
            raw_width: self.raw_width,
            kind: self.kind,
        }
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeColumnRef {
    pub index: u32,
    pub offset: usize,
    pub width: usize,
    pub end: usize,
    pub raw_width: u32,
    pub kind: ColumnKind,
}
