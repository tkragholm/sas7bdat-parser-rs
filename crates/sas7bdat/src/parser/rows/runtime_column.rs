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

    #[must_use]
    pub const fn as_numeric_ref(&self) -> Option<NumericRuntimeColumnRef> {
        match self.kind {
            ColumnKind::Numeric(_) => Some(NumericRuntimeColumnRef {
                index: self.index,
                offset: self.offset,
                end: self.end,
            }),
            ColumnKind::Character => None,
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

#[derive(Clone, Copy)]
pub struct NumericRuntimeColumnRef {
    pub index: u32,
    pub offset: usize,
    pub end: usize,
}

#[derive(Clone, Copy)]
pub enum CompiledDecodeKind {
    Generic { kind: ColumnKind, raw_width: u32 },
    Character,
    NumericDouble { raw_width: u32 },
    NumericDateRaw { raw_width: u32 },
    NumericDateTemporal { raw_width: u32 },
    NumericDateTimeRaw { raw_width: u32 },
    NumericDateTimeTemporal { raw_width: u32 },
    NumericTimeRaw { raw_width: u32 },
    NumericTimeTemporal { raw_width: u32 },
}

#[derive(Clone, Copy)]
pub struct CompiledRuntimeColumnRef {
    pub index: u32,
    pub offset: usize,
    pub end: usize,
    pub decode: CompiledDecodeKind,
}

impl RuntimeColumnRef {
    #[must_use]
    pub const fn as_numeric_ref(self) -> Option<NumericRuntimeColumnRef> {
        match self.kind {
            ColumnKind::Numeric(_) => Some(NumericRuntimeColumnRef {
                index: self.index,
                offset: self.offset,
                end: self.end,
            }),
            ColumnKind::Character => None,
        }
    }
}
