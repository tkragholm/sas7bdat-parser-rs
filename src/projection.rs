#![allow(clippy::missing_errors_doc)]

use crate::{
    dataset::Dataset,
    error::{Error, ProjectionError, Result},
    internal::{ProjectedColumnPlan, ProjectionPlan},
    metadata::ColumnMeta,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum ProjectionItem {
    All,
    ColumnName(String),
    ColumnIndex(usize),
    ExcludeName(String),
}

#[derive(Debug, Clone)]
pub struct ProjectedColumnMeta {
    pub index: usize,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Projection {
    #[allow(dead_code)]
    pub(crate) inner: Arc<ProjectionPlan>,
    pub(crate) columns: Arc<[ProjectedColumnMeta]>,
    #[allow(dead_code)]
    pub(crate) names: Arc<[String]>,
}

impl Projection {
    #[must_use]
    pub fn columns(&self) -> &[ProjectedColumnMeta] {
        &self.columns
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

#[derive(Debug)]
pub struct ProjectionBuilder<'a> {
    pub(crate) ds: &'a Dataset,
    items: Vec<ProjectionItem>,
}

impl<'a> ProjectionBuilder<'a> {
    pub(crate) const fn new(ds: &'a Dataset) -> Self {
        Self {
            ds,
            items: Vec::new(),
        }
    }

    #[must_use]
    pub fn all(mut self) -> Self {
        self.items.push(ProjectionItem::All);
        self
    }

    #[must_use]
    pub fn column(mut self, name: &str) -> Self {
        self.items.push(ProjectionItem::ColumnName(name.to_owned()));
        self
    }

    #[must_use]
    pub fn column_idx(mut self, idx: usize) -> Self {
        self.items.push(ProjectionItem::ColumnIndex(idx));
        self
    }

    #[must_use]
    pub fn columns<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.items.extend(
            names
                .into_iter()
                .map(|name| ProjectionItem::ColumnName(name.as_ref().to_owned())),
        );
        self
    }

    #[must_use]
    pub fn exclude(mut self, name: &str) -> Self {
        self.items
            .push(ProjectionItem::ExcludeName(name.to_owned()));
        self
    }

    pub fn build(self) -> Result<Projection> {
        let available = self.ds.columns();
        if available.is_empty() {
            return Err(Error::Projection(ProjectionError {
                message: "cannot build a projection without parsed columns".to_owned(),
            }));
        }

        let mut selected: Vec<usize> = Vec::new();
        let mut select_all = false;
        let mut excludes: Vec<String> = Vec::new();

        for item in self.items {
            match item {
                ProjectionItem::All => {
                    select_all = true;
                }
                ProjectionItem::ColumnName(name) => {
                    let idx = find_column(available, &name)?;
                    if !selected.contains(&idx) {
                        selected.push(idx);
                    }
                }
                ProjectionItem::ColumnIndex(idx) => {
                    let Some(_) = available.get(idx) else {
                        return Err(Error::Projection(ProjectionError {
                            message: format!("column index {idx} is out of range"),
                        }));
                    };
                    if !selected.contains(&idx) {
                        selected.push(idx);
                    }
                }
                ProjectionItem::ExcludeName(name) => excludes.push(name),
            }
        }

        if select_all || selected.is_empty() {
            selected = (0..available.len()).collect();
        }

        selected.retain(|idx| {
            let name = available[*idx].borrowed_name();
            !excludes.iter().any(|excluded| excluded == name)
        });

        let columns: Vec<ProjectedColumnMeta> = selected
            .iter()
            .map(|idx| {
                let column = &available[*idx];
                ProjectedColumnMeta {
                    index: *idx,
                    name: column.name.clone(),
                }
            })
            .collect();
        let names: Vec<String> = columns.iter().map(|col| col.name.clone()).collect();
        let compiled_columns: Vec<ProjectedColumnPlan> = selected
            .iter()
            .map(|idx| compile_projected_column_plan(&available[*idx], *idx))
            .collect::<Result<Vec<_>>>()?;
        let max_end = compiled_columns
            .iter()
            .map(|column| column.end)
            .max()
            .unwrap_or(0);

        Ok(Projection {
            inner: Arc::new(ProjectionPlan {
                columns: compiled_columns.into_boxed_slice(),
                max_end,
            }),
            columns: Arc::from(columns),
            names: Arc::from(names),
        })
    }
}

fn find_column(columns: &[ColumnMeta], name: &str) -> Result<usize> {
    columns
        .iter()
        .position(|column| column.borrowed_name() == name)
        .ok_or_else(|| {
            Error::Projection(ProjectionError {
                message: format!("unknown column {name:?}"),
            })
        })
}

fn compile_projected_column_plan(column: &ColumnMeta, index: usize) -> Result<ProjectedColumnPlan> {
    let end = column
        .offset
        .checked_add(column.physical_width)
        .ok_or_else(|| {
            Error::Projection(ProjectionError {
                message: format!("column {:?} end overflowed u32", column.name),
            })
        })?;
    Ok(ProjectedColumnPlan {
        index,
        offset: column.offset,
        width: column.physical_width,
        end,
        logical_type: column.logical_type,
    })
}
