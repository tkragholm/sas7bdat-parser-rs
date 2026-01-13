use crate::{
    dataset::DatasetMetadata,
    error::{Error, Result},
};
use std::collections::{HashMap, HashSet};

/// Defines pagination and column projection for row reading.
#[derive(Debug, Clone, Default)]
pub struct RowSelection {
    skip_rows: u64,
    max_rows: Option<u64>,
    column_indices: Option<Vec<usize>>,
    column_names: Option<Vec<String>>,
}

impl RowSelection {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            skip_rows: 0,
            max_rows: None,
            column_indices: None,
            column_names: None,
        }
    }

    #[must_use]
    pub const fn skip_rows(mut self, count: u64) -> Self {
        self.skip_rows = count;
        self
    }

    #[must_use]
    pub const fn max_rows(mut self, count: u64) -> Self {
        self.max_rows = Some(count);
        self
    }

    #[must_use]
    pub fn column_indices<I>(mut self, indices: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        let collected: Vec<usize> = indices.into_iter().collect();
        if collected.is_empty() {
            self.column_indices = None;
        } else {
            self.column_indices = Some(collected);
        }
        self
    }

    #[must_use]
    pub fn column_names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut collected: Vec<String> = names.into_iter().map(Into::into).collect();
        collected.retain(|name| !name.is_empty());
        if collected.is_empty() {
            self.column_names = None;
        } else {
            self.column_names = Some(collected);
        }
        self
    }

    pub(crate) const fn skip_count(&self) -> u64 {
        self.skip_rows
    }

    pub(crate) const fn max_count(&self) -> Option<u64> {
        self.max_rows
    }

    pub(crate) const fn has_projection(&self) -> bool {
        self.column_indices.is_some() || self.column_names.is_some()
    }

    pub(crate) fn resolve_projection(
        &self,
        metadata: &DatasetMetadata,
    ) -> Result<Option<Vec<usize>>> {
        if let Some(indices) = &self.column_indices {
            Self::ensure_unique_indices(indices)?;
            return Ok(Some(indices.clone()));
        }

        let Some(names) = &self.column_names else {
            return Ok(None);
        };

        let mut lookup: HashMap<String, usize> = HashMap::with_capacity(metadata.variables.len());
        for variable in &metadata.variables {
            let trimmed = variable.name.trim_end();
            lookup
                .entry(trimmed.to_owned())
                .or_insert(variable.index as usize);
            lookup
                .entry(variable.name.clone())
                .or_insert(variable.index as usize);
        }

        let mut resolved = Vec::with_capacity(names.len());
        let mut seen = HashSet::with_capacity(names.len());
        for name in names {
            if let Some(index) = lookup.get(name) {
                Self::insert_projection_index(name, *index, &mut seen, &mut resolved)?;
                continue;
            }
            let normalized = name.trim_end();
            if let Some(index) = lookup.get(normalized) {
                Self::insert_projection_index(name, *index, &mut seen, &mut resolved)?;
                continue;
            }
            return Err(Error::InvalidMetadata {
                details: format!("column name '{name}' not found in metadata").into(),
            });
        }
        if resolved.is_empty() {
            return Err(Error::InvalidMetadata {
                details: "column projection resolved to an empty set".into(),
            });
        }
        Ok(Some(resolved))
    }

    fn ensure_unique_indices(indices: &[usize]) -> Result<()> {
        let mut seen = HashSet::with_capacity(indices.len());
        for &index in indices {
            if !seen.insert(index) {
                return Err(Error::InvalidMetadata {
                    details: format!("duplicate column projection index {index} in selection")
                        .into(),
                });
            }
        }
        Ok(())
    }

    fn insert_projection_index(
        name: &str,
        index: usize,
        seen: &mut HashSet<usize>,
        resolved: &mut Vec<usize>,
    ) -> Result<()> {
        if !seen.insert(index) {
            return Err(Error::InvalidMetadata {
                details: format!(
                    "column projection resolves duplicate column index {index} for name '{name}'"
                )
                .into(),
            });
        }
        resolved.push(index);
        Ok(())
    }
}
