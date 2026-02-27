use crate::{
    dataset::DatasetMetadata,
    error::{Error, Result},
};
#[cfg(feature = "fast-string")]
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};

#[cfg(feature = "fast-string")]
type IndexList = SmallVec<[usize; 8]>;
#[cfg(not(feature = "fast-string"))]
type IndexList = Vec<usize>;
#[cfg(feature = "fast-string")]
type NameList = SmallVec<[String; 8]>;
#[cfg(not(feature = "fast-string"))]
type NameList = Vec<String>;

/// Defines pagination and column projection for row reading.
#[derive(Debug, Clone, Default)]
pub struct RowSelection {
    skip_rows: u64,
    max_rows: Option<u64>,
    column_indices: Option<IndexList>,
    column_names: Option<NameList>,
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
        let mut collected: IndexList = IndexList::new();
        collected.extend(indices);
        if collected.is_empty() {
            self.column_indices = None;
        } else {
            self.column_indices = Some(collected);
        }
        self
    }

    /// Convenience wrapper for specifying column names from a slice.
    #[must_use]
    pub fn columns(mut self, names: &[&str]) -> Self {
        if names.is_empty() {
            self.column_names = None;
            return self;
        }
        let mut collected: NameList = NameList::new();
        collected.extend(names.iter().copied().map(std::string::ToString::to_string));
        collected.retain(|name| !name.is_empty());
        if collected.is_empty() {
            self.column_names = None;
        } else {
            self.column_names = Some(collected);
        }
        self
    }

    #[must_use]
    pub fn column_names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut collected: NameList = NameList::new();
        collected.extend(names.into_iter().map(Into::into));
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
            #[cfg(feature = "fast-string")]
            return Ok(Some(indices.iter().copied().collect()));
            #[cfg(not(feature = "fast-string"))]
            return Ok(Some(indices.clone()));
        }

        let Some(names) = &self.column_names else {
            return Ok(None);
        };

        Ok(Some(resolve_column_name_projection(
            metadata,
            names.as_slice(),
        )?))
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
}

/// Resolves projection names against metadata with trailing-space-tolerant matching.
///
/// # Errors
///
/// Returns an error when a name is unknown, resolves to duplicate indices, or if
/// the resolved projection is empty.
pub fn resolve_column_name_projection(
    metadata: &DatasetMetadata,
    names: &[String],
) -> Result<Vec<usize>> {
    let lookup = build_column_name_lookup(metadata);
    let mut resolved = Vec::with_capacity(names.len());
    let mut seen = HashSet::with_capacity(names.len());

    for name in names {
        let Some(index) = resolve_column_index(&lookup, name) else {
            return Err(Error::InvalidMetadata {
                details: format!("column name '{name}' not found in metadata").into(),
            });
        };
        if !seen.insert(index) {
            return Err(Error::InvalidMetadata {
                details: format!(
                    "column projection resolves duplicate column index {index} for name '{name}'"
                )
                .into(),
            });
        }
        resolved.push(index);
    }

    if resolved.is_empty() {
        return Err(Error::InvalidMetadata {
            details: "column projection resolved to an empty set".into(),
        });
    }

    Ok(resolved)
}

pub(super) fn build_column_name_lookup(metadata: &DatasetMetadata) -> HashMap<String, usize> {
    let mut lookup: HashMap<String, usize> = HashMap::with_capacity(metadata.variables.len() * 2);
    for variable in &metadata.variables {
        let trimmed = variable.name.trim_end();
        lookup
            .entry(trimmed.to_owned())
            .or_insert(variable.index as usize);
        lookup
            .entry(variable.name.clone())
            .or_insert(variable.index as usize);
    }
    lookup
}

pub(super) fn resolve_column_index(lookup: &HashMap<String, usize>, name: &str) -> Option<usize> {
    if let Some(index) = lookup.get(name) {
        return Some(*index);
    }
    let normalized = name.trim_end();
    if normalized != name {
        return lookup.get(normalized).copied();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::resolve_column_name_projection;
    use crate::{
        dataset::{DatasetMetadata, Variable, VariableKind},
        error::Error,
    };

    fn sample_metadata() -> DatasetMetadata {
        let mut metadata = DatasetMetadata::new(3);
        metadata
            .variables
            .push(Variable::new(0, "ID".to_string(), VariableKind::Numeric, 8));
        metadata.variables.push(Variable::new(
            1,
            "NAME   ".to_string(),
            VariableKind::Character,
            16,
        ));
        metadata.variables.push(Variable::new(
            2,
            "SCORE".to_string(),
            VariableKind::Numeric,
            8,
        ));
        metadata
    }

    #[test]
    fn resolve_projection_accepts_trimmed_names() {
        let metadata = sample_metadata();
        let names = vec!["NAME".to_string(), "SCORE".to_string()];
        let resolved = resolve_column_name_projection(&metadata, &names).unwrap();
        assert_eq!(resolved, vec![1, 2]);
    }

    #[test]
    fn resolve_projection_rejects_duplicate_names() {
        let metadata = sample_metadata();
        let names = vec!["NAME".to_string(), "NAME   ".to_string()];
        let err = resolve_column_name_projection(&metadata, &names).unwrap_err();
        assert!(matches!(err, Error::InvalidMetadata { .. }));
    }
}
