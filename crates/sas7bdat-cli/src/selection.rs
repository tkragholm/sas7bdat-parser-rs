use anyhow::{Result, anyhow, bail};
use sas7bdat::{ColumnMeta, Dataset, Projection, RowSelection};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, Default)]
pub struct ColumnSelection<'a> {
    pub names: Option<&'a [String]>,
    pub indices: Option<&'a [usize]>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RowWindow {
    pub skip: Option<u64>,
    pub max_rows: Option<u64>,
}

impl RowWindow {
    #[must_use]
    pub const fn new(skip: Option<u64>, max_rows: Option<u64>) -> Self {
        Self { skip, max_rows }
    }
}

/// # Errors
///
/// Returns an error if no valid projection can be built from the supplied selection.
pub fn projection_from_selection(
    dataset: &Dataset,
    selection: ColumnSelection<'_>,
) -> Result<Option<Projection>> {
    if selection.names.is_none() && selection.indices.is_none() {
        return Ok(None);
    }

    let mut builder = dataset.projection();
    if let Some(columns) = selection.names {
        builder = builder.columns(columns.iter().map(String::as_str));
    }
    if let Some(indices) = selection.indices {
        for idx in indices {
            builder = builder.column_idx(*idx);
        }
    }
    Ok(Some(builder.build()?))
}

/// # Errors
///
/// Returns an error if the requested columns are missing, duplicated, or out of range.
pub fn resolve_column_indices(
    dataset: &Dataset,
    selection: ColumnSelection<'_>,
) -> Result<Option<Vec<usize>>> {
    let total = dataset.columns().len();
    if let Some(indices) = selection.indices {
        let mut seen = HashSet::with_capacity(indices.len());
        for &idx in indices {
            if idx >= total {
                bail!("column index {idx} out of range ({total} columns)");
            }
            if !seen.insert(idx) {
                bail!("duplicate column index {idx}");
            }
        }
        return Ok(Some(indices.to_vec()));
    }

    if let Some(names) = selection.names {
        let mut lookup = HashMap::with_capacity(total);
        for column in dataset.columns() {
            lookup.entry(column.name.clone()).or_insert(column.index);
            lookup
                .entry(column.name.trim_end().to_owned())
                .or_insert(column.index);
        }

        let mut seen = HashSet::with_capacity(names.len());
        let mut resolved = Vec::with_capacity(names.len());
        for name in names {
            let Some(&idx) = lookup.get(name).or_else(|| lookup.get(name.trim_end())) else {
                return Err(anyhow!("column '{name}' not found"));
            };
            if !seen.insert(idx) {
                return Err(anyhow!("duplicate column '{name}' (index {idx})"));
            }
            resolved.push(idx);
        }
        return Ok(Some(resolved));
    }

    Ok(None)
}

#[must_use]
pub const fn row_selection_from_window(window: RowWindow, row_count: u64) -> Option<RowSelection> {
    match (window.skip, window.max_rows) {
        (None, None) => None,
        (Some(skip), None) => Some(RowSelection::range(skip, row_count)),
        (None, Some(max_rows)) => Some(RowSelection::First(max_rows)),
        (Some(skip), Some(max_rows)) => {
            Some(RowSelection::range(skip, skip.saturating_add(max_rows)))
        }
    }
}

#[must_use]
pub fn selected_columns_refs<'a>(
    dataset: &'a Dataset,
    selected: Option<&[usize]>,
) -> Vec<&'a ColumnMeta> {
    selected.map_or_else(
        || dataset.columns().iter().collect(),
        |indices| {
            indices
                .iter()
                .filter_map(|&idx| dataset.columns().get(idx))
                .collect()
        },
    )
}
