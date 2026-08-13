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
                return Err(unknown_column_error(name, dataset));
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

/// Build a helpful "unknown column" error, suggesting the closest existing name.
fn unknown_column_error(name: &str, dataset: &Dataset) -> anyhow::Error {
    closest_column(name, dataset).map_or_else(
        || anyhow!("No column named '{name}'. Run 'sas7bdat info <file>' to list columns."),
        |suggestion| {
            anyhow!(
                "No column named '{name}'. Did you mean '{suggestion}'? \
                 Run 'sas7bdat info <file>' to list columns."
            )
        },
    )
}

/// Closest column name by case-insensitive edit distance, if one is reasonably near.
fn closest_column(name: &str, dataset: &Dataset) -> Option<String> {
    let target = name.trim_end().to_ascii_lowercase();
    dataset
        .columns()
        .iter()
        .map(|column| column.name.trim_end().to_owned())
        .filter_map(|column| {
            let distance = levenshtein(&target, &column.to_ascii_lowercase());
            // Only suggest when the names are genuinely close.
            let threshold = (column.chars().count() / 2).max(2);
            (distance <= threshold).then_some((distance, column))
        })
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, column)| column)
}

/// Classic two-row Levenshtein edit distance over Unicode scalars.
fn levenshtein(a: &str, b: &str) -> usize {
    let b_chars: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b_chars.len()).collect();
    let mut curr = vec![0usize; b_chars.len() + 1];
    for (i, ca) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, &cb) in b_chars.iter().enumerate() {
            let cost = usize::from(ca != cb);
            curr[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(curr[j] + 1);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b_chars.len()]
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

#[cfg(test)]
mod tests {
    use super::levenshtein;

    #[test]
    fn levenshtein_measures_edit_distance() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("gender", "gender"), 0);
        assert_eq!(levenshtein("gendr", "gender"), 1); // one insertion
        assert_eq!(levenshtein("sexa", "sexb"), 1); // one substitution
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("kitten", "sitting"), 3); // classic example
    }
}
