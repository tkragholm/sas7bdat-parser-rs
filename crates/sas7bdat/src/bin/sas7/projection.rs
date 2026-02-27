use crate::{AnyError, cli::ConvertArgs};
use sas7bdat::{dataset::DatasetMetadata, parser::ColumnInfo};
use std::collections::HashSet;

type ProjectionResult = (
    Option<Vec<usize>>,
    Vec<usize>,
    DatasetMetadata,
    Vec<ColumnInfo>,
);

pub fn resolve_projection(
    meta: &DatasetMetadata,
    cols: &[ColumnInfo],
    args: &ConvertArgs,
) -> Result<ProjectionResult, AnyError> {
    let column_count = meta.column_count as usize;
    let mut indices: Option<Vec<usize>> = None;
    if let Some(ref idxs) = args.column_indices {
        let mut seen = HashSet::with_capacity(idxs.len());
        for &i in idxs {
            if i >= column_count {
                return Err(
                    format!("column index {i} out of range ({column_count} columns)").into(),
                );
            }
            if !seen.insert(i) {
                return Err(format!("duplicate column index {i}").into());
            }
        }
        indices = Some(idxs.clone());
    } else if let Some(ref names) = args.columns {
        indices = Some(sas7bdat::reader::resolve_column_name_projection(
            meta, names,
        )?);
    }

    let selected: Vec<usize> = indices
        .clone()
        .unwrap_or_else(|| (0..column_count).collect());

    // Filter metadata clone and columns to match the projection.
    let mut filtered = meta.clone();
    filtered.column_count = u32::try_from(selected.len())
        .map_err(|_| "projected column count exceeds u32 range".to_string())?;
    let mut new_vars = Vec::with_capacity(selected.len());
    for (new_idx, &old_idx) in selected.iter().enumerate() {
        let mut v = meta.variables[old_idx].clone();
        v.index = u32::try_from(new_idx)
            .map_err(|_| "projected column index exceeds u32 range".to_string())?;
        new_vars.push(v);
    }
    filtered.variables = new_vars;
    // `column_list` is not needed for sink output, so keep it unchanged.

    let filtered_cols: Vec<ColumnInfo> = selected.iter().map(|&i| cols[i].clone()).collect();

    Ok((indices, selected, filtered, filtered_cols))
}
