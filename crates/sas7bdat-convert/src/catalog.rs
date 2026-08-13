//! Format-name lookup over a `.sas7bcat` value-label catalog.
//!
//! The decoding lives in the core crate ([`sas7bdat::catalog`]); this module only indexes
//! the resulting label sets by normalized format name and adds the `$`-prefix fallback the
//! CLI needs, since a numeric-looking format on a character column resolves to `$NAME`.

use anyhow::{Context, Result};
use sas7bdat::catalog::{normalize_format_name, parse_catalog_file};
use std::collections::HashMap;
use std::path::Path;

pub use sas7bdat::{LabelSet, ValueKey, ValueLabel, ValueType};

#[derive(Debug, Clone)]
pub struct Catalog {
    label_sets: HashMap<String, LabelSet>,
}

impl Catalog {
    /// Load and index a `.sas7bcat` catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be opened or parsed.
    pub fn load(path: &Path) -> Result<Self> {
        let layout = parse_catalog_file(path)
            .with_context(|| format!("failed to read catalog {}", path.display()))?;
        let label_sets = layout
            .label_sets
            .into_iter()
            .map(|set| (normalize_format_name(&set.name), set))
            .collect();
        Ok(Self { label_sets })
    }

    /// Look up the label set for a column's format name.
    ///
    /// A character column's format is stored without the leading `$` on the column but with
    /// it on the label set, so an exact miss retries with the `$` prefix.
    #[must_use]
    pub fn label_set_for_format(&self, format_name: &str) -> Option<&LabelSet> {
        let normalized = normalize_format_name(format_name);
        self.label_sets.get(&normalized).or_else(|| {
            if normalized.starts_with('$') {
                None
            } else {
                self.label_sets.get(&format!("${normalized}"))
            }
        })
    }
}
