use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Error, Result};
use crate::iter_utils::next_from_result;
use crate::metadata::{
    DatasetMetadata, LabelSet, MissingLiteral, MissingRange, MissingValuePolicy, TaggedMissing,
    ValueKey, ValueType,
};
use crate::parser::{ParsedMetadata, RowIterator, parse_catalog, parse_metadata};
use crate::sinks::{RowSink, SinkContext};
use crate::value::{MissingValue, Value};

pub struct SasFile<R: Read + Seek> {
    reader: R,
    metadata: ParsedMetadata,
}

/// Configures pagination and projection behaviour for row readers.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    skip_rows: Option<u64>,
    max_rows: Option<u64>,
    column_indices: Option<Vec<usize>>,
    column_names: Option<Vec<String>>,
}

impl ReadOptions {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            skip_rows: None,
            max_rows: None,
            column_indices: None,
            column_names: None,
        }
    }

    #[must_use]
    pub const fn with_skip_rows(mut self, count: u64) -> Self {
        self.skip_rows = Some(count);
        self
    }

    #[must_use]
    pub const fn with_max_rows(mut self, count: u64) -> Self {
        self.max_rows = Some(count);
        self
    }

    #[must_use]
    pub fn with_column_indices<I>(mut self, indices: I) -> Self
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
    pub fn with_column_names<I, S>(mut self, names: I) -> Self
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

    fn skip_rows(&self) -> u64 {
        self.skip_rows.unwrap_or(0)
    }

    const fn max_rows(&self) -> Option<u64> {
        self.max_rows
    }

    const fn has_projection(&self) -> bool {
        self.column_indices.is_some() || self.column_names.is_some()
    }

    fn resolve_projection(&self, metadata: &DatasetMetadata) -> Result<Option<Vec<usize>>> {
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
                    details: format!("duplicate column projection index {index} in options").into(),
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

pub struct ProjectedRows<'a, R: Read + Seek> {
    inner: RowIterator<'a, R>,
    indices: Vec<usize>,
    sorted_indices: Vec<(usize, usize)>,
    exhausted: bool,
}

trait SkippableRows {
    fn advance(&mut self) -> Result<bool>;
}

impl<R: Read + Seek> SkippableRows for RowIterator<'_, R> {
    fn advance(&mut self) -> Result<bool> {
        Ok(self.try_next()?.is_some())
    }
}

impl<R: Read + Seek> SkippableRows for ProjectedRows<'_, R> {
    fn advance(&mut self) -> Result<bool> {
        Ok(self.try_next()?.is_some())
    }
}

trait WindowSource: SkippableRows {
    type Row<'a>
    where
        Self: 'a;

    fn next_row(&mut self) -> Result<Option<Self::Row<'_>>>;
}

impl<R: Read + Seek> WindowSource for RowIterator<'_, R> {
    type Row<'s>
        = Vec<Value<'s>>
    where
        Self: 's;

    fn next_row(&mut self) -> Result<Option<Self::Row<'_>>> {
        self.try_next()
    }
}

impl<R: Read + Seek> WindowSource for ProjectedRows<'_, R> {
    type Row<'s>
        = Vec<Value<'static>>
    where
        Self: 's;

    fn next_row(&mut self) -> Result<Option<Self::Row<'_>>> {
        self.try_next()
    }
}

struct WindowState<I> {
    inner: I,
    skip_remaining: u64,
    remaining: Option<u64>,
    skipped: bool,
}

impl<I> WindowState<I> {
    const fn new(inner: I, skip: u64, remaining: Option<u64>) -> Self {
        Self {
            inner,
            skip_remaining: skip,
            remaining,
            skipped: skip == 0,
        }
    }
}

impl<I: SkippableRows> WindowState<I> {
    fn consume_skip(&mut self) -> Result<Option<()>> {
        consume_skip_helper(&mut self.skip_remaining, &mut self.skipped, &mut self.inner)
    }
}

impl<I: WindowSource> WindowState<I> {
    fn try_next(&mut self) -> Result<Option<I::Row<'_>>> {
        if !self.skipped && self.consume_skip()?.is_none() {
            return Ok(None);
        }
        fetch_with_remaining(&mut self.remaining, self.inner.next_row())
    }
}

struct WindowedInner<I> {
    state: WindowState<I>,
}

impl<I> WindowedInner<I> {
    const fn new(inner: I, skip: u64, remaining: Option<u64>) -> Self {
        Self {
            state: WindowState::new(inner, skip, remaining),
        }
    }
}

impl<I: WindowSource> WindowedInner<I> {
    fn try_next(&mut self) -> Result<Option<I::Row<'_>>> {
        self.state.try_next()
    }
}

pub struct WindowedRows<'a, R: Read + Seek>(WindowedInner<RowIterator<'a, R>>);

pub struct WindowedProjectedRows<'a, R: Read + Seek>(WindowedInner<ProjectedRows<'a, R>>);

macro_rules! impl_windowed_type {
    ($name:ident => $inner:ty, $row:ty) => {
        #[allow(clippy::missing_errors_doc)]
        impl<'a, R: Read + Seek> $name<'a, R> {
            const fn new(inner: $inner, skip: u64, remaining: Option<u64>) -> Self {
                Self(WindowedInner::new(inner, skip, remaining))
            }

            /// Advances the iterator by one row.
            ///
            /// # Errors
            ///
            /// Returns an error if row decoding fails.
            #[cfg_attr(feature = "hotpath", hotpath::measure)]
            pub fn try_next(&mut self) -> Result<Option<$row>> {
                self.0.try_next()
            }
        }
    };
}

impl_windowed_type!(WindowedRows => RowIterator<'a, R>, Vec<Value<'_>>);
impl_windowed_type!(WindowedProjectedRows => ProjectedRows<'a, R>, Vec<Value<'static>>);

impl<R: Read + Seek> Iterator for WindowedRows<'_, R> {
    type Item = Result<Vec<Value<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        map_next(self.try_next(), |row| {
            row.into_iter().map(Value::into_owned).collect()
        })
    }
}

impl<R: Read + Seek> Iterator for WindowedProjectedRows<'_, R> {
    type Item = Result<Vec<Value<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        map_next(self.try_next(), |row| row)
    }
}

fn map_next<T, F>(result: Result<Option<T>>, mut map_row: F) -> Option<Result<Vec<Value<'static>>>>
where
    F: FnMut(T) -> Vec<Value<'static>>,
{
    match result {
        Ok(Some(row)) => Some(Ok(map_row(row))),
        Ok(None) => None,
        Err(err) => Some(Err(err)),
    }
}

fn fetch_with_remaining<T>(
    remaining: &mut Option<u64>,
    row: Result<Option<T>>,
) -> Result<Option<T>> {
    if matches!(remaining, Some(0)) {
        return Ok(None);
    }
    let row = row?;
    row.map_or_else(
        || Ok(None),
        |row| {
            if let Some(rem) = remaining.as_mut() {
                *rem = rem.saturating_sub(1);
            }
            Ok(Some(row))
        },
    )
}

fn consume_skip_helper<S: SkippableRows>(
    skip_remaining: &mut u64,
    skipped: &mut bool,
    source: &mut S,
) -> Result<Option<()>> {
    while *skip_remaining > 0 {
        if source.advance()? {
            *skip_remaining = skip_remaining.saturating_sub(1);
        } else {
            *skipped = true;
            return Ok(None);
        }
    }
    *skipped = true;
    Ok(Some(()))
}

impl SasFile<File> {
    /// Opens a SAS7BDAT file from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or if the metadata
    /// cannot be parsed.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Self::from_reader(file)
    }
}

impl<R: Read + Seek> SasFile<R> {
    /// Builds a reader from any `Read + Seek` implementor.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata parsing fails.
    pub fn from_reader(mut reader: R) -> Result<Self> {
        let metadata = parse_metadata(&mut reader)?;
        reader.seek(SeekFrom::Start(0))?;
        Ok(Self { reader, metadata })
    }

    pub const fn metadata(&self) -> &DatasetMetadata {
        &self.metadata.header.metadata
    }

    /// Loads value-label catalog metadata from a companion file.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be opened or parsed.
    pub fn load_catalog<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let mut file = File::open(path)?;
        self.load_catalog_from_reader(&mut file)
    }

    /// Loads value-label catalog metadata from the provided reader.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be parsed.
    pub fn load_catalog_from_reader<C: Read + Seek>(&mut self, reader: &mut C) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        let catalog = parse_catalog(reader)?;

        {
            let metadata = &mut self.metadata.header.metadata;

            for set in catalog.label_sets {
                metadata.label_sets.insert(set.name.clone(), set);
            }

            let lookup = build_label_lookup(&metadata.label_sets);
            for variable in &mut metadata.variables {
                if let Some(format) = &variable.format {
                    let normalized = normalize_label_name(&format.name);
                    if let Some(matched) = lookup.get(&normalized) {
                        variable.value_labels = Some(matched.clone());
                    } else if !normalized.starts_with('$') {
                        let prefixed = format!("${normalized}");
                        if let Some(matched) = lookup.get(&prefixed) {
                            variable.value_labels = Some(matched.clone());
                        }
                    }
                }

                if let Some(label_name) = variable.value_labels.clone()
                    && let Some(set) = metadata.label_sets.get(&label_name)
                {
                    merge_missing_policy_from_label_set(&mut variable.missing, set);
                }
            }
        }

        self.populate_missing_policies()?;
        Ok(())
    }

    /// Populates missing-value policies by scanning the dataset.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration fails.
    pub fn populate_missing_policies(&mut self) -> Result<()> {
        let variable_count = self.metadata.header.metadata.variables.len();
        if variable_count == 0 {
            return Ok(());
        }

        let mut policies: Vec<MissingValuePolicy> = self
            .metadata
            .header
            .metadata
            .variables
            .iter()
            .map(|var| var.missing.clone())
            .collect();

        self.reader.seek(SeekFrom::Start(0))?;
        {
            let mut rows = self.metadata.row_iterator(&mut self.reader)?;
            for row in rows.by_ref() {
                let row = row?;
                for (idx, value) in row.iter().enumerate() {
                    if let Value::Missing(missing) = value {
                        record_missing_value(&mut policies[idx], missing);
                    }
                }
            }
        }
        self.reader.seek(SeekFrom::Start(0))?;

        for (variable, policy) in self
            .metadata
            .header
            .metadata
            .variables
            .iter_mut()
            .zip(policies.into_iter())
        {
            let mut normalized_policy = policy;
            dedup_tagged_missing(&mut normalized_policy.tagged_missing);
            dedup_missing_ranges(&mut normalized_policy.ranges);
            variable.missing = normalized_policy;
        }

        Ok(())
    }

    /// Creates a row iterator over the dataset.
    ///
    /// # Errors
    ///
    /// Returns an error if row iteration cannot be initialised.
    pub fn rows(&mut self) -> Result<RowIterator<'_, R>> {
        self.reader.seek(SeekFrom::Start(0))?;
        self.metadata.row_iterator(&mut self.reader)
    }

    /// Creates a row iterator configured by the provided read options.
    ///
    /// This method is intended for pagination without column projection. Use
    /// [`project_rows_with_options`] when selecting a subset of columns.
    ///
    /// # Errors
    ///
    /// Returns an error if the options specify a projection, if the reader
    /// cannot be positioned, or if row iteration cannot be initialised.
    pub fn rows_with_options(&mut self, options: &ReadOptions) -> Result<WindowedRows<'_, R>> {
        if options.has_projection() {
            return Err(Error::InvalidMetadata {
                details: "rows_with_options does not accept column projection; use project_rows_with_options instead".into(),
            });
        }
        self.reader.seek(SeekFrom::Start(0))?;
        let iterator = self.metadata.row_iterator(&mut self.reader)?;
        Ok(WindowedRows::new(
            iterator,
            options.skip_rows(),
            options.max_rows(),
        ))
    }

    /// Creates an iterator that yields a subset of columns for each row.
    ///
    /// # Errors
    ///
    /// Returns an error if any requested column index is invalid or if row
    /// decoding fails.
    pub fn project_rows(&mut self, indices: &[usize]) -> Result<ProjectedRows<'_, R>> {
        let column_count = self.metadata.header.metadata.column_count as usize;
        if indices.is_empty() {
            return Err(Error::InvalidMetadata {
                details: "projected column list may not be empty".into(),
            });
        }
        let mut normalized = Vec::with_capacity(indices.len());
        let mut seen = HashSet::with_capacity(indices.len());
        for &idx in indices {
            if idx >= column_count {
                return Err(Error::InvalidMetadata {
                    details: format!(
                        "column projection index {idx} exceeds column count {column_count}"
                    )
                    .into(),
                });
            }
            if !seen.insert(idx) {
                return Err(Error::InvalidMetadata {
                    details: format!("duplicate column projection index {idx}").into(),
                });
            }
            normalized.push(idx);
        }
        self.reader.seek(SeekFrom::Start(0))?;
        let inner = self.metadata.row_iterator(&mut self.reader)?;
        let mut sorted_indices: Vec<(usize, usize)> = normalized
            .iter()
            .copied()
            .enumerate()
            .map(|(position, column_index)| (column_index, position))
            .collect();
        sorted_indices.sort_unstable_by_key(|entry| entry.0);
        Ok(ProjectedRows {
            inner,
            indices: normalized,
            sorted_indices,
            exhausted: false,
        })
    }

    /// Creates an iterator configured by read options with column projection.
    ///
    /// # Errors
    ///
    /// Returns an error when projection cannot be resolved or row decoding fails.
    pub fn project_rows_with_options(
        &mut self,
        options: &ReadOptions,
    ) -> Result<WindowedProjectedRows<'_, R>> {
        let metadata = &self.metadata.header.metadata;
        let indices =
            options
                .resolve_projection(metadata)?
                .ok_or_else(|| Error::InvalidMetadata {
                    details: "column projection not specified".into(),
                })?;
        let projected = self.project_rows(&indices)?;
        Ok(WindowedProjectedRows::new(
            projected,
            options.skip_rows(),
            options.max_rows(),
        ))
    }

    /// Streams the full dataset into a custom sink implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or if the sink reports a failure.
    pub fn write_into_sink<S: RowSink>(&mut self, sink: &mut S) -> Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        let context = SinkContext::new(&self.metadata);
        sink.begin(context)?;
        let mut iterator = self.metadata.row_iterator(&mut self.reader)?;
        iterator.stream_all(|row| sink.write_streaming_row(row))?;
        sink.finish()?;
        self.reader.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn into_parts(self) -> (R, ParsedMetadata) {
        (self.reader, self.metadata)
    }
}

impl<R: Read + Seek> ProjectedRows<'_, R> {
    /// Advances the projection iterator.
    ///
    /// # Errors
    ///
    /// Returns an error if row decoding fails or if a requested column is
    /// missing from the row data.
    pub fn try_next(&mut self) -> Result<Option<Vec<Value<'static>>>> {
        if self.exhausted {
            return Ok(None);
        }
        let maybe_row = match self.inner.try_next() {
            Ok(value) => value,
            Err(err) => {
                self.exhausted = true;
                return Err(err);
            }
        };
        if let Some(row) = maybe_row {
            let mut slots: Vec<Option<Value<'static>>> = vec![None; self.indices.len()];
            let mut sorted_pos = 0usize;
            let sorted_len = self.sorted_indices.len();
            let mut filled = 0usize;
            for (column_index, value) in row.into_iter().enumerate() {
                if sorted_pos < sorted_len {
                    let (target_index, result_position) = self.sorted_indices[sorted_pos];
                    if target_index < column_index {
                        return Err(Error::InvalidMetadata {
                            details: format!(
                                "projected column index {target_index} missing from row data"
                            )
                            .into(),
                        });
                    }
                    if target_index == column_index {
                        slots[result_position] = Some(value.into_owned());
                        sorted_pos += 1;
                        filled += 1;
                        if filled == sorted_len {
                            break;
                        }
                        continue;
                    }
                }
                if filled == sorted_len {
                    break;
                }
            }
            if filled != sorted_len {
                return Err(Error::InvalidMetadata {
                    details: "row did not contain all projected columns".into(),
                });
            }
            let mut projected = Vec::with_capacity(self.indices.len());
            for slot in slots {
                if let Some(value) = slot {
                    projected.push(value);
                } else {
                    return Err(Error::InvalidMetadata {
                        details: "projected column resolved to empty slot".into(),
                    });
                }
            }
            Ok(Some(projected))
        } else {
            self.exhausted = true;
            Ok(None)
        }
    }
}

impl<R: Read + Seek> Iterator for ProjectedRows<'_, R> {
    type Item = Result<Vec<Value<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.try_next();
        next_from_result(
            result,
            |row| row,
            || {
                self.exhausted = true;
            },
        )
    }
}

fn merge_missing_policy_from_label_set(policy: &mut MissingValuePolicy, set: &LabelSet) {
    if matches!(set.value_type, ValueType::Numeric) {
        policy.system_missing = true;
        for value_label in &set.labels {
            if let ValueKey::Tagged(tag) = value_label.key {
                if tag == '_' {
                    policy.system_missing = true;
                } else if !policy
                    .tagged_missing
                    .iter()
                    .any(|item| item.tag == Some(tag))
                {
                    policy.tagged_missing.push(TaggedMissing {
                        tag: Some(tag),
                        literal: MissingLiteral::Numeric(f64::NAN),
                    });
                }
            }
        }
    }
}

fn record_missing_value(policy: &mut MissingValuePolicy, missing: &MissingValue) {
    match missing {
        MissingValue::System => {
            policy.system_missing = true;
        }
        MissingValue::Tagged(tagged) => {
            if let Some(tag) = tagged.tag {
                if tag == '_' {
                    policy.system_missing = true;
                }
            } else {
                policy.system_missing = true;
            }
            if !policy.tagged_missing.iter().any(|item| item == tagged) {
                policy.tagged_missing.push(tagged.clone());
            }
        }
        MissingValue::Range { lower, upper } => {
            let range = match (lower, upper) {
                (MissingLiteral::Numeric(start), MissingLiteral::Numeric(end)) => {
                    MissingRange::Numeric {
                        start: *start,
                        end: *end,
                    }
                }
                (MissingLiteral::String(start), MissingLiteral::String(end)) => {
                    MissingRange::String {
                        start: start.clone(),
                        end: end.clone(),
                    }
                }
                _ => return,
            };
            if !policy.ranges.iter().any(|item| item == &range) {
                policy.ranges.push(range);
            }
        }
    }
}

fn dedup_tagged_missing(entries: &mut Vec<TaggedMissing>) {
    let mut seen = HashSet::with_capacity(entries.len());
    entries.retain(|entry| seen.insert(TaggedMissingKey::from(entry)));
}

fn dedup_missing_ranges(entries: &mut Vec<MissingRange>) {
    let mut seen = HashSet::with_capacity(entries.len());
    entries.retain(|entry| seen.insert(MissingRangeKey::from(entry)));
}

#[derive(Hash, PartialEq, Eq)]
struct TaggedMissingKey {
    tag: Option<char>,
    literal: MissingLiteralKey,
}

impl From<&TaggedMissing> for TaggedMissingKey {
    fn from(value: &TaggedMissing) -> Self {
        Self {
            tag: value.tag,
            literal: MissingLiteralKey::from(&value.literal),
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
enum MissingLiteralKey {
    Numeric(u64),
    String(String),
}

impl From<&MissingLiteral> for MissingLiteralKey {
    fn from(value: &MissingLiteral) -> Self {
        match value {
            MissingLiteral::Numeric(number) => Self::Numeric(number.to_bits()),
            MissingLiteral::String(text) => Self::String(text.clone()),
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
enum MissingRangeKey {
    Numeric { start: u64, end: u64 },
    String { start: String, end: String },
}

impl From<&MissingRange> for MissingRangeKey {
    fn from(value: &MissingRange) -> Self {
        match value {
            MissingRange::Numeric { start, end } => Self::Numeric {
                start: start.to_bits(),
                end: end.to_bits(),
            },
            MissingRange::String { start, end } => Self::String {
                start: start.clone(),
                end: end.clone(),
            },
        }
    }
}

fn build_label_lookup(label_sets: &HashMap<String, LabelSet>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for name in label_sets.keys() {
        let normalized = normalize_label_name(name);
        map.entry(normalized.clone())
            .or_insert_with(|| name.clone());
        if !normalized.starts_with('$') {
            let prefixed = format!("${normalized}");
            map.entry(prefixed).or_insert_with(|| name.clone());
        }
    }
    map
}

fn normalize_label_name(name: &str) -> String {
    name.trim()
        .trim_end_matches('.')
        .trim()
        .to_ascii_uppercase()
}
