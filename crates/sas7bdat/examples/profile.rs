use num_traits::ToPrimitive;
use sas7bdat::{CellValue, SasReader, dataset::VariableKind};
use serde::Serialize;
use std::{collections::HashMap, env, error::Error, path::Path};

#[derive(Debug, Serialize)]
struct TopValue {
    value: String,
    count: u64,
}

#[derive(Debug, Default, Serialize)]
struct NumericProfile {
    non_missing: u64,
    missing: u64,
    min: Option<f64>,
    max: Option<f64>,
    date_min: Option<String>,
    date_max: Option<String>,
    datetime_min: Option<String>,
    datetime_max: Option<String>,
    time_min: Option<String>,
    time_max: Option<String>,
}

#[derive(Debug, Default, Serialize)]
struct CharacterProfile {
    missing: u64,
    non_missing: u64,
    distinct: usize,
    high_cardinality: bool,
    top_values: Vec<TopValue>,
}

#[derive(Debug, Serialize)]
struct ColumnProfile {
    index: usize,
    name: String,
    label: Option<String>,
    kind: &'static str,
    numeric: Option<NumericProfile>,
    character: Option<CharacterProfile>,
}

#[derive(Debug, Serialize)]
struct DatasetProfile {
    row_count: u64,
    columns: Vec<ColumnProfile>,
}

enum ColumnAccumulator {
    Numeric {
        non_missing: u64,
        missing: u64,
        min: Option<f64>,
        max: Option<f64>,
        date_min: Option<time::OffsetDateTime>,
        date_max: Option<time::OffsetDateTime>,
        datetime_min: Option<time::OffsetDateTime>,
        datetime_max: Option<time::OffsetDateTime>,
        time_min: Option<time::Duration>,
        time_max: Option<time::Duration>,
    },
    Character {
        missing: u64,
        non_missing: u64,
        high_cardinality: bool,
        distinct_tracked: usize,
        counts: HashMap<String, u64>,
    },
}

const fn update_numeric(acc: &mut ColumnAccumulator, value: f64) {
    let ColumnAccumulator::Numeric {
        non_missing,
        min,
        max,
        ..
    } = acc
    else {
        return;
    };

    if !value.is_finite() {
        return;
    }

    *non_missing += 1;
    *min = Some(match min {
        Some(current) => current.min(value),
        None => value,
    });
    *max = Some(match max {
        Some(current) => current.max(value),
        None => value,
    });
}

fn update_date(
    non_missing: &mut u64,
    min: &mut Option<time::OffsetDateTime>,
    max: &mut Option<time::OffsetDateTime>,
    value: time::OffsetDateTime,
) {
    *non_missing += 1;
    match min {
        Some(current) => {
            if value < *current {
                *current = value;
            }
        }
        None => {
            *min = Some(value);
        }
    }
    match max {
        Some(current) => {
            if value > *current {
                *current = value;
            }
        }
        None => {
            *max = Some(value);
        }
    }
}

fn update_time(
    non_missing: &mut u64,
    min: &mut Option<time::Duration>,
    max: &mut Option<time::Duration>,
    value: time::Duration,
) {
    *non_missing += 1;
    match min {
        Some(current) => {
            if value < *current {
                *current = value;
            }
        }
        None => {
            *min = Some(value);
        }
    }
    match max {
        Some(current) => {
            if value > *current {
                *current = value;
            }
        }
        None => {
            *max = Some(value);
        }
    }
}

fn track_character_value(
    non_missing: &mut u64,
    high_cardinality: bool,
    distinct_tracked: &mut usize,
    counts: &mut HashMap<String, u64>,
    value: &str,
) {
    *non_missing += 1;
    if !high_cardinality {
        *counts.entry(value.to_string()).or_insert(0) += 1;
        *distinct_tracked = counts.len();
    }
}

fn track_character_text(
    missing: &mut u64,
    non_missing: &mut u64,
    high_cardinality: bool,
    distinct_tracked: &mut usize,
    counts: &mut HashMap<String, u64>,
    value: &str,
) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        *missing += 1;
    } else {
        track_character_value(
            non_missing,
            high_cardinality,
            distinct_tracked,
            counts,
            trimmed,
        );
    }
}

#[allow(clippy::too_many_lines)]
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args_os();
    let program = args
        .next()
        .and_then(|os| os.into_string().ok())
        .unwrap_or_else(|| "profile".to_owned());
    let Some(path) = args.next() else {
        eprintln!("usage: {program} <path-to-sas7bdat> [top_n]");
        std::process::exit(2);
    };
    let top_n = args.next().map_or(10, |arg| {
        arg.to_string_lossy().parse::<usize>().unwrap_or(10)
    });
    let distinct_cap = 10_000usize;
    let ratio_cap_numerator = 1u64;
    let ratio_cap_denominator = 5u64;
    let ratio_min_rows = 1_000u64;

    let path = Path::new(&path);
    let mut sas = SasReader::open(path)?;
    let metadata = sas.metadata().clone();

    let mut accumulators = metadata
        .variables
        .iter()
        .map(|variable| match variable.kind {
            VariableKind::Numeric => ColumnAccumulator::Numeric {
                non_missing: 0,
                missing: 0,
                min: None,
                max: None,
                date_min: None,
                date_max: None,
                datetime_min: None,
                datetime_max: None,
                time_min: None,
                time_max: None,
            },
            VariableKind::Character => ColumnAccumulator::Character {
                missing: 0,
                non_missing: 0,
                high_cardinality: false,
                distinct_tracked: 0,
                counts: HashMap::new(),
            },
        })
        .collect::<Vec<_>>();

    let mut row_count = 0u64;
    let mut rows = sas.rows()?;
    while let Some(row) = rows.try_next()? {
        row_count += 1;
        for (value, acc) in row.iter().zip(accumulators.iter_mut()) {
            match acc {
                ColumnAccumulator::Numeric { missing, .. } => match value {
                    CellValue::Missing(_) => *missing += 1,
                    CellValue::Float(actual) => update_numeric(acc, *actual),
                    CellValue::Int32(actual) => update_numeric(acc, f64::from(*actual)),
                    CellValue::Int64(actual) => {
                        if let Some(value) = actual.to_f64() {
                            update_numeric(acc, value);
                        } else {
                            *missing += 1;
                        }
                    }
                    CellValue::NumericString(actual) => {
                        if let Ok(parsed) = actual.parse::<f64>() {
                            update_numeric(acc, parsed);
                        } else {
                            *missing += 1;
                        }
                    }
                    CellValue::Date(actual) => {
                        if let ColumnAccumulator::Numeric {
                            non_missing,
                            date_min,
                            date_max,
                            ..
                        } = acc
                        {
                            update_date(non_missing, date_min, date_max, *actual);
                        }
                    }
                    CellValue::DateTime(actual) => {
                        if let ColumnAccumulator::Numeric {
                            non_missing,
                            datetime_min,
                            datetime_max,
                            ..
                        } = acc
                        {
                            update_date(non_missing, datetime_min, datetime_max, *actual);
                        }
                    }
                    CellValue::Time(actual) => {
                        if let ColumnAccumulator::Numeric {
                            non_missing,
                            time_min,
                            time_max,
                            ..
                        } = acc
                        {
                            update_time(non_missing, time_min, time_max, *actual);
                        }
                    }
                    CellValue::Str(_) | CellValue::Bytes(_) => {
                        *missing += 1;
                    }
                },
                ColumnAccumulator::Character {
                    missing,
                    non_missing,
                    high_cardinality,
                    distinct_tracked,
                    counts,
                } => match value {
                    CellValue::Missing(_) => *missing += 1,
                    CellValue::Str(actual) | CellValue::NumericString(actual) => {
                        track_character_text(
                            missing,
                            non_missing,
                            *high_cardinality,
                            distinct_tracked,
                            counts,
                            actual,
                        );
                    }
                    CellValue::Bytes(actual) => {
                        let text = String::from_utf8_lossy(actual);
                        track_character_text(
                            missing,
                            non_missing,
                            *high_cardinality,
                            distinct_tracked,
                            counts,
                            &text,
                        );
                    }
                    CellValue::Float(actual) => {
                        let value = actual.to_string();
                        track_character_value(
                            non_missing,
                            *high_cardinality,
                            distinct_tracked,
                            counts,
                            &value,
                        );
                    }
                    CellValue::Int32(actual) => {
                        let value = actual.to_string();
                        track_character_value(
                            non_missing,
                            *high_cardinality,
                            distinct_tracked,
                            counts,
                            &value,
                        );
                    }
                    CellValue::Int64(actual) => {
                        let value = actual.to_string();
                        track_character_value(
                            non_missing,
                            *high_cardinality,
                            distinct_tracked,
                            counts,
                            &value,
                        );
                    }
                    CellValue::Date(actual) | CellValue::DateTime(actual) => {
                        let value = actual.to_string();
                        track_character_value(
                            non_missing,
                            *high_cardinality,
                            distinct_tracked,
                            counts,
                            &value,
                        );
                    }
                    CellValue::Time(actual) => {
                        let value = actual.to_string();
                        track_character_value(
                            non_missing,
                            *high_cardinality,
                            distinct_tracked,
                            counts,
                            &value,
                        );
                    }
                },
            }
            if let ColumnAccumulator::Character {
                non_missing,
                high_cardinality,
                distinct_tracked,
                counts,
                ..
            } = acc
                && !*high_cardinality
            {
                let distinct = counts.len();
                if distinct > distinct_cap
                    || (*non_missing >= ratio_min_rows
                        && (u128::from(distinct as u64) * u128::from(ratio_cap_denominator)
                            > u128::from(*non_missing) * u128::from(ratio_cap_numerator)))
                {
                    *high_cardinality = true;
                    *distinct_tracked = distinct;
                    counts.clear();
                }
            }
        }
    }

    let columns = metadata
        .variables
        .iter()
        .zip(accumulators)
        .enumerate()
        .map(|(index, (variable, acc))| {
            let kind = match variable.kind {
                VariableKind::Numeric => "numeric",
                VariableKind::Character => "character",
            };
            match acc {
                ColumnAccumulator::Numeric {
                    non_missing,
                    missing,
                    min,
                    max,
                    date_min,
                    date_max,
                    datetime_min,
                    datetime_max,
                    time_min,
                    time_max,
                } => ColumnProfile {
                    index,
                    name: variable.name.clone(),
                    label: variable.label.clone().filter(|s| !s.trim().is_empty()),
                    kind,
                    numeric: Some(NumericProfile {
                        non_missing,
                        missing,
                        min,
                        max,
                        date_min: date_min.map(|v| v.to_string()),
                        date_max: date_max.map(|v| v.to_string()),
                        datetime_min: datetime_min.map(|v| v.to_string()),
                        datetime_max: datetime_max.map(|v| v.to_string()),
                        time_min: time_min.map(|v| v.to_string()),
                        time_max: time_max.map(|v| v.to_string()),
                    }),
                    character: None,
                },
                ColumnAccumulator::Character {
                    missing,
                    non_missing,
                    high_cardinality,
                    distinct_tracked,
                    counts,
                } => {
                    let distinct = distinct_tracked;
                    let top_values = if high_cardinality {
                        Vec::new()
                    } else {
                        let mut entries = counts.into_iter().collect::<Vec<_>>();
                        entries.sort_by(|(left_value, left_count), (right_value, right_count)| {
                            right_count
                                .cmp(left_count)
                                .then_with(|| left_value.cmp(right_value))
                        });
                        entries
                            .into_iter()
                            .take(top_n)
                            .map(|(value, count)| TopValue { value, count })
                            .collect()
                    };
                    ColumnProfile {
                        index,
                        name: variable.name.clone(),
                        label: variable.label.clone().filter(|s| !s.trim().is_empty()),
                        kind,
                        numeric: None,
                        character: Some(CharacterProfile {
                            missing,
                            non_missing,
                            distinct,
                            high_cardinality,
                            top_values,
                        }),
                    }
                }
            }
        })
        .collect::<Vec<_>>();

    let profile = DatasetProfile { row_count, columns };
    serde_json::to_writer_pretty(std::io::stdout(), &profile)?;
    println!();
    Ok(())
}
