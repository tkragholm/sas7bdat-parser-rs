use sas7bdat_parser_rs::value::Value;
use serde_json::{Value as JsonValue, json};
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time};

const SECONDS_PER_DAY: f64 = 86_400.0;

fn sas_epoch() -> PrimitiveDateTime {
    PrimitiveDateTime::new(
        Date::from_calendar_date(1960, Month::January, 1).expect("valid SAS epoch"),
        Time::MIDNIGHT,
    )
}

pub fn value_to_json(value: &Value<'_>) -> JsonValue {
    match value {
        Value::Float(v) => json!({ "kind": "number", "value": *v }),
        Value::Int32(v) => json!({ "kind": "number", "value": *v as f64 }),
        Value::Int64(v) => json!({ "kind": "number", "value": *v as f64 }),
        Value::NumericString(s) => json!({ "kind": "string", "value": s }),
        Value::Str(s) => json!({ "kind": "string", "value": s }),
        Value::Bytes(b) => json!({
            "kind": "bytes",
            "value": b.iter().copied().collect::<Vec<u8>>()
        }),
        Value::DateTime(dt) => json!({
            "kind": "datetime",
            "value": datetime_to_seconds(dt)
        }),
        Value::Date(dt) => json!({
            "kind": "date",
            "value": datetime_to_days(dt)
        }),
        Value::Time(duration) => json!({
            "kind": "time",
            "value": duration_to_seconds(duration)
        }),
        Value::Missing(_) => json!({ "kind": "missing", "value": null }),
    }
}

#[allow(dead_code)]
pub fn format_iso_seconds(dt: &OffsetDateTime) -> String {
    let rounded = round_to_millisecond(dt);
    let date = rounded.date();
    let time = rounded.time();
    if time.nanosecond() == 0 {
        format!(
            "{} {:02}:{:02}:{:02}",
            date,
            time.hour(),
            time.minute(),
            time.second()
        )
    } else {
        format!(
            "{} {:02}:{:02}:{:02}.{:03}",
            date,
            time.hour(),
            time.minute(),
            time.second(),
            time.nanosecond() / 1_000_000
        )
    }
}

#[allow(dead_code)]
pub fn format_iso_date(dt: &OffsetDateTime) -> String {
    dt.date().to_string()
}

#[allow(dead_code)]
pub fn round_to_millisecond(dt: &OffsetDateTime) -> OffsetDateTime {
    let nanos = dt.time().nanosecond() as u64;
    let mut millis = (nanos + 500_000) / 1_000_000;
    let mut adjusted = *dt;
    if millis == 1_000 {
        millis = 0;
        if let Some(next) = adjusted.checked_add(Duration::seconds(1)) {
            adjusted = next;
        } else {
            return *dt;
        }
    }
    let new_nanos = (millis * 1_000_000) as u32;
    adjusted.replace_nanosecond(new_nanos).unwrap_or(*dt)
}

fn datetime_to_seconds(dt: &OffsetDateTime) -> f64 {
    let epoch = sas_epoch().assume_utc();
    (*dt - epoch).whole_microseconds() as f64 / 1_000_000.0
}

fn datetime_to_days(dt: &OffsetDateTime) -> f64 {
    datetime_to_seconds(dt) / SECONDS_PER_DAY
}

fn duration_to_seconds(duration: &Duration) -> f64 {
    duration.whole_microseconds() as f64 / 1_000_000.0
}
