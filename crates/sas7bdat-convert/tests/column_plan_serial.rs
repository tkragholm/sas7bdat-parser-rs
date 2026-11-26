//! The column plan through the single-threaded writer.

#[path = "support/column_plan_cases.rs"]
mod cases;

const THREADS: Option<usize> = Some(1);

#[test]
fn renames_retypes_and_adds_a_source_column() {
    cases::renames_retypes_and_adds_a_source_column(THREADS);
}

#[test]
fn times_of_day() {
    cases::times_of_day(THREADS);
}

#[test]
fn csv_takes_names_and_constants() {
    cases::csv_takes_names_and_constants(THREADS);
}

#[test]
fn bad_plans_fail_before_any_output() {
    cases::bad_plans_fail_before_any_output(THREADS);
}
