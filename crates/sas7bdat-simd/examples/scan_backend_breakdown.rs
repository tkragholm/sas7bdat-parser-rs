use sas7bdat_simd::{BatchHint, Dataset, IoBackendPreference, OpenOptions, Projection};
use std::{env, ops::ControlFlow, path::Path, time::Instant};

fn parse_backend(value: &str) -> Result<IoBackendPreference, String> {
    match value {
        "auto" => Ok(IoBackendPreference::Auto),
        "mmap" | "mmap-preferred" => Ok(IoBackendPreference::MmapPreferred),
        "buffered" | "buffered-preferred" => Ok(IoBackendPreference::BufferedPreferred),
        "buffered-only" => Ok(IoBackendPreference::BufferedOnly),
        other => Err(format!("unsupported backend `{other}`")),
    }
}

fn parse_projection(ds: &Dataset, columns: Option<&str>) -> Result<Projection, String> {
    match columns {
        Some(raw) if !raw.is_empty() => ds
            .projection()
            .columns(
                raw.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty()),
            )
            .build()
            .map_err(|err| err.to_string()),
        _ => ds.projection().all().build().map_err(|err| err.to_string()),
    }
}

fn time_raw_scan(ds: &Dataset, projection: &Projection) -> Result<u128, String> {
    let start = Instant::now();
    ds.scan()
        .with_projection(projection)
        .visit_raw_rows(|_| Ok(ControlFlow::Continue(())))
        .map_err(|err| err.to_string())?;
    Ok(start.elapsed().as_nanos())
}

fn main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let path = args.next().ok_or_else(|| {
        "usage: scan_backend_breakdown <fixture> <backend> [columns_csv]".to_owned()
    })?;
    let backend = parse_backend(&args.next().ok_or_else(|| "missing backend".to_owned())?)?;
    let columns = args.next();

    let open_start = Instant::now();
    let ds = Dataset::open_with(
        Path::new(&path),
        OpenOptions::builder().io_backend(backend).build(),
    )
    .map_err(|err| err.to_string())?;
    let open_ns = open_start.elapsed().as_nanos();

    let projection = parse_projection(&ds, columns.as_deref())?;

    let first_raw_ns = time_raw_scan(&ds, &projection)?;
    let second_raw_ns = time_raw_scan(&ds, &projection)?;
    let first_owned_breakdown = ds
        .scan()
        .with_projection(&projection)
        .with_batch_hint(BatchHint::Rows(4096))
        .owned_batch_scan_breakdown()
        .map_err(|err| err.to_string())?;
    let second_owned_breakdown = ds
        .scan()
        .with_projection(&projection)
        .with_batch_hint(BatchHint::Rows(4096))
        .owned_batch_scan_breakdown()
        .map_err(|err| err.to_string())?;
    let descriptor_start = Instant::now();
    let descriptor = ds.descriptor_breakdown().map_err(|err| err.to_string())?;
    let descriptor_ns = descriptor_start.elapsed().as_nanos();

    println!(
        concat!(
            "{{",
            "\"fixture\":\"{}\",",
            "\"backend\":\"{}\",",
            "\"columns\":\"{}\",",
            "\"row_count\":{},",
            "\"open_ns\":{},",
            "\"descriptor_ns\":{},",
            "\"descriptor_total_ns\":{},",
            "\"descriptor_page_read_ns\":{},",
            "\"descriptor_header_read_ns\":{},",
            "\"descriptor_classify_ns\":{},",
            "\"first_raw_ns\":{},",
            "\"second_raw_ns\":{},",
            "\"first_owned_batches_ns\":{},",
            "\"first_owned_push_row_ns\":{},",
            "\"first_owned_take_batch_ns\":{},",
            "\"first_owned_reset_after_flush_ns\":{},",
            "\"first_owned_scan_row_bytes_ns\":{},",
            "\"second_owned_batches_ns\":{},",
            "\"second_owned_push_row_ns\":{},",
            "\"second_owned_take_batch_ns\":{},",
            "\"second_owned_reset_after_flush_ns\":{},",
            "\"second_owned_scan_row_bytes_ns\":{}",
            "}}"
        ),
        path,
        match backend {
            IoBackendPreference::Auto => "auto",
            IoBackendPreference::MmapPreferred => "mmap-preferred",
            IoBackendPreference::BufferedPreferred => "buffered-preferred",
            IoBackendPreference::BufferedOnly => "buffered-only",
        },
        columns.unwrap_or_default(),
        ds.metadata().row_count,
        open_ns,
        descriptor_ns,
        descriptor.total_ns,
        descriptor.page_read_ns,
        descriptor.header_read_ns,
        descriptor.classify_ns,
        first_raw_ns,
        second_raw_ns,
        first_owned_breakdown.total_ns,
        first_owned_breakdown.push_row_ns,
        first_owned_breakdown.take_batch_ns,
        first_owned_breakdown.reset_after_flush_ns,
        first_owned_breakdown.scan_row_bytes_ns,
        second_owned_breakdown.total_ns,
        second_owned_breakdown.push_row_ns,
        second_owned_breakdown.take_batch_ns,
        second_owned_breakdown.reset_after_flush_ns,
        second_owned_breakdown.scan_row_bytes_ns
    );

    Ok(())
}
