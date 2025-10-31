use std::env;
use std::fs::{self, File};
use std::io::copy;
use std::path::{Path, PathBuf};
use std::process;

use reqwest::blocking::get;
use sas7bdat_parser_rs::{ParquetSink, SasFile};
use tempfile::tempdir;
use zip::ZipArchive;

#[cfg(feature = "hotpath")]
use hotpath::{Format, GuardBuilder};

const ZIP_URL: &str = "https://www2.census.gov/programs-surveys/ahs/2013/AHS%202013%20National%20PUF%20v2.0%20Flat%20SAS.zip";
const DEFAULT_OUTPUT: &str = "ahs2013n.parquet";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "hotpath")]
    let _hotpath = GuardBuilder::new("sas_to_parquet")
        .percentiles(&[50, 90, 95, 99])
        .limit(15)
        .format(Format::Table)
        .build();

    let output_path = parse_args()?;
    let temp_dir = tempdir()?;

    let zip_path = temp_dir.path().join("ahs2013.zip");
    println!("Downloading dataset from {ZIP_URL}...");
    download_zip(ZIP_URL, &zip_path)?;

    println!("Extracting SAS dataset...");
    let sas_path = extract_sas7bdat(&zip_path, temp_dir.path())?;

    let mut sas = SasFile::open(&sas_path)?;
    let file = File::create(&output_path)?;
    let mut sink = ParquetSink::new(file).with_row_group_size(16_384);

    sas.write_into_sink(&mut sink)?;
    let _ = sink.into_inner()?;

    println!(
        "Wrote '{}' to '{}'",
        sas_path.display(),
        output_path.display()
    );

    Ok(())
}

fn parse_args() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "sas_to_parquet".to_owned());
    let output = match (args.next(), args.next()) {
        (Some(path), None) => PathBuf::from(path),
        (None, None) => PathBuf::from(DEFAULT_OUTPUT),
        _ => {
            eprintln!("Usage: {program} [output.parquet]");
            process::exit(1);
        }
    };
    Ok(output)
}

fn download_zip(url: &str, destination: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut response = get(url)?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("failed to download dataset: {status}").into());
    }
    let mut file = File::create(destination)?;
    copy(&mut response, &mut file)?;
    Ok(())
}

fn extract_sas7bdat(
    zip_path: &Path,
    destination_root: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let Some(enclosed_name) = entry.enclosed_name() else {
            continue;
        };

        if entry.is_dir() {
            let dir_path = destination_root.join(&enclosed_name);
            fs::create_dir_all(&dir_path)?;
            continue;
        }
        if !enclosed_name
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sas7bdat"))
        {
            continue;
        }

        let output_path = destination_root.join(&enclosed_name);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut outfile = File::create(&output_path)?;
        copy(&mut entry, &mut outfile)?;
        return Ok(output_path);
    }
    Err("no .sas7bdat file found in archive".into())
}
