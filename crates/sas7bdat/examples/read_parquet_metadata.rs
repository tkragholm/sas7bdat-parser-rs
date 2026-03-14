use parquet::file::reader::{FileReader, SerializedFileReader};
use sas7bdat::{
    ColumnInfoJson, ParquetSink, RowSink, SasReader, TableInfoJson, dataset::VariableKind,
};
use std::fs::File;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Prepare input SAS file (using a project fixture)
    let input_path = "fixtures/raw_data/ahs2013/ratiov.sas7bdat";
    let mut sas = SasReader::open(input_path)?;
    let meta_filtered = sas.metadata().clone();

    // 2. Convert to Parquet with embedded metadata
    let dir = tempdir()?;
    let output_path = dir.path().join("output.parquet");
    let file = File::create(&output_path)?;

    // Manually prepare the JSON payload (similar to what convert.rs does)
    let columns = meta_filtered
        .variables
        .iter()
        .map(|v| ColumnInfoJson {
            index: v.index,
            name: v.name.clone(),
            label: v.label.clone(),
            kind: match v.kind {
                VariableKind::Numeric => "numeric".to_string(),
                VariableKind::Character => "character".to_string(),
            },
            format: v.format.as_ref().map(|f| f.name.clone()),
            width: v.storage_width,
        })
        .collect();

    let payload = TableInfoJson {
        table_name: meta_filtered.table_name.clone(),
        file_label: meta_filtered.file_label.clone(),
        row_count: meta_filtered.row_count,
        column_count: meta_filtered.column_count,
        columns,
    };

    let json = serde_json::to_string(&payload)?;

    let mut sink =
        ParquetSink::new(file).with_key_value_metadata(vec![parquet::file::metadata::KeyValue {
            key: "sas7bdat.metadata".to_string(),
            value: Some(json),
        }]);

    sas.stream_into(&mut sink)?;
    sink.finish()?;

    // 3. Read Parquet file and extract metadata
    println!("Reading Parquet file: {}", output_path.display());
    let read_file = File::open(&output_path)?;
    let reader = SerializedFileReader::new(read_file)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

    if let Some(kv_vec) = file_metadata.key_value_metadata() {
        if let Some(kv) = kv_vec.iter().find(|kv| kv.key == "sas7bdat.metadata") {
            if let Some(json_val) = &kv.value {
                println!("\nSAS Metadata found in Parquet file:");

                // We can parse it back into ExportJson
                let parsed: TableInfoJson = serde_json::from_str(json_val)?;
                println!("{}", serde_json::to_string_pretty(&parsed)?);

                // Basic validation for the example
                assert_eq!(parsed.table_name.as_deref(), Some("RATIOV"));
                println!("\nSuccess: Metadata correctly extracted and validated.");
            }
        } else {
            return Err("Key 'sas7bdat.metadata' not found in Parquet file".into());
        }
    } else {
        return Err("No key_value_metadata found in Parquet file".into());
    }

    Ok(())
}
