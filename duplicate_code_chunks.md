Found a 21 line (150 tokens) duplication in the following files:
* Starting at line 645 of /home/tkragholm/dev/sas7bdat-simd/src/layout.rs
* Starting at line 708 of /home/tkragholm/dev/sas7bdat-simd/src/pages.rs

```
}

const fn read_u32(endianness: crate::metadata::Endianness, bytes: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes);
    match endianness {
        crate::metadata::Endianness::Little => u32::from_le_bytes(buf),
        crate::metadata::Endianness::Big => u32::from_be_bytes(buf),
    }
}

const fn read_u64(endianness: crate::metadata::Endianness, bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    match endianness {
        crate::metadata::Endianness::Little => u64::from_le_bytes(buf),
        crate::metadata::Endianness::Big => u64::from_be_bytes(buf),
    }
}

fn get_range<'a>(bytes: &'a [u8], start: usize, end: usize, what: &str) -> Result<&'a [u8]> {
```

---

Found a 18 line (138 tokens) duplication in the following files:
* Starting at line 802 of /home/tkragholm/dev/sas7bdat-simd/src/pages.rs
* Starting at line 820 of /home/tkragholm/dev/sas7bdat-simd/src/pages.rs

```
        let bytes = make_compressed_page(&[0xC1u8, b'A'], 64, 4);
        let mut layout = simple_layout(64, 1, 4, 1);
        layout.compression = CompressionKind::Row;
        let mut cursor = Cursor::new(bytes);
        let descriptors = compile_page_descriptors(&mut cursor, &layout).expect("descriptors");

        assert_eq!(descriptors.pages.len(), 1);
        assert_eq!(
            descriptors.pages[0].exec_class,
            PageExecClass::IndexedCompressedRows
        );
        assert_eq!(descriptors.pages[0].row_count, 1);
        assert_eq!(descriptors.pages[0].row_span_count, 1);
        assert_eq!(descriptors.row_spans[0].kind, RowSpanKind::Compressed);
    }

    #[test]
    fn compressed_meta_pages_compile_row_spans() {
```

---

Found a 18 line (130 tokens) duplication in the following files:
* Starting at line 197 of /home/tkragholm/dev/sas7bdat-simd/src/scan/batch.rs
* Starting at line 214 of /home/tkragholm/dev/sas7bdat-simd/src/scan/batch.rs

```
    fn push_all_staged_numeric(&mut self, row: &[u8]) -> Result<()> {
        for &idx in &self.plan.families.staged_numeric {
            let batch_column = &mut self.columns[idx];
            let column = &self.plan.row_plan.columns[idx];
            let slice = RowDecodePlan::slice_in_bounds(row, column);
            let raw = decode_numeric_raw_bits_or_missing(slice, self.plan.row_plan.endianness);
            let appended = batch_column.append_staged_numeric_bits_fast(raw);
            debug_assert!(appended, "compiled staged numeric batch must match builder");
            if !appended {
                return Err(Error::unsupported(
                    "compiled staged numeric batch plan did not match column builder",
                ));
            }
        }
        Ok(())
    }

    fn push_staged_numeric_family(&mut self, row: &[u8]) -> Result<()> {
```

---

Found a 27 line (124 tokens) duplication in the following files:
* Starting at line 55 of /home/tkragholm/dev/sas7bdat-simd/src/columnar.rs
* Starting at line 70 of /home/tkragholm/dev/sas7bdat-simd/src/scan/batch.rs

```
        valid: Option<Vec<u8>>,
    },
    Date {
        values: Vec<SasDate>,
        valid: Option<Vec<u8>>,
    },
    DateTime {
        values: Vec<SasDateTime>,
        valid: Option<Vec<u8>>,
    },
    Time {
        values: Vec<SasTime>,
        valid: Option<Vec<u8>>,
    },
    Utf8 {
        offsets: Vec<u32>,
        data: Vec<u8>,
        valid: Option<Vec<u8>>,
    },
    RawBytes {
        offsets: Vec<u32>,
        data: Vec<u8>,
        valid: Option<Vec<u8>>,
    },
}

impl OwnedColumnBuffer {
```

---

Found a 9 line (121 tokens) duplication in the following files:
* Starting at line 179 of /home/tkragholm/dev/sas7bdat-simd/src/test_utils.rs
* Starting at line 227 of /home/tkragholm/dev/sas7bdat-simd/src/test_utils.rs

```
    page[(24 - 8)..(24 - 6)].copy_from_slice(&0x0200u16.to_le_bytes());
    page[(24 - 6)..(24 - 4)].copy_from_slice(&1u16.to_le_bytes());
    page[(24 - 4)..(24 - 2)].copy_from_slice(&1u16.to_le_bytes());

    let data_offset = 40u32;
    let data_len = u32::try_from(compressed.len()).unwrap_or(u32::MAX);
    page[24..28].copy_from_slice(&data_offset.to_le_bytes());
    page[28..32].copy_from_slice(&data_len.to_le_bytes());
    page[32] = compression_flag;
```

---

Found a 13 line (119 tokens) duplication in the following files:
* Starting at line 359 of /home/tkragholm/dev/sas7bdat-simd/src/scan/tests.rs
* Starting at line 390 of /home/tkragholm/dev/sas7bdat-simd/src/scan/tests.rs

```
fn batch_decode_plan_does_not_compile_single_byte_utf8_family_for_uncompressed_scan() {
    let row = {
        let mut row = [0u8; 9];
        row[..8].copy_from_slice(&1.0f64.to_bits().to_le_bytes());
        row[8] = b'B';
        row
    };
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("code", LogicalType::String, 1, 8)
        .with_row_len(9)
        .with_encoding(Some("ISO-8859-1".to_owned()))
```

---

Found a 19 line (116 tokens) duplication in the following files:
* Starting at line 73 of /home/tkragholm/dev/sas7bdat-simd/src/scan/tests.rs
* Starting at line 101 of /home/tkragholm/dev/sas7bdat-simd/src/scan/tests.rs

```
    let bytes = Arc::<[u8]>::from(make_pointer_page(&[b"ABCD", b"EFGH"], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_row_len(4)
        .with_total_rows(2)
        .with_rows_per_page(2)
        .build();

    let mut rows = Vec::new();
    let stats = ScanBuilder::new(&ds)
        .visit_raw_rows(|row| {
            rows.push((row.row_index, row.bytes.to_vec()));
            Ok(ControlFlow::Continue(()))
        })
        .expect("scan succeeds");

    assert_eq!(
        rows,
        vec![
            (crate::types::RowIndex(0), b"ABCD".to_vec()),
```

---

Found a 18 line (112 tokens) duplication in the following files:
* Starting at line 38 of /home/tkragholm/dev/sas7bdat-simd/src/scan/raw.rs
* Starting at line 123 of /home/tkragholm/dev/sas7bdat-simd/src/scan/raw.rs

```
    F: FnMut(RowIndex, &[u8]) -> Result<ControlFlow<()>>,
{
    let plan = RawScanPlan::compile(builder);
    if plan.row_len == 0 {
        return Ok(ScanStats::default());
    }

    if builder.ds.layout.compression != crate::metadata::CompressionKind::None
        && builder.ds.metadata.row_count > 0
        && builder.ds.descriptors.total_candidate_rows == 0
    {
        return Err(Error::unsupported(
            "compressed dataset layout compiled no row producers; this compressed page layout is not implemented yet",
        ));
    }

    let mut stats = ScanStats::default();
    let mut page = vec![0u8; plan.page_size];
```

---

Found a 17 line (110 tokens) duplication in the following files:
* Starting at line 788 of /home/tkragholm/dev/sas7bdat-simd/src/scan/batch.rs
* Starting at line 817 of /home/tkragholm/dev/sas7bdat-simd/src/scan/batch.rs
* Starting at line 846 of /home/tkragholm/dev/sas7bdat-simd/src/scan/batch.rs

```
                PlannedCell::Date(value) => {
                    push_primitive_valid(values, valid, value);
                    Ok(())
                }
                PlannedCell::Int32(value) => {
                    self.widen_temporal_to_f64();
                    self.append(PlannedCell::Int32(value), owned_strings)
                }
                PlannedCell::Int64(value) => {
                    self.widen_temporal_to_f64();
                    self.append(PlannedCell::Int64(value), owned_strings)
                }
                PlannedCell::Float64(value) => {
                    self.widen_temporal_to_f64();
                    self.append(PlannedCell::Float64(value), owned_strings)
                }
                other => Err(unexpected_batch_cell("date", other)),
```

---

Found a 12 line (110 tokens) duplication in the following files:
* Starting at line 202 of /home/tkragholm/dev/sas7bdat-simd/src/scan/builder.rs
* Starting at line 213 of /home/tkragholm/dev/sas7bdat-simd/src/scan/builder.rs

```
            DecodeMode::Typed => {
                scan_row_bytes(self, &mut |row_index, bytes| {
                    plan.validate_row_bounds(bytes)?;
                    let mut cells = Vec::with_capacity(plan.columns.len());
                    for (column, kind) in plan.columns.iter().zip(&plan.owned_kinds) {
                        cells.push(plan.materialize_owned_cell_fast(bytes, column, *kind)?);
                    }
                    rows.push(OwnedRow { row_index, cells });
                    Ok(ControlFlow::Continue(()))
                })?;
            }
            DecodeMode::TypedLossless => {
```

---

Found a 11 line (103 tokens) duplication in the following files:
* Starting at line 418 of /home/tkragholm/dev/sas7bdat-simd/src/scan/tests.rs
* Starting at line 436 of /home/tkragholm/dev/sas7bdat-simd/src/scan/tests.rs

```
fn typed_rows_decode_ascii_strings_without_utf8_encoding() {
    let row = make_numeric_text_row(1.0, *b"pear");
    let bytes = Arc::<[u8]>::from(make_page(0x0100, 1, 0, &[&row], 64));
    let ds = MockDatasetBuilder::new(bytes)
        .with_column("num", LogicalType::Float, 8, 0)
        .with_column("txt", LogicalType::String, 4, 8)
        .with_row_len(12)
        .with_encoding(Some("WINDOWS-1252".to_owned()))
        .build();

    let rows = ScanBuilder::new(&ds).collect_rows().expect("rows");
```
