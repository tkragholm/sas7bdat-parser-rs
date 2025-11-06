# Sas7Bdat.Core

A high-performance, memory-efficient .NET library for reading SAS7BDAT files with zero-copy semantics and minimal garbage collection pressure.

## Features

- **High Performance**: Optimized for speed with minimal memory allocations
- **Low Memory Footprint**: Uses `Span<T>` and `Memory<T>` for zero-copy operations
- **Pooled Buffers**: Leverages `ArrayPool<T>` to eliminate unnecessary allocations
- **Extended Format Support**: Handles SAS 9.3+ extended observation counter format
- **Async Streaming**: Memory-efficient async enumeration with cancellation support
- **Comprehensive Encoding**: Supports all SAS character encodings
- **Automatic Decompression**: Built-in RLE and RDC decompression
- **Format Detection**: Automatic detection of file format variants
- **Native Type Conversion**: Automatic parsing of SAS dates, times, and datetimes to .NET types

## Installation

```bash
dotnet add package Sas7Bdat
```

## Quick Start

```csharp
using Sas7Bdat.Core;

// Basic usage - read all rows
await using var reader = await SasDataReader.OpenFileAsync("data.sas7bdat");
await foreach (var row in reader.ReadRowsAsync())
{
    // Process row immediately - do NOT store references!
    for (int i = 0; i < row.Length; i++)
    {
        var value = row.Span[i];
        var column = reader.Columns[i];
        
        // Native type conversion based on column type
        switch (column.Type)
        {
            case ColumnType.String:
                Console.WriteLine($"{column.Name}: {value}");
                break;
            case ColumnType.Date:
                var date = (DateTime)value!; // SAS dates automatically parsed to DateTime
                Console.WriteLine($"{column.Name}: {date:yyyy-MM-dd}");
                break;
            case ColumnType.DateTime:
                var datetime = (DateTime)value!; // SAS datetimes to DateTime
                Console.WriteLine($"{column.Name}: {datetime:yyyy-MM-dd HH:mm:ss}");
                break;
            case ColumnType.Time:
                var time = (TimeSpan)value!; // SAS times to TimeSpan
                Console.WriteLine($"{column.Name}: {time}");
                break;
            case ColumnType.Number:
                Console.WriteLine($"{column.Name}: {value}");
                break;
        }
    }
    // Row data is invalid after this loop iteration
}
```

## Performance Characteristics

### Memory Efficiency
- **Zero-Copy Operations**: Uses `Memory<T>` and `Span<T>` to avoid unnecessary data copying
- **Pooled Buffers**: Page and row buffers are rented from `ArrayPool<T>` and automatically returned
- **Streaming Architecture**: Processes one page at a time without loading entire datasets into memory
- **Minimal GC Pressure**: Designed to minimize object allocations and garbage collection

### Speed Optimizations
- **Async I/O**: Non-blocking file operations, streaming one page of data at a time
- **Efficient Parsing**: Direct binary parsing without intermediate string conversions
- **Column Selection**: Process only required columns to reduce work

## ⚠️ Critical Usage Guidelines

### Memory Safety with Pooled Arrays

This library uses pooled arrays for maximum performance. **You MUST follow these rules:**

```csharp
// ✅ CORRECT: Process data immediately
await foreach (var row in reader.ReadRowsAsync())
{
    ProcessRow(row.Span); // Use data immediately
    // Data becomes invalid after this iteration
}

// ❌ WRONG: Don't store row references
var storedRows = new List<ReadOnlyMemory<object?>>();
await foreach (var row in reader.ReadRowsAsync())
{
    storedRows.Add(row); // DON'T DO THIS - data will be corrupted!
}

// ✅ CORRECT: Copy data if you need to store it
var storedData = new List<object[]>();
await foreach (var row in reader.ReadRowsAsync())
{
    storedData.Add(row.ToArray()); // Safe to store as the call to ToArray() creates a new array for the data

    // Or, can be done manually
    var copy = new object[row.Length];
    row.Span.CopyTo(copy);
    storedData.Add(copy); // Safe to store
}
```

**Why this matters**: The library reuses the same underlying buffer for performance. Storing references to `ReadOnlyMemory<object?>` will result in data corruption as the buffer gets reused for subsequent rows.

## Advanced Usage

### Column Selection
```csharp
var options = new RecordReadOptions
{
    SelectedColumns = new HashSet<string> { "Name", "Age", "Salary" }
};

await foreach (var row in reader.ReadRowsAsync(options))
{
    // Only selected columns are processed
}
```

### Row Filtering
```csharp
var options = new RecordReadOptions
{
    SkipRows = 100,
    MaxRows = 1000
};

await foreach (var row in reader.ReadRowsAsync(options))
{
    // Process rows 101-1100
}
```

### Custom Transformation
```csharp
await foreach (var trip in reader.ReadRecordsAsync(TransformToTrip))
{
    Console.WriteLine($"Trip to {trip.Destination} on {trip.When:yyyy-MM-dd}");
}

static Trip TransformToTrip(ReadOnlyMemory<object?> row)
{
    var span = row.Span;
    return new Trip
    {
        When = (DateTime)span[0]!, // SAS date automatically converted to DateTime
        Departure = (DateTime)span[1]!, // SAS date and time automatically converted to DateTime
        Duration = (TimeSpan)span[2]!, // SAS time automatically converted to TimeSpan
        Destination = (string?)span[3], // Strings interpreted using the correct encoding
        Speed = (double)span[4]!
    };
}


public sealed class Trip
{
    public DateTime When { get; init; }
    public DateTime Departure { get; init; }
    public TimeSpan Duration { get; init; }
    public string? Destination { get; init; } = "";
    public double Speed { get; init; }
}

```

## File Format Support

- **Standard SAS7BDAT**: Classic format from SAS 7.0+
- **Extended Observation Counter**: SAS 9.3+ format with `EXTENDOBSCOUNTER=YES`
- **Compressed Files**: RLE and RDC compression algorithms
- **Deleted Records**: Files containing logically deleted rows
- **Mixed Pages**: Files with metadata and data on same pages
- **Big/Little Endian**: Cross-platform compatibility
- **32/64-bit Formats**: Both architecture variants supported

## Metadata Access

```csharp
await using var reader = await SasDataReader.OpenFileAsync("data.sas7bdat");

Console.WriteLine($"Dataset: {reader.Metadata.DatasetName}");
Console.WriteLine($"Rows: {reader.Metadata.RowCount}");
Console.WriteLine($"Columns: {reader.Metadata.ColumnCount}");
Console.WriteLine($"Created: {reader.Metadata.DateCreated}");
Console.WriteLine($"Encoding: {reader.Metadata.Encoding}");

foreach (var column in reader.Columns)
{
    Console.WriteLine($"Column: {column.Name} ({column.Type}) - {column.Label}");
}
```

## Error Handling

```csharp
try
{
    await using var reader = await SasDataReader.OpenFileAsync("data.sas7bdat");
    await foreach (var row in reader.ReadRowsAsync(cancellationToken))
    {
        // Process row
    }
}
catch (FileNotFoundException)
{
    Console.WriteLine("SAS file not found");
}
catch (InvalidDataException ex)
{
    Console.WriteLine($"Invalid SAS file format: {ex.Message}");
}
catch (OperationCanceledException)
{
    Console.WriteLine("Operation was cancelled");
}
```

## Performance Tips

1. **Use column selection** when you don't need all columns
2. **Process data immediately** - don't store row references
3. **Enable cancellation** for long-running operations
4. **Use stream processing of data** where possible

## System Requirements

- .NET 8.0 or later
- Any platform supported by .NET (Windows, Linux, macOS)

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- SAS7BDAT format documentation by [Matthew S. Shotwell](https://github.com/BioStatMatt/sas7bdat/blob/master/inst/doc/sas7bdat.pdf)
- Additional format insights from the Python [`sas7bdat` library](https://github.com/jared-hwang/sas7bdat) by Jared Hwang
- The original [ReadStat C library](https://github.com/WizardMac/ReadStat) which includes comprehensive SAS format support
- Based on reverse engineering work from the R [`sas7bdat` package](https://cran.r-project.org/package=sas7bdat) by Matt Shotwell
- The [Pandas SAS reader](https://github.com/pandas-dev/pandas/blob/main/pandas/io/sas/sas7bdat.py) implementation
- Test data files from the [SasReader.NET repository](https://github.com/pseudomo/SasReader.NET) by pseudomo
- C++ implementation insights from [cpp-sas7bdat](https://github.com/olivia76/cpp-sas7bdat) by Olivia Quinet
- Performance optimizations inspired by modern .NET practices and the [.NET Performance Guidelines](https://docs.microsoft.com/en-us/dotnet/framework/performance/)