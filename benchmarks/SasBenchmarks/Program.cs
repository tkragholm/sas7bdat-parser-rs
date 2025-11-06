using System.Diagnostics;
using Sas7Bdat.Core;

if (args.Length == 0)
{
    Console.WriteLine("Usage: SasBenchmarks <path-to-sas7bdat>");
    return;
}

var inputPath = Path.GetFullPath(args[0]);
if (!File.Exists(inputPath))
{
    Console.Error.WriteLine($"Input file not found: {inputPath}");
    Environment.Exit(1);
}

var stopwatch = Stopwatch.StartNew();

await using var reader = await SasDataReader.OpenFileAsync(inputPath);
var columnCount = reader.Metadata.ColumnCount;
var rowCount = 0L;

await foreach (var row in reader.ReadRowsAsync())
{
    _ = row.Length;
    rowCount++;
}

stopwatch.Stop();

Console.WriteLine($"File           : {inputPath}");
Console.WriteLine($"Rows processed : {rowCount}");
Console.WriteLine($"Columns        : {columnCount}");
Console.WriteLine($"Elapsed (ms)   : {stopwatch.Elapsed.TotalMilliseconds:F2}");
