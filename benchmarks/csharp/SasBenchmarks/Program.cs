using System.Diagnostics;
using System.Globalization;
using Sas7Bdat.Core;

if (args.Length == 0)
{
    Console.WriteLine("Usage: SasBenchmarks <path-to-sas7bdat>");
    Console.WriteLine("       SasBenchmarks --csv <output.csv> <path-to-sas7bdat>");
    return;
}

string? csvOutput = null;
string? inputPath = null;
if (args.Length == 1)
{
    inputPath = args[0];
}
else if (args.Length == 3 && args[0] == "--csv")
{
    csvOutput = args[1];
    inputPath = args[2];
}
else
{
    Console.Error.WriteLine("Invalid arguments. Expected <input> or --csv <output> <input>.");
    Environment.Exit(1);
}

inputPath = Path.GetFullPath(inputPath);
if (!File.Exists(inputPath))
{
    Console.Error.WriteLine($"Input file not found: {inputPath}");
    Environment.Exit(1);
}

if (csvOutput is not null)
{
    csvOutput = Path.GetFullPath(csvOutput);
}

var stopwatch = Stopwatch.StartNew();

await using var reader = await SasDataReader.OpenFileAsync(inputPath);
var columnCount = reader.Metadata.ColumnCount;
var rowCount = 0L;

if (csvOutput is not null)
{
    await CsvHelpers.WriteCsvAsync(reader, csvOutput);
    return;
}

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

static class CsvHelpers
{
    private static readonly DateTime SasEpoch = new(1960, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public static async Task WriteCsvAsync(SasDataReader reader, string outputPath)
    {
        await using var stream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        await using var writer = new StreamWriter(stream);

        var columns = reader.Columns;
        await writer.WriteLineAsync(string.Join(",", columns.Select(c => c.Name.TrimEnd())));

        await foreach (var row in reader.ReadRowsAsync())
        {
            var fields = new string[columns.Length];
            for (var i = 0; i < columns.Length; i++)
            {
                fields[i] = FormatField(row.Span[i], columns[i]);
            }
            await writer.WriteLineAsync(string.Join(",", fields));
        }
    }

    private static string FormatField(object? value, SasColumnInfo column)
    {
        if (column.ColumnType == ColumnType.String)
        {
            var text = value?.ToString() ?? string.Empty;
            return QuoteCsv(text);
        }

        if (value is null)
        {
            return string.Empty;
        }

        return column.ColumnType switch
        {
            ColumnType.Number => FormatNumber(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
            ColumnType.DateTime => FormatNumber(ToSasSeconds((DateTime)value)),
            ColumnType.Date => FormatNumber(ToSasDays((DateTime)value)),
            ColumnType.Time => FormatNumber(ToSasSeconds((TimeSpan)value)),
            _ => string.Empty
        };
    }

    private static string QuoteCsv(string value)
    {
        var escaped = value.Replace("\"", "\"\"");
        return $"\"{escaped}\"";
    }

    private static string FormatNumber(double value)
    {
        return value.ToString("G17", CultureInfo.InvariantCulture);
    }

    private static double ToSasSeconds(DateTime dt)
    {
        var utc = dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime();
        return (utc - SasEpoch).TotalSeconds;
    }

    private static double ToSasDays(DateTime dt)
    {
        var date = dt.Date;
        var utc = date.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(date, DateTimeKind.Utc) : date.ToUniversalTime();
        return (utc - SasEpoch).TotalDays;
    }

    private static double ToSasSeconds(TimeSpan span)
    {
        return span.TotalSeconds;
    }
}
