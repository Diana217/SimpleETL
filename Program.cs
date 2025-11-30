using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SimpleETL;
using System.Data;
using System.Globalization;
using Constants = SimpleETL.Constants;

// Load config
var config = new ConfigurationBuilder()
    .AddJsonFile(Constants.AppSettingsFile, optional: true)
    .Build();

var connectionString = config.GetConnectionString(Constants.DbName);
if (string.IsNullOrEmpty(connectionString))
{
    Console.WriteLine("ERROR: ConnectionString is missing. Add to appsettings.json");
    return 1;
}

using var conn = new SqlConnection(connectionString);
await conn.OpenAsync();

// Handle arguments
if (args.Length == 0)
{
    Console.WriteLine("ERROR: arguments missing. Use: dotnet run <input.csv> <duplicates.csv>");
    return 1;
}

var inputCsv = args[0];
var duplicatesCsv = args.Length > 1 ? args[1] : Constants.DuplicatesFile;

if (!File.Exists(inputCsv))
{
    Console.WriteLine($"Input file not found: {inputCsv}");
    return 1;
}

Console.WriteLine($"Starting ETL: {inputCsv}");

try
{
    await ProcessCsvAndBulkInsertAsync(inputCsv, duplicatesCsv);
    Console.WriteLine("ETL finished");
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine("ETL failed: " + ex.Message);
    return 2;
}

#region ETL Logic

async Task ProcessCsvAndBulkInsertAsync(string inputCsvPath, string duplicatesCsvPath)
{
    var uniqueKeys = new HashSet<string>();

    var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    {
        HasHeaderRecord = true,
        IgnoreBlankLines = true,
        TrimOptions = TrimOptions.Trim
    };

    using var dupWriter = new StreamWriter(duplicatesCsvPath);
    using var csvDup = new CsvWriter(dupWriter, CultureInfo.InvariantCulture);

    csvDup.WriteCsv(
    [
        OldFieldNames.TpepPickupDatetime,
        OldFieldNames.TpepDropoffDatetime,
        OldFieldNames.PassengerCount,
        OldFieldNames.TripDistance,
        OldFieldNames.StoreAndFwdFlag,
        OldFieldNames.PULocationID,
        OldFieldNames.DOLocationID,
        OldFieldNames.FareAmount,
        OldFieldNames.TipAmount
    ]);

    DataTable tableBuffer = Helper.CreateDataTableSchema();
    int rowsBuffered = 0;
    long totalInserted = 0;
    long totalDuplicates = 0;

    using var reader = new StreamReader(inputCsvPath);
    using var csv = new CsvReader(reader, config);

    await csv.ReadAsync();
    csv.ReadHeader();

    string? GetField(string name)
    {
        if (csv.TryGetField(name, out string? val))
            return val?.Trim();
        return null;
    }

    while (await csv.ReadAsync())
    {
        var pickStr = GetField(OldFieldNames.TpepPickupDatetime);
        var dropStr = GetField(OldFieldNames.TpepDropoffDatetime);
        var passengerStr = GetField(OldFieldNames.PassengerCount);
        var tripDistStr = GetField(OldFieldNames.TripDistance);
        var storeFlag = GetField(OldFieldNames.StoreAndFwdFlag);
        var puIdStr = GetField(OldFieldNames.PULocationID);
        var doIdStr = GetField(OldFieldNames.DOLocationID);
        var fareStr = GetField(OldFieldNames.FareAmount);
        var tipStr = GetField(OldFieldNames.TipAmount);

        if (string.IsNullOrWhiteSpace(pickStr) ||
            string.IsNullOrWhiteSpace(dropStr) ||
            string.IsNullOrWhiteSpace(passengerStr))
            continue;

        if (!byte.TryParse(passengerStr, out byte passengerCount))
            continue;

        if (!DateTime.TryParse(pickStr, out DateTime pickupLocal))
            continue;

        if (!DateTime.TryParse(dropStr, out DateTime dropoffLocal))
            continue;

        TimeZoneInfo easternTz = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
        DateTime pickupUtc = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, easternTz);
        DateTime dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, easternTz);

        string uniqueKey = $"{pickupUtc:O}|{dropoffUtc:O}|{passengerCount}";

        if (uniqueKeys.Contains(uniqueKey))
        {
            csvDup.WriteCsv(
            [
                pickupUtc.ToString("o"),
                dropoffUtc.ToString("o"),
                passengerCount.ToString(),
                tripDistStr,
                storeFlag.NormalizeStoreAndFwd(),
                puIdStr,
                doIdStr,
                fareStr,
                tipStr
            ]);

            totalDuplicates++;
            continue;
        }

        uniqueKeys.Add(uniqueKey);

        decimal.TryParse(tripDistStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal tripDistance);
        decimal.TryParse(fareStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal fare);
        decimal.TryParse(tipStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal tip);

        _ = int.TryParse(puIdStr, out int puId);
        _ = int.TryParse(doIdStr, out int doId);

        var row = tableBuffer.NewRow();
        row[NewFieldNames.TpepPickupDatetime] = pickupUtc;
        row[NewFieldNames.TpepDropoffDatetime] = dropoffUtc;
        row[NewFieldNames.PassengerCount] = passengerCount;
        row[NewFieldNames.TripDistance] = tripDistance;
        row[NewFieldNames.StoreAndFwdFlag] = storeFlag.NormalizeStoreAndFwd();
        row[NewFieldNames.PULocationID] = puId;
        row[NewFieldNames.DOLocationID] = doId;
        row[NewFieldNames.FareAmount] = fare;
        row[NewFieldNames.TipAmount] = tip;

        tableBuffer.Rows.Add(row);
        rowsBuffered++;

        if (rowsBuffered >= Constants.BulkBatchSize)
        {
            var inserted = await tableBuffer.BulkInsertAsync(connectionString);
            totalInserted += inserted;
            tableBuffer.Clear();
            rowsBuffered = 0;
            Console.WriteLine($"Inserted batch. Total inserted: {totalInserted}. Duplicates: {totalDuplicates}");
        }
    }

    if (tableBuffer.Rows.Count > 0)
    {
        var inserted = await tableBuffer.BulkInsertAsync(connectionString);
        totalInserted += inserted;
    }

    Console.WriteLine($"Total inserted: {totalInserted}. Total duplicates: {totalDuplicates}.");
}

#endregion