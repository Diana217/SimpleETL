using CsvHelper;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;

namespace SimpleETL;

public static class Helper
{
    public static DataTable CreateDataTableSchema()
    {
        var dt = new DataTable
        {
            Locale = CultureInfo.InvariantCulture
        };

        dt.Columns.Add(new DataColumn(NewFieldNames.TpepPickupDatetime, typeof(DateTime)));
        dt.Columns.Add(new DataColumn(NewFieldNames.TpepDropoffDatetime, typeof(DateTime)));
        dt.Columns.Add(new DataColumn(NewFieldNames.PassengerCount, typeof(byte)));
        dt.Columns.Add(new DataColumn(NewFieldNames.TripDistance, typeof(decimal)));
        dt.Columns.Add(new DataColumn(NewFieldNames.StoreAndFwdFlag, typeof(string)));
        dt.Columns.Add(new DataColumn(NewFieldNames.PULocationID, typeof(int)));
        dt.Columns.Add(new DataColumn(NewFieldNames.DOLocationID, typeof(int)));
        dt.Columns.Add(new DataColumn(NewFieldNames.FareAmount, typeof(decimal)));
        dt.Columns.Add(new DataColumn(NewFieldNames.TipAmount, typeof(decimal)));

        return dt;
    }

    public static void WriteCsv(this CsvWriter csv, IEnumerable<string?> fields)
    {
        foreach (var field in fields)
        {
            csv.WriteField(field);
        }

        csv.NextRecord();
    }

    public static string NormalizeStoreAndFwd(this string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "No";

        return raw.ToUpper() switch
        {
            "Y" => "Yes",
            "N" => "No",
            _ => raw
        };
    }

    public static async Task<long> BulkInsertAsync(this DataTable data, string connectionString)
    {
        if (data.Rows.Count == 0) return 0;

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        using var bulk = new SqlBulkCopy(conn)
        {
            DestinationTableName = Constants.TableName,
            BatchSize = data.Rows.Count
        };

        foreach (DataColumn col in data.Columns)
            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

        await bulk.WriteToServerAsync(data);
        return data.Rows.Count;
    }
}
