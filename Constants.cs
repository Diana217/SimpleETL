namespace SimpleETL;

public static class Constants
{
    public const string AppSettingsFile = "appsettings.Development.json";
    public const string DuplicatesFile = "duplicates.csv";

    public const string DbName = "TripsDb";
    public const string TableName = "dbo.Trips";
    public const int BulkBatchSize = 5000;
}

public class OldFieldNames
{
    public const string TpepPickupDatetime = "tpep_pickup_datetime";
    public const string TpepDropoffDatetime = "tpep_dropoff_datetime";
    public const string PassengerCount = "passenger_count";
    public const string TripDistance = "trip_distance";
    public const string StoreAndFwdFlag = "store_and_fwd_flag";
    public const string PULocationID = "PULocationID";
    public const string DOLocationID = "DOLocationID";
    public const string FareAmount = "fare_amount";
    public const string TipAmount = "tip_amount";
}

public class NewFieldNames
{
    public const string TpepPickupDatetime = "TpepPickupDatetime";
    public const string TpepDropoffDatetime = "TpepDropoffDatetime";
    public const string PassengerCount = "PassengerCount";
    public const string TripDistance = "TripDistance";
    public const string StoreAndFwdFlag = "StoreAndFwdFlag";
    public const string PULocationID = "PULocationID";
    public const string DOLocationID = "DOLocationID";
    public const string FareAmount = "FareAmount";
    public const string TipAmount = "TipAmount";
}
