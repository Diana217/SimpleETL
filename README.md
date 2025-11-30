**Setup**

Create `appsettings.Development.json` (not tracked by Git) with your connection string:

```
{
  "ConnectionStrings": {
    "TripsDb": ""
  }
}
```

**Run**

`dotnet run --project SimpleETL.csproj -- "input.csv" "duplicates.csv"`

`input.csv` - CSV file with trip data

`duplicates.csv` - file to store duplicates
