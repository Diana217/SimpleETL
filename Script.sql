-- Create database
IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = N'TripsDb')
BEGIN
    CREATE DATABASE TripsDb;
END
GO

USE TripsDb;
GO

-- Create destination table
IF OBJECT_ID('dbo.Trips', 'U') IS NOT NULL
    DROP TABLE dbo.Trips;
GO

CREATE TABLE dbo.Trips
(
    ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    TpepPickupDatetime DATETIME2(3) NOT NULL,
    TpepDropoffDatetime DATETIME2(3) NOT NULL,
    PassengerCount TINYINT NOT NULL,
    TripDistance DECIMAL(9,2) NOT NULL,
    StoreAndFwdFlag NVARCHAR(3) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    FareAmount DECIMAL(10,2) NULL,
    TipAmount DECIMAL(10,2) NULL
);
GO

-- Indexes to support expected queries
-- 1) finding which PULocationID has highest average TipAmount
CREATE NONCLUSTERED INDEX IX_Trips_PULocationID_TipAmount ON dbo.Trips (PULocationID) INCLUDE (TipAmount);

-- 2) top 100 longest fares by TripDistance
CREATE NONCLUSTERED INDEX IX_Trips_TripDistance ON dbo.Trips (TripDistance DESC);

-- 3) top 100 longest fares by time spent
CREATE NONCLUSTERED INDEX IX_Trips_pickup_dropoff ON dbo.Trips (TpepPickupDatetime, TpepDropoffDatetime);

GO