
-- 03_bulkload.sql
USE appdb;
GO

IF OBJECT_ID('dbo.UserProfile_stage','U') IS NOT NULL DROP TABLE dbo.UserProfile_stage;
CREATE TABLE dbo.UserProfile_stage
(
  Id          BIGINT NULL,
  Email       NVARCHAR(320) NULL,
  DisplayName NVARCHAR(200) NULL,
  CreatedAt   DATETIME2(3) NULL,
  UpdatedAt   DATETIME2(3) NULL
);

BULK INSERT dbo.UserProfile_stage
FROM '/var/opt/mssql/import/UserProfile.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '0x0a',
  TABLOCK,
  CODEPAGE = '65001'
);

SET IDENTITY_INSERT dbo.UserProfile ON;
INSERT INTO dbo.UserProfile (Id, Email, DisplayName, CreatedAt, UpdatedAt)
SELECT Id, Email, DisplayName, CreatedAt, UpdatedAt
FROM dbo.UserProfile_stage
WHERE Email IS NOT NULL;
SET IDENTITY_INSERT dbo.UserProfile OFF;

DROP TABLE dbo.UserProfile_stage;
GO

IF OBJECT_ID('dbo.LogEvent_stage','U') IS NOT NULL DROP TABLE dbo.LogEvent_stage;
CREATE TABLE dbo.LogEvent_stage
(
  LogId         BIGINT NULL,
  EventTime     DATETIME2(3) NULL,
  Source        NVARCHAR(100) NULL,
  Level         VARCHAR(20) NULL,
  Message       NVARCHAR(MAX) NULL,
  Payload       NVARCHAR(MAX) NULL,
  UserId        BIGINT NULL,
  CorrelationId UNIQUEIDENTIFIER NULL
);

BULK INSERT dbo.LogEvent_stage
FROM '/var/opt/mssql/import/LogEvent.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '0x0a',
  TABLOCK,
  CODEPAGE = '65001'
);

SET IDENTITY_INSERT dbo.LogEvent ON;
INSERT INTO dbo.LogEvent (LogId, EventTime, Source, Level, Message, Payload, UserId, CorrelationId)
SELECT LogId, EventTime, Source, Level, Message, Payload, UserId, CorrelationId
FROM dbo.LogEvent_stage
WHERE EventTime IS NOT NULL;
SET IDENTITY_INSERT dbo.LogEvent OFF;

DROP TABLE dbo.LogEvent_stage;
GO
