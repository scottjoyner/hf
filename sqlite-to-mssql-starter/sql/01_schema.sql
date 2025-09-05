
-- 01_schema.sql
-- Base example schema (safe for local dev). You can rely solely on generated DDL instead.

IF OBJECT_ID('dbo.LogEvent', 'U') IS NOT NULL DROP TABLE dbo.LogEvent;
IF OBJECT_ID('dbo.UserProfile', 'U') IS NOT NULL DROP TABLE dbo.UserProfile;
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'PS_LogEventByMonth')
BEGIN
  DROP PARTITION SCHEME PS_LogEventByMonth;
END
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_LogEventByMonth')
BEGIN
  DROP PARTITION FUNCTION PF_LogEventByMonth;
END
GO

CREATE PARTITION FUNCTION PF_LogEventByMonth (date)
AS RANGE RIGHT FOR VALUES (
  ('2024-02-01'),('2024-03-01'),('2024-04-01'),('2024-05-01'),('2024-06-01'),
  ('2024-07-01'),('2024-08-01'),('2024-09-01'),('2024-10-01'),('2024-11-01'),('2024-12-01'),
  ('2025-01-01'),('2025-02-01'),('2025-03-01'),('2025-04-01'),('2025-05-01'),('2025-06-01'),
  ('2025-07-01'),('2025-08-01'),('2025-09-01'),('2025-10-01'),('2025-11-01'),('2025-12-01'),
  ('2026-01-01')
);
GO

CREATE PARTITION SCHEME PS_LogEventByMonth
AS PARTITION PF_LogEventByMonth
ALL TO ([PRIMARY]);
GO

CREATE TABLE dbo.UserProfile
(
  Id          BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_UserProfile PRIMARY KEY,
  Email       NVARCHAR(320) NOT NULL UNIQUE,
  DisplayName NVARCHAR(200) NULL,
  CreatedAt   DATETIME2(3) NOT NULL CONSTRAINT DF_UserProfile_CreatedAt DEFAULT (SYSUTCDATETIME()),
  UpdatedAt   DATETIME2(3) NOT NULL CONSTRAINT DF_UserProfile_UpdatedAt DEFAULT (SYSUTCDATETIME())
);
GO

CREATE OR ALTER TRIGGER dbo.trg_UserProfile_SetUpdatedAt
ON dbo.UserProfile
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  UPDATE u
    SET UpdatedAt = SYSUTCDATETIME()
  FROM dbo.UserProfile u
  JOIN inserted i ON u.Id = i.Id;
END
GO

CREATE TABLE dbo.LogEvent
(
  LogId         BIGINT IDENTITY(1,1) NOT NULL,
  EventTime     DATETIME2(3) NOT NULL CONSTRAINT DF_LogEvent_EventTime DEFAULT (SYSUTCDATETIME()),
  EventDate     AS CAST(EventTime AS date) PERSISTED,
  Source        NVARCHAR(100) NOT NULL,
  Level         VARCHAR(20) NOT NULL,
  Message       NVARCHAR(MAX) NULL,
  Payload       NVARCHAR(MAX) NULL,
  UserId        BIGINT NULL,
  CorrelationId UNIQUEIDENTIFIER NULL,
  CONSTRAINT PK_LogEvent PRIMARY KEY CLUSTERED (EventDate, LogId)
) ON PS_LogEventByMonth(EventDate);
GO

CREATE INDEX IX_LogEvent_User_EventTime
  ON dbo.LogEvent (EventDate, UserId, EventTime DESC);

CREATE INDEX IX_LogEvent_Source_Level_Time
  ON dbo.LogEvent (EventDate, Source, Level, EventTime DESC);
GO
