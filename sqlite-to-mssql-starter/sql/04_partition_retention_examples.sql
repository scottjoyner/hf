
-- 04_partition_retention_examples.sql
USE appdb;
GO

IF OBJECT_ID('dbo.LogEvent_Archive','U') IS NULL
BEGIN
  CREATE TABLE dbo.LogEvent_Archive
  (
    LogId         BIGINT NOT NULL,
    EventTime     DATETIME2(3) NOT NULL,
    Source        NVARCHAR(100) NOT NULL,
    Level         VARCHAR(20) NOT NULL,
    Message       NVARCHAR(MAX) NULL,
    Payload       NVARCHAR(MAX) NULL,
    UserId        BIGINT NULL,
    CorrelationId UNIQUEIDENTIFIER NULL
  );
END
GO

-- Example:
-- ALTER TABLE dbo.LogEvent SWITCH PARTITION 1 TO dbo.LogEvent_Archive;
-- TRUNCATE TABLE dbo.LogEvent_Archive;
