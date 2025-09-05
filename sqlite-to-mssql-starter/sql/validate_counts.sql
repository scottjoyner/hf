
-- validate_counts.sql
SET NOCOUNT ON;
DECLARE @sql nvarchar(max) = N'';

SELECT @sql = STRING_AGG(CONCAT('SELECT ''', s.name, '.', t.name, ''' AS TableName, COUNT_BIG(*) AS RowCount FROM ', QUOTENAME(s.name), '.', QUOTENAME(t.name), ';'), CHAR(10))
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = 'dbo';

EXEC sp_executesql @sql;
