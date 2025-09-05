
-- 02_sample_upsert.sql
DECLARE @Email NVARCHAR(320) = N'alice@example.com';
DECLARE @DisplayName NVARCHAR(200) = N'Alice Cooper';

MERGE dbo.UserProfile AS target
USING (VALUES (@Email, @DisplayName)) AS src(Email, DisplayName)
   ON target.Email = src.Email
WHEN MATCHED THEN
  UPDATE SET DisplayName = src.DisplayName, UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT (Email, DisplayName) VALUES (src.Email, src.DisplayName)
OUTPUT $action AS MergeAction, inserted.Id, inserted.Email;
GO

DECLARE @Email2 NVARCHAR(320) = N'bob@example.com';
DECLARE @DisplayName2 NVARCHAR(200) = N'Bob Smith';

UPDATE u
   SET u.DisplayName = @DisplayName2, u.UpdatedAt = SYSUTCDATETIME()
  FROM dbo.UserProfile u
 WHERE u.Email = @Email2;

IF @@ROWCOUNT = 0
BEGIN
  INSERT INTO dbo.UserProfile (Email, DisplayName) VALUES (@Email2, @DisplayName2);
END
GO
