USE StackOverflow2010Minimal;
GO

IF COL_LENGTH('dbo.Users', 'new_note') IS NULL
BEGIN
    ALTER TABLE dbo.Users ADD new_note NVARCHAR(100) NULL;
END
GO

UPDATE dbo.Users
SET new_note = N'schema-contract-evolve-ok'
WHERE Id = 1;
GO

ALTER TABLE dbo.Users ALTER COLUMN AccountId BIGINT NULL;
GO

ALTER TABLE dbo.Users ALTER COLUMN DisplayName NVARCHAR(40) NULL;
GO
