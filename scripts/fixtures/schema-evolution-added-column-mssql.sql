USE StackOverflow2010Minimal;
GO

IF COL_LENGTH('dbo.Users', 'new_note') IS NULL
    ALTER TABLE dbo.Users ADD new_note NVARCHAR(100) NULL;
GO

UPDATE dbo.Users
SET new_note = N'schema-evolution-auto-ok'
WHERE Id = 1;
GO
