USE StackOverflow2010Minimal;
GO

UPDATE dbo.Users
SET
    DisplayName = N'Jeff Atwood Daily Driver',
    Reputation = 9010,
    LastAccessDate = GETUTCDATE()
WHERE Id = 1;
GO

SELECT
    Id,
    DisplayName,
    Reputation,
    LastAccessDate
FROM dbo.Users
WHERE Id = 1;
GO
