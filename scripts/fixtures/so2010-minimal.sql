-- SO2010-minimal fixture for dmt (#178)
--
-- A reproducible, CI-friendly subset of the StackOverflow2010 schema:
-- same 9 tables and 61 columns as the public dataset (Brent Ozar's
-- StackOverflow2010 from https://downloads.brentozar.com/), with a
-- few representative rows per table instead of the full ~19M.
--
-- The DDL is extracted verbatim from the public dataset; the seed is
-- synthetic but exercises every column type the real dataset uses
-- (int, datetime, nvarchar with explicit lengths, varchar, NVARCHAR(MAX)
-- via -1, nullable + NOT NULL mix). Round-trip type-mapping tests can
-- run against this without needing the 10 GB .bak file.
--
-- Loads cleanly into:
--   - mssql-test (make test-dbs-up)
--   - mssql-bench (make bench-dbs-up)
--   - any other SQL Server 2017+ instance
--
-- See docs/FIXTURES.md for the full inventory + the procedure to load
-- the *complete* SO2010/SO2013/WWI .bak archives for benchmarking.

-- Create the database. IF EXISTS lets us re-run idempotently.
IF DB_ID('StackOverflow2010Minimal') IS NULL
    CREATE DATABASE StackOverflow2010Minimal;
GO

USE StackOverflow2010Minimal;
GO

-- ---------- DDL ----------
-- Order matters: VoteTypes, PostTypes, LinkTypes have no FKs; Users
-- and Posts are widely referenced; Comments/Votes/Badges/PostLinks
-- come last. PKs are clustered indexes named PK_<Table>.
IF OBJECT_ID('dbo.VoteTypes', 'U') IS NOT NULL DROP TABLE dbo.VoteTypes;
IF OBJECT_ID('dbo.PostTypes', 'U') IS NOT NULL DROP TABLE dbo.PostTypes;
IF OBJECT_ID('dbo.LinkTypes', 'U') IS NOT NULL DROP TABLE dbo.LinkTypes;
IF OBJECT_ID('dbo.Comments', 'U') IS NOT NULL DROP TABLE dbo.Comments;
IF OBJECT_ID('dbo.Votes', 'U') IS NOT NULL DROP TABLE dbo.Votes;
IF OBJECT_ID('dbo.Badges', 'U') IS NOT NULL DROP TABLE dbo.Badges;
IF OBJECT_ID('dbo.PostLinks', 'U') IS NOT NULL DROP TABLE dbo.PostLinks;
IF OBJECT_ID('dbo.Posts', 'U') IS NOT NULL DROP TABLE dbo.Posts;
IF OBJECT_ID('dbo.Users', 'U') IS NOT NULL DROP TABLE dbo.Users;
GO

CREATE TABLE dbo.VoteTypes (
    Id   INT          NOT NULL CONSTRAINT PK_VoteTypes PRIMARY KEY,
    Name VARCHAR(50)  NOT NULL
);
CREATE TABLE dbo.PostTypes (
    Id   INT           NOT NULL CONSTRAINT PK_PostTypes PRIMARY KEY,
    Type NVARCHAR(100) NOT NULL
);
CREATE TABLE dbo.LinkTypes (
    Id   INT          NOT NULL CONSTRAINT PK_LinkTypes PRIMARY KEY,
    Type VARCHAR(50)  NOT NULL
);

CREATE TABLE dbo.Users (
    Id              INT             NOT NULL CONSTRAINT PK_Users PRIMARY KEY,
    AboutMe         NVARCHAR(MAX)   NULL,
    Age             INT             NULL,
    CreationDate    DATETIME        NOT NULL,
    DisplayName     NVARCHAR(40)    NOT NULL,
    DownVotes       INT             NOT NULL,
    EmailHash       NVARCHAR(40)    NULL,
    LastAccessDate  DATETIME        NOT NULL,
    Location        NVARCHAR(100)   NULL,
    Reputation      INT             NOT NULL,
    UpVotes         INT             NOT NULL,
    Views           INT             NOT NULL,
    WebsiteUrl      NVARCHAR(200)   NULL,
    AccountId       INT             NULL
);

CREATE TABLE dbo.Posts (
    Id                      INT             NOT NULL CONSTRAINT PK_Posts PRIMARY KEY,
    AcceptedAnswerId        INT             NULL,
    AnswerCount             INT             NULL,
    Body                    NVARCHAR(MAX)   NOT NULL,
    ClosedDate              DATETIME        NULL,
    CommentCount            INT             NULL,
    CommunityOwnedDate      DATETIME        NULL,
    CreationDate            DATETIME        NOT NULL,
    FavoriteCount           INT             NULL,
    LastActivityDate        DATETIME        NOT NULL,
    LastEditDate            DATETIME        NULL,
    LastEditorDisplayName   NVARCHAR(40)    NULL,
    LastEditorUserId        INT             NULL,
    OwnerUserId             INT             NULL,
    ParentId                INT             NULL,
    PostTypeId              INT             NOT NULL,
    Score                   INT             NOT NULL,
    Tags                    NVARCHAR(150)   NULL,
    Title                   NVARCHAR(250)   NULL,
    ViewCount               INT             NOT NULL
);

CREATE TABLE dbo.Comments (
    Id           INT             NOT NULL CONSTRAINT PK_Comments PRIMARY KEY,
    CreationDate DATETIME        NOT NULL,
    PostId       INT             NOT NULL,
    Score        INT             NULL,
    Text         NVARCHAR(700)   NOT NULL,
    UserId       INT             NULL
);

CREATE TABLE dbo.Votes (
    Id           INT      NOT NULL CONSTRAINT PK_Votes PRIMARY KEY,
    PostId       INT      NOT NULL,
    UserId       INT      NULL,
    BountyAmount INT      NULL,
    VoteTypeId   INT      NOT NULL,
    CreationDate DATETIME NOT NULL
);

CREATE TABLE dbo.Badges (
    Id     INT          NOT NULL CONSTRAINT PK_Badges PRIMARY KEY,
    Name   NVARCHAR(40) NOT NULL,
    UserId INT          NOT NULL,
    Date   DATETIME     NOT NULL
);

CREATE TABLE dbo.PostLinks (
    Id            INT      NOT NULL CONSTRAINT PK_PostLinks PRIMARY KEY,
    CreationDate  DATETIME NOT NULL,
    PostId        INT      NOT NULL,
    RelatedPostId INT      NOT NULL,
    LinkTypeId    INT      NOT NULL
);
GO

-- ---------- Seed data ----------
-- Lookup tables match the canonical StackOverflow2010 values
-- byte-for-byte (extracted from the public dataset) so round-trip
-- tests can assert known IDs without divergence. Note ID 14 is
-- legitimately absent from VoteTypes in the canonical dataset, and
-- "TagWikiExerpt" carries its canonical typo (no 'c' before 'erpt').
INSERT INTO dbo.VoteTypes (Id, Name) VALUES
    (1,  'AcceptedByOriginator'),
    (2,  'UpMod'),
    (3,  'DownMod'),
    (4,  'Offensive'),
    (5,  'Favorite'),
    (6,  'Close'),
    (7,  'Reopen'),
    (8,  'BountyStart'),
    (9,  'BountyClose'),
    (10, 'Deletion'),
    (11, 'Undeletion'),
    (12, 'Spam'),
    (13, 'InformModerator'),
    (15, 'ModeratorReview'),
    (16, 'ApproveEditSuggestion');

INSERT INTO dbo.PostTypes (Id, Type) VALUES
    (1, N'Question'),
    (2, N'Answer'),
    (3, N'Wiki'),
    (4, N'TagWikiExerpt'),         -- sic, matches canonical typo
    (5, N'TagWiki'),
    (6, N'ModeratorNomination'),
    (7, N'WikiPlaceholder'),
    (8, N'PrivilegeWiki');

INSERT INTO dbo.LinkTypes (Id, Type) VALUES
    (1, 'Linked'),
    (3, 'Duplicate');

-- Users: mix of present/null fields, varied UTF-16 strings.
INSERT INTO dbo.Users (Id, AboutMe, Age, CreationDate, DisplayName, DownVotes, EmailHash, LastAccessDate, Location, Reputation, UpVotes, Views, WebsiteUrl, AccountId) VALUES
    (-1, N'Community wiki user', NULL, '2008-07-31 00:00:00', N'Community', 0, NULL, '2008-07-31 00:00:00', NULL, 1, 0, 0, NULL, NULL),
    (1, N'Site founder', 34, '2008-07-31 14:22:31', N'Jeff Atwood', 12, NULL, '2010-12-31 23:55:00', N'El Cerrito, CA', 9001, 200, 1234, N'http://www.codinghorror.com/blog/', 1),
    (2, NULL, 28, '2008-07-31 14:22:31', N'Geoff Dalgas', 3, NULL, '2010-12-31 23:55:00', N'Corvallis, OR', 1234, 56, 789, N'http://stackoverflow.com', 2),
    (3, N'  ', 22, '2008-08-01 12:00:00', N'Jarrod Dixon', 0, NULL, '2010-06-15 09:30:00', NULL, 500, 12, 100, NULL, 3),
    (4, N'Test user with non-ASCII: 日本語 🚀', NULL, '2009-01-01 00:00:00', N'TestUser', 0, NULL, '2010-12-30 12:00:00', N'München, Deutschland', 10, 1, 5, NULL, 4);

-- Posts: questions + answers, includes NVARCHAR(MAX) bodies.
INSERT INTO dbo.Posts (Id, AcceptedAnswerId, AnswerCount, Body, ClosedDate, CommentCount, CommunityOwnedDate, CreationDate, FavoriteCount, LastActivityDate, LastEditDate, LastEditorDisplayName, LastEditorUserId, OwnerUserId, ParentId, PostTypeId, Score, Tags, Title, ViewCount) VALUES
    (1, 2, 1, N'Sample question body. Could be very long in real data — testing the wide-text path.', NULL, 0, NULL, '2008-08-01 12:11:31', 5, '2010-06-15 09:30:00', NULL, NULL, NULL, 1, NULL, 1, 100, N'<sql><database>', N'Sample SO2010 question title', 1500),
    (2, NULL, 0, N'Sample answer body.', NULL, 0, NULL, '2008-08-01 12:30:00', NULL, '2008-08-01 12:30:00', NULL, NULL, NULL, 2, 1, 2, 50, NULL, NULL, 500),
    (3, NULL, 0, N'A wiki page body with some content.', NULL, 1, '2009-01-01 00:00:00', '2009-01-01 00:00:00', NULL, '2010-06-15 09:30:00', '2010-06-15 09:30:00', NULL, 3, 3, NULL, 3, 0, N'<wiki>', N'Wiki entry', 10);

INSERT INTO dbo.Comments (Id, CreationDate, PostId, Score, Text, UserId) VALUES
    (1, '2008-08-01 12:20:00', 1, 1, N'Great question!', 2),
    (2, '2008-08-01 12:35:00', 2, NULL, N'Thanks for the answer.', 1),
    (3, '2009-01-02 00:00:00', 3, 0, N'Updated this wiki.', 3);

INSERT INTO dbo.Votes (Id, PostId, UserId, BountyAmount, VoteTypeId, CreationDate) VALUES
    (1, 1, 2, NULL, 2, '2008-08-01 13:00:00'),
    (2, 2, 1, NULL, 2, '2008-08-01 14:00:00'),
    (3, 1, NULL, 50, 8, '2010-01-01 00:00:00'),
    (4, 1, NULL, 50, 9, '2010-01-08 00:00:00'),
    (5, 3, NULL, NULL, 10, '2010-06-15 09:30:00');

INSERT INTO dbo.Badges (Id, Name, UserId, Date) VALUES
    (1, N'Autobiographer', 1, '2008-09-15 04:08:31'),
    (2, N'Editor', 1, '2008-09-15 04:08:31'),
    (3, N'Teacher', 2, '2008-09-15 04:08:31'),
    (4, N'Yearling', 3, '2010-08-01 00:00:00');

INSERT INTO dbo.PostLinks (Id, CreationDate, PostId, RelatedPostId, LinkTypeId) VALUES
    (1, '2010-06-01 00:00:00', 1, 3, 1),
    (2, '2010-07-01 00:00:00', 1, 2, 3);
GO

PRINT 'SO2010-minimal fixture loaded:';
SELECT 'Users'      AS tbl, COUNT(*) AS rows_loaded FROM dbo.Users
UNION ALL SELECT 'Posts',     COUNT(*) FROM dbo.Posts
UNION ALL SELECT 'Comments',  COUNT(*) FROM dbo.Comments
UNION ALL SELECT 'Votes',     COUNT(*) FROM dbo.Votes
UNION ALL SELECT 'Badges',    COUNT(*) FROM dbo.Badges
UNION ALL SELECT 'PostLinks', COUNT(*) FROM dbo.PostLinks
UNION ALL SELECT 'VoteTypes', COUNT(*) FROM dbo.VoteTypes
UNION ALL SELECT 'PostTypes', COUNT(*) FROM dbo.PostTypes
UNION ALL SELECT 'LinkTypes', COUNT(*) FROM dbo.LinkTypes;
GO
