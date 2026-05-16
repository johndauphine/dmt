-- SO2010-minimal SQLite fixture for dmt CI.
--
-- Mirrors scripts/fixtures/so2010-minimal.sql (the MSSQL source-of-truth
-- fixture for the existing mssql→postgres integration test) — same 9
-- tables, same column shapes, same seed rows — but translated to
-- SQLite syntax so the sqlite→sqlite integration test can run with no
-- service containers and no external client beyond the sqlite3 CLI.
--
-- Translation rules from the MSSQL fixture:
--   NVARCHAR(N)      → TEXT (sqlite has type affinity, length is advisory)
--   NVARCHAR(MAX)    → TEXT
--   DATETIME         → DATETIME (TEXT affinity; ISO-8601 stored verbatim)
--   CONSTRAINT name PRIMARY KEY → inline PRIMARY KEY (no named constraint)
--   N'...' string prefix → '...' (sqlite stores TEXT as UTF-8 natively)
--   IF OBJECT_ID ... DROP TABLE → DROP TABLE IF EXISTS
--   GO batch separator → omitted (sqlite3 accepts the whole file as one batch)
--
-- Load with:
--   sqlite3 source.db < scripts/fixtures/so2010-minimal-sqlite.sql
-- (or via dmt's own sqlite reader once the table is populated — for the
-- CI test we use the sqlite3 CLI directly so the loader doesn't depend
-- on the artifact we're testing).
--
-- Row counts (asserted in scripts/integration-test-sqlite.sh):
--   VoteTypes  15
--   PostTypes   8
--   LinkTypes   2
--   Users       5
--   Posts       3
--   Comments    3
--   Votes       5
--   Badges      4
--   PostLinks   2

PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS PostLinks;
DROP TABLE IF EXISTS Badges;
DROP TABLE IF EXISTS Votes;
DROP TABLE IF EXISTS Comments;
DROP TABLE IF EXISTS Posts;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS LinkTypes;
DROP TABLE IF EXISTS PostTypes;
DROP TABLE IF EXISTS VoteTypes;

-- ---------- DDL ----------
-- Order doesn't matter for SQLite (no FK enforcement unless pragma is on
-- and no FKs are declared here) but matches the MSSQL fixture's order
-- for diff readability.

CREATE TABLE VoteTypes (
    Id   INTEGER     NOT NULL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL
);

CREATE TABLE PostTypes (
    Id   INTEGER      NOT NULL PRIMARY KEY,
    Type VARCHAR(100) NOT NULL
);

CREATE TABLE LinkTypes (
    Id   INTEGER     NOT NULL PRIMARY KEY,
    Type VARCHAR(50) NOT NULL
);

CREATE TABLE Users (
    Id              INTEGER       NOT NULL PRIMARY KEY,
    AboutMe         TEXT          NULL,
    Age             INTEGER       NULL,
    CreationDate    DATETIME      NOT NULL,
    DisplayName     VARCHAR(40)   NOT NULL,
    DownVotes       INTEGER       NOT NULL,
    EmailHash       VARCHAR(40)   NULL,
    LastAccessDate  DATETIME      NOT NULL,
    Location        VARCHAR(100)  NULL,
    Reputation      INTEGER       NOT NULL,
    UpVotes         INTEGER       NOT NULL,
    Views           INTEGER       NOT NULL,
    WebsiteUrl      VARCHAR(200)  NULL,
    AccountId       INTEGER       NULL
);

CREATE TABLE Posts (
    Id                      INTEGER       NOT NULL PRIMARY KEY,
    AcceptedAnswerId        INTEGER       NULL,
    AnswerCount             INTEGER       NULL,
    Body                    TEXT          NOT NULL,
    ClosedDate              DATETIME      NULL,
    CommentCount            INTEGER       NULL,
    CommunityOwnedDate      DATETIME      NULL,
    CreationDate            DATETIME      NOT NULL,
    FavoriteCount           INTEGER       NULL,
    LastActivityDate        DATETIME      NOT NULL,
    LastEditDate            DATETIME      NULL,
    LastEditorDisplayName   VARCHAR(40)   NULL,
    LastEditorUserId        INTEGER       NULL,
    OwnerUserId             INTEGER       NULL,
    ParentId                INTEGER       NULL,
    PostTypeId              INTEGER       NOT NULL,
    Score                   INTEGER       NOT NULL,
    Tags                    VARCHAR(150)  NULL,
    Title                   VARCHAR(250)  NULL,
    ViewCount               INTEGER       NOT NULL
);

CREATE TABLE Comments (
    Id           INTEGER      NOT NULL PRIMARY KEY,
    CreationDate DATETIME     NOT NULL,
    PostId       INTEGER      NOT NULL,
    Score        INTEGER      NULL,
    Text         VARCHAR(700) NOT NULL,
    UserId       INTEGER      NULL
);

CREATE TABLE Votes (
    Id           INTEGER  NOT NULL PRIMARY KEY,
    PostId       INTEGER  NOT NULL,
    UserId       INTEGER  NULL,
    BountyAmount INTEGER  NULL,
    VoteTypeId   INTEGER  NOT NULL,
    CreationDate DATETIME NOT NULL
);

CREATE TABLE Badges (
    Id     INTEGER     NOT NULL PRIMARY KEY,
    Name   VARCHAR(40) NOT NULL,
    UserId INTEGER     NOT NULL,
    Date   DATETIME    NOT NULL
);

CREATE TABLE PostLinks (
    Id            INTEGER  NOT NULL PRIMARY KEY,
    CreationDate  DATETIME NOT NULL,
    PostId        INTEGER  NOT NULL,
    RelatedPostId INTEGER  NOT NULL,
    LinkTypeId    INTEGER  NOT NULL
);

-- ---------- Seed data ----------
-- Mirrors scripts/fixtures/so2010-minimal.sql byte-for-byte where it
-- matters (lookup-table values are the canonical StackOverflow2010
-- catalog; user/post/comment/vote/badge/link rows are the same synthetic
-- examples). "TagWikiExerpt" carries its canonical typo on purpose.

BEGIN TRANSACTION;

INSERT INTO VoteTypes (Id, Name) VALUES
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

INSERT INTO PostTypes (Id, Type) VALUES
    (1, 'Question'),
    (2, 'Answer'),
    (3, 'Wiki'),
    (4, 'TagWikiExerpt'),         -- sic, matches canonical typo
    (5, 'TagWiki'),
    (6, 'ModeratorNomination'),
    (7, 'WikiPlaceholder'),
    (8, 'PrivilegeWiki');

INSERT INTO LinkTypes (Id, Type) VALUES
    (1, 'Linked'),
    (3, 'Duplicate');

INSERT INTO Users (Id, AboutMe, Age, CreationDate, DisplayName, DownVotes, EmailHash, LastAccessDate, Location, Reputation, UpVotes, Views, WebsiteUrl, AccountId) VALUES
    (-1, 'Community wiki user',                NULL, '2008-07-31 00:00:00', 'Community',   0,  NULL, '2008-07-31 00:00:00', NULL,                    1, 0,   0,    NULL,                                NULL),
    (1,  'Site founder',                       34,   '2008-07-31 14:22:31', 'Jeff Atwood', 12, NULL, '2010-12-31 23:55:00', 'El Cerrito, CA',     9001, 200, 1234, 'http://www.codinghorror.com/blog/', 1),
    (2,  NULL,                                 28,   '2008-07-31 14:22:31', 'Geoff Dalgas', 3, NULL, '2010-12-31 23:55:00', 'Corvallis, OR',      1234, 56,  789,  'http://stackoverflow.com',           2),
    (3,  '  ',                                 22,   '2008-08-01 12:00:00', 'Jarrod Dixon', 0, NULL, '2010-06-15 09:30:00', NULL,                  500, 12,  100,  NULL,                                3),
    (4,  'Test user with non-ASCII: 日本語 🚀', NULL, '2009-01-01 00:00:00', 'TestUser',     0, NULL, '2010-12-30 12:00:00', 'München, Deutschland', 10, 1,   5,    NULL,                                4);

INSERT INTO Posts (Id, AcceptedAnswerId, AnswerCount, Body, ClosedDate, CommentCount, CommunityOwnedDate, CreationDate, FavoriteCount, LastActivityDate, LastEditDate, LastEditorDisplayName, LastEditorUserId, OwnerUserId, ParentId, PostTypeId, Score, Tags, Title, ViewCount) VALUES
    (1, 2,    1, 'Sample question body. Could be very long in real data — testing the wide-text path.', NULL, 0, NULL,                  '2008-08-01 12:11:31', 5,    '2010-06-15 09:30:00', NULL,                  NULL, NULL, 1,    NULL, 1, 100, '<sql><database>', 'Sample SO2010 question title', 1500),
    (2, NULL, 0, 'Sample answer body.',                                                            NULL, 0, NULL,                  '2008-08-01 12:30:00', NULL, '2008-08-01 12:30:00', NULL,                  NULL, NULL, 2,    1,    2, 50,  NULL,              NULL,                           500),
    (3, NULL, 0, 'A wiki page body with some content.',                                            NULL, 1, '2009-01-01 00:00:00', '2009-01-01 00:00:00', NULL, '2010-06-15 09:30:00', '2010-06-15 09:30:00', NULL, 3,    3,    NULL, 3, 0,   '<wiki>',          'Wiki entry',                   10);

INSERT INTO Comments (Id, CreationDate, PostId, Score, Text, UserId) VALUES
    (1, '2008-08-01 12:20:00', 1, 1,    'Great question!',       2),
    (2, '2008-08-01 12:35:00', 2, NULL, 'Thanks for the answer.', 1),
    (3, '2009-01-02 00:00:00', 3, 0,    'Updated this wiki.',     3);

INSERT INTO Votes (Id, PostId, UserId, BountyAmount, VoteTypeId, CreationDate) VALUES
    (1, 1, 2,    NULL, 2,  '2008-08-01 13:00:00'),
    (2, 2, 1,    NULL, 2,  '2008-08-01 14:00:00'),
    (3, 1, NULL, 50,   8,  '2010-01-01 00:00:00'),
    (4, 1, NULL, 50,   9,  '2010-01-08 00:00:00'),
    (5, 3, NULL, NULL, 10, '2010-06-15 09:30:00');

INSERT INTO Badges (Id, Name, UserId, Date) VALUES
    (1, 'Autobiographer', 1, '2008-09-15 04:08:31'),
    (2, 'Editor',         1, '2008-09-15 04:08:31'),
    (3, 'Teacher',        2, '2008-09-15 04:08:31'),
    (4, 'Yearling',       3, '2010-08-01 00:00:00');

INSERT INTO PostLinks (Id, CreationDate, PostId, RelatedPostId, LinkTypeId) VALUES
    (1, '2010-06-01 00:00:00', 1, 3, 1),
    (2, '2010-07-01 00:00:00', 1, 2, 3);

COMMIT;
