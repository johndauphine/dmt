-- SO2010-minimal MySQL fixture for dmt (#291).
--
-- MySQL-source counterpart to scripts/fixtures/so2010-minimal.sql (the
-- MSSQL source-of-truth fixture). Mirrors the same 9 tables, the same
-- column shapes, and the same seed rows — translated to MySQL dialect
-- so the mysql → {mssql,postgres,sqlite} integration tests can run with
-- a real MySQL source.
--
-- Translation rules from the MSSQL fixture:
--   NVARCHAR(N)    → VARCHAR(N)   (utf8mb4 charset → handles BMP + emoji)
--   NVARCHAR(MAX)  → LONGTEXT     (4 GiB upper bound, same effective shape
--                                  as MSSQL's MAX for fixture-sized data)
--   DATETIME       → DATETIME     (MySQL native, second-precision, no TZ)
--   CONSTRAINT name PRIMARY KEY → inline PRIMARY KEY (constraint names are
--                                  not part of the cross-DB type-mapping surface)
--   N'...' literal → '...'        (column-level utf8mb4 carries the encoding)
--   IF OBJECT_ID ... DROP TABLE → DROP TABLE IF EXISTS
--
-- Identifier case: lowercase. On default Linux MySQL builds
-- `lower_case_table_names = 0`, so table names are case-sensitive
-- on disk. Lowercase here keeps the fixture portable across
-- Linux CI runners, macOS dev boxes (default 2 — stored lowercase,
-- compared case-insensitively), and Windows.
--
-- Engine + charset: InnoDB / utf8mb4 / utf8mb4_unicode_ci. utf8mb4 is
-- required for the 4-byte UTF-8 sequences in the seed data (the 🚀 row
-- in users.aboutme would silently fail to insert on `utf8`).
--
-- Loads cleanly into:
--   - mysql-bench (make mysql-bench-up)
--   - any other MySQL 8.0+ instance
--
-- Load with:
--   docker exec -i mysql-bench mysql -uroot -p"$MYSQL_ROOT_PASSWORD" \
--     < scripts/fixtures/so2010-minimal-mysql.sql
-- or via the loader:
--   ./scripts/load-fixture-so2010-minimal.sh --source mysql
--
-- Row counts (asserted by the cross-engine integration tests):
--   votetypes 15  posttypes 8  linktypes 2
--   users      5  posts     3  comments  3
--   votes      5  badges    4  postlinks 2

CREATE DATABASE IF NOT EXISTS so2010_minimal_src
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE so2010_minimal_src;

-- DDL is auto-committed in MySQL (every CREATE/DROP TABLE issues an
-- implicit COMMIT), so wrapping the schema in a transaction is a no-op.
-- The data INSERTs below are wrapped in BEGIN/COMMIT so a fixture
-- reload either fully succeeds or leaves the previously-loaded state.

DROP TABLE IF EXISTS postlinks;
DROP TABLE IF EXISTS badges;
DROP TABLE IF EXISTS votes;
DROP TABLE IF EXISTS comments;
DROP TABLE IF EXISTS posts;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS linktypes;
DROP TABLE IF EXISTS posttypes;
DROP TABLE IF EXISTS votetypes;

-- ---------- DDL ----------

CREATE TABLE votetypes (
    id   INT         NOT NULL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE posttypes (
    id   INT          NOT NULL PRIMARY KEY,
    type VARCHAR(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE linktypes (
    id   INT         NOT NULL PRIMARY KEY,
    type VARCHAR(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE users (
    id              INT           NOT NULL PRIMARY KEY,
    aboutme         LONGTEXT      NULL,
    age             INT           NULL,
    creationdate    DATETIME      NOT NULL,
    displayname     VARCHAR(40)   NOT NULL,
    downvotes       INT           NOT NULL,
    emailhash       VARCHAR(40)   NULL,
    lastaccessdate  DATETIME      NOT NULL,
    location        VARCHAR(100)  NULL,
    reputation      INT           NOT NULL,
    upvotes         INT           NOT NULL,
    views           INT           NOT NULL,
    websiteurl      VARCHAR(200)  NULL,
    accountid       INT           NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE posts (
    id                      INT           NOT NULL PRIMARY KEY,
    acceptedanswerid        INT           NULL,
    answercount             INT           NULL,
    body                    LONGTEXT      NOT NULL,
    closeddate              DATETIME      NULL,
    commentcount            INT           NULL,
    communityowneddate      DATETIME      NULL,
    creationdate            DATETIME      NOT NULL,
    favoritecount           INT           NULL,
    lastactivitydate        DATETIME      NOT NULL,
    lasteditdate            DATETIME      NULL,
    lasteditordisplayname   VARCHAR(40)   NULL,
    lasteditoruserid        INT           NULL,
    owneruserid             INT           NULL,
    parentid                INT           NULL,
    posttypeid              INT           NOT NULL,
    score                   INT           NOT NULL,
    tags                    VARCHAR(150)  NULL,
    title                   VARCHAR(250)  NULL,
    viewcount               INT           NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE comments (
    id           INT          NOT NULL PRIMARY KEY,
    creationdate DATETIME     NOT NULL,
    postid       INT          NOT NULL,
    score        INT          NULL,
    text         VARCHAR(700) NOT NULL,
    userid       INT          NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE votes (
    id           INT      NOT NULL PRIMARY KEY,
    postid       INT      NOT NULL,
    userid       INT      NULL,
    bountyamount INT      NULL,
    votetypeid   INT      NOT NULL,
    creationdate DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE badges (
    id     INT         NOT NULL PRIMARY KEY,
    name   VARCHAR(40) NOT NULL,
    userid INT         NOT NULL,
    date   DATETIME    NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE postlinks (
    id            INT      NOT NULL PRIMARY KEY,
    creationdate  DATETIME NOT NULL,
    postid        INT      NOT NULL,
    relatedpostid INT      NOT NULL,
    linktypeid    INT      NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- Seed data ----------
-- Lookup tables match the canonical StackOverflow2010 catalog
-- byte-for-byte. "TagWikiExerpt" carries the canonical typo on purpose
-- — round-trip tests assert against the canonical spelling. ID 14 is
-- legitimately absent from votetypes in the public dataset.

START TRANSACTION;

INSERT INTO votetypes (id, name) VALUES
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

INSERT INTO posttypes (id, type) VALUES
    (1, 'Question'),
    (2, 'Answer'),
    (3, 'Wiki'),
    (4, 'TagWikiExerpt'),         -- sic, matches canonical typo
    (5, 'TagWiki'),
    (6, 'ModeratorNomination'),
    (7, 'WikiPlaceholder'),
    (8, 'PrivilegeWiki');

INSERT INTO linktypes (id, type) VALUES
    (1, 'Linked'),
    (3, 'Duplicate');

INSERT INTO users (id, aboutme, age, creationdate, displayname, downvotes, emailhash, lastaccessdate, location, reputation, upvotes, views, websiteurl, accountid) VALUES
    (-1, 'Community wiki user',                NULL, '2008-07-31 00:00:00', 'Community',    0,  NULL, '2008-07-31 00:00:00', NULL,                    1, 0,   0,    NULL,                                NULL),
    (1,  'Site founder',                       34,   '2008-07-31 14:22:31', 'Jeff Atwood',  12, NULL, '2010-12-31 23:55:00', 'El Cerrito, CA',     9001, 200, 1234, 'http://www.codinghorror.com/blog/', 1),
    (2,  NULL,                                 28,   '2008-07-31 14:22:31', 'Geoff Dalgas',  3, NULL, '2010-12-31 23:55:00', 'Corvallis, OR',      1234, 56,  789,  'http://stackoverflow.com',           2),
    (3,  '  ',                                 22,   '2008-08-01 12:00:00', 'Jarrod Dixon',  0, NULL, '2010-06-15 09:30:00', NULL,                  500, 12,  100,  NULL,                                3),
    (4,  'Test user with non-ASCII: 日本語 🚀', NULL, '2009-01-01 00:00:00', 'TestUser',      0, NULL, '2010-12-30 12:00:00', 'München, Deutschland', 10, 1,   5,    NULL,                                4);

INSERT INTO posts (id, acceptedanswerid, answercount, body, closeddate, commentcount, communityowneddate, creationdate, favoritecount, lastactivitydate, lasteditdate, lasteditordisplayname, lasteditoruserid, owneruserid, parentid, posttypeid, score, tags, title, viewcount) VALUES
    (1, 2,    1, 'Sample question body. Could be very long in real data — testing the LONGTEXT path.', NULL, 0, NULL,                  '2008-08-01 12:11:31', 5,    '2010-06-15 09:30:00', NULL,                  NULL, NULL, 1,    NULL, 1, 100, '<sql><database>', 'Sample SO2010 question title', 1500),
    (2, NULL, 0, 'Sample answer body.',                                                                NULL, 0, NULL,                  '2008-08-01 12:30:00', NULL, '2008-08-01 12:30:00', NULL,                  NULL, NULL, 2,    1,    2, 50,  NULL,              NULL,                           500),
    (3, NULL, 0, 'A wiki page body with some content.',                                                NULL, 1, '2009-01-01 00:00:00', '2009-01-01 00:00:00', NULL, '2010-06-15 09:30:00', '2010-06-15 09:30:00', NULL, 3,    3,    NULL, 3, 0,   '<wiki>',          'Wiki entry',                   10);

INSERT INTO comments (id, creationdate, postid, score, text, userid) VALUES
    (1, '2008-08-01 12:20:00', 1, 1,    'Great question!',        2),
    (2, '2008-08-01 12:35:00', 2, NULL, 'Thanks for the answer.', 1),
    (3, '2009-01-02 00:00:00', 3, 0,    'Updated this wiki.',     3);

INSERT INTO votes (id, postid, userid, bountyamount, votetypeid, creationdate) VALUES
    (1, 1, 2,    NULL, 2,  '2008-08-01 13:00:00'),
    (2, 2, 1,    NULL, 2,  '2008-08-01 14:00:00'),
    (3, 1, NULL, 50,   8,  '2010-01-01 00:00:00'),
    (4, 1, NULL, 50,   9,  '2010-01-08 00:00:00'),
    (5, 3, NULL, NULL, 10, '2010-06-15 09:30:00');

INSERT INTO badges (id, name, userid, date) VALUES
    (1, 'Autobiographer', 1, '2008-09-15 04:08:31'),
    (2, 'Editor',         1, '2008-09-15 04:08:31'),
    (3, 'Teacher',        2, '2008-09-15 04:08:31'),
    (4, 'Yearling',       3, '2010-08-01 00:00:00');

INSERT INTO postlinks (id, creationdate, postid, relatedpostid, linktypeid) VALUES
    (1, '2010-06-01 00:00:00', 1, 3, 1),
    (2, '2010-07-01 00:00:00', 1, 2, 3);

COMMIT;

-- Row counts so the loader log mirrors the MSSQL fixture's closing
-- PRINT block. SELECT ... UNION ALL is portable across mysql clients.
SELECT 'SO2010-minimal (mysql) fixture loaded:' AS info;
SELECT 'votetypes' AS tbl, COUNT(*) AS rows_loaded FROM votetypes
UNION ALL SELECT 'posttypes', COUNT(*) FROM posttypes
UNION ALL SELECT 'linktypes', COUNT(*) FROM linktypes
UNION ALL SELECT 'users',     COUNT(*) FROM users
UNION ALL SELECT 'posts',     COUNT(*) FROM posts
UNION ALL SELECT 'comments',  COUNT(*) FROM comments
UNION ALL SELECT 'votes',     COUNT(*) FROM votes
UNION ALL SELECT 'badges',    COUNT(*) FROM badges
UNION ALL SELECT 'postlinks', COUNT(*) FROM postlinks;
