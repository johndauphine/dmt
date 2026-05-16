-- SO2010-minimal PostgreSQL fixture for dmt (#291).
--
-- PG-source counterpart to scripts/fixtures/so2010-minimal.sql (the
-- MSSQL source-of-truth fixture). Mirrors the same 9 tables, the same
-- column shapes, and the same seed rows — translated to PG dialect so
-- the postgres → {mssql,mysql,sqlite} integration tests can run with a
-- real PG source.
--
-- Translation rules from the MSSQL fixture:
--   NVARCHAR(N)    → VARCHAR(N)   (PG VARCHAR is UTF-8 natively, no N'' prefix)
--   NVARCHAR(MAX)  → TEXT
--   DATETIME       → TIMESTAMP    (naive, no TZ — matches the MSSQL fixture's
--                                  unzoned datetimes so round-trip values stay
--                                  byte-identical across engines)
--   CONSTRAINT name PRIMARY KEY → inline PRIMARY KEY (constraint names are
--                                  not part of the cross-DB type-mapping surface)
--   N'...' literal → '...'        (UTF-8 throughout)
--   IF OBJECT_ID ... DROP TABLE → DROP TABLE IF EXISTS
--
-- Identifier case: lowercase, unquoted. dmt's PG writer sanitizer
-- already produces lowercase identifiers when migrating mssql→pg
-- (see internal/driver/postgres), so a pg-source fixture that's
-- already lowercase keeps the round-trip identifier-shape stable and
-- matches what integration-test.sh queries on the pg side today
-- (`SELECT COUNT(*) FROM "votetypes"`, etc.).
--
-- Loads cleanly into:
--   - pg-test  (make test-dbs-up)
--   - pg-bench (make bench-dbs-up)
--   - any other PostgreSQL 12+ instance
--
-- Load with:
--   psql -h localhost -p 5432 -U postgres -d so2010_minimal_src \
--        -f scripts/fixtures/so2010-minimal-pg.sql
-- or via the loader:
--   ./scripts/load-fixture-so2010-minimal.sh --source pg
--
-- Row counts (asserted by the cross-engine integration tests):
--   votetypes 15  posttypes 8  linktypes 2
--   users      5  posts     3  comments  3
--   votes      5  badges    4  postlinks 2

-- Wrap the entire load in a transaction. PG supports DDL in
-- transactions, so a failure mid-script leaves the database in its
-- pre-load state — useful for re-running the fixture in dirty envs.
BEGIN;

-- DROP order is reverse of the dependency order. With no FKs declared
-- here it doesn't strictly matter, but keep it parallel to the MSSQL
-- fixture so a side-by-side diff stays readable.
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
    id   INTEGER     NOT NULL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE posttypes (
    id   INTEGER      NOT NULL PRIMARY KEY,
    type VARCHAR(100) NOT NULL
);

CREATE TABLE linktypes (
    id   INTEGER     NOT NULL PRIMARY KEY,
    type VARCHAR(50) NOT NULL
);

CREATE TABLE users (
    id              INTEGER       NOT NULL PRIMARY KEY,
    aboutme         TEXT          NULL,
    age             INTEGER       NULL,
    creationdate    TIMESTAMP     NOT NULL,
    displayname     VARCHAR(40)   NOT NULL,
    downvotes       INTEGER       NOT NULL,
    emailhash       VARCHAR(40)   NULL,
    lastaccessdate  TIMESTAMP     NOT NULL,
    location        VARCHAR(100)  NULL,
    reputation      INTEGER       NOT NULL,
    upvotes         INTEGER       NOT NULL,
    views           INTEGER       NOT NULL,
    websiteurl      VARCHAR(200)  NULL,
    accountid       INTEGER       NULL
);

CREATE TABLE posts (
    id                      INTEGER       NOT NULL PRIMARY KEY,
    acceptedanswerid        INTEGER       NULL,
    answercount             INTEGER       NULL,
    body                    TEXT          NOT NULL,
    closeddate              TIMESTAMP     NULL,
    commentcount            INTEGER       NULL,
    communityowneddate      TIMESTAMP     NULL,
    creationdate            TIMESTAMP     NOT NULL,
    favoritecount           INTEGER       NULL,
    lastactivitydate        TIMESTAMP     NOT NULL,
    lasteditdate            TIMESTAMP     NULL,
    lasteditordisplayname   VARCHAR(40)   NULL,
    lasteditoruserid        INTEGER       NULL,
    owneruserid             INTEGER       NULL,
    parentid                INTEGER       NULL,
    posttypeid              INTEGER       NOT NULL,
    score                   INTEGER       NOT NULL,
    tags                    VARCHAR(150)  NULL,
    title                   VARCHAR(250)  NULL,
    viewcount               INTEGER       NOT NULL
);

CREATE TABLE comments (
    id           INTEGER      NOT NULL PRIMARY KEY,
    creationdate TIMESTAMP    NOT NULL,
    postid       INTEGER      NOT NULL,
    score        INTEGER      NULL,
    text         VARCHAR(700) NOT NULL,
    userid       INTEGER      NULL
);

CREATE TABLE votes (
    id           INTEGER   NOT NULL PRIMARY KEY,
    postid       INTEGER   NOT NULL,
    userid       INTEGER   NULL,
    bountyamount INTEGER   NULL,
    votetypeid   INTEGER   NOT NULL,
    creationdate TIMESTAMP NOT NULL
);

CREATE TABLE badges (
    id     INTEGER     NOT NULL PRIMARY KEY,
    name   VARCHAR(40) NOT NULL,
    userid INTEGER     NOT NULL,
    date   TIMESTAMP   NOT NULL
);

CREATE TABLE postlinks (
    id            INTEGER   NOT NULL PRIMARY KEY,
    creationdate  TIMESTAMP NOT NULL,
    postid        INTEGER   NOT NULL,
    relatedpostid INTEGER   NOT NULL,
    linktypeid    INTEGER   NOT NULL
);

-- ---------- Seed data ----------
-- Lookup tables (votetypes/posttypes/linktypes) match the canonical
-- StackOverflow2010 catalog byte-for-byte. "TagWikiExerpt" carries its
-- canonical typo on purpose — round-trip tests assert against the
-- canonical spelling, not a corrected one. ID 14 is legitimately absent
-- from votetypes in the public dataset.

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
    (1, 2,    1, 'Sample question body. Could be very long in real data — testing the TEXT path.', NULL, 0, NULL,                  '2008-08-01 12:11:31', 5,    '2010-06-15 09:30:00', NULL,                  NULL, NULL, 1,    NULL, 1, 100, '<sql><database>', 'Sample SO2010 question title', 1500),
    (2, NULL, 0, 'Sample answer body.',                                                            NULL, 0, NULL,                  '2008-08-01 12:30:00', NULL, '2008-08-01 12:30:00', NULL,                  NULL, NULL, 2,    1,    2, 50,  NULL,              NULL,                           500),
    (3, NULL, 0, 'A wiki page body with some content.',                                            NULL, 1, '2009-01-01 00:00:00', '2009-01-01 00:00:00', NULL, '2010-06-15 09:30:00', '2010-06-15 09:30:00', NULL, 3,    3,    NULL, 3, 0,   '<wiki>',          'Wiki entry',                   10);

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

-- Print row counts so the loader log mirrors the MSSQL fixture's
-- closing PRINT block. \echo is psql-specific; the SELECT is portable.
\echo 'SO2010-minimal (pg) fixture loaded:'
SELECT 'votetypes' AS tbl, COUNT(*) AS rows_loaded FROM votetypes
UNION ALL SELECT 'posttypes', COUNT(*) FROM posttypes
UNION ALL SELECT 'linktypes', COUNT(*) FROM linktypes
UNION ALL SELECT 'users',     COUNT(*) FROM users
UNION ALL SELECT 'posts',     COUNT(*) FROM posts
UNION ALL SELECT 'comments',  COUNT(*) FROM comments
UNION ALL SELECT 'votes',     COUNT(*) FROM votes
UNION ALL SELECT 'badges',    COUNT(*) FROM badges
UNION ALL SELECT 'postlinks', COUNT(*) FROM postlinks;
