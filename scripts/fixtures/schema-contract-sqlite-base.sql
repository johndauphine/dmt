-- Minimal SQLite fixture for schema_contract integration coverage.
--
-- Kept intentionally small so the contract matrix can run several baseline
-- and drift follow-up migrations inside the regular SQLite CI job.

PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id     INTEGER     NOT NULL PRIMARY KEY,
    name   VARCHAR(20) NOT NULL,
    email  VARCHAR(80) NULL,
    age    INTEGER     NULL,
    status VARCHAR(20) NULL,
    notes  TEXT        NULL
);

CREATE TABLE orders (
    id      INTEGER     NOT NULL PRIMARY KEY,
    user_id INTEGER     NOT NULL,
    amount  INTEGER     NOT NULL,
    note    VARCHAR(50) NULL
);

INSERT INTO users (id, name, email, age, status, notes) VALUES
    (1, 'Ada',   'ada@example.test',   34, 'active', 'baseline user'),
    (2, 'Grace', 'grace@example.test', 37, 'active', NULL);

INSERT INTO orders (id, user_id, amount, note) VALUES
    (10, 1, 100, 'first order'),
    (11, 2, 200, 'second order');
