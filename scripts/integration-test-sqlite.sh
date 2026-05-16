#!/usr/bin/env bash
# SQLite → SQLite integration test for dmt.
#
# Companion to scripts/integration-test.sh (mssql → postgres). This one
# is intentionally service-container-free — both endpoints are file-based
# SQLite databases, the loader uses the host sqlite3 CLI, and the
# assertions read back the same way. Should finish in seconds.
#
# Why a sqlite-to-sqlite test is worth running on every PR:
#   - Catches regressions in the SQLite driver itself (a fourth driver
#     dmt now ships) without needing the full mssql/pg matrix.
#   - Exercises the partition-cleanup dispatch (transfer.go), keyset
#     pagination, the orchestrator's drop_recreate flow, and the
#     checkpoint store end-to-end.
#   - Cheap: no docker, no service waits — just two temp .db files.
#
# Invoked by:
#   - make integration-test-sqlite (locally + in CI)
#   - .github/workflows/integration.yml (sqlite-to-sqlite job)
#
# Exits non-zero if any step fails — the CI workflow surfaces dmt logs
# as artifacts on failure.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="$REPO_ROOT/scripts/fixtures/ci-sqlite-sqlite.yaml"
FIXTURE="$REPO_ROOT/scripts/fixtures/so2010-minimal-sqlite.sql"
LOG="${INTEGRATION_TEST_SQLITE_LOG:-/tmp/dmt-ci-sqlite.log}"

# Isolate dmt state so a local run doesn't clobber the operator's
# ~/.dmt/migrate.db. Same convention as scripts/integration-test.sh.
DMT_STATE_DIR="${DMT_STATE_DIR:-$REPO_ROOT/.dmt-ci-sqlite-state}"
export DMT_STATE_DIR

# Source / target .db files. Place them in a dedicated subdir so a stale
# run doesn't leave files lying around the repo. The directory is wiped
# at the start of every run to keep the test hermetic.
WORK_DIR="${SQLITE_WORK_DIR:-$REPO_ROOT/.dmt-ci-sqlite-work}"
export SQLITE_SOURCE_DB="$WORK_DIR/source.db"
export SQLITE_TARGET_DB="$WORK_DIR/target.db"

if [[ ! -x "$REPO_ROOT/dmt" ]]; then
    echo "ERROR: dmt binary not found at $REPO_ROOT/dmt — run 'make build' first" >&2
    exit 1
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "ERROR: sqlite3 CLI not found on PATH." >&2
    echo "       Install with: apt-get install sqlite3   (Ubuntu)" >&2
    echo "                 or: brew install sqlite       (macOS)" >&2
    exit 1
fi

echo "=== dmt sqlite → sqlite (CI integration test) ==="
echo "  config:    $CONFIG"
echo "  fixture:   $FIXTURE"
echo "  source DB: $SQLITE_SOURCE_DB"
echo "  target DB: $SQLITE_TARGET_DB"
echo "  state dir: $DMT_STATE_DIR"
echo "  log:       $LOG"
echo ""

# Hermetic reset: wipe both the work dir (source/target .db) and the
# dmt state dir (checkpoint DB, profiles). A stale checkpoint from a
# previous run would otherwise engage the resume path mid-test.
rm -rf "$WORK_DIR" "$DMT_STATE_DIR"
mkdir -p "$WORK_DIR" "$DMT_STATE_DIR"

# Truncate the log explicitly so a writability problem surfaces here
# (with a clear shell error) rather than as a confusing pipe failure
# inside the `tee` below. Matches the hermetic-reset intent above:
# every artifact this script touches starts fresh.
: > "$LOG"

# Load the SO2010-minimal fixture into the source database. sqlite3 reads
# the .sql file on stdin; CREATE TABLE statements create the schema,
# INSERT statements populate the seed rows. The fixture's `DROP TABLE
# IF EXISTS` headers make repeated loads safe (though we just nuked the
# file, so this is belt-and-suspenders).
echo "=== Load fixture into source.db ==="
sqlite3 "$SQLITE_SOURCE_DB" < "$FIXTURE"

# Quick sanity check: source row counts before running dmt. If these
# don't match expected, the fixture loader broke and we'd waste time
# diagnosing dmt for a fixture bug.
src_users="$(sqlite3 "$SQLITE_SOURCE_DB" "SELECT COUNT(*) FROM Users")"
if [[ "$src_users" != "5" ]]; then
    echo "ERROR: source Users count = $src_users, expected 5 — fixture didn't load cleanly" >&2
    exit 1
fi
echo "  source loaded: Users=$src_users (sanity check passed)"
echo ""

# Run the migration. dmt expands ${SQLITE_SOURCE_DB} / ${SQLITE_TARGET_DB}
# from the env. --confirm-backup acknowledges drop_recreate would destroy
# data on the target — but the target file is fresh (we just nuked the
# directory), so this is just satisfying the safety guard.
echo "=== Run dmt migration ==="
"$REPO_ROOT/dmt" -c "$CONFIG" run --confirm-backup 2>&1 | tee "$LOG"

# Verify row count parity for every table the fixture defines. Parallel
# arrays instead of associative — macOS ships bash 3.2 (no `declare -A`)
# and we want identical behavior locally and in GHA.
#
# Note: SQLite preserves identifier case in quoted form. The dmt sqlite
# target writer emits `CREATE TABLE "Users"` so case-preserved table
# names match the fixture exactly.
TABLES=(VoteTypes PostTypes LinkTypes Users Posts Comments Votes Badges PostLinks)
EXPECTED=(15      8         2         5     3     3        5     4      2)

echo ""
echo "=== Target row count verification ==="
fail=0
for i in $(seq 0 $((${#TABLES[@]} - 1))); do
    table="${TABLES[$i]}"
    want="${EXPECTED[$i]}"
    got="$(sqlite3 "$SQLITE_TARGET_DB" "SELECT COUNT(*) FROM \"$table\";")"
    got="$(echo "$got" | tr -d '[:space:]')"
    if [[ "$got" != "$want" ]]; then
        echo "  FAIL: $table got=$got want=$want"
        fail=1
    else
        echo "  OK:   $table = $got"
    fi
done

if [[ "$fail" -ne 0 ]]; then
    echo ""
    echo "Integration test FAILED — see $LOG for dmt output."
    exit 1
fi

# Spot-check a handful of representative values to catch silent data
# corruption that row-count parity wouldn't surface. Picks values that
# would fail if encoding, NULL handling, or numeric parsing went wrong.
echo ""
echo "=== Value spot-check ==="
check_value() {
    local label="$1" query="$2" expected="$3"
    local got
    got="$(sqlite3 "$SQLITE_TARGET_DB" "$query" | tr -d '[:space:]')"
    if [[ "$got" != "$expected" ]]; then
        echo "  FAIL: $label got=$got want=$expected"
        fail=1
    else
        echo "  OK:   $label"
    fi
}

# Non-ASCII string preserved end-to-end (the UTF-8 path through the
# pipeline). User Id=4's AboutMe column carries Japanese characters
# (日本語) and an emoji (🚀); the LIKE form yields a clean YES/NO so the
# whitespace-stripping inside check_value doesn't mangle the assertion
# value, and BOTH substrings have to survive for the check to pass.
# Asserting on plain ASCII (e.g. DisplayName='TestUser') would not
# catch a UTF-8 encoding regression — Copilot review caught this on the
# initial commit, fix moved the assertion onto the actually-non-ASCII
# column.
check_value "Users.AboutMe for Id=4 preserves Japanese + emoji" \
    "SELECT CASE WHEN AboutMe LIKE '%日本語%' AND AboutMe LIKE '%🚀%' THEN 'YES' ELSE 'NO' END FROM Users WHERE Id = 4" \
    "YES"

# Non-ASCII Location preserved as well (the umlaut path — Latin-1
# supplement chars, distinct from CJK and emoji code points).
check_value "Users.Location for Id=4 preserves 'München'" \
    "SELECT CASE WHEN Location LIKE '%München%' THEN 'YES' ELSE 'NO' END FROM Users WHERE Id = 4" \
    "YES"

# NULL preserved (Users.AboutMe for Geoff Dalgas is NULL in source).
check_value "Users.AboutMe IS NULL for Id=2" \
    "SELECT CASE WHEN AboutMe IS NULL THEN 'NULL' ELSE 'NOT_NULL' END FROM Users WHERE Id = 2" \
    "NULL"

# Negative integer PK preserved (Community user has Id = -1).
check_value "Users.Id = -1 row exists" \
    "SELECT COUNT(*) FROM Users WHERE Id = -1" \
    "1"

# Lookup table contents preserved (PostTypes id 4 carries the canonical
# typo 'TagWikiExerpt' — if a string column got mangled this would
# surface).
check_value "PostTypes.Type for Id=4 = 'TagWikiExerpt'" \
    "SELECT Type FROM PostTypes WHERE Id = 4" \
    "TagWikiExerpt"

# Sparse PKs: VoteTypes legitimately skips Id 14 between 13 and 15 in
# the canonical StackOverflow dataset. A target with Id 14 present
# would indicate dmt fabricated rows. (Comment in the initial commit
# said "PostTypes" — wrong table; Copilot review caught the typo.)
check_value "VoteTypes has no Id=14 (canonical gap)" \
    "SELECT COUNT(*) FROM VoteTypes WHERE Id = 14" \
    "0"

if [[ "$fail" -ne 0 ]]; then
    echo ""
    echo "Integration test FAILED on value spot-check — see $LOG."
    exit 1
fi

echo ""
echo "Integration test PASSED (sqlite → sqlite)."
