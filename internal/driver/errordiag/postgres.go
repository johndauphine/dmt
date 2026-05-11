package errordiag

import (
	"fmt"
	"regexp"
)

// postgresPatterns is the deterministic catalog for PostgreSQL errors.
// Error message shapes follow lib/pq's `pq: ` prefix convention but the
// regexes don't require it (pgx and some wrappers strip the prefix).
//
// Pattern order matters — earlier patterns win. Place more specific
// patterns (e.g. a foreign-key violation) before generic ones (e.g.
// a generic constraint violation).
var postgresPatterns = []Pattern{
	{
		Name:  "pg_duplicate_key",
		Regex: regexp.MustCompile(`duplicate key value violates unique constraint "([^"]+)"`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("A row with a duplicate value for unique constraint %q already exists in the target.", m[1]),
				Suggestions: []string{
					"For drop_recreate mode: confirm the target table was empty before the migration (a prior partial run may have left rows).",
					"For upsert mode: verify the primary key columns match across source and target schemas.",
					"Check the source data for duplicates on the constrained column set.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "pg_fk_violation",
		Regex: regexp.MustCompile(`insert or update on table "([^"]+)" violates foreign key constraint "([^"]+)"`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Insert into %q references a row missing in the parent table (FK %q).", m[1], m[2]),
				Suggestions: []string{
					"Ensure the referenced parent table is migrated before the child (orchestrator handles this, but exclude_tables or partial runs can break the order).",
					"Verify the source data is referentially consistent — orphaned children are a common pre-migration cleanup task.",
					"For upsert mode: consider deferring FK creation until after data load (TaskCreateFKs runs last by default).",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "pg_fk_unimplementable",
		Regex: regexp.MustCompile(`foreign key constraint "([^"]+)" cannot be implemented`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Foreign key %q references a column whose type or collation differs between child and parent.", m[1]),
				Suggestions: []string{
					"Verify the referenced and referencing column types match exactly (including length and collation).",
					"Common cause: source uses VARCHAR(50) on one side and TEXT on the other; type mapping should align them.",
					"Inspect the generated CREATE TABLE statements for both tables; the column types and collations must be identical.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "pg_not_null_violation",
		Regex: regexp.MustCompile(`null value in column "([^"]+)" (?:of relation "([^"]+)" )?violates not-null constraint`),
		Diagnose: func(m []string) Diagnosis {
			col := m[1]
			return Diagnosis{
				Cause: fmt.Sprintf("Column %q was NOT NULL in the target schema but the source row contained NULL.", col),
				Suggestions: []string{
					fmt.Sprintf("Verify the source column for %q is also NOT NULL — a NULL drift indicates either a schema mismatch or a stale type mapping.", col),
					"For drop_recreate mode: regenerate target DDL after fixing the type mapping that produced the mismatched nullability.",
					"For upsert mode: provide a default value or alter the target column to allow NULL.",
				},
				Confidence: "high",
				Category:   "data_quality",
			}
		},
	},
	{
		Name:  "pg_value_too_long",
		Regex: regexp.MustCompile(`value too long for type ([a-z]+ ?[a-z]*)\(?(\d+)?\)?`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("A row's value exceeds the target column's declared %s length.", m[1]),
				Suggestions: []string{
					"The source's max length is larger than the target's; widen the target column or use TEXT for the affected column.",
					"This frequently indicates that the type mapping under-sized a VARCHAR — review the mapped types for the failing table.",
					"For upsert mode where DDL is fixed: pre-process the source to truncate or quarantine over-length rows.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "pg_invalid_input_syntax",
		Regex: regexp.MustCompile(`invalid input syntax for type ([a-z ]+):\s*"([^"]*)"`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Source value %q cannot be parsed as PostgreSQL type %s.", m[2], m[1]),
				Suggestions: []string{
					"Check that the source column's actual values are compatible with the mapped target type.",
					"Common offenders: empty strings into numeric/date columns, locale-specific decimal separators, mismatched date formats.",
					"Consider mapping the column to TEXT and converting post-migration, or fixing the source data.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		// MUST precede pg_relation_exists — the column-exists message
		// contains the relation-exists substring and would otherwise lose
		// to it (FindStringSubmatch picks the first registered match).
		Name:  "pg_column_exists",
		Regex: regexp.MustCompile(`column "([^"]+)" of relation "([^"]+)" already exists`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("ALTER TABLE attempted to add column %q to %q, but it already exists.", m[1], m[2]),
				Suggestions: []string{
					"This usually indicates an idempotency bug — the DDL generator should check for existing columns or be ordered after a clean DROP.",
					"For drop_recreate mode: confirm the table was actually dropped before recreation.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "pg_relation_exists",
		Regex: regexp.MustCompile(`relation "([^"]+)" already exists`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Target already contains a relation named %q.", m[1]),
				Suggestions: []string{
					"For drop_recreate mode: dmt is supposed to drop existing tables first; check that the user has DROP privilege.",
					"For upsert mode: this likely indicates a configuration mismatch — upsert assumes the target table exists with the right schema.",
					"If a prior run left orphan tables (e.g. dmt staging tables), they may need manual cleanup.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "pg_relation_missing",
		Regex: regexp.MustCompile(`relation "([^"]+)" does not exist`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Target schema does not contain relation %q.", m[1]),
				Suggestions: []string{
					"For drop_recreate mode: TaskCreateTables runs before TaskTransfer — if it failed silently, look earlier in the run logs.",
					"Confirm the configured target schema (Target.Schema in your YAML) matches where the table was created.",
					"For upsert mode: the table must pre-exist; verify it was created and is in the configured schema.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "pg_column_missing",
		Regex: regexp.MustCompile(`column "([^"]+)" does not exist`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Target table has no column named %q.", m[1]),
				Suggestions: []string{
					"Check whether identifier case folding differs between source and target — PostgreSQL lowercases unquoted identifiers.",
					"For upsert mode: the source has a column the target doesn't; either add the column to target or exclude it.",
					"Run `dmt analyze` to see the resolved schema mapping for this table.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "pg_permission_schema",
		Regex: regexp.MustCompile(`permission denied for schema "?([^"\s]+)"?`),
		Diagnose: func(m []string) Diagnosis {
			schema := m[1]
			return Diagnosis{
				Cause: fmt.Sprintf("The migration user lacks privileges on schema %q.", schema),
				Suggestions: []string{
					fmt.Sprintf("Grant the required privileges:  GRANT USAGE, CREATE ON SCHEMA %q TO <migration_user>;", schema),
					"See docs/PRIVILEGES.md for the full minimum-privilege list per driver and mode (#232).",
					"Run `dmt preflight` (#228) to surface privilege gaps before the migration starts.",
				},
				Confidence: "high",
				Category:   "permission",
			}
		},
	},
	{
		Name:  "pg_permission_table",
		Regex: regexp.MustCompile(`permission denied for (?:table|relation) "?([^"\s]+)"?`),
		Diagnose: func(m []string) Diagnosis {
			table := m[1]
			return Diagnosis{
				Cause: fmt.Sprintf("The migration user lacks privileges on table %q.", table),
				Suggestions: []string{
					fmt.Sprintf("Grant the appropriate privileges:  GRANT SELECT, INSERT, UPDATE ON TABLE %q TO <migration_user>;", table),
					"For drop_recreate mode you also need DROP and CREATE on the table's schema.",
					"See docs/PRIVILEGES.md for the full minimum-privilege list (#232).",
				},
				Confidence: "high",
				Category:   "permission",
			}
		},
	},
	{
		Name:  "pg_must_be_owner",
		Regex: regexp.MustCompile(`must be owner of (?:table|relation|sequence|index|database) ([^\s]+)`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Operation requires ownership of object %q.", m[1]),
				Suggestions: []string{
					"ALTER, DROP, and some metadata operations require the user to own the object.",
					fmt.Sprintf("Transfer ownership: ALTER TABLE %s OWNER TO <migration_user>;", m[1]),
					"Or grant the user membership in the owning role.",
				},
				Confidence: "high",
				Category:   "permission",
			}
		},
	},
	{
		Name:  "pg_password_auth",
		Regex: regexp.MustCompile(`password authentication failed for user "([^"]+)"`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("PostgreSQL rejected the password for user %q.", m[1]),
				Suggestions: []string{
					"Verify the password in your config — secret expansion (${env:VAR}, ${file:/path}) may not be resolving as expected.",
					"Confirm the user exists and the password matches: psql -h <host> -U <user> -d <db>.",
					"Check pg_hba.conf — the authentication method (md5, scram-sha-256, peer, etc.) must match what your client supports.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "pg_no_pghba_entry",
		Regex: regexp.MustCompile(`no pg_hba\.conf entry for host "?([^"\s,]+)"?`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("PostgreSQL has no pg_hba.conf rule that permits this client host (%s) to connect.", m[1]),
				Suggestions: []string{
					"Add an entry to pg_hba.conf permitting connections from the migration host's IP, then reload PostgreSQL.",
					"Verify SSL settings — pg_hba can require SSL on some rules and reject non-SSL connections.",
					"On RDS / Cloud SQL, pg_hba is managed via security groups / authorized networks, not the file.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "pg_database_missing",
		Regex: regexp.MustCompile(`database "([^"]+)" does not exist`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Configured database %q does not exist on the server.", m[1]),
				Suggestions: []string{
					"Create the database first: CREATE DATABASE \"" + m[1] + "\";",
					"Verify the Database field in your YAML matches the server's database name exactly (PostgreSQL is case-sensitive when the name is quoted).",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "pg_connection_refused",
		Regex: regexp.MustCompile(`dial tcp [^:]+:\d+: connect: connection refused`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "TCP connection refused — nothing is listening on the host:port, or a firewall is blocking the connection.",
				Suggestions: []string{
					"Verify the PostgreSQL server is running and listening on the expected port.",
					"Check that listen_addresses in postgresql.conf includes the relevant interface (default 'localhost' rejects remote connections).",
					"Confirm no firewall, security group, or VPN is dropping the connection.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "pg_connection_timeout",
		Regex: regexp.MustCompile(`dial tcp [^:]+:\d+: i/o timeout`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "TCP connection attempt timed out — the host is unreachable or silently dropping packets.",
				Suggestions: []string{
					"Check network reachability: `nc -zv <host> <port>` from the migration host.",
					"Confirm the target is not behind a NAT / proxy that requires special routing.",
					"Increase connection timeout in the driver if the network is slow but reachable.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "pg_out_of_memory",
		Regex: regexp.MustCompile(`out of memory`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "PostgreSQL backend ran out of memory while processing the request.",
				Suggestions: []string{
					"Reduce chunk_size in your config — bulk inserts allocate per-chunk memory in the backend.",
					"Reduce write_ahead_writers (fewer concurrent inserts means less peak memory).",
					"On the server: review work_mem and maintenance_work_mem; large index builds in TaskCreateIndexes can spike memory.",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
	{
		Name:  "pg_disk_full",
		Regex: regexp.MustCompile(`(?:could not extend file [^:]+: No space left on device|No space left on device)`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Target server ran out of disk space.",
				Suggestions: []string{
					"Free space or expand the target volume; PostgreSQL refuses writes when the tablespace is full.",
					"For drop_recreate mode you can resume after expanding (`dmt resume`).",
					"Sizing rule: target free space should be >= 1.5x the source data size (pre-flight check #228 enforces this).",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "pg_invalid_encoding",
		Regex: regexp.MustCompile(`invalid byte sequence for encoding "([^"]+)"`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Source bytes are not valid under target encoding %s.", m[1]),
				Suggestions: []string{
					"Source data contains bytes the target encoding cannot represent — common when migrating Latin-1 / Windows-1252 data into a UTF-8 PostgreSQL database.",
					"Convert the column to UTF-8 in the source query, or use a target encoding that accepts the source's bytes.",
					"For binary content masquerading as text, consider mapping the column to BYTEA instead.",
				},
				Confidence: "high",
				Category:   "data_quality",
			}
		},
	},
	{
		Name:  "pg_tx_aborted",
		Regex: regexp.MustCompile(`current transaction is aborted, commands ignored until end of transaction block`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "An earlier statement in the same transaction failed; PostgreSQL is refusing all subsequent statements until ROLLBACK.",
				Suggestions: []string{
					"This is a symptom — look earlier in the log for the underlying error that aborted the transaction.",
					"The original error and its diagnosis is what to act on; this message is downstream noise.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "pg_deadlock",
		Regex: regexp.MustCompile(`deadlock detected`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Two concurrent operations deadlocked; PostgreSQL aborted one to break the cycle.",
				Suggestions: []string{
					"Reduce write_ahead_writers — concurrent writers contending for the same indexes is the typical cause.",
					"Ensure no other workload is writing to the target tables during the migration.",
					"For drop_recreate mode this is rare; for upsert mode it's more common (UPDATE acquires row locks that can deadlock).",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
	{
		Name:  "pg_statement_timeout",
		Regex: regexp.MustCompile(`canceling statement due to statement timeout`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "A statement ran longer than the configured statement_timeout.",
				Suggestions: []string{
					"Increase statement_timeout on the migration role (ALTER ROLE <user> SET statement_timeout = 0).",
					"Check whether a large COPY or CREATE INDEX is the culprit — those scale with table size.",
					"For very large index builds, consider deferring secondary indexes until after data load is complete.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "pg_admin_termination",
		Regex: regexp.MustCompile(`terminating connection due to administrator command`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "PostgreSQL terminated the connection — typically pg_terminate_backend, server restart, or failover.",
				Suggestions: []string{
					"Check for concurrent admin activity on the target (restarts, failovers, manual kills).",
					"On managed services (RDS, Cloud SQL), this can happen during maintenance windows.",
					"Use `dmt resume` to continue from the checkpoint once the target is back.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "pg_ssl_required",
		Regex: regexp.MustCompile(`(?:SSL (?:connection )?(?:is )?required|server does not support SSL|pq: SSL is not enabled)`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "SSL/TLS negotiation failed — either required by server and missing on client, or vice versa.",
				Suggestions: []string{
					"Add sslmode=require (or sslmode=disable, depending on which side is misconfigured) to the connection string.",
					"On managed services, SSL is usually required; sslmode=disable will be rejected.",
					"For self-signed certs, sslmode=require accepts any cert; sslmode=verify-ca or verify-full validate it.",
				},
				Confidence: "medium",
				Category:   "connection",
			}
		},
	},
}

func init() {
	Register("postgres", postgresPatterns)
	Register("postgresql", postgresPatterns)
	Register("pg", postgresPatterns)
}
