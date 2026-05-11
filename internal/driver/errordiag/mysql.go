package errordiag

import (
	"fmt"
	"regexp"
)

// mysqlPatterns is the deterministic catalog for MySQL / MariaDB errors.
// Error message shapes follow github.com/go-sql-driver/mysql; the regexes
// don't depend on the exact "Error NNNN (SSSSS):" prefix because that
// varies across driver versions and wrappers.
//
// Pattern order matters — earlier patterns win. Place more specific
// patterns before generic ones.
var mysqlPatterns = []Pattern{
	{
		Name:  "mysql_duplicate_entry",
		Regex: regexp.MustCompile(`Duplicate entry '([^']*)' for key '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Insert rejected — value %q already exists for key %q.", m[1], m[2]),
				Suggestions: []string{
					"For drop_recreate mode: confirm the target table was empty before the migration started.",
					"For upsert mode: verify the unique-key column set matches between source and target.",
					"Check the source data for duplicates on the constrained columns.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "mysql_null_violation",
		Regex: regexp.MustCompile(`Column '([^']+)' cannot be null`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Column %q is NOT NULL but a source row contained NULL.", m[1]),
				Suggestions: []string{
					"Verify the source column also disallows NULL — a mismatch points to a type-mapping bug or stale schema.",
					"For drop_recreate mode: regenerate target DDL after fixing the nullability mismatch.",
					"For upsert mode: alter the column to permit NULL or provide a default.",
				},
				Confidence: "high",
				Category:   "data_quality",
			}
		},
	},
	{
		Name:  "mysql_data_too_long",
		Regex: regexp.MustCompile(`Data too long for column '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Column %q is shorter than the source value being inserted.", m[1]),
				Suggestions: []string{
					"Widen the column or use TEXT/LONGTEXT for the affected column.",
					"This usually indicates a type-mapping bug that under-sized the column.",
					"Strict mode (STRICT_ALL_TABLES) makes this an error; non-strict mode silently truncates — confirm strict mode is what you want.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mysql_incorrect_string",
		Regex: regexp.MustCompile(`Incorrect string value: '([^']+)' for column '?([^']+)'?`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Bytes in source don't match column %q's character set.", m[2]),
				Suggestions: []string{
					"Most common cause: column is utf8 (3-byte) but source has 4-byte UTF-8 chars (emoji). Use utf8mb4.",
					"ALTER TABLE <tbl> CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
					"For drop_recreate mode, set the target connection's charset to utf8mb4 so created tables use it.",
				},
				Confidence: "high",
				Category:   "data_quality",
			}
		},
	},
	{
		Name:  "mysql_incorrect_value",
		Regex: regexp.MustCompile(`Incorrect ([a-z]+) value: '([^']*)' for column '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Value %q cannot be stored as %s in column %q.", m[2], m[1], m[3]),
				Suggestions: []string{
					"Common offenders: empty strings into numeric/date columns, '0000-00-00' dates under NO_ZERO_DATE, locale-specific decimals.",
					"Check the source data and the column's mapped type.",
					"Consider mapping to a wider type or pre-cleaning the source values.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mysql_out_of_range",
		Regex: regexp.MustCompile(`Out of range value for column '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Source value exceeds the range of column %q's type.", m[1]),
				Suggestions: []string{
					"Widen the target column (BIGINT, DECIMAL(p,s) with larger precision, etc.).",
					"Common when the type mapping picked INT for a column that source had as BIGINT.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mysql_table_exists",
		Regex: regexp.MustCompile(`Table '([^']+)' already exists`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("CREATE TABLE failed — %q already exists.", m[1]),
				Suggestions: []string{
					"For drop_recreate mode: dmt should drop tables first; verify DROP privileges.",
					"For upsert mode: this is expected, but the schema may not match what dmt expects.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mysql_table_missing",
		Regex: regexp.MustCompile(`Table '([^']+)' doesn't exist`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Table %q does not exist in the target database.", m[1]),
				Suggestions: []string{
					"For drop_recreate mode: TaskCreateTables runs before TaskTransfer — check earlier in the log if it failed.",
					"For upsert mode: the table must pre-exist. Confirm the database name in your YAML matches the server.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mysql_unknown_column",
		Regex: regexp.MustCompile(`Unknown column '([^']+)' in '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("No column named %q in %s.", m[1], m[2]),
				Suggestions: []string{
					"Check identifier case sensitivity — MySQL's behavior varies by platform (lower_case_table_names).",
					"For upsert mode: the source has a column the target doesn't; add it or exclude it.",
					"Run `dmt analyze` to see the resolved schema mapping.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mysql_access_denied",
		Regex: regexp.MustCompile(`Access denied for user '([^']+)'@'([^']+)' \(using password: (YES|NO)\)`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("MySQL rejected the connection for %q from %q (password supplied: %s).", m[1], m[2], m[3]),
				Suggestions: []string{
					"Verify the user/password — secret expansion (${env:VAR}, ${file:/path}) may not be resolving.",
					"Confirm the user exists for this client host: SELECT host FROM mysql.user WHERE user = '" + m[1] + "';",
					"For RDS / managed MySQL, the user often must be bound to '%' rather than 'localhost'.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "mysql_privilege_required",
		Regex: regexp.MustCompile(`Access denied; you need \(at least one of\) the (.+) privilege`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Operation requires %s, which the migration user lacks.", m[1]),
				Suggestions: []string{
					fmt.Sprintf("Grant the privilege: GRANT %s ON <db>.* TO <user>; FLUSH PRIVILEGES;", m[1]),
					"For drop_recreate mode the typical minimum is ALL PRIVILEGES on the target database; SUPER is rarely required.",
					"For source roles, SELECT on the source database plus PROCESS (for visibility into system metadata) is usually enough.",
				},
				Confidence: "high",
				Category:   "permission",
			}
		},
	},
	{
		Name:  "mysql_connection_refused",
		Regex: regexp.MustCompile(`dial tcp [^:]+:\d+: connect: connection refused`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "TCP connection refused — nothing listening on the host:port, or a firewall is blocking it.",
				Suggestions: []string{
					"Verify mysqld is running and listening: `ss -tlnp | grep 3306` on the server.",
					"Check bind-address in my.cnf — 127.0.0.1 only accepts local connections.",
					"Confirm no firewall / security group is dropping the connection.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "mysql_unknown_database",
		Regex: regexp.MustCompile(`Unknown database '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Configured database %q does not exist.", m[1]),
				Suggestions: []string{
					"Create the database first: CREATE DATABASE `" + m[1] + "`;",
					"Verify the Database field in your YAML matches the server's database name.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "mysql_max_allowed_packet",
		Regex: regexp.MustCompile(`Got a packet bigger than 'max_allowed_packet' bytes`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Bulk-insert packet exceeded the server's max_allowed_packet.",
				Suggestions: []string{
					"Reduce chunk_size in your config — large chunks of wide rows quickly exceed max_allowed_packet (often 16MB or 64MB by default).",
					"Or raise the server limit: SET GLOBAL max_allowed_packet = 1073741824; (1GB) — needs reconnect.",
					"#166 will make dmt automatically probe and respect max_allowed_packet at runtime.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mysql_lock_wait_timeout",
		Regex: regexp.MustCompile(`Lock wait timeout exceeded; try restarting transaction`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "InnoDB row-lock wait exceeded innodb_lock_wait_timeout (default 50s).",
				Suggestions: []string{
					"Reduce write_ahead_writers — concurrent writers contending for the same rows are the typical cause.",
					"Identify what's holding locks: SELECT * FROM information_schema.innodb_trx;",
					"Avoid running the migration during peak hours on the target.",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mysql_deadlock",
		Regex: regexp.MustCompile(`Deadlock found when trying to get lock`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "InnoDB detected a deadlock and rolled back this transaction.",
				Suggestions: []string{
					"Reduce write_ahead_writers.",
					"Ensure no other workload is writing to the same target tables.",
					"dmt retries deadlock victims; persistent deadlocks suggest contention on a hot key — check the source's index design.",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mysql_fk_cant_add",
		Regex: regexp.MustCompile(`Cannot add foreign key constraint`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Foreign-key creation failed — referenced and referencing column types/charsets don't match, or referenced index is missing.",
				Suggestions: []string{
					"Verify both columns have identical type, length, charset, and collation.",
					"The referenced column needs to be the first column of an index on the parent table.",
					"Inspect the DDL with SHOW CREATE TABLE on both sides.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		// MySQL FK violation messages typically include a
		// `REFERENCES \`<parent>\`` clause; capture the referenced table
		// name when present. `.+` matches across the embedded `)` of
		// `FOREIGN KEY (\`col\`)` because `.` matches any non-newline byte.
		Name:  "mysql_fk_parent_violation",
		Regex: regexp.MustCompile("Cannot delete or update a parent row: a foreign key constraint fails(?:.+REFERENCES `([^`]+)`)?"),
		Diagnose: func(m []string) Diagnosis {
			ref := "the parent table"
			if len(m) > 1 && m[1] != "" {
				ref = fmt.Sprintf("parent table `%s`", m[1])
			}
			return Diagnosis{
				Cause: fmt.Sprintf("Cannot modify a row in %s — dependent rows in a child table reference it.", ref),
				Suggestions: []string{
					"For drop_recreate mode: dmt should drop in reverse dependency order; if FKs are present pre-drop, ordering may be off.",
					"Drop FKs before destructive operations, or use SET FOREIGN_KEY_CHECKS=0 (risky; only during clean rebuilds).",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "mysql_fk_child_violation",
		Regex: regexp.MustCompile("Cannot add or update a child row: a foreign key constraint fails(?:.+REFERENCES `([^`]+)`)?"),
		Diagnose: func(m []string) Diagnosis {
			ref := "the parent table"
			if len(m) > 1 && m[1] != "" {
				ref = fmt.Sprintf("parent table `%s`", m[1])
			}
			return Diagnosis{
				Cause: fmt.Sprintf("Insert/update failed — the referenced row is missing in %s.", ref),
				Suggestions: []string{
					"Ensure the parent table is migrated before children; the orchestrator handles ordering normally.",
					"Verify the source data is referentially consistent.",
					"TaskCreateFKs creates FKs after TaskTransfer to avoid this during data load.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "mysql_key_too_long",
		Regex: regexp.MustCompile(`Specified key was too long; max key length is (\d+) bytes`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Index/PK definition exceeds MySQL's max key length (%s bytes).", m[1]),
				Suggestions: []string{
					"On utf8mb4, each char counts as 4 bytes — a VARCHAR(255) PK exceeds the 767-byte legacy limit (3072 with innodb_large_prefix).",
					"Either shorten the column, switch to utf8 (3-byte), or use index prefix length: KEY (col(191)).",
					"Modern MySQL (5.7+) typically has innodb_large_prefix=ON and supports 3072-byte keys.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mysql_table_full",
		Regex: regexp.MustCompile(`The table '([^']+)' is full`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Table %q reached a storage limit.", m[1]),
				Suggestions: []string{
					"InnoDB: usually means filesystem out of space — check df -h on the data directory.",
					"MEMORY engine: bumping max_heap_table_size and tmp_table_size resolves it.",
					"Partitioned tables can hit per-partition limits; review partitioning scheme.",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mysql_server_gone",
		Regex: regexp.MustCompile(`MySQL server has gone away`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Connection to MySQL was lost — wait_timeout exceeded, server killed the connection, or it crashed.",
				Suggestions: []string{
					"For long-running migrations, increase wait_timeout / interactive_timeout on the server side.",
					"This can also be triggered by oversized packets — see the max_allowed_packet diagnosis.",
					"Use `dmt resume` to continue from the checkpoint.",
				},
				Confidence: "medium",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "mysql_lost_connection",
		Regex: regexp.MustCompile(`Lost connection to MySQL server during query`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Connection dropped mid-query — network instability or server restart.",
				Suggestions: []string{
					"Check the server logs for crashes or OOM-killer activity.",
					"For unstable networks, set net_read_timeout / net_write_timeout higher on the server.",
					"`dmt resume` continues from the last checkpoint.",
				},
				Confidence: "medium",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "mysql_row_too_large",
		Regex: regexp.MustCompile(`Row size too large`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "A single row's column data exceeds InnoDB's row-size limit.",
				Suggestions: []string{
					"Set innodb_strict_mode=OFF (allows the row but truncates off-page columns to TEXT-style storage).",
					"Use ROW_FORMAT=DYNAMIC or COMPRESSED with innodb_file_per_table.",
					"This is rare; usually triggered by very wide tables with many long VARCHARs.",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mysql_lock_table_full",
		Regex: regexp.MustCompile(`The total number of locks exceeds the lock table size`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "InnoDB ran out of lock table memory (innodb_buffer_pool_size is exhausted by row locks).",
				Suggestions: []string{
					"Reduce chunk_size — fewer rows per statement means fewer simultaneous locks.",
					"Increase innodb_buffer_pool_size (requires server restart).",
					"For large UPDATE/DELETE operations, batch them rather than running one big statement.",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
}

func init() {
	Register("mysql", mysqlPatterns)
	Register("mariadb", mysqlPatterns)
	Register("maria", mysqlPatterns)
}
