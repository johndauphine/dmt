package errordiag

import (
	"fmt"
	"regexp"
)

// mssqlPatterns is the deterministic catalog for SQL Server errors.
// Error message shapes follow what github.com/microsoft/go-mssqldb
// emits (typically prefixed with "mssql: " through database/sql).
//
// Pattern order matters — earlier patterns win. Place more specific
// patterns before generic ones.
var mssqlPatterns = []Pattern{
	{
		Name:  "mssql_database_snapshot_create",
		Regex: regexp.MustCompile(`creating SQL Server strict snapshot(?: \[[^\]]+\])?:`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "SQL Server could not create the migration-scoped database snapshot, so strict transfer stopped before target mutation.",
				Suggestions: []string{
					"Grant CREATE ANY DATABASE or ALTER ANY DATABASE at server scope, grant CREATE DATABASE in master, or add the migration login to dbcreator.",
					"Confirm the source is not Azure SQL Database and that SQL Server supports database snapshots (2016 SP1+ for non-Enterprise editions).",
					"Check free disk space and write access beside every source data file; snapshot sparse files are created in those directories.",
					"Use strict_consistency_scope: table if migration-scoped snapshots are not operationally available.",
				},
				Confidence: "high",
				Category:   "permission",
			}
		},
	},
	{
		Name:  "mssql_pk_duplicate",
		Regex: regexp.MustCompile(`Violation of PRIMARY KEY constraint '([^']+)'\.`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("A row with a duplicate primary-key value already exists; PK constraint %q rejected the insert.", m[1]),
				Suggestions: []string{
					"For drop_recreate mode: confirm the target table was empty before the migration started.",
					"Inspect the source for duplicate PK values — they're allowed in some loose schemas but fail on bulk insert.",
					"For upsert mode: verify the PK column set matches between source and target.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "mssql_unique_duplicate",
		Regex: regexp.MustCompile(`Violation of UNIQUE KEY constraint '([^']+)'\.`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Unique constraint %q rejected a duplicate value.", m[1]),
				Suggestions: []string{
					"Check the source for duplicate values in the constrained columns.",
					"If the source allows them but target shouldn't, you'll need to dedupe before migration.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "mssql_null_violation",
		Regex: regexp.MustCompile(`Cannot insert the value NULL into column '([^']+)', table '([^']+)'; column does not allow nulls`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Column %q in %s is NOT NULL but a source row contained NULL.", m[1], m[2]),
				Suggestions: []string{
					"Verify the source column also disallows NULL — a mismatch points to a type-mapping bug or stale schema.",
					"For drop_recreate mode: regenerate target DDL after fixing the nullability mismatch.",
					"For upsert mode: provide a default or alter the column to permit NULL.",
				},
				Confidence: "high",
				Category:   "data_quality",
			}
		},
	},
	{
		Name:  "mssql_truncation",
		Regex: regexp.MustCompile(`String or binary data would be truncated(?: in table '([^']+)', column '([^']+)')?`),
		Diagnose: func(m []string) Diagnosis {
			tail := "the target column is shorter than the source value."
			if len(m) > 2 && m[1] != "" {
				tail = fmt.Sprintf("column %q in %s is shorter than the source value.", m[2], m[1])
			}
			return Diagnosis{
				Cause: "String/binary truncation: " + tail,
				Suggestions: []string{
					"Widen the target column or use VARCHAR(MAX) / NVARCHAR(MAX) for the affected column.",
					"This commonly indicates a type-mapping bug that under-sized the column.",
					"Pre-2017 SQL Server lacks the helpful detail; newer versions include the failing value — upgrade if you can.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mssql_conversion_failed",
		Regex: regexp.MustCompile(`Conversion failed when converting (?:the )?([a-z]+(?: ?[a-z]+)?) value '([^']*)' to data type ([a-z]+)`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Cannot convert source value %q from %s to target type %s.", m[2], m[1], m[3]),
				Suggestions: []string{
					"Check the source's actual values against the mapped target type — empty strings into numeric columns are a common offender.",
					"Locale-specific decimal separators (',' vs '.') can fail conversion.",
					"Consider mapping to a wider type and converting post-migration.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mssql_identity_insert",
		Regex: regexp.MustCompile(`Cannot insert explicit value for identity column in table '([^']+)' when IDENTITY_INSERT is set to OFF`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Table %s has IDENTITY_INSERT OFF but the writer attempted to insert explicit identity values.", m[1]),
				Suggestions: []string{
					"dmt enables IDENTITY_INSERT around bulk inserts for tables with identity columns — verify the table really is identity-defined.",
					"If a manual ALTER changed identity behavior, recreate the table or run SET IDENTITY_INSERT <tbl> ON before the load.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mssql_invalid_object",
		Regex: regexp.MustCompile(`Invalid object name '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Object %q is not visible to the current user or does not exist.", m[1]),
				Suggestions: []string{
					"Confirm the target schema in your YAML matches where the table was created (dbo vs custom schema).",
					"Verify the migration user has access — Invalid object name can also be a permissions message in disguise.",
					"For drop_recreate mode: if TaskCreateTables failed silently, the table simply doesn't exist.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mssql_invalid_column",
		Regex: regexp.MustCompile(`Invalid column name '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Target table has no column named %q.", m[1]),
				Suggestions: []string{
					"For upsert mode: confirm the column exists in the target (it may have been dropped or renamed since dmt extracted the source schema).",
					"Run `dmt analyze` to see the resolved schema mapping.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mssql_object_exists",
		Regex: regexp.MustCompile(`There is already an object named '([^']+)' in the database`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("CREATE failed — %q already exists in the target.", m[1]),
				Suggestions: []string{
					"For drop_recreate mode: dmt should drop tables first; verify the user has DROP privilege.",
					"For upsert mode: this means schema is partially divergent — review the affected object.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mssql_fk_insert_violation",
		Regex: regexp.MustCompile(`The INSERT statement conflicted with the FOREIGN KEY constraint "([^"]+)"`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("FK constraint %q rejected the insert — parent row is missing.", m[1]),
				Suggestions: []string{
					"Ensure the parent table is migrated before the child; the orchestrator normally handles ordering.",
					"Verify the source data is referentially consistent.",
					"TaskCreateFKs runs after TaskTransfer by default; if you're seeing this during TaskTransfer with FKs already present, FK creation order is off.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "mssql_fk_reference_conflict",
		Regex: regexp.MustCompile(`The DELETE statement conflicted with the REFERENCE constraint "([^"]+)"`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("DELETE blocked by FK %q — dependent rows reference this row.", m[1]),
				Suggestions: []string{
					"For drop_recreate mode: dmt should drop in reverse dependency order; check whether DROP TABLE order is correct.",
					"Manually drop the FK or delete dependent rows first if doing manual cleanup.",
				},
				Confidence: "high",
				Category:   "constraint",
			}
		},
	},
	{
		Name:  "mssql_login_failed",
		Regex: regexp.MustCompile(`Login failed for user '([^']+)'`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("SQL Server rejected the login for user %q.", m[1]),
				Suggestions: []string{
					"Verify the user/password — secret expansion (${env:VAR}, ${file:/path}) may not be resolving as expected.",
					"Confirm the user exists and has CONNECT permission on the database.",
					"For Windows-authenticated logins on Linux clients, you'll need a Kerberos-aware connection string.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		Name:  "mssql_cannot_open_db",
		Regex: regexp.MustCompile(`Cannot open database "([^"]+)" requested by the login`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Login succeeded but the user lacks access to database %q.", m[1]),
				Suggestions: []string{
					"Grant CONNECT or db_datareader/db_datawriter on the target database.",
					"Verify the Database field in the config matches an existing database name.",
				},
				Confidence: "high",
				Category:   "permission",
			}
		},
	},
	{
		Name:  "mssql_disk_full",
		Regex: regexp.MustCompile(`Could not allocate space for object '([^']+)'(?:\.[^.]+)? in database '([^']+)' because the '([^']+)' filegroup is full`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Filegroup %q in database %q is full; cannot allocate space for %s.", m[3], m[2], m[1]),
				Suggestions: []string{
					"Expand the filegroup or add a new data file: ALTER DATABASE [" + m[2] + "] ADD FILE ...",
					"Pre-flight (#228) should catch insufficient space before the migration starts.",
					"Use `dmt resume` once space is available.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mssql_deadlock_victim",
		Regex: regexp.MustCompile(`(?:Transaction \(Process ID \d+\) was deadlocked|chosen as the deadlock victim)`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Transaction was chosen as deadlock victim and rolled back.",
				Suggestions: []string{
					"Reduce write_ahead_writers — concurrent writers contending for the same indexes are the typical cause.",
					"Ensure no other workload is writing to the same target tables during migration.",
					"dmt retries deadlock victims by default; if a single chunk fails after retries, check whether the table has triggers or DDL changes interfering.",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mssql_lock_timeout",
		Regex: regexp.MustCompile(`Lock request time out period exceeded`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "A SQL Server lock request waited longer than LOCK_TIMEOUT permitted.",
				Suggestions: []string{
					"For table-scoped strict reads, identify the writer blocking the shared table lock. Dmt reports this timeout and degrades to one reader.",
					"That strict fallback remains consistent but loses parallel-reader throughput; use migration-scoped database snapshots when table-long writer blocking is unacceptable.",
					"Identify what's holding the conflicting lock (sp_who2, sys.dm_exec_requests).",
					"Increase LOCK_TIMEOUT on the migration session (SET LOCK_TIMEOUT -1 disables it; risky in production).",
					"Avoid running the migration during peak hours on the target.",
				},
				Confidence: "medium",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mssql_object_or_permission",
		Regex: regexp.MustCompile(`Cannot find the object "([^"]+)" because it does not exist or you do not have permissions`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Either object %q doesn't exist or the user lacks permission to see it.", m[1]),
				Suggestions: []string{
					"SQL Server collapses 'not found' and 'no permission' into one message to avoid information leakage — try both angles.",
					"Verify the migration user has at minimum VIEW DEFINITION on the schema.",
					"Run `dmt analyze` to confirm the object is enumerable.",
				},
				Confidence: "medium",
				Category:   "permission",
			}
		},
	},
	{
		Name:  "mssql_operand_clash",
		Regex: regexp.MustCompile(`Operand type clash: ([a-z]+) is incompatible with ([a-z]+)`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Type-incompatibility: %s value used where %s expected.", m[1], m[2]),
				Suggestions: []string{
					"Common in MERGE/UPSERT comparisons (e.g. date vs datetime). Check the type mapping for affected columns.",
					"Pre-cast with CONVERT() in custom transforms if dmt's mapping is ambiguous.",
				},
				Confidence: "medium",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mssql_arithmetic_overflow",
		Regex: regexp.MustCompile(`Arithmetic overflow error converting [a-z]+ to data type ([a-z]+)`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Source value is out of range for target type %s.", m[1]),
				Suggestions: []string{
					"Widen the target column to a larger numeric type (decimal(38, n), bigint, etc.).",
					"Inspect the failing rows in source for extreme values that wouldn't fit the mapped type.",
					"This typically indicates the type mapping under-sized a numeric column.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mssql_permission_denied",
		Regex: regexp.MustCompile(`(?:SELECT|INSERT|UPDATE|DELETE|EXECUTE|ALTER|CREATE|DROP|REFERENCES) permission (?:was )?denied on (?:object|the object) '([^']+)'(?:, database '([^']+)')?(?:, schema '([^']+)')?`),
		Diagnose: func(m []string) Diagnosis {
			object := m[1]
			return Diagnosis{
				Cause: fmt.Sprintf("Migration user lacks the required permission on %q.", object),
				Suggestions: []string{
					"Grant the appropriate permission (the exact verb is in the error message).",
					"For drop_recreate the user needs CREATE, DROP, INSERT, ALTER on the schema (db_ddladmin + db_datawriter typically covers it).",
					"For source roles, db_datareader on the source database is usually sufficient.",
				},
				Confidence: "high",
				Category:   "permission",
			}
		},
	},
	{
		Name:  "mssql_network_error",
		Regex: regexp.MustCompile(`(?:A network-related or instance-specific error|Unable to open .* connection.* SQL Server)`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "SQL Server connection failed at the network layer.",
				Suggestions: []string{
					"Verify the server is running and the configured port (default 1433) is reachable.",
					"Check that TCP/IP is enabled in SQL Server Configuration Manager (Named Pipes alone is not enough).",
					"On Linux clients, ensure ODBC/Kerberos prerequisites are in place if using Windows auth.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
	{
		// SQL Server's 2100-parameter cap appears in several phrasings;
		// match on the load-bearing phrases without trying to span the
		// whole leading sentence (which contains periods that break a
		// `[^.]+` middle).
		Name:  "mssql_parameter_cap",
		Regex: regexp.MustCompile(`(?:[Tt]oo many parameters were provided|maximum of \d+ parameters)`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Bulk insert exceeded SQL Server's per-statement parameter limit (2100).",
				Suggestions: []string{
					"Reduce chunk_size — dmt has a runtime rule that should shrink chunks on this error, but a manual override in config is the surest fix.",
					"The cap is parameter count = chunk_size * column_count; very wide tables need much smaller chunks.",
				},
				Confidence: "high",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mssql_truncated_or_overflow",
		Regex: regexp.MustCompile(`The conversion of (?:the |a )?([a-z]+) (?:data type to|value to) (?:a )?([a-z]+) (?:data type )?(?:resulted in an out-of-range|value resulted in an out-of-range)`),
		Diagnose: func(m []string) Diagnosis {
			return Diagnosis{
				Cause: fmt.Sprintf("Value of type %s does not fit in target type %s.", m[1], m[2]),
				Suggestions: []string{
					"Widen the target column.",
					"This usually points to under-sized numeric or date type mappings.",
				},
				Confidence: "high",
				Category:   "type_mismatch",
			}
		},
	},
	{
		Name:  "mssql_invalid_tablock",
		Regex: regexp.MustCompile(`The number of (?:rows |)retrieved from .* exceeds`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "Bulk insert raised a row-count consistency error.",
				Suggestions: []string{
					"Rerun the failed chunk via `dmt resume`; transient bulk-protocol errors usually clear on retry.",
					"If the error persists, check the table for triggers that may be filtering/expanding rows during insert.",
				},
				Confidence: "low",
				Category:   "other",
			}
		},
	},
	{
		Name:  "mssql_cert_validation",
		Regex: regexp.MustCompile(`(?:certificate is not (?:trusted|valid)|certificate signed by unknown authority)`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{
				Cause: "TLS certificate validation failed during the SQL Server connection.",
				Suggestions: []string{
					"For development: set `encrypt=optional` or `trustservercertificate=true` in the connection string.",
					"For production: install the server's CA certificate in the client trust store.",
					"Self-signed certs are common with default SQL Server installations — verify the cert chain.",
				},
				Confidence: "high",
				Category:   "connection",
			}
		},
	},
}

func init() {
	Register("mssql", mssqlPatterns)
	Register("sqlserver", mssqlPatterns)
	Register("sql-server", mssqlPatterns)
}
