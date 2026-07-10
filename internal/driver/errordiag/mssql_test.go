package errordiag

import (
	"strings"
	"testing"
)

var mssqlCases = []struct {
	pattern   string
	errMsg    string
	mustMatch string
	mustCateg string
}{
	{
		pattern:   "mssql_database_snapshot_create",
		errMsg:    `creating SQL Server strict snapshot [dmt_strict_abc12345]: mssql: CREATE DATABASE permission denied in database 'master'.`,
		mustMatch: "CREATE ANY DATABASE",
		mustCateg: "permission",
	},
	{
		pattern:   "mssql_pk_duplicate",
		errMsg:    `mssql: Violation of PRIMARY KEY constraint 'PK_users'. Cannot insert duplicate key in object 'dbo.users'.`,
		mustMatch: "PK_users",
		mustCateg: "constraint",
	},
	{
		pattern:   "mssql_unique_duplicate",
		errMsg:    `mssql: Violation of UNIQUE KEY constraint 'UQ_users_email'. Cannot insert duplicate key in object 'dbo.users'.`,
		mustMatch: "UQ_users_email",
		mustCateg: "constraint",
	},
	{
		pattern:   "mssql_null_violation",
		errMsg:    `mssql: Cannot insert the value NULL into column 'email', table 'prod.dbo.users'; column does not allow nulls. INSERT fails.`,
		mustMatch: "email",
		mustCateg: "data_quality",
	},
	{
		pattern:   "mssql_truncation",
		errMsg:    `mssql: String or binary data would be truncated in table 'prod.dbo.users', column 'email'.`,
		mustMatch: "email",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mssql_conversion_failed",
		errMsg:    `mssql: Conversion failed when converting the varchar value 'abc' to data type int.`,
		mustMatch: "abc",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mssql_identity_insert",
		errMsg:    `mssql: Cannot insert explicit value for identity column in table 'users' when IDENTITY_INSERT is set to OFF.`,
		mustMatch: "users",
		mustCateg: "other",
	},
	{
		pattern:   "mssql_invalid_object",
		errMsg:    `mssql: Invalid object name 'dbo.orders'.`,
		mustMatch: "dbo.orders",
		mustCateg: "other",
	},
	{
		pattern:   "mssql_invalid_column",
		errMsg:    `mssql: Invalid column name 'email'.`,
		mustMatch: "email",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mssql_object_exists",
		errMsg:    `mssql: There is already an object named 'users' in the database.`,
		mustMatch: "users",
		mustCateg: "other",
	},
	{
		pattern:   "mssql_fk_insert_violation",
		errMsg:    `mssql: The INSERT statement conflicted with the FOREIGN KEY constraint "FK_orders_users". The conflict occurred in database "prod", table "dbo.users", column 'id'.`,
		mustMatch: "FK_orders_users",
		mustCateg: "constraint",
	},
	{
		pattern:   "mssql_fk_reference_conflict",
		errMsg:    `mssql: The DELETE statement conflicted with the REFERENCE constraint "FK_orders_users".`,
		mustMatch: "FK_orders_users",
		mustCateg: "constraint",
	},
	{
		pattern:   "mssql_login_failed",
		errMsg:    `mssql: Login failed for user 'dmt'.`,
		mustMatch: "dmt",
		mustCateg: "connection",
	},
	{
		pattern:   "mssql_cannot_open_db",
		errMsg:    `mssql: Cannot open database "prod" requested by the login. The login failed.`,
		mustMatch: "prod",
		mustCateg: "permission",
	},
	{
		pattern:   "mssql_disk_full",
		errMsg:    `mssql: Could not allocate space for object 'dbo.users'.'PK_users' in database 'prod' because the 'PRIMARY' filegroup is full.`,
		mustMatch: "PRIMARY",
		mustCateg: "other",
	},
	{
		pattern:   "mssql_deadlock_victim",
		errMsg:    `mssql: Transaction (Process ID 52) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction.`,
		mustMatch: "deadlock",
		mustCateg: "other",
	},
	{
		pattern:   "mssql_lock_timeout",
		errMsg:    `mssql: Lock request time out period exceeded.`,
		mustMatch: "LOCK_TIMEOUT",
		mustCateg: "other",
	},
	{
		pattern:   "mssql_object_or_permission",
		errMsg:    `mssql: Cannot find the object "dbo.users" because it does not exist or you do not have permissions.`,
		mustMatch: "permission",
		mustCateg: "permission",
	},
	{
		pattern:   "mssql_operand_clash",
		errMsg:    `mssql: Operand type clash: int is incompatible with date`,
		mustMatch: "int",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mssql_arithmetic_overflow",
		errMsg:    `mssql: Arithmetic overflow error converting numeric to data type numeric.`,
		mustMatch: "numeric",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mssql_permission_denied",
		errMsg:    `mssql: INSERT permission was denied on the object 'users', database 'prod', schema 'dbo'.`,
		mustMatch: "users",
		mustCateg: "permission",
	},
	{
		pattern:   "mssql_network_error",
		errMsg:    `mssql: A network-related or instance-specific error occurred while establishing a connection to SQL Server.`,
		mustMatch: "network",
		mustCateg: "connection",
	},
	{
		pattern:   "mssql_parameter_cap",
		errMsg:    `mssql: The incoming tabular data stream (TDS) remote procedure call (RPC) protocol stream is incorrect. Too many parameters were provided in this RPC request. The maximum is 2100.`,
		mustMatch: "parameter",
		mustCateg: "other",
	},
	{
		pattern:   "mssql_truncated_or_overflow",
		errMsg:    `mssql: The conversion of a varchar data type to a datetime data type resulted in an out-of-range value.`,
		mustMatch: "datetime",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mssql_invalid_tablock",
		errMsg:    `mssql: The number of rows retrieved from the bulk source exceeds`,
		mustMatch: "resume",
		mustCateg: "other",
	},
	{
		pattern:   "mssql_cert_validation",
		errMsg:    `mssql: certificate is not trusted`,
		mustMatch: "certificate",
		mustCateg: "connection",
	},
}

func TestMSSQLPatterns_AllCasesMatch(t *testing.T) {
	for _, tc := range mssqlCases {
		t.Run(tc.pattern, func(t *testing.T) {
			m, ok := Lookup("mssql", tc.errMsg)
			if !ok {
				t.Fatalf("expected Lookup to match for pattern %q with msg %q", tc.pattern, tc.errMsg)
			}
			if m.PatternName != tc.pattern {
				t.Fatalf("matched the wrong pattern: got %q, want %q (error: %q)", m.PatternName, tc.pattern, tc.errMsg)
			}
			assertDiagnosisShape(t, m.Diagnosis, tc.pattern)
			if tc.mustCateg != "" && m.Diagnosis.Category != tc.mustCateg {
				t.Errorf("category mismatch: got %q, want %q", m.Diagnosis.Category, tc.mustCateg)
			}
			if tc.mustMatch != "" {
				combined := m.Diagnosis.Cause + " " + strings.Join(m.Diagnosis.Suggestions, " ")
				if !strings.Contains(combined, tc.mustMatch) {
					t.Errorf("diagnosis should mention %q; got cause=%q suggestions=%v",
						tc.mustMatch, m.Diagnosis.Cause, m.Diagnosis.Suggestions)
				}
			}
		})
	}
}

func TestMSSQLPatterns_EveryCatalogPatternHasACase(t *testing.T) {
	covered := make(map[string]struct{}, len(mssqlCases))
	for _, tc := range mssqlCases {
		covered[tc.pattern] = struct{}{}
	}
	for _, p := range mssqlPatterns {
		if _, ok := covered[p.Name]; !ok {
			t.Errorf("pattern %q has no test case in mssqlCases (add one or remove the pattern)", p.Name)
		}
	}
}
