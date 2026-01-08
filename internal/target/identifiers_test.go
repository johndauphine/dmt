package target

import "testing"

func TestSanitizePGIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Empty and edge cases
		{
			name:  "empty string",
			input: "",
			want:  "col_",
		},

		// No changes needed
		{
			name:  "simple lowercase",
			input: "users",
			want:  "users",
		},
		{
			name:  "lowercase with underscore",
			input: "user_id",
			want:  "user_id",
		},
		{
			name:  "lowercase with digits",
			input: "table1",
			want:  "table1",
		},

		// Case conversion
		{
			name:  "uppercase to lowercase",
			input: "USERS",
			want:  "users",
		},
		{
			name:  "mixed case to lowercase",
			input: "UserName",
			want:  "username",
		},
		{
			name:  "camelCase",
			input: "firstName",
			want:  "firstname",
		},
		{
			name:  "PascalCase",
			input: "FirstName",
			want:  "firstname",
		},

		// Special characters replaced with underscores
		{
			name:  "space replaced",
			input: "user name",
			want:  "user_name",
		},
		{
			name:  "hyphen replaced",
			input: "user-name",
			want:  "user_name",
		},
		{
			name:  "dot replaced",
			input: "user.name",
			want:  "user_name",
		},
		{
			name:  "at sign replaced",
			input: "user@domain",
			want:  "user_domain",
		},
		{
			name:  "hash replaced",
			input: "temp#table",
			want:  "temp_table",
		},
		{
			name:  "dollar replaced",
			input: "price$",
			want:  "price_",
		},
		{
			name:  "multiple special chars",
			input: "user-name@domain.com",
			want:  "user_name_domain_com",
		},

		// Identifiers starting with digits
		{
			name:  "starts with digit",
			input: "1column",
			want:  "col_1column",
		},
		{
			name:  "starts with digit and uppercase",
			input: "1Column",
			want:  "col_1column",
		},
		{
			name:  "all digits",
			input: "123",
			want:  "col_123",
		},

		// Complex combinations
		{
			name:  "SQL Server temp table style",
			input: "#TempUsers",
			want:  "_tempusers",
		},
		{
			name:  "SQL Server global temp table style",
			input: "##GlobalTemp",
			want:  "__globaltemp",
		},
		{
			name:  "brackets in name",
			input: "[UserName]",
			want:  "_username_",
		},
		{
			name:  "mixed special and case",
			input: "User-First Name",
			want:  "user_first_name",
		},

		// Unicode handling
		{
			name:  "accented characters preserved",
			input: "café",
			want:  "café",
		},
		{
			name:  "unicode letters preserved",
			input: "naïve",
			want:  "naïve",
		},

		// Real-world SQL Server identifiers
		{
			name:  "typical SQL Server column",
			input: "CustomerID",
			want:  "customerid",
		},
		{
			name:  "SQL Server with spaces",
			input: "Order Date",
			want:  "order_date",
		},
		{
			name:  "SQL Server with special naming",
			input: "Sales$Amount",
			want:  "sales_amount",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizePGIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("SanitizePGIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestQuotePGIdent(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple identifier",
			input: "users",
			want:  `"users"`,
		},
		{
			name:  "identifier with quote",
			input: `user"name`,
			want:  `"user""name"`,
		},
		{
			name:  "identifier with multiple quotes",
			input: `a"b"c`,
			want:  `"a""b""c"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quotePGIdent(tt.input)
			if got != tt.want {
				t.Errorf("quotePGIdent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestQuoteMSSQLIdent(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple identifier",
			input: "users",
			want:  "[users]",
		},
		{
			name:  "identifier with bracket",
			input: "user]name",
			want:  "[user]]name]",
		},
		{
			name:  "identifier with multiple brackets",
			input: "a]b]c",
			want:  "[a]]b]]c]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteMSSQLIdent(tt.input)
			if got != tt.want {
				t.Errorf("quoteMSSQLIdent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// mockTable implements TableInfo for testing
type mockTable struct {
	name    string
	columns []string
}

func (m *mockTable) GetName() string         { return m.name }
func (m *mockTable) GetColumnNames() []string { return m.columns }

func TestCollectPGIdentifierChanges(t *testing.T) {
	tests := []struct {
		name                   string
		tables                 []TableInfo
		wantTotalTableChanges  int
		wantTotalColumnChanges int
		wantTablesWithChanges  int
		wantTablesUnchanged    int
		wantHasChanges         bool
	}{
		{
			name:                   "no tables",
			tables:                 []TableInfo{},
			wantTotalTableChanges:  0,
			wantTotalColumnChanges: 0,
			wantTablesWithChanges:  0,
			wantTablesUnchanged:    0,
			wantHasChanges:         false,
		},
		{
			name: "no changes needed",
			tables: []TableInfo{
				&mockTable{name: "users", columns: []string{"id", "name", "email"}},
				&mockTable{name: "orders", columns: []string{"id", "user_id", "total"}},
			},
			wantTotalTableChanges:  0,
			wantTotalColumnChanges: 0,
			wantTablesWithChanges:  0,
			wantTablesUnchanged:    2,
			wantHasChanges:         false,
		},
		{
			name: "table name change only",
			tables: []TableInfo{
				&mockTable{name: "Users", columns: []string{"id", "name"}},
			},
			wantTotalTableChanges:  1,
			wantTotalColumnChanges: 0,
			wantTablesWithChanges:  1,
			wantTablesUnchanged:    0,
			wantHasChanges:         true,
		},
		{
			name: "column name changes only",
			tables: []TableInfo{
				&mockTable{name: "users", columns: []string{"UserID", "FirstName", "LastName"}},
			},
			wantTotalTableChanges:  0,
			wantTotalColumnChanges: 3,
			wantTablesWithChanges:  1,
			wantTablesUnchanged:    0,
			wantHasChanges:         true,
		},
		{
			name: "mixed changes",
			tables: []TableInfo{
				&mockTable{name: "Users", columns: []string{"UserID", "name"}},     // table + 1 column
				&mockTable{name: "orders", columns: []string{"id", "Order-Date"}},  // 1 column only
				&mockTable{name: "products", columns: []string{"id", "name"}},       // no changes
			},
			wantTotalTableChanges:  1,
			wantTotalColumnChanges: 2,
			wantTablesWithChanges:  2,
			wantTablesUnchanged:    1,
			wantHasChanges:         true,
		},
		{
			name: "special characters in names",
			tables: []TableInfo{
				&mockTable{name: "User-Data", columns: []string{"First Name", "Email@Work"}},
			},
			wantTotalTableChanges:  1,
			wantTotalColumnChanges: 2,
			wantTablesWithChanges:  1,
			wantTablesUnchanged:    0,
			wantHasChanges:         true,
		},
		{
			name: "numeric prefix handling",
			tables: []TableInfo{
				&mockTable{name: "1table", columns: []string{"2column", "normal"}},
			},
			wantTotalTableChanges:  1,
			wantTotalColumnChanges: 1,
			wantTablesWithChanges:  1,
			wantTablesUnchanged:    0,
			wantHasChanges:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := CollectPGIdentifierChanges(tt.tables)

			if report.TotalTableChanges != tt.wantTotalTableChanges {
				t.Errorf("TotalTableChanges = %d, want %d", report.TotalTableChanges, tt.wantTotalTableChanges)
			}
			if report.TotalColumnChanges != tt.wantTotalColumnChanges {
				t.Errorf("TotalColumnChanges = %d, want %d", report.TotalColumnChanges, tt.wantTotalColumnChanges)
			}
			if report.TablesWithChanges != tt.wantTablesWithChanges {
				t.Errorf("TablesWithChanges = %d, want %d", report.TablesWithChanges, tt.wantTablesWithChanges)
			}
			if report.TablesUnchanged != tt.wantTablesUnchanged {
				t.Errorf("TablesUnchanged = %d, want %d", report.TablesUnchanged, tt.wantTablesUnchanged)
			}
			if report.HasChanges() != tt.wantHasChanges {
				t.Errorf("HasChanges() = %v, want %v", report.HasChanges(), tt.wantHasChanges)
			}
		})
	}
}

func TestCollectPGIdentifierChanges_DetailedChanges(t *testing.T) {
	tables := []TableInfo{
		&mockTable{name: "UserAccounts", columns: []string{"User-ID", "email", "Created At"}},
	}

	report := CollectPGIdentifierChanges(tables)

	if len(report.Tables) != 1 {
		t.Fatalf("Expected 1 table with changes, got %d", len(report.Tables))
	}

	tc := report.Tables[0]

	// Check table name change
	if !tc.HasTableChange {
		t.Error("Expected HasTableChange to be true")
	}
	if tc.TableName.Original != "UserAccounts" {
		t.Errorf("TableName.Original = %q, want %q", tc.TableName.Original, "UserAccounts")
	}
	if tc.TableName.Sanitized != "useraccounts" {
		t.Errorf("TableName.Sanitized = %q, want %q", tc.TableName.Sanitized, "useraccounts")
	}

	// Check column changes (should be 2: User-ID and Created At, but not email which is already lowercase)
	if len(tc.ColumnChanges) != 2 {
		t.Errorf("Expected 2 column changes, got %d", len(tc.ColumnChanges))
	}

	// Verify specific column changes
	expectedChanges := map[string]string{
		"User-ID":    "user_id",
		"Created At": "created_at",
	}
	for _, cc := range tc.ColumnChanges {
		expected, ok := expectedChanges[cc.Original]
		if !ok {
			t.Errorf("Unexpected column change for %q", cc.Original)
			continue
		}
		if cc.Sanitized != expected {
			t.Errorf("Column %q sanitized to %q, want %q", cc.Original, cc.Sanitized, expected)
		}
	}
}
