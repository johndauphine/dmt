package transfer

import (
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/data-transfer-tool/internal/driver"
	// Import driver packages to register dialects
	_ "github.com/johndauphine/data-transfer-tool/internal/driver/mssql"
	_ "github.com/johndauphine/data-transfer-tool/internal/driver/postgres"
)

func TestBuildKeysetQueryWithDateFilter(t *testing.T) {
	testTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name       string
		dbType     string
		hasMaxPK   bool
		dateFilter *DateFilter
		wantClause string // substring that should be present in query
	}{
		{
			name:       "postgres no date filter",
			dbType:     "postgres",
			hasMaxPK:   true,
			dateFilter: nil,
			wantClause: "WHERE",
		},
		{
			name:       "postgres with date filter and maxPK",
			dbType:     "postgres",
			hasMaxPK:   true,
			dateFilter: &DateFilter{Column: "ModifiedDate", Timestamp: testTime},
			wantClause: `("ModifiedDate" > $4 OR "ModifiedDate" IS NULL)`,
		},
		{
			name:       "postgres with date filter no maxPK",
			dbType:     "postgres",
			hasMaxPK:   false,
			dateFilter: &DateFilter{Column: "ModifiedDate", Timestamp: testTime},
			wantClause: `("ModifiedDate" > $3 OR "ModifiedDate" IS NULL)`,
		},
		{
			name:       "mssql no date filter",
			dbType:     "mssql",
			hasMaxPK:   true,
			dateFilter: nil,
			wantClause: "WHERE",
		},
		{
			name:       "mssql with date filter and maxPK",
			dbType:     "mssql",
			hasMaxPK:   true,
			dateFilter: &DateFilter{Column: "ModifiedDate", Timestamp: testTime},
			wantClause: "([ModifiedDate] > @lastSyncDate OR [ModifiedDate] IS NULL)",
		},
		{
			name:       "mssql with date filter no maxPK",
			dbType:     "mssql",
			hasMaxPK:   false,
			dateFilter: &DateFilter{Column: "ModifiedDate", Timestamp: testTime},
			wantClause: "([ModifiedDate] > @lastSyncDate OR [ModifiedDate] IS NULL)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := driver.GetDialect(tt.dbType)
			if d == nil {
				t.Fatalf("dialect for dbType %q is not registered", tt.dbType)
			}
			query := d.BuildKeysetQuery(
				"col1, col2, pk",
				"pk",
				"dbo",
				"Orders",
				"", // tableHint
				tt.hasMaxPK,
				tt.dateFilter,
			)

			if !strings.Contains(query, tt.wantClause) {
				t.Errorf("Query missing expected clause.\nWant substring: %s\nGot query: %s", tt.wantClause, query)
			}

			// Verify no date clause when filter is nil
			// Date filter adds "(column > param OR column IS NULL)" pattern
			// We check for "IS NULL" which only appears in date filter clause
			if tt.dateFilter == nil {
				if strings.Contains(query, "lastSyncDate") || strings.Contains(query, "IS NULL") {
					t.Errorf("Query should not contain date filter clause when dateFilter is nil.\nGot: %s", query)
				}
			}
		})
	}
}

func TestBuildKeysetArgsWithDateFilter(t *testing.T) {
	testTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name       string
		dbType     string
		hasMaxPK   bool
		dateFilter *DateFilter
		wantCount  int // expected number of arguments
	}{
		{
			name:       "postgres no maxPK no date",
			dbType:     "postgres",
			hasMaxPK:   false,
			dateFilter: nil,
			wantCount:  2, // lastPK, limit
		},
		{
			name:       "postgres with maxPK no date",
			dbType:     "postgres",
			hasMaxPK:   true,
			dateFilter: nil,
			wantCount:  3, // lastPK, maxPK, limit
		},
		{
			name:       "postgres no maxPK with date",
			dbType:     "postgres",
			hasMaxPK:   false,
			dateFilter: &DateFilter{Column: "ModifiedDate", Timestamp: testTime},
			wantCount:  3, // lastPK, limit, timestamp
		},
		{
			name:       "postgres with maxPK with date",
			dbType:     "postgres",
			hasMaxPK:   true,
			dateFilter: &DateFilter{Column: "ModifiedDate", Timestamp: testTime},
			wantCount:  4, // lastPK, maxPK, limit, timestamp
		},
		{
			name:       "mssql no maxPK no date",
			dbType:     "mssql",
			hasMaxPK:   false,
			dateFilter: nil,
			wantCount:  2, // limit, lastPK (named)
		},
		{
			name:       "mssql with maxPK no date",
			dbType:     "mssql",
			hasMaxPK:   true,
			dateFilter: nil,
			wantCount:  3, // limit, lastPK, maxPK (named)
		},
		{
			name:       "mssql no maxPK with date",
			dbType:     "mssql",
			hasMaxPK:   false,
			dateFilter: &DateFilter{Column: "ModifiedDate", Timestamp: testTime},
			wantCount:  3, // limit, lastPK, lastSyncDate (named)
		},
		{
			name:       "mssql with maxPK with date",
			dbType:     "mssql",
			hasMaxPK:   true,
			dateFilter: &DateFilter{Column: "ModifiedDate", Timestamp: testTime},
			wantCount:  4, // limit, lastPK, maxPK, lastSyncDate (named)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := driver.GetDialect(tt.dbType)
			if d == nil {
				t.Fatalf("no dialect registered for dbType %q", tt.dbType)
			}
			args := d.BuildKeysetArgs(
				int64(0),  // lastPK
				int64(99), // maxPK
				100,       // limit
				tt.hasMaxPK,
				tt.dateFilter,
			)

			if len(args) != tt.wantCount {
				t.Errorf("Argument count mismatch: got %d, want %d", len(args), tt.wantCount)
			}

			// Verify timestamp is included when date filter is set
			if tt.dateFilter != nil {
				found := false
				for _, arg := range args {
					if ts, ok := arg.(time.Time); ok && ts.Equal(testTime) {
						found = true
						break
					}
				}
				// For postgres, timestamp is passed directly
				if tt.dbType == "postgres" && !found {
					t.Error("Expected timestamp argument for postgres with date filter")
				}
			}
		})
	}
}

func TestDateFilterStruct(t *testing.T) {
	testTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	df := &DateFilter{
		Column:    "ModifiedDate",
		Timestamp: testTime,
	}

	if df.Column != "ModifiedDate" {
		t.Errorf("Column mismatch: got %s, want ModifiedDate", df.Column)
	}
	if !df.Timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: got %v, want %v", df.Timestamp, testTime)
	}
}
