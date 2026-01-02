package target

import (
	"strings"
	"testing"
)

func TestSafePGStagingName(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		table       string
		writerID    int
		partitionID *int
		wantPrefix  string
		wantSuffix  string
		maxLen      int
	}{
		{
			name:       "short names",
			schema:     "public",
			table:      "users",
			writerID:   0,
			wantPrefix: "_stg_public_users",
			wantSuffix: "_w0",
			maxLen:     63,
		},
		{
			name:        "with partition",
			schema:      "public",
			table:       "orders",
			writerID:    1,
			partitionID: intPtr(5),
			wantPrefix:  "_stg_public_orders",
			wantSuffix:  "_p5_w1",
			maxLen:      63,
		},
		{
			name:       "long table name uses hash",
			schema:     "very_long_schema_name_that_exceeds_limits",
			table:      "extremely_long_table_name_that_definitely_exceeds_postgresql_identifier_limits",
			writerID:   0,
			wantPrefix: "_stg_",
			wantSuffix: "_w0",
			maxLen:     63,
		},
		{
			name:       "max writer ID",
			schema:     "dbo",
			table:      "data",
			writerID:   99,
			wantPrefix: "_stg_dbo_data",
			wantSuffix: "_w99",
			maxLen:     63,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safePGStagingName(tt.schema, tt.table, tt.writerID, tt.partitionID)

			// Check length constraint
			if len(got) > tt.maxLen {
				t.Errorf("safePGStagingName() length = %d, want <= %d", len(got), tt.maxLen)
			}

			// Check prefix
			if !strings.HasPrefix(got, tt.wantPrefix) {
				// For long names, just check it starts with _stg_
				if !strings.HasPrefix(got, "_stg_") {
					t.Errorf("safePGStagingName() = %v, want prefix %v", got, tt.wantPrefix)
				}
			}

			// Check suffix
			if !strings.HasSuffix(got, tt.wantSuffix) {
				t.Errorf("safePGStagingName() = %v, want suffix %v", got, tt.wantSuffix)
			}
		})
	}
}

func TestBuildPGStagingMergeSQL(t *testing.T) {
	tests := []struct {
		name         string
		schema       string
		table        string
		stagingTable string
		cols         []string
		pkCols       []string
		wantContains []string
	}{
		{
			name:         "basic upsert",
			schema:       "public",
			table:        "users",
			stagingTable: "_stg_public_users_w0",
			cols:         []string{"id", "name", "email"},
			pkCols:       []string{"id"},
			wantContains: []string{
				"INSERT INTO",
				"public",
				"users",
				"SELECT",
				"_stg_public_users_w0",
				"ON CONFLICT",
				"DO UPDATE SET",
				"IS DISTINCT FROM",
			},
		},
		{
			name:         "composite primary key",
			schema:       "sales",
			table:        "order_items",
			stagingTable: "_stg_sales_order_items_w1",
			cols:         []string{"order_id", "item_id", "quantity", "price"},
			pkCols:       []string{"order_id", "item_id"},
			wantContains: []string{
				"ON CONFLICT",
				"order_id",
				"item_id",
				"DO UPDATE SET",
				"quantity",
				"price",
			},
		},
		{
			name:         "pk-only table uses DO NOTHING",
			schema:       "public",
			table:        "tags",
			stagingTable: "_stg_public_tags_w0",
			cols:         []string{"id"},
			pkCols:       []string{"id"},
			wantContains: []string{
				"ON CONFLICT",
				"DO NOTHING",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildPGStagingMergeSQL(tt.schema, tt.table, tt.stagingTable, tt.cols, tt.pkCols)

			for _, want := range tt.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("buildPGStagingMergeSQL() missing %q\nGot: %s", want, got)
				}
			}

			// Check IS DISTINCT FROM is present for non-PK-only tables
			if len(tt.cols) > len(tt.pkCols) {
				if !strings.Contains(got, "IS DISTINCT FROM") {
					t.Errorf("buildPGStagingMergeSQL() should contain IS DISTINCT FROM for change detection")
				}
			}
		})
	}
}

// Helper function
func intPtr(i int) *int {
	return &i
}
