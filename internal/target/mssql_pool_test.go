package target

import (
	"errors"
	"strings"
	"testing"
)

func TestSafeMSSQLStagingName(t *testing.T) {
	tests := []struct {
		name        string
		table       string
		writerID    int
		partitionID *int
		wantPrefix  string
		wantSuffix  string
		maxLen      int
	}{
		{
			name:       "short name",
			table:      "users",
			writerID:   0,
			wantPrefix: "#stg_users",
			wantSuffix: "_w0",
			maxLen:     116,
		},
		{
			name:        "with partition",
			table:       "orders",
			writerID:    2,
			partitionID: intPtr(3),
			wantPrefix:  "#stg_orders",
			wantSuffix:  "_p3_w2",
			maxLen:      116,
		},
		{
			name:       "long table name uses hash",
			table:      "this_is_an_extremely_long_table_name_that_definitely_exceeds_the_mssql_temp_table_identifier_limit_of_116_characters",
			writerID:   0,
			wantPrefix: "#stg_",
			wantSuffix: "_w0",
			maxLen:     116,
		},
		{
			name:       "max writer ID",
			table:      "data",
			writerID:   99,
			wantPrefix: "#stg_data",
			wantSuffix: "_w99",
			maxLen:     116,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safeMSSQLStagingName(tt.table, tt.writerID, tt.partitionID)

			// Check length constraint
			if len(got) > tt.maxLen {
				t.Errorf("safeMSSQLStagingName() length = %d, want <= %d", len(got), tt.maxLen)
			}

			// Check starts with #
			if !strings.HasPrefix(got, "#") {
				t.Errorf("safeMSSQLStagingName() = %v, should start with #", got)
			}

			// Check prefix (for short names)
			if !strings.HasPrefix(got, tt.wantPrefix) {
				// For long names, just check it starts with #stg_
				if !strings.HasPrefix(got, "#stg_") {
					t.Errorf("safeMSSQLStagingName() = %v, want prefix %v", got, tt.wantPrefix)
				}
			}

			// Check suffix
			if !strings.HasSuffix(got, tt.wantSuffix) {
				t.Errorf("safeMSSQLStagingName() = %v, want suffix %v", got, tt.wantSuffix)
			}
		})
	}
}

func TestBuildMSSQLMergeWithTablock(t *testing.T) {
	tests := []struct {
		name            string
		targetTable     string
		stagingTable    string
		cols            []string
		pkCols          []string
		spatialCols     []SpatialColumn // columns that are geography/geometry with their types
		isCrossEngine   bool
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:          "basic merge",
			targetTable:   "[dbo].[users]",
			stagingTable:  "#stg_users_w0",
			cols:          []string{"id", "name", "email"},
			pkCols:        []string{"id"},
			spatialCols:   nil,
			isCrossEngine: false,
			wantContains: []string{
				"MERGE INTO",
				"WITH (TABLOCK)",
				"AS target",
				"USING",
				"AS source",
				"ON",
				"WHEN MATCHED",
				"THEN UPDATE SET",
				"WHEN NOT MATCHED",
				"THEN INSERT",
			},
		},
		{
			name:          "composite primary key",
			targetTable:   "[sales].[order_items]",
			stagingTable:  "#stg_order_items_w1",
			cols:          []string{"order_id", "item_id", "quantity", "price"},
			pkCols:        []string{"order_id", "item_id"},
			spatialCols:   nil,
			isCrossEngine: false,
			wantContains: []string{
				"target.[order_id] = source.[order_id]",
				"target.[item_id] = source.[item_id]",
				"[quantity] = source.[quantity]",
				"[price] = source.[price]",
			},
		},
		{
			name:          "null-safe change detection",
			targetTable:   "[dbo].[data]",
			stagingTable:  "#stg_data_w0",
			cols:          []string{"id", "value"},
			pkCols:        []string{"id"},
			spatialCols:   nil,
			isCrossEngine: false,
			wantContains: []string{
				"target.[value] <> source.[value]",
				"target.[value] IS NULL AND source.[value] IS NOT NULL",
				"target.[value] IS NOT NULL AND source.[value] IS NULL",
			},
		},
		{
			name:          "geography column excluded from change detection (same-engine)",
			targetTable:   "[sales].[customers]",
			stagingTable:  "#stg_customers_w0",
			cols:          []string{"id", "name", "location"},
			pkCols:        []string{"id"},
			spatialCols:   []SpatialColumn{{Name: "location", TypeName: "geography"}},
			isCrossEngine: false,
			wantContains: []string{
				"[location] = source.[location]", // geography is updated directly
				"target.[name] <> source.[name]", // name has change detection
			},
			wantNotContains: []string{
				"target.[location] <> source.[location]", // geography NOT in change detection
				"STGeomFromText", // no conversion for same-engine
			},
		},
		{
			name:          "geometry column excluded from change detection (same-engine)",
			targetTable:   "[dbo].[shapes]",
			stagingTable:  "#stg_shapes_w0",
			cols:          []string{"id", "shape", "label"},
			pkCols:        []string{"id"},
			spatialCols:   []SpatialColumn{{Name: "shape", TypeName: "geometry"}},
			isCrossEngine: false,
			wantContains: []string{
				"[shape] = source.[shape]", // geometry is updated directly
				"target.[label] <> source.[label]", // label has change detection
			},
			wantNotContains: []string{
				"target.[shape] <> source.[shape]", // geometry NOT in change detection
				"STGeomFromText", // no conversion for same-engine
			},
		},
		{
			name:          "cross-engine geography uses STGeomFromText",
			targetTable:   "[sales].[customers]",
			stagingTable:  "#stg_customers_w0",
			cols:          []string{"id", "name", "location"},
			pkCols:        []string{"id"},
			spatialCols:   []SpatialColumn{{Name: "location", TypeName: "geography"}},
			isCrossEngine: true,
			wantContains: []string{
				"geography::STGeomFromText(source.[location], 4326)", // WKT conversion in SET
				"target.[name] <> source.[name]",                     // name has change detection
			},
			wantNotContains: []string{
				"target.[location] <> source.[location]", // geography NOT in change detection
			},
		},
		{
			name:          "cross-engine geometry uses STGeomFromText",
			targetTable:   "[dbo].[shapes]",
			stagingTable:  "#stg_shapes_w0",
			cols:          []string{"id", "shape", "label"},
			pkCols:        []string{"id"},
			spatialCols:   []SpatialColumn{{Name: "shape", TypeName: "geometry"}},
			isCrossEngine: true,
			wantContains: []string{
				"geometry::STGeomFromText(source.[shape], 4326)", // WKT conversion
				"target.[label] <> source.[label]",               // label has change detection
			},
			wantNotContains: []string{
				"target.[shape] <> source.[shape]", // geometry NOT in change detection
			},
		},
		{
			name:          "cross-engine geography with custom SRID",
			targetTable:   "[sales].[locations]",
			stagingTable:  "#stg_locations_w0",
			cols:          []string{"id", "name", "point"},
			pkCols:        []string{"id"},
			spatialCols:   []SpatialColumn{{Name: "point", TypeName: "geography", SRID: 2163}}, // NAD83 / US National Atlas Equal Area
			isCrossEngine: true,
			wantContains: []string{
				"geography::STGeomFromText(source.[point], 2163)", // Custom SRID used
				"target.[name] <> source.[name]",
			},
			wantNotContains: []string{
				"STGeomFromText(source.[point], 4326)", // Should NOT use default SRID
			},
		},
		{
			name:          "cross-engine geometry with custom SRID",
			targetTable:   "[dbo].[map_features]",
			stagingTable:  "#stg_map_features_w0",
			cols:          []string{"id", "feature", "description"},
			pkCols:        []string{"id"},
			spatialCols:   []SpatialColumn{{Name: "feature", TypeName: "geometry", SRID: 3857}}, // Web Mercator
			isCrossEngine: true,
			wantContains: []string{
				"geometry::STGeomFromText(source.[feature], 3857)", // Custom SRID used
			},
			wantNotContains: []string{
				"STGeomFromText(source.[feature], 4326)", // Should NOT use default SRID
			},
		},
		{
			name:          "cross-engine geography with SRID 0 uses default",
			targetTable:   "[dbo].[places]",
			stagingTable:  "#stg_places_w0",
			cols:          []string{"id", "name", "coords"},
			pkCols:        []string{"id"},
			spatialCols:   []SpatialColumn{{Name: "coords", TypeName: "geography", SRID: 0}}, // SRID not set
			isCrossEngine: true,
			wantContains: []string{
				"geography::STGeomFromText(source.[coords], 4326)", // Default SRID used when 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildMSSQLMergeWithTablock(tt.targetTable, tt.stagingTable, tt.cols, tt.pkCols, tt.spatialCols, tt.isCrossEngine)

			for _, want := range tt.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("buildMSSQLMergeWithTablock() missing %q\nGot: %s", want, got)
				}
			}

			// Check that unwanted strings are NOT present
			for _, notWant := range tt.wantNotContains {
				if strings.Contains(got, notWant) {
					t.Errorf("buildMSSQLMergeWithTablock() should NOT contain %q\nGot: %s", notWant, got)
				}
			}

			// TABLOCK is critical for deadlock prevention
			if !strings.Contains(got, "TABLOCK") {
				t.Errorf("buildMSSQLMergeWithTablock() must contain TABLOCK hint")
			}
		})
	}
}

func TestIsDeadlockError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "regular error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "deadlock in message",
			err:  errors.New("Transaction was deadlocked on lock resources with another process"),
			want: true,
		},
		{
			name: "error code 1205",
			err:  errors.New("mssql: error 1205: Transaction was deadlocked"),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDeadlockError(tt.err); got != tt.want {
				t.Errorf("isDeadlockError() = %v, want %v", got, tt.want)
			}
		})
	}
}

// mockMSSQLError implements the SQLErrorNumber interface for testing
type mockMSSQLError struct {
	errNum int32
}

func (e mockMSSQLError) Error() string {
	return "mock mssql error"
}

func (e mockMSSQLError) SQLErrorNumber() int32 {
	return e.errNum
}

func TestIsDeadlockError_WithMSSQLError(t *testing.T) {
	tests := []struct {
		name   string
		errNum int32
		want   bool
	}{
		{
			name:   "deadlock error 1205",
			errNum: 1205,
			want:   true,
		},
		{
			name:   "other error",
			errNum: 547, // FK violation
			want:   false,
		},
		{
			name:   "timeout error",
			errNum: -2, // timeout
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mockMSSQLError{errNum: tt.errNum}
			if got := isDeadlockError(err); got != tt.want {
				t.Errorf("isDeadlockError() = %v, want %v", got, tt.want)
			}
		})
	}
}
