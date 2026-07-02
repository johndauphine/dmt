package driver

import "testing"

type emptyColumnMapper struct{}

func (emptyColumnMapper) MapType(TypeInfo) string    { return "" }
func (emptyColumnMapper) CanMap(string, string) bool { return true }
func (emptyColumnMapper) SupportedTargets() []string { return []string{"*"} }

func TestMapColumnType(t *testing.T) {
	got, err := MapColumnType(NewDeterministicMapper(), "postgres", "mysql", Column{
		Name:      "display_name",
		DataType:  "varchar",
		MaxLength: 50,
	})
	if err != nil {
		t.Fatalf("MapColumnType returned error: %v", err)
	}
	if got != "VARCHAR(50)" {
		t.Fatalf("MapColumnType() = %q, want VARCHAR(50)", got)
	}
}

func TestMapColumnTypeUsesFullDataType(t *testing.T) {
	tests := []struct {
		name   string
		target string
		col    Column
		want   string
	}{
		{
			name:   "mysql int unsigned promotes to bigint",
			target: "postgres",
			col:    Column{Name: "id", DataType: "int", FullDataType: "int unsigned"},
			want:   "BIGINT",
		},
		{
			name:   "mysql enum values come from column type",
			target: "mysql",
			col:    Column{Name: "status", DataType: "enum", FullDataType: "enum('active','inactive')"},
			want:   "ENUM('active', 'inactive')",
		},
		{
			name:   "mysql tinyint one uses full type",
			target: "postgres",
			col:    Column{Name: "flag", DataType: "tinyint", FullDataType: "tinyint(1)"},
			want:   "BOOLEAN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MapColumnType(NewDeterministicMapper(), "mysql", tt.target, tt.col)
			if err != nil {
				t.Fatalf("MapColumnType returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("MapColumnType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMapColumnTypeRejectsInvalidInput(t *testing.T) {
	if _, err := MapColumnType(nil, "postgres", "mysql", Column{Name: "x", DataType: "varchar"}); err == nil {
		t.Fatal("MapColumnType returned nil error for nil mapper")
	}

	if _, err := MapColumnType(emptyColumnMapper{}, "postgres", "mysql", Column{
		Name:     "unsupported",
		DataType: "unknown",
	}); err == nil {
		t.Fatal("MapColumnType returned nil error for empty mapping")
	}
}
