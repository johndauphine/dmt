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
