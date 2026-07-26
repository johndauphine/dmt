package driver

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestSMTDDLBoundaryHasNoLocalRendererEscapeHatch keeps the ownership boundary
// narrow and reviewable. Create/finalization scheduling stays in DMT while SMT
// owns SQL for adopted tables and side objects; drops/alters remain later work.
func TestSMTDDLBoundaryHasNoLocalRendererEscapeHatch(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locating boundary test source")
	}
	driverDir := filepath.Dir(thisFile)

	assertSourceContainsNone(t, filepath.Join(driverDir, "deterministic_mapper.go"), []string{
		"ddl.GenerateCreateTable",
		"strings.TrimSpace(createDDL) + \";\"",
		"GenerateTableDDL(ctx, req)", // AI fallback must not be called here.
	})
	assertSourceContains(t, filepath.Join(driverDir, "deterministic_mapper.go"), "smtddl.RenderCreateTable(smtDDLRequest(req))")
	mapperPath := filepath.Join(driverDir, "deterministic_mapper.go")
	assertFunctionContains(t, mapperPath, "func (m *DeterministicMapper) GenerateFinalizationDDL", "smtddl.RenderIndex")
	assertFunctionContains(t, mapperPath, "func (m *DeterministicMapper) GenerateFinalizationDDL", "smtddl.RenderForeignKey")
	assertFunctionContains(t, mapperPath, "func (m *DeterministicMapper) GenerateFinalizationDDL", "smtddl.RenderCheckConstraint")
	assertFunctionContainsNone(t, mapperPath, "func (m *DeterministicMapper) GenerateFinalizationDDL", []string{
		"ddl.GenerateIndex",
		"ddl.GenerateAddForeignKey",
		"ddl.GenerateAddCheck",
		"CheckExpressionNeedsFallback",
	})
	assertFunctionContains(t, mapperPath, "func PlanCreatePrimaryKey", "smtddl.RenderPrimaryKey")
	assertSourceContains(t, filepath.Join(driverDir, "typemap_chain.go"), "return c.primary.GenerateTableDDL(ctx, req)")
	assertSourceContainsNone(t, filepath.Join(driverDir, "typemap_chain.go"), []string{
		"tableMapper.GenerateTableDDL",
		"fallback.GenerateTableDDL",
	})

	writerPath := filepath.Join(driverDir, "generic", "writer.go")
	assertFunctionContains(t, writerPath, "func (w *Writer) CreateSchema", "smtddl.RenderCreateSchema")
	assertFunctionContainsNone(t, writerPath, "func (w *Writer) CreateSchema", []string{
		"strings.NewReplacer",
		".Replace(w.cat.DDL.CreateSchema)",
	})
	assertFunctionContains(t, writerPath, "func (w *Writer) CreatePrimaryKey", "driver.PlanCreatePrimaryKey")
	assertFunctionContainsNone(t, writerPath, "func (w *Writer) CreatePrimaryKey", []string{
		"strings.NewReplacer",
		".Replace(w.cat.DDL.CreatePrimaryKey)",
	})
	assertFunctionContains(t, writerPath, "func (w *Writer) CreateTableWithOptions", "driver.PlanCreateTable(req)")
	assertFunctionContainsNone(t, writerPath, "func (w *Writer) CreateTableWithOptions", []string{
		"typeddl.",
		"strings.NewReplacer",
		"GenerateTableDDL",
	})
	assertFunctionContains(t, writerPath, "func (w *Writer) CreateIndex", "GenerateFinalizationDDL")
	assertFunctionContainsNone(t, writerPath, "func (w *Writer) CreateIndex", []string{
		"typeddl.",
		"strings.NewReplacer",
	})
}

func assertSourceContains(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(data), want) {
		t.Fatalf("%s does not contain required SMT boundary %q", path, want)
	}
}

func assertSourceContainsNone(t *testing.T, path string, forbidden []string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, text := range forbidden {
		if strings.Contains(string(data), text) {
			t.Fatalf("%s contains forbidden create-path renderer reference %q", path, text)
		}
	}
}

func assertFunctionContains(t *testing.T, path, signature, want string) {
	t.Helper()
	function := sourceFunction(t, path, signature)
	if !strings.Contains(function, want) {
		t.Fatalf("%s %s does not contain %q", path, signature, want)
	}
}

func assertFunctionContainsNone(t *testing.T, path, signature string, forbidden []string) {
	t.Helper()
	function := sourceFunction(t, path, signature)
	for _, text := range forbidden {
		if strings.Contains(function, text) {
			t.Fatalf("%s %s contains forbidden create-path renderer reference %q", path, signature, text)
		}
	}
}

func sourceFunction(t *testing.T, path, signature string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	start := strings.Index(string(data), signature)
	if start < 0 {
		t.Fatalf("%s does not contain %s", path, signature)
	}
	open := strings.Index(string(data)[start:], "{")
	if open < 0 {
		t.Fatalf("%s %s has no body", path, signature)
	}
	open += start
	depth := 0
	for i := open; i < len(data); i++ {
		switch data[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return string(data[start : i+1])
			}
		}
	}
	t.Fatalf("%s %s has an unclosed body", path, signature)
	return ""
}
