package driver

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestSMTDDLBoundaryHasNoLocalRendererEscapeHatch keeps the ownership boundary
// narrow and reviewable. Create/finalization/evolution scheduling stays in DMT
// while SMT owns SQL for tables, side objects, and production mutation methods.
func TestSMTDDLBoundaryHasNoLocalRendererEscapeHatch(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locating boundary test source")
	}
	driverDir := filepath.Dir(thisFile)

	assertSourceContainsNone(t, filepath.Join(driverDir, "deterministic_mapper.go"), []string{
		"internal/typemap/ddl",
		"ddl.GenerateCreateTable",
		"strings.TrimSpace(createDDL) + \";\"",
		"GenerateTableDDL(ctx, req)", // AI fallback must not be called here.
	})
	if _, err := os.Stat(filepath.Join(driverDir, "ai_typemapper_dropddl.go")); !os.IsNotExist(err) {
		t.Fatalf("obsolete local/AI drop-table renderer still exists: %v", err)
	}
	assertSourceContains(t, filepath.Join(driverDir, "deterministic_mapper.go"), "smtddl.RenderCreateTable(smtDDLRequest(req))")
	mapperPath := filepath.Join(driverDir, "deterministic_mapper.go")
	assertFunctionContains(t, mapperPath, "func (m *DeterministicMapper) GenerateFinalizationDDL", "return PlanFinalizationDDL(req)")
	assertFunctionContains(t, mapperPath, "func PlanFinalizationDDL", "smtddl.RenderIndex")
	assertFunctionContains(t, mapperPath, "func PlanFinalizationDDL", "smtddl.RenderForeignKey")
	assertFunctionContains(t, mapperPath, "func PlanFinalizationDDL", "smtddl.RenderCheckConstraint")
	assertFunctionContainsNone(t, mapperPath, "func PlanFinalizationDDL", []string{
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
	assertFunctionContains(t, writerPath, "func (w *Writer) CreateIndex", "driver.PlanFinalizationDDL")
	assertFunctionContainsNone(t, writerPath, "func (w *Writer) CreateIndex", []string{
		"typeddl.",
		"strings.NewReplacer",
		"GenerateFinalizationDDL",
	})
	assertSourceContainsNone(t, writerPath, []string{"FinalizationDDLMapper", "finalizationMapper"})
	for signature, smtCall := range map[string]string{
		"func (w *Writer) AddColumn":         "smtddl.RenderAddColumn",
		"func (w *Writer) DropColumnNotNull": "smtddl.RenderAlterColumnNullability",
		"func (w *Writer) AlterColumnType":   "smtddl.RenderAlterColumnType",
		"func (w *Writer) DropTable":         "smtddl.RenderDropTable",
		"func (w *Writer) TruncateTable":     "smtddl.RenderTruncateTable",
	} {
		assertFunctionContains(t, writerPath, signature, smtCall)
		assertFunctionContainsNone(t, writerPath, signature, []string{
			"MapColumnType",
			"strings.NewReplacer",
			"w.cat.DDL",
			"execDDLStatementList",
		})
	}

	batchPath := filepath.Join(driverDir, "generic", "smt_batch.go")
	assertFunctionContains(t, batchPath, "func executeSMTBatchOn", "execer.ExecContext(ctx, statement.SQL)")
	assertFunctionContainsNone(t, batchPath, "func executeSMTBatchOn", []string{
		"strings.TrimSpace",
		"strings.Replace",
	})

	catalogDir := filepath.Join(driverDir, "generic", "catalogs")
	catalogs, err := filepath.Glob(filepath.Join(catalogDir, "*.yaml"))
	if err != nil {
		t.Fatalf("glob catalogs: %v", err)
	}
	for _, catalog := range catalogs {
		assertSourceContainsNone(t, catalog, []string{"\nddl:"})
	}
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
