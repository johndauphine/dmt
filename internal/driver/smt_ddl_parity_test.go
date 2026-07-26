package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/typemap"
	"github.com/johndauphine/dmt/internal/typemap/ddl"
	"github.com/johndauphine/smt/schema"
)

// TestSMTDDLRepresentativeParity pins exact SMT PlanCreate table statements
// for every supported deterministic target. Each case also records DMT's
// pre-SMT baseline and explicitly documents a difference, so a renderer
// upgrade cannot silently change DMT's observable create behavior.
func TestSMTDDLRepresentativeParity(t *testing.T) {
	base := &Table{
		Name: "events",
		Columns: []Column{
			{Name: "id", DataType: "int8", IsNullable: false},
			{Name: "name", DataType: "varchar", MaxLength: 100, IsNullable: false},
			{Name: "payload", DataType: "text", IsNullable: true},
		},
		PrimaryKey: []string{"id"},
	}
	cases := []struct {
		name, target, schema string
		legacy, want         string
		intentionalChange    string
	}{
		{
			name: "postgres", target: typemap.DialectPostgres, schema: "public",
			legacy: `CREATE TABLE "events" (
    "id" BIGINT NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    "payload" TEXT,
    CONSTRAINT "pk_events" PRIMARY KEY ("id")
);`,
			want: `CREATE TABLE "events" (
    "id" bigint NOT NULL,
    "name" character varying(100) NOT NULL,
    "payload" text,
    CONSTRAINT "pk_events" PRIMARY KEY ("id")
)`,
			intentionalChange: "SMT uses PostgreSQL's canonical type spellings and returns its unterminated plan statement verbatim",
		},
		{
			name: "mssql", target: typemap.DialectMSSQL, schema: "dbo",
			legacy: `CREATE TABLE [events] (
    [id] BIGINT NOT NULL,
    [name] NVARCHAR(100) NOT NULL,
    [payload] NVARCHAR(MAX),
    CONSTRAINT [pk_events] PRIMARY KEY ([id])
);`,
			want: `CREATE TABLE [events] (
    [id] BIGINT NOT NULL,
    [name] NVARCHAR(100) NOT NULL,
    [payload] NVARCHAR(MAX),
    CONSTRAINT [pk_events] PRIMARY KEY ([id])
)`,
			intentionalChange: "SMT returns its unterminated plan statement verbatim",
		},
		{
			name: "mysql", target: typemap.DialectMySQL, schema: "app",
			legacy: "CREATE TABLE `events` (\n" +
				"    `id` BIGINT NOT NULL,\n" +
				"    `name` VARCHAR(100) NOT NULL,\n" +
				"    `payload` LONGTEXT,\n" +
				"    CONSTRAINT `pk_events` PRIMARY KEY (`id`)\n" +
				");",
			want: "CREATE TABLE `events` (\n" +
				"    `id` BIGINT NOT NULL,\n" +
				"    `name` VARCHAR(100) NOT NULL,\n" +
				"    `payload` LONGTEXT,\n" +
				"    CONSTRAINT `pk_events` PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			intentionalChange: "SMT pins InnoDB and utf8mb4 instead of relying on server defaults, and returns an unterminated plan statement",
		},
		{
			name: "sqlite", target: typemap.DialectSQLite, schema: "ignored",
			legacy: `CREATE TABLE "events" (
    "id" BIGINT NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    "payload" TEXT,
    CONSTRAINT "pk_events" PRIMARY KEY ("id")
);`,
			want: `CREATE TABLE "events" (
    "id" INTEGER NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    "payload" TEXT,
    CONSTRAINT "pk_events" PRIMARY KEY ("id")
)`,
			intentionalChange: "SMT renders SQLite's canonical 64-bit INTEGER affinity for BIGINT and returns an unterminated plan statement",
		},
		{
			name: "clickhouse", target: typemap.DialectClickHouse, schema: "analytics",
			legacy: "CREATE TABLE `analytics`.`events` (\n" +
				"    `id` Int64,\n" +
				"    `name` String,\n" +
				"    `payload` Nullable(String)\n" +
				") ENGINE = MergeTree ORDER BY (`id`);",
			want: "CREATE TABLE `analytics`.`events` (\n" +
				"    `id` Int64,\n" +
				"    `name` String,\n" +
				"    `payload` Nullable(String),\n" +
				"    PRIMARY KEY (`id`)\n" +
				") ENGINE = MergeTree ORDER BY (`id`)",
			intentionalChange: "SMT explicitly declares ClickHouse's non-unique sparse primary key alongside ORDER BY and returns an unterminated plan statement",
		},
	}

	m := NewDeterministicMapper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			legacy := ddl.GenerateCreateTable(driverTableToDDL(base, tc.schema, tc.target), typemap.DialectPostgres, tc.target)
			if got := strings.TrimSpace(legacy); got != tc.legacy {
				t.Fatalf("legacy baseline drifted before SMT comparison:\n got: %s\nwant: %s", got, tc.legacy)
			}
			got, err := m.GenerateTableDDL(context.Background(), TableDDLRequest{
				SourceDBType: typemap.DialectPostgres,
				TargetDBType: tc.target,
				SourceTable:  base,
				TargetSchema: tc.schema,
			})
			if err != nil {
				t.Fatalf("GenerateTableDDL: %v", err)
			}
			if gotDDL := got.CreateTableDDL; gotDDL != tc.want {
				t.Fatalf("SMT output:\n got: %s\nwant: %s", gotDDL, tc.want)
			}
			if tc.intentionalChange == "" && tc.want != tc.legacy {
				t.Fatal("parity deviation must document its intentional behavior change")
			}
		})
	}
}

// TestSMTSideObjectRepresentativeParity pins exact public-SMT side-object
// output for every DMT target dialect. DMT represents unique source artifacts
// as Index{IsUnique:true}, so this intentionally exercises a UNIQUE INDEX, not
// SMT's distinct named-UNIQUE-constraint API.
func TestSMTSideObjectRepresentativeParity(t *testing.T) {
	table := &Table{
		Schema: "public",
		Name:   "events",
		Columns: []Column{
			{Name: "id", DataType: "int8", IsNullable: false},
			{Name: "code", DataType: "varchar", MaxLength: 80, IsNullable: false},
		},
		PrimaryKey: []string{"id"},
	}
	index := &Index{Name: "ix_events_code", Columns: []string{"code"}, IsUnique: true}
	foreignKey := &ForeignKey{
		Name:       "fk_events_parent",
		Columns:    []string{"id"},
		RefSchema:  "public",
		RefTable:   "parent_events",
		RefColumns: []string{"id"},
		OnDelete:   "CASCADE",
		OnUpdate:   "NO ACTION",
	}
	check := &CheckConstraint{Name: "ck_events_code", Definition: "code <> ''"}

	cases := []struct {
		name, target, schema string
		index                string
		primaryKey           string
		foreignKey           string
		check                string
		unsupported          bool
		intentionalChange    string
	}{
		{
			name: "postgres", target: typemap.DialectPostgres, schema: "public",
			index:             `CREATE UNIQUE INDEX "ix_events_code" ON "events" ("code")`,
			primaryKey:        `ALTER TABLE "events" ADD CONSTRAINT "pk_events" PRIMARY KEY ("id")`,
			foreignKey:        `ALTER TABLE "events" ADD CONSTRAINT "fk_events_parent" FOREIGN KEY ("id") REFERENCES "parent_events" ("id") ON DELETE CASCADE ON UPDATE NO ACTION`,
			check:             `ALTER TABLE "events" ADD CONSTRAINT "ck_events_code" CHECK ("code" <> '')`,
			intentionalChange: "SMT emits unterminated SQL, quotes translated check identifiers, and preserves explicit NO ACTION.",
		},
		{
			name: "mssql", target: typemap.DialectMSSQL, schema: "dbo",
			index:             `CREATE UNIQUE INDEX [ix_events_code] ON [events] ([code])`,
			primaryKey:        `ALTER TABLE [events] ADD CONSTRAINT [pk_events] PRIMARY KEY ([id])`,
			foreignKey:        `ALTER TABLE [events] ADD CONSTRAINT [fk_events_parent] FOREIGN KEY ([id]) REFERENCES [parent_events] ([id]) ON DELETE CASCADE ON UPDATE NO ACTION`,
			check:             `ALTER TABLE [events] ADD CONSTRAINT [ck_events_code] CHECK ([code] <> '')`,
			intentionalChange: "SMT emits unterminated SQL and preserves explicit NO ACTION instead of DMT's former omission.",
		},
		{
			name: "mysql", target: typemap.DialectMySQL, schema: "app",
			index:             "CREATE UNIQUE INDEX `ix_events_code` ON `events` (`code`)",
			primaryKey:        "ALTER TABLE `events` ADD CONSTRAINT `pk_events` PRIMARY KEY (`id`)",
			foreignKey:        "ALTER TABLE `events` ADD CONSTRAINT `fk_events_parent` FOREIGN KEY (`id`) REFERENCES `parent_events` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION",
			check:             "ALTER TABLE `events` ADD CONSTRAINT `ck_events_code` CHECK (`code` <> '')",
			intentionalChange: "SMT emits unterminated SQL and follows DMT's connection-selected MySQL database contract.",
		},
		{
			name: "sqlite", target: typemap.DialectSQLite, schema: "ignored",
			index:             `CREATE UNIQUE INDEX "ix_events_code" ON "events" ("code")`,
			unsupported:       true,
			intentionalChange: "SMT supports standalone indexes but explicitly rejects SQLite ALTER TABLE constraints.",
		},
		{
			name: "clickhouse", target: typemap.DialectClickHouse, schema: "analytics",
			unsupported:       true,
			intentionalChange: "SMT explicitly rejects row-store side objects for ClickHouse rather than emitting incompatible SQL.",
		},
	}

	mapper := NewDeterministicMapper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := FinalizationDDLRequest{SourceDBType: typemap.DialectPostgres, TargetDBType: tc.target, Table: table, TargetSchema: tc.schema}
			req.Type, req.Index = DDLTypeIndex, index
			gotIndex, err := mapper.GenerateFinalizationDDL(context.Background(), req)
			if tc.unsupported && tc.target == typemap.DialectClickHouse {
				assertSMTUnsupported(t, err, tc.target)
				return
			}
			if err != nil {
				t.Fatalf("index: %v", err)
			}
			if gotIndex != tc.index {
				t.Fatalf("index SQL:\n got: %s\nwant: %s", gotIndex, tc.index)
			}

			req.Type, req.Index, req.ForeignKey = DDLTypeForeignKey, nil, foreignKey
			gotFK, err := mapper.GenerateFinalizationDDL(context.Background(), req)
			if tc.unsupported {
				assertSMTUnsupported(t, err, tc.target)
			} else if err != nil || gotFK != tc.foreignKey {
				t.Fatalf("foreign-key SQL = %q, err = %v; want %q", gotFK, err, tc.foreignKey)
			}

			req.Type, req.ForeignKey, req.CheckConstraint = DDLTypeCheckConstraint, nil, check
			gotCheck, err := mapper.GenerateFinalizationDDL(context.Background(), req)
			if tc.unsupported {
				assertSMTUnsupported(t, err, tc.target)
			} else if err != nil || gotCheck != tc.check {
				t.Fatalf("check SQL = %q, err = %v; want %q", gotCheck, err, tc.check)
			}

			pk, err := PlanCreatePrimaryKey(TableDDLRequest{SourceDBType: typemap.DialectPostgres, TargetDBType: tc.target, SourceTable: table, TargetSchema: tc.schema})
			if tc.unsupported {
				assertSMTUnsupported(t, err, tc.target)
			} else if err != nil || pk != tc.primaryKey {
				t.Fatalf("primary-key SQL = %q, err = %v; want %q", pk, err, tc.primaryKey)
			}

			if tc.intentionalChange == "" {
				t.Fatal("side-object parity change requires an explicit compatibility note")
			}
		})
	}
}

func assertSMTUnsupported(t *testing.T, err error, dialect string) {
	t.Helper()
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("error = %T %v, want SMT UnsupportedFeatureError", err, err)
	}
	if unsupported.Dialect != dialect {
		t.Fatalf("unsupported dialect = %q, want %q", unsupported.Dialect, dialect)
	}
}

// TestSMTDDLCompatibilityFallbackForSQLiteIdentity documents the one narrow
// renderer escape hatch: SMT's public SQLite dialect correctly declares that
// portable identity columns are unsupported, while DMT retains its historical
// SQLite AUTOINCREMENT behavior for existing migrations.
func TestSMTDDLCompatibilityFallbackForSQLiteIdentity(t *testing.T) {
	table := &Table{
		Name: "events",
		Columns: []Column{
			{Name: "id", DataType: "int8", IsNullable: false, IsIdentity: true},
			{Name: "payload", DataType: "text", IsNullable: true},
		},
		PrimaryKey: []string{"id"},
	}
	got, err := NewDeterministicMapper().GenerateTableDDL(context.Background(), TableDDLRequest{
		SourceDBType: typemap.DialectPostgres,
		TargetDBType: typemap.DialectSQLite,
		SourceTable:  table,
	})
	if err != nil {
		t.Fatalf("GenerateTableDDL: %v", err)
	}
	if !strings.Contains(got.CreateTableDDL, `"id" INTEGER PRIMARY KEY AUTOINCREMENT`) {
		t.Fatalf("SQLite identity compatibility DDL lost AUTOINCREMENT:\n%s", got.CreateTableDDL)
	}
}

// TestSMTDDLPostgresMatchesDMTIdentifierContract verifies that the SMT seam
// uses DMT's established PostgreSQL names before SMT quotes them. Transfer and
// finalization paths use the same sanitization contract, so this guards against
// CREATE TABLE succeeding with case-preserved or silently truncated names that
// those later phases cannot find.
func TestSMTDDLPostgresMatchesDMTIdentifierContract(t *testing.T) {
	tableName := "MixedCaseTable" + strings.Repeat("X", 64)
	columnName := "MixedCasePrimaryKey" + strings.Repeat("Y", 64)
	targetSchema := "CustomTargetSchema"
	table := &Table{
		Name: tableName,
		Columns: []Column{
			{Name: columnName, DataType: "bigint", IsNullable: false},
		},
		PrimaryKey: []string{columnName},
	}
	req := TableDDLRequest{
		SourceDBType: typemap.DialectMSSQL,
		TargetDBType: typemap.DialectPostgres,
		SourceTable:  table,
		TargetSchema: targetSchema,
	}

	wantSchema := sanitizeForTarget(targetSchema, typemap.DialectPostgres)
	wantTable := sanitizeForTarget(tableName, typemap.DialectPostgres)
	wantColumn := sanitizeForTarget(columnName, typemap.DialectPostgres)
	if got := smtDDLTargetSchema("Public", typemap.DialectPostgres); got != "" {
		t.Errorf("mixed-case PostgreSQL default schema = %q, want suppressed empty schema", got)
	}
	if len(wantTable) > 63 || len(wantColumn) > 63 {
		t.Fatalf("DMT PostgreSQL sanitizer did not bound identifiers: table=%d column=%d", len(wantTable), len(wantColumn))
	}

	smtReq := smtDDLRequest(req)
	if smtReq.TargetSchema != wantSchema {
		t.Errorf("SMT target schema = %q, want DMT target %q", smtReq.TargetSchema, wantSchema)
	}
	if smtReq.Table.Name != wantTable {
		t.Errorf("SMT table name = %q, want DMT transfer target %q", smtReq.Table.Name, wantTable)
	}
	if got := smtReq.Table.Columns[0].Name; got != wantColumn {
		t.Errorf("SMT column name = %q, want DMT transfer target %q", got, wantColumn)
	}
	if got := smtReq.Table.PrimaryKey[0]; got != wantColumn {
		t.Errorf("SMT primary key column = %q, want DMT finalization target %q", got, wantColumn)
	}

	mapper := NewDeterministicMapper()
	created, err := mapper.GenerateTableDDL(context.Background(), req)
	if err != nil {
		t.Fatalf("GenerateTableDDL: %v", err)
	}
	for _, want := range []string{
		`CREATE TABLE "` + wantSchema + `"."` + wantTable + `"`,
		`"` + wantColumn + `" bigint NOT NULL`,
		`PRIMARY KEY ("` + wantColumn + `")`,
	} {
		if !strings.Contains(created.CreateTableDDL, want) {
			t.Errorf("CREATE TABLE is missing %q:\n%s", want, created.CreateTableDDL)
		}
	}

	finalized, err := mapper.GenerateFinalizationDDL(context.Background(), FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: typemap.DialectMSSQL,
		TargetDBType: typemap.DialectPostgres,
		Table:        table,
		TargetSchema: targetSchema,
		Index: &Index{
			Name:    "IX_" + tableName,
			Columns: []string{columnName},
		},
	})
	if err != nil {
		t.Fatalf("GenerateFinalizationDDL: %v", err)
	}
	if want := `ON "` + wantSchema + `"."` + wantTable + `" ("` + wantColumn + `")`; !strings.Contains(finalized, want) {
		t.Errorf("finalization DDL does not target the CREATE TABLE identity %q:\n%s", want, finalized)
	}
}
