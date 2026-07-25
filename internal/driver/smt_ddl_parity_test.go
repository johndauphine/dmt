package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/typemap"
	"github.com/johndauphine/dmt/internal/typemap/ddl"
)

// TestSMTDDLRepresentativeParity pins representative DMT CREATE TABLE output
// for every supported deterministic target. Each case records both the
// pre-SMT baseline and the accepted output from SMT's public renderer so a
// renderer upgrade cannot silently change DMT's externally visible DDL.
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
);`,
			intentionalChange: "SMT uses PostgreSQL's canonical type spellings",
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
);`,
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
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			intentionalChange: "SMT pins InnoDB and utf8mb4 instead of relying on server defaults",
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
);`,
			intentionalChange: "SMT renders SQLite's canonical 64-bit INTEGER affinity for BIGINT",
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
				") ENGINE = MergeTree ORDER BY (`id`);",
			intentionalChange: "SMT explicitly declares ClickHouse's non-unique sparse primary key alongside ORDER BY",
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
			if gotDDL := strings.TrimSpace(got.CreateTableDDL); gotDDL != tc.want {
				t.Fatalf("SMT output:\n got: %s\nwant: %s", gotDDL, tc.want)
			}
			if tc.intentionalChange == "" && tc.want != tc.legacy {
				t.Fatal("parity deviation must document its intentional behavior change")
			}
		})
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
