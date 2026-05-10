// Package ddl is the deterministic DDL generator that pairs with the
// canonical type IR in internal/typemap (#168) to emit full CREATE
// TABLE / index / FK / CHECK statements without an LLM round-trip.
// It implements #169 of the AI-optional epic #167.
//
// Architecture: pure data-in / SQL-out. Inputs are the schema types in
// this file plus typemap.ColumnInfo for per-column metadata. Outputs
// are SQL strings, with no I/O, no driver dependency, and no shared
// state. The dmt-side adapter (#169c) wraps these functions to satisfy
// internal/driver's TypeMapper / TableTypeMapper / FinalizationDDLMapper
// / TableDropDDLMapper interfaces.
//
// Ported from UVG /Users/john/repos/uvg/src/codegen/ddl.rs (the
// raw-SQL portion; UVG's tables.rs and relationships.rs are
// SQLAlchemy-codegen and intentionally not in scope per the UVG-first
// memory note). Pinned URL:
// https://github.com/johndauphine/uvg/blob/3106f79c/src/codegen/ddl.rs
//
// dmt's three target dialects map 1:1 to UVG's: postgres, mssql, mysql.
// UVG also has sqlite — dmt does not, so the sqlite branches are
// simply omitted in the port rather than ported as dead code.
package ddl

// TableInfo carries everything the DDL generator needs to assemble a
// CREATE TABLE statement: the table name + schema, its columns (with
// per-column metadata via typemap.ColumnInfo from #168), constraints,
// and indexes.
//
// Mirrors UVG's schema::TableInfo; the TableType field is dropped
// because dmt's introspection layer already filters views out before
// reaching the writer (CREATE TABLE never runs on a view).
type TableInfo struct {
	Schema      string
	Name        string
	Comment     string
	Columns     []Column
	Constraints []Constraint
	Indexes     []Index
}

// Column is the per-column metadata the DDL generator consumes. Wraps
// typemap.ColumnInfo (the column-type half from #168) with DDL-specific
// fields: column default, identity metadata, comment, and a couple of
// flags the type mapper doesn't need but DDL emission does (is_identity,
// autoincrement).
//
// The wrapping is intentional rather than a flat struct — typemap's
// ColumnInfo is the canonical column-level shape consumed by ToCanonical
// / FromCanonical; the DDL generator needs strictly more. Keeping the
// types separate lets typemap stay minimal and keeps the DDL surface
// from leaking back into the column-level mapper.
type Column struct {
	// Name is the column identifier as it appears in source DDL.
	Name string

	// UDTName is the source dialect's underlying type name ("int4",
	// "uniqueidentifier", "tinyint"). The primary dispatch key for
	// the column-level type mapper at typemap.ToCanonical.
	UDTName string

	// DataType is the source dialect's normalized type string ("integer",
	// "nvarchar(max)", "tinyint(1)"). MySQL's tinyint(1) → Boolean
	// detection and ENUM value parsing depend on this.
	DataType string

	// CharacterMaximumLength: nil = "no length specified"; -1 (the MSSQL
	// MAX sentinel) is normalized to nil at the typemap layer.
	CharacterMaximumLength *int
	NumericPrecision       *int
	NumericScale           *int

	IsNullable bool

	// ColumnDefault is the source's DEFAULT expression verbatim (e.g.
	// "now()", "((0))", "'hello'::character varying"). The defaults
	// translator strips dialect-specific syntax and emits a target-
	// shaped expression.
	ColumnDefault string

	// IsIdentity true when the column is GENERATED AS IDENTITY (PG /
	// MSSQL) or AUTO_INCREMENT (MySQL). Drives auto-increment
	// emission at the target.
	IsIdentity bool

	// Identity carries the start/increment values when the source
	// reports them (MSSQL IDENTITY(seed, increment), PG sequence
	// metadata). Nil when the source doesn't expose them.
	Identity *IdentityInfo

	// Autoincrement is the source's flag for non-identity auto-increment
	// (MySQL AUTO_INCREMENT mostly). Tri-state via *bool to distinguish
	// "false" from "absent" — UVG's schema treats them differently.
	Autoincrement *bool

	Comment string
}

// IdentityInfo is the start / increment / range metadata for an identity
// column. PG identity columns expose all six fields; MSSQL IDENTITY only
// exposes start + increment (the rest default).
type IdentityInfo struct {
	Start     int64
	Increment int64
	MinValue  int64
	MaxValue  int64
	Cycle     bool
	Cache     int64
}

// ConstraintType discriminates which constraint variant a Constraint
// represents.
type ConstraintType int

const (
	ConstraintPrimaryKey ConstraintType = iota
	ConstraintForeignKey
	ConstraintUnique
	ConstraintCheck
)

// Constraint is the union of PK / FK / Unique / CHECK constraint
// metadata. Only the fields relevant to the Type are populated:
//
//	PrimaryKey, Unique → Name + Columns
//	ForeignKey         → Name + Columns + ForeignKey (the *FK pointer)
//	Check              → Name + CheckExpression
type Constraint struct {
	Name            string
	Type            ConstraintType
	Columns         []string
	ForeignKey      *ForeignKey
	CheckExpression string
}

// ForeignKey carries the referenced-side metadata for a FK constraint
// (the local columns are on the parent Constraint).
type ForeignKey struct {
	RefSchema  string
	RefTable   string
	RefColumns []string

	// UpdateRule and DeleteRule are the verbatim ON UPDATE / ON DELETE
	// action strings ("CASCADE", "SET NULL", "RESTRICT", "NO ACTION").
	// Values match information_schema.referential_constraints.
	UpdateRule string
	DeleteRule string
}

// Index is the metadata for a non-PK index. Btree is implied; vendor
// kwargs (PG postgresql_using=hash, MySQL prefix lengths) live on
// Kwargs as opaque key/value strings the per-dialect index emitter
// can interpret.
type Index struct {
	Name     string
	IsUnique bool
	Columns  []string
	Kwargs   map[string]string
}
