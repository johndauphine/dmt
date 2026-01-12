// Package source provides type aliases for backward compatibility.
// All types are now defined in the driver package.
package source

import "github.com/johndauphine/mssql-pg-migrate/internal/driver"

// Type aliases for backward compatibility.
// These allow existing code using source.Table, source.Column, etc.
// to continue working while the canonical types are in the driver package.

// Table is an alias for driver.Table.
type Table = driver.Table

// Column is an alias for driver.Column.
type Column = driver.Column

// Partition is an alias for driver.Partition.
type Partition = driver.Partition

// Index is an alias for driver.Index.
type Index = driver.Index

// ForeignKey is an alias for driver.ForeignKey.
type ForeignKey = driver.ForeignKey

// CheckConstraint is an alias for driver.CheckConstraint.
type CheckConstraint = driver.CheckConstraint
