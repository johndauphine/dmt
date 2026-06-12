package typemap

import (
	"fmt"
	"sort"
	"sync"
)

// DialectMapper is one engine's two-way mapping between its native
// column types and the canonical IR (#479). Built-in engines register
// theirs below; catalog engines (#191) call Register at catalog-load
// time, referencing the mapper from the catalog's `type_map` field.
type DialectMapper struct {
	// ToCanonical maps a native source column to the canonical IR.
	ToCanonical func(col ColumnInfo) CanonicalType
	// FromCanonical emits the canonical IR as a DDL fragment for this
	// engine as target.
	FromCanonical func(ct CanonicalType) DdlType
}

var (
	registryMu     sync.RWMutex
	dialectMappers = map[string]DialectMapper{
		DialectPostgres:   {ToCanonical: postgresToCanonical, FromCanonical: postgresFromCanonical},
		DialectMSSQL:      {ToCanonical: mssqlToCanonical, FromCanonical: mssqlFromCanonical},
		DialectMySQL:      {ToCanonical: mysqlToCanonical, FromCanonical: mysqlFromCanonical},
		DialectSQLite:     {ToCanonical: sqliteToCanonical, FromCanonical: sqliteFromCanonical},
		DialectClickHouse: {ToCanonical: clickhouseToCanonical, FromCanonical: clickhouseFromCanonical},
	}
)

// Register installs a mapper under the given dialect name. Panics on a
// duplicate name or a partial mapper — registration happens at init /
// catalog-load time where loud failure beats silent shadowing,
// mirroring the driver registry's contract.
func Register(name string, m DialectMapper) {
	if m.ToCanonical == nil || m.FromCanonical == nil {
		panic(fmt.Sprintf("typemap: Register(%q) with incomplete mapper", name))
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := dialectMappers[name]; dup {
		panic(fmt.Sprintf("typemap: duplicate dialect mapper %q", name))
	}
	dialectMappers[name] = m
}

// Supported reports whether a dialect mapper is registered for name.
func Supported(name string) bool {
	registryMu.RLock()
	defer registryMu.RUnlock()
	_, ok := dialectMappers[name]
	return ok
}

// SupportedDialects returns the registered dialect names, sorted.
func SupportedDialects() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(dialectMappers))
	for name := range dialectMappers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func lookupMapper(name string) (DialectMapper, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	m, ok := dialectMappers[name]
	return m, ok
}
