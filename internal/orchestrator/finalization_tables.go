package orchestrator

import (
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
)

func finalizableTables(tables []source.Table, failedTableNames map[string]bool) []source.Table {
	success := make([]source.Table, 0, len(tables))
	for _, table := range tables {
		if failedTableNames[table.Name] {
			continue
		}

		finalTable := table
		if len(table.ForeignKeys) > 0 {
			finalTable.ForeignKeys = filterFinalizationForeignKeys(table, failedTableNames)
		}
		success = append(success, finalTable)
	}
	return success
}

func filterFinalizationForeignKeys(table source.Table, failedTableNames map[string]bool) []source.ForeignKey {
	foreignKeys := make([]source.ForeignKey, 0, len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		if failedTableNames[fk.RefTable] {
			logging.Warn("Skipping FK %s on %s because referenced table %s failed transfer",
				fk.Name, table.Name, fk.RefTable)
			continue
		}
		foreignKeys = append(foreignKeys, fk)
	}
	return foreignKeys
}
