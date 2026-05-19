package drift

import (
	"fmt"
	"sort"
	"strings"
)

func (r Report) HasChanges() bool {
	return len(r.Changes) > 0
}

func (r Report) TablesAffected() int {
	tables := make(map[string]bool)
	for _, change := range r.Changes {
		tables[change.tableLabel()] = true
	}
	return len(tables)
}

func (r Report) Format() string {
	if !r.HasChanges() {
		return "No schema drift detected."
	}

	var b strings.Builder
	tablesAffected := r.TablesAffected()
	tableNoun := "tables"
	if tablesAffected == 1 {
		tableNoun = "table"
	}
	fmt.Fprintf(&b, "Schema drift detected (%d %s affected):\n", tablesAffected, tableNoun)
	for _, table := range r.groupedTables() {
		fmt.Fprintf(&b, "  %s:\n", table)
		for _, change := range r.changesForTable(table) {
			fmt.Fprintf(&b, "    %s\n", change.format())
		}
	}
	b.WriteString("\nNo automatic schema alignment will be applied (read-only mode).")
	return b.String()
}

func (r Report) groupedTables() []string {
	seen := make(map[string]bool)
	var tables []string
	for _, change := range r.Changes {
		table := change.tableLabel()
		if seen[table] {
			continue
		}
		seen[table] = true
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func (r Report) changesForTable(table string) []Change {
	var changes []Change
	for _, change := range r.Changes {
		if change.tableLabel() == table {
			changes = append(changes, change)
		}
	}
	return changes
}

func (c Change) tableLabel() string {
	if c.Schema == "" {
		return c.TableName
	}
	return c.Schema + "." + c.TableName
}

func (c Change) format() string {
	switch c.Kind {
	case TableAdded:
		return "+ added table"
	case TableDropped:
		return "! dropped table"
	case AddedColumn:
		return fmt.Sprintf("+ added column   %s %s", c.ObjectName, c.Current)
	case DroppedColumn:
		return fmt.Sprintf("! dropped column %s %s", c.ObjectName, c.Previous)
	case TypeWidened:
		return fmt.Sprintf("~ widened type   %s %s -> %s", c.ObjectName, c.Previous, c.Current)
	case TypeNarrowed:
		return fmt.Sprintf("! narrowed type  %s %s -> %s", c.ObjectName, c.Previous, c.Current)
	case TypeChangedLossy:
		return fmt.Sprintf("! changed type   %s %s -> %s", c.ObjectName, c.Previous, c.Current)
	case NullabilityChange:
		return fmt.Sprintf("~ nullability    %s %s -> %s", c.ObjectName, c.Previous, c.Current)
	case DefaultChange:
		return fmt.Sprintf("~ default        %s %s -> %s", c.ObjectName, emptyLabel(c.Previous), emptyLabel(c.Current))
	case PKChange:
		return fmt.Sprintf("! primary key    %s -> %s", emptyLabel(c.Previous), emptyLabel(c.Current))
	case IndexAdded:
		return fmt.Sprintf("+ added index    %s %s", c.ObjectName, c.Current)
	case IndexDropped:
		return fmt.Sprintf("! dropped index  %s %s", c.ObjectName, c.Previous)
	case FKAdded:
		return fmt.Sprintf("+ added FK       %s %s", c.ObjectName, c.Current)
	case FKDropped:
		return fmt.Sprintf("! dropped FK     %s %s", c.ObjectName, c.Previous)
	case CheckAdded:
		return fmt.Sprintf("+ added check    %s", c.ObjectName)
	case CheckDropped:
		return fmt.Sprintf("! dropped check  %s", c.ObjectName)
	default:
		return fmt.Sprintf("%s %s", c.Kind, c.ObjectName)
	}
}

func emptyLabel(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
