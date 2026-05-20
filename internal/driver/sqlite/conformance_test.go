package sqlite

import (
	"testing"

	"github.com/johndauphine/dmt/internal/driver/conformance"
)

func TestDriverConformance(t *testing.T) {
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "sqlite",
		Driver:  &Driver{},
		Aliases: []string{"sqlite3", "sqlitedb"},

		QuoteName:     `weird"name`,
		QuoteNameWant: `"weird""name"`,

		QualifiedSchema:   "ignored",
		QualifiedTable:    "Users",
		QualifiedNameWant: `"Users"`,

		PlaceholderIndex: 1,
		PlaceholderWant:  "?",

		ColumnList:     []string{"id", `weird"name`},
		ColumnListWant: `"id", "weird""name"`,

		DateType: "text",

		SchemaIgnoredInQualification: true,
		RequireAIPromptAugmentation:  true,
	})
}
