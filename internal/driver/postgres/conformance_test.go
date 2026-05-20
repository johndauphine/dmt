package postgres

import (
	"testing"

	"github.com/johndauphine/dmt/internal/driver/conformance"
)

func TestDriverConformance(t *testing.T) {
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "postgres",
		Driver:  &Driver{},
		Aliases: []string{"postgresql", "pg"},

		QuoteName:     `weird"name`,
		QuoteNameWant: `"weird""name"`,

		QualifiedSchema:   "public",
		QualifiedTable:    "Users",
		QualifiedNameWant: `"public"."Users"`,

		PlaceholderIndex: 1,
		PlaceholderWant:  "$1",

		ColumnList:     []string{"id", `weird"name`},
		ColumnListWant: `"id", "weird""name"`,

		DateType: "timestamp",

		RequireSchemaQualification:  true,
		RequireAIPromptAugmentation: true,
	})
}
