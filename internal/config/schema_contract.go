package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// UnmarshalYAML accepts DLT's two schema_contract forms:
//
//	schema_contract: report
//	schema_contract:
//	  columns: discard_value
//	  data_type: freeze
func (c *SchemaContractConfig) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag == "!!null" || strings.TrimSpace(node.Value) == "" {
			return nil
		}
		mode := SchemaContractMode(strings.TrimSpace(node.Value))
		c.Tables = mode
		c.Columns = mode
		c.DataType = mode
		return nil
	case yaml.MappingNode:
		type plain SchemaContractConfig
		var decoded plain
		if err := node.Decode(&decoded); err != nil {
			return err
		}
		*c = SchemaContractConfig(decoded)
		return nil
	default:
		return fmt.Errorf("migration.schema_contract must be a mode string or mapping")
	}
}
