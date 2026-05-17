package config

import (
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

func expandRawNonStringTemplates(node *yaml.Node) error {
	return expandRawNode(node, reflect.TypeOf(Config{}))
}

func expandRawNode(node *yaml.Node, typ reflect.Type) error {
	if node == nil {
		return nil
	}

	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) == 0 {
			return nil
		}
		return expandRawNode(node.Content[0], typ)
	case yaml.MappingNode:
		return expandRawMapping(node, derefType(typ))
	case yaml.SequenceNode:
		typ = derefType(typ)
		if typ.Kind() != reflect.Slice && typ.Kind() != reflect.Array {
			return nil
		}
		elemType := typ.Elem()
		for _, child := range node.Content {
			if err := expandRawNode(child, elemType); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		return expandRawScalar(node, derefType(typ))
	}

	return nil
}

func expandRawMapping(node *yaml.Node, typ reflect.Type) error {
	if typ.Kind() != reflect.Struct {
		return nil
	}

	fields := yamlFieldTypes(typ)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i]
		value := node.Content[i+1]
		fieldType, ok := fields[key.Value]
		if !ok {
			continue
		}
		if err := expandRawNode(value, fieldType); err != nil {
			return err
		}
	}
	return nil
}

func expandRawScalar(node *yaml.Node, typ reflect.Type) error {
	if typ.Kind() == reflect.String || !isTemplateValue(node.Value) {
		return nil
	}

	expanded, err := Expand(node.Value)
	if err != nil {
		return err
	}
	node.Value = expanded
	node.Tag = yamlScalarTag(typ)
	return nil
}

func yamlFieldTypes(typ reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}

		name := field.Name
		if tag := field.Tag.Get("yaml"); tag != "" {
			name = strings.Split(tag, ",")[0]
		}
		if name == "-" {
			continue
		}
		if name == "" {
			name = strings.ToLower(field.Name)
		}

		fields[name] = field.Type
	}
	return fields
}

func derefType(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return typ
}

func isTemplateValue(value string) bool {
	return filePattern.MatchString(value) || envPattern.MatchString(value) || legacyEnvPattern.MatchString(value)
}

func yamlScalarTag(typ reflect.Type) string {
	switch typ.Kind() {
	case reflect.Bool:
		return "!!bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "!!int"
	case reflect.Float32, reflect.Float64:
		return "!!float"
	default:
		return "!!str"
	}
}
