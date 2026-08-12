package config

import (
	"fmt"
	"github.com/johndauphine/dmt/v5/internal/secrets"
	"gopkg.in/yaml.v3"
	"os"
	"reflect"
	"regexp"
	"strings"
)

// Template patterns for secret expansion:
// - filePattern: ${file:/path/to/file} - any path characters allowed
// - envPattern: ${env:VAR_NAME} - valid env var names only [A-Za-z_][A-Za-z0-9_]*
// - legacyEnvPattern: ${VAR_NAME} - legacy shorthand for ${env:VAR_NAME}
//
// The *Pattern vars are anchored (^...$) and match a value that is entirely a
// single template. embeddedTemplatePattern is the unanchored form used to
// expand one or more templates appearing anywhere within a scalar value.
var filePattern = regexp.MustCompile(`^\$\{file:(.+)\}$`)
var envPattern = regexp.MustCompile(`^\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}$`)
var legacyEnvPattern = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$`)
var embeddedTemplatePattern = regexp.MustCompile(`\$\{file:([^}]+)\}|\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}|\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEmbeddedTemplates expands every ${file:...}/${env:...}/${VAR} template
// occurring anywhere within a single scalar string, leaving the surrounding
// text intact (so composite values like "host=${env:HOST}" work). The result
// is the literal scalar content — callers set it as a yaml.Node value, so a
// '#', newline, or ':' in an expanded secret stays part of the string and
// cannot truncate or inject YAML structure the way whole-document text
// substitution could (#552).
func expandEmbeddedTemplates(value string) (string, error) {
	var firstErr error
	result := embeddedTemplatePattern.ReplaceAllStringFunc(value, func(match string) string {
		if firstErr != nil {
			return match
		}
		expanded, err := expandTemplateValue(match)
		if err != nil {
			firstErr = err
			return match
		}
		return expanded
	})
	if firstErr != nil {
		return "", firstErr
	}
	return result, nil
}

// expandTemplateValue expands template patterns in a string value.
// Supported patterns:
//   - ${file:/path/to/file} - reads value from file (trimmed of whitespace)
//   - ${env:VAR_NAME} - reads value from environment variable (explicit)
//   - ${VAR_NAME} - reads value from environment variable (legacy shorthand)
//   - Any other value is returned as-is (cleartext password)
//
// Returns the expanded value and any error encountered.
func expandTemplateValue(value string) (string, error) {
	if value == "" {
		return value, nil
	}

	// Check for ${file:...} pattern
	if matches := filePattern.FindStringSubmatch(value); matches != nil {
		filePath := expandTilde(matches[1])
		if err := secrets.ValidateFilePermissions(filePath); err != nil {
			return "", err
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("reading secret from file %s: %w", filePath, err)
		}
		return strings.TrimSpace(string(data)), nil
	}

	// Check for ${env:VAR_NAME} pattern (explicit, restricted to valid env var names)
	if matches := envPattern.FindStringSubmatch(value); matches != nil {
		// Return empty string if env var not set - allows optional env vars
		// but may cause silent auth failures if variable name is misspelled.
		return os.Getenv(matches[1]), nil
	}

	// Check for legacy ${VAR_NAME} pattern (shorthand for ${env:VAR_NAME})
	if matches := legacyEnvPattern.FindStringSubmatch(value); matches != nil {
		// Return empty string if env var not set - matches ${env:VAR} behavior.
		// This allows optional env vars but may cause silent auth failures
		// if the variable name is misspelled. Use explicit ${env:VAR} for clarity.
		return os.Getenv(matches[1]), nil
	}

	// Not a template pattern - return as-is (cleartext)
	return value, nil
}

// expandRawNonStringTemplates expands templates in non-string scalar fields
// only, leaving string placeholders literal. Used by LoadRaw so the setup
// wizard can round-trip ${env:...} secrets back to disk unresolved.
func expandRawNonStringTemplates(node *yaml.Node) error {
	return expandRawNode(node, reflect.TypeOf(Config{}), false)
}

// expandAllTemplates expands templates in every scalar field, including
// strings. Used by the runtime load path (LoadBytes) so connection secrets
// resolve before use, per-scalar on the parsed node tree rather than as raw
// text substitution over the document (#552).
func expandAllTemplates(node *yaml.Node) error {
	return expandRawNode(node, reflect.TypeOf(Config{}), true)
}

func expandRawNode(node *yaml.Node, typ reflect.Type, expandStrings bool) error {
	if node == nil {
		return nil
	}

	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) == 0 {
			return nil
		}
		return expandRawNode(node.Content[0], typ, expandStrings)
	case yaml.MappingNode:
		return expandRawMapping(node, derefType(typ), expandStrings)
	case yaml.SequenceNode:
		typ = derefType(typ)
		if typ.Kind() != reflect.Slice && typ.Kind() != reflect.Array {
			return nil
		}
		elemType := typ.Elem()
		for _, child := range node.Content {
			if err := expandRawNode(child, elemType, expandStrings); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		return expandRawScalar(node, derefType(typ), expandStrings)
	}

	return nil
}

func expandRawMapping(node *yaml.Node, typ reflect.Type, expandStrings bool) error {
	switch typ.Kind() {
	case reflect.Struct:
		fields := yamlFieldTypes(typ)
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := node.Content[i]
			value := node.Content[i+1]
			fieldType, ok := fields[key.Value]
			if !ok {
				continue
			}
			if err := expandRawNode(value, fieldType, expandStrings); err != nil {
				return err
			}
		}
	case reflect.Map:
		elemType := typ.Elem()
		for i := 0; i+1 < len(node.Content); i += 2 {
			value := node.Content[i+1]
			if err := expandRawNode(value, elemType, expandStrings); err != nil {
				return err
			}
		}
	}
	return nil
}

func expandRawScalar(node *yaml.Node, typ reflect.Type, expandStrings bool) error {
	if typ.Kind() == reflect.String {
		if !expandStrings || !embeddedTemplatePattern.MatchString(node.Value) {
			return nil
		}
		// Expand one or more embedded templates and store the result as the
		// literal scalar value. Force the !!str tag so the expanded content
		// is decoded verbatim — a secret containing '#', a newline, or ':'
		// cannot truncate the value or inject config structure (#552).
		expanded, err := expandEmbeddedTemplates(node.Value)
		if err != nil {
			return err
		}
		node.Value = expanded
		node.Tag = "!!str"
		node.Style = 0
		return nil
	}

	if !isTemplateValue(node.Value) {
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
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

func isTemplateValue(value string) bool {
	return filePattern.MatchString(value) || envPattern.MatchString(value) || legacyEnvPattern.MatchString(value)
}

// IsTemplateValue reports whether s is a secret template (${file:…}, ${env:…},
// or legacy ${VAR}) that Load/Expand would resolve — as opposed to a literal
// value that merely happens to contain braces. Callers that rewrite secrets
// (e.g. the setup wizard) use this so a literal password like "${p@ss}" is not
// mistaken for a reference.
func IsTemplateValue(s string) bool { return isTemplateValue(s) }

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
