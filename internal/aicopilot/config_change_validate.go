package aicopilot

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/johndauphine/dmt/internal/config"
	"gopkg.in/yaml.v3"
)

const (
	configChangeUnknownPathError  = "path is not a recognized DMT config key"
	configChangeInvalidValueError = "value is not accepted by DMT config validation"
)

var (
	configChangeSchemaOnce  sync.Once
	configChangeSchemaPaths map[string]bool
)

type aiConfigChangeContext struct {
	TargetMode string
}

func validateAIConfigChange(path string, value any, ctx aiConfigChangeContext) []string {
	path = strings.TrimPrefix(strings.TrimSpace(path), ".")
	var ok bool
	path, ok = normalizeAIConfigChangePath(path)
	if !ok {
		return []string{configChangeUnknownPathError}
	}
	if errPath := validateAIConfigChangeNestedPaths(path, value); errPath != "" {
		return []string{fmt.Sprintf("%s: %s", configChangeUnknownPathError, errPath)}
	}
	if err := validateAIConfigChangeValue(path, value, ctx); err != nil {
		return []string{fmt.Sprintf("%s: %s", configChangeInvalidValueError, configChangeValidationFailureSummary(path, err))}
	}
	return nil
}

func validateAIConfigChangeNestedPaths(path string, value any) string {
	switch v := value.(type) {
	case map[string]any:
		for key, nested := range v {
			child := path + "." + strings.TrimSpace(key)
			if _, ok := normalizeAIConfigChangePath(child); !ok {
				return child
			}
			if errPath := validateAIConfigChangeNestedPaths(child, nested); errPath != "" {
				return errPath
			}
		}
	case []any:
		for _, item := range v {
			if errPath := validateAIConfigChangeNestedPaths(path, item); errPath != "" {
				return errPath
			}
		}
	}
	return ""
}

func validateAIConfigChangeValue(path string, value any, ctx aiConfigChangeContext) error {
	if err := rejectAIConfigTemplateSyntax(path, value); err != nil {
		return err
	}
	doc, err := yaml.Marshal(aiConfigChangeDocument(path, value, ctx))
	if err != nil {
		return err
	}
	if _, err := config.LoadBytes(doc); err != nil {
		return err
	}
	return validateAIConfigRuntimeValue(path, value)
}

func rejectAIConfigTemplateSyntax(path string, value any) error {
	if containsConfigTemplateSyntax(path) {
		return fmt.Errorf("template expansion syntax is not allowed in AI config suggestions")
	}
	return rejectAIConfigTemplateSyntaxValue(value)
}

func rejectAIConfigTemplateSyntaxValue(value any) error {
	switch v := value.(type) {
	case string:
		if containsConfigTemplateSyntax(v) {
			return fmt.Errorf("template expansion syntax is not allowed in AI config suggestions")
		}
	case map[string]any:
		for key, nested := range v {
			if containsConfigTemplateSyntax(key) {
				return fmt.Errorf("template expansion syntax is not allowed in AI config suggestions")
			}
			if err := rejectAIConfigTemplateSyntaxValue(nested); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range v {
			if err := rejectAIConfigTemplateSyntaxValue(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func containsConfigTemplateSyntax(s string) bool {
	start := strings.Index(s, "${")
	return start >= 0 && strings.Contains(s[start+2:], "}")
}

func normalizeAIConfigChangePath(path string) (string, bool) {
	path = strings.Trim(strings.TrimSpace(path), "`\"'.,;:()[]{}")
	path = strings.TrimPrefix(path, ".")
	if aiConfigChangePathKnown(path) {
		return path, true
	}
	switch {
	case strings.HasPrefix(path, "schema_evolution."),
		strings.HasPrefix(path, "schema_contract."),
		strings.HasPrefix(path, "validation."),
		strings.HasPrefix(path, "deletes."),
		strings.HasPrefix(path, "notify."):
		path = "migration." + path
	case path == "tables" || path == "columns" || path == "data_type":
		path = "migration.schema_contract." + path
	case path == "added_column" || path == "nullability_change" || path == "type_change":
		path = "migration.schema_evolution." + path
	}
	if aiConfigChangePathKnown(path) {
		return path, true
	}
	return "", false
}

func validateAIConfigRuntimeValue(path string, value any) error {
	switch v := value.(type) {
	case map[string]any:
		for key, nested := range v {
			child := path + "." + strings.TrimSpace(key)
			if normalized, ok := normalizeAIConfigChangePath(child); ok {
				child = normalized
			}
			if err := validateAIConfigRuntimeValue(child, nested); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range v {
			if err := validateAIConfigRuntimeValue(path, item); err != nil {
				return err
			}
		}
	}
	switch path {
	case "migration.validation.mode":
		return validateAIConfigValidationMode(fmt.Sprint(value))
	case "migration.validation":
		if m, ok := value.(map[string]any); ok {
			if mode, ok := m["mode"]; ok {
				return validateAIConfigValidationMode(fmt.Sprint(mode))
			}
		}
	}
	return nil
}

func validateAIConfigValidationMode(mode string) error {
	switch strings.TrimSpace(mode) {
	case "", "count_only", "null_parity", "sample":
		return nil
	case "full":
		return fmt.Errorf("validation.mode: full is reserved for a future row-hashing pass; use count_only, null_parity, or sample")
	default:
		return fmt.Errorf("validation.mode %q is not recognized; valid values are count_only, null_parity, sample", mode)
	}
}

func configChangeValidationFailureSummary(path string, err error) string {
	if err == nil {
		return "DMT rejected the proposed value"
	}
	summary := configReviewSafeGuidanceText(err.Error(), nil, 180)
	if summary == "" || strings.Contains(strings.ToLower(summary), "unsafe guidance suppressed") {
		return "DMT rejected the proposed value for " + path
	}
	return summary
}

func aiConfigChangeDocument(path string, value any, ctx aiConfigChangeContext) map[string]any {
	targetMode := strings.TrimSpace(ctx.TargetMode)
	if targetMode == "" {
		targetMode = "upsert"
	}
	doc := map[string]any{
		"source": map[string]any{
			"type":     "postgres",
			"host":     "source.example",
			"database": "source_db",
		},
		"target": map[string]any{
			"type":     "sqlite",
			"host":     "target.example",
			"database": "target.db",
		},
		"migration": map[string]any{
			"target_mode": targetMode,
		},
	}
	setNestedConfigValue(doc, strings.Split(path, "."), value)
	return doc
}

func setNestedConfigValue(root map[string]any, parts []string, value any) {
	if len(parts) == 0 {
		return
	}
	if len(parts) == 1 {
		root[parts[0]] = value
		return
	}
	next, ok := root[parts[0]].(map[string]any)
	if !ok {
		next = map[string]any{}
		root[parts[0]] = next
	}
	setNestedConfigValue(next, parts[1:], value)
}

func aiConfigChangePathKnown(path string) bool {
	configChangeSchemaOnce.Do(func() {
		configChangeSchemaPaths = buildAIConfigChangeSchemaPaths()
	})
	return configChangeSchemaPaths[path]
}

func buildAIConfigChangeSchemaPaths() map[string]bool {
	paths := map[string]bool{}
	collectAIConfigChangeSchemaPaths(reflect.TypeOf(config.Config{}), "", paths)
	return paths
}

func collectAIConfigChangeSchemaPaths(t reflect.Type, prefix string, paths map[string]bool) {
	t = indirectConfigChangeType(t)
	if t.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name := yamlFieldName(field)
		if name == "" || name == "-" {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		paths[path] = true
		ft := indirectConfigChangeType(field.Type)
		if ft.Kind() == reflect.Struct && ft.PkgPath() != "time" {
			collectAIConfigChangeSchemaPaths(ft, path, paths)
		}
	}
}

func indirectConfigChangeType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func yamlFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("yaml")
	if tag == "" {
		return strings.ToLower(field.Name)
	}
	name := strings.Split(tag, ",")[0]
	if name == "" {
		return strings.ToLower(field.Name)
	}
	return name
}

func invalidConfigAssignmentInText(text string, ctx aiConfigChangeContext) string {
	for _, assignment := range parseConfigAssignmentTexts(text) {
		if errs := validateAIConfigChange(assignment.Path, assignment.Value, ctx); len(errs) > 0 {
			return strings.Join(errs, "; ")
		}
	}
	return ""
}

type parsedConfigAssignment struct {
	Path  string
	Value any
}

func parseConfigAssignmentText(text string) (string, any, bool) {
	assignments := parseConfigAssignmentTexts(text)
	if len(assignments) == 0 {
		return "", nil, false
	}
	return assignments[0].Path, assignments[0].Value, true
}

func parseConfigAssignmentTexts(text string) []parsedConfigAssignment {
	text = strings.TrimSpace(text)
	var assignments []parsedConfigAssignment
	for _, idx := range configAssignmentSeparatorIndexes(text) {
		pathEnd := idx
		for pathEnd > 0 && isConfigSpaceByte(text[pathEnd-1]) {
			pathEnd--
		}
		pathStart := pathEnd
		for pathStart > 0 && isConfigPathCandidateByte(text[pathStart-1]) {
			pathStart--
		}
		valueStart := idx + 1
		for valueStart < len(text) && isConfigSpaceByte(text[valueStart]) {
			valueStart++
		}
		valueEnd := valueStart
		for valueEnd < len(text) && isConfigValueCandidateByte(text[valueEnd]) {
			valueEnd++
		}
		path := strings.TrimSpace(text[pathStart:pathEnd])
		valueText := strings.TrimSpace(text[valueStart:valueEnd])
		if path == "" || valueText == "" {
			continue
		}
		path, ok := normalizeAIConfigChangePath(path)
		if !ok {
			continue
		}
		assignments = append(assignments, parsedConfigAssignment{
			Path:  path,
			Value: parseScalarConfigValue(valueText),
		})
	}
	return assignments
}

func configAssignmentSeparatorIndexes(text string) []int {
	var indexes []int
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '=', ':':
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func isConfigPathCandidateByte(b byte) bool {
	return b >= '0' && b <= '9' ||
		b >= 'A' && b <= 'Z' ||
		b >= 'a' && b <= 'z' ||
		b == '_' ||
		b == '-' ||
		b == '.'
}

func isConfigValueCandidateByte(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', ',', ';':
		return false
	}
	return true
}

func isConfigSpaceByte(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func parseScalarConfigValue(value string) any {
	value = strings.Trim(strings.TrimSpace(value), "`\"'")
	switch strings.ToLower(value) {
	case "true":
		return true
	case "false":
		return false
	}
	if i, err := strconv.Atoi(value); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil && strings.Contains(value, ".") {
		return f
	}
	return value
}

func knownAIConfigChangePathsForTest() []string {
	configChangeSchemaOnce.Do(func() {
		configChangeSchemaPaths = buildAIConfigChangeSchemaPaths()
	})
	paths := make([]string, 0, len(configChangeSchemaPaths))
	for path := range configChangeSchemaPaths {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}
