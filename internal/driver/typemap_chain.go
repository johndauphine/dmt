// FallbackChain decorates the deterministic type mapper with the AI
// type mapper as a registered fallback for the cases the deterministic
// path can't handle:
//
//   - column-level: source UDT name doesn't appear in the canonical
//     catalog (KindRaw fallthrough — vendor-specific types like PG
//     inet/cidr/macaddr, MSSQL hierarchyid)
//   - finalization-level: GenerateFinalizationDDL returned the
//     ErrUnsupportedDDL sentinel (vendor index features — clustered,
//     covering, filtered)
//
// The chain implements all four type-mapper interfaces by satisfying
// each method with a "try deterministic, fall back to AI" decision.
// The create path is deliberately excluded from AI fallback. SMT owns
// CREATE TABLE, column, and primary-key rendering; unsupported create inputs
// must return SMT's explicit policy rather than allowing a second renderer to
// synthesize different SQL.
//
// Part of #170 (AI-second epic #167).

package driver

import (
	"context"
	"errors"
	"fmt"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/typemap"
)

// UnmappedAction selects what the chain does at column-level when the
// deterministic mapper returns Raw AND no AI fallback is configured.
// Mirrors the migration.unmapped_type_action config knob.
type UnmappedAction string

const (
	// UnmappedActionFail emits an empty SQL type for unmapped columns,
	// causing downstream DDL emission to fail visibly. The default —
	// safer than silent skip; users explicitly opt into degraded modes.
	UnmappedActionFail UnmappedAction = "fail"

	// UnmappedActionSkip emits empty (same as Fail today since the
	// TypeMapper interface can't carry a "skip this column" signal;
	// future writer changes can interpret empty as "skip" rather
	// than "fail").
	UnmappedActionSkip UnmappedAction = "skip"

	// UnmappedActionConservativeText emits the target dialect's most-
	// permissive text type (NVARCHAR(MAX) on MSSQL, TEXT on PG/MySQL).
	// Lossy but lets the migration progress when no AI is available.
	UnmappedActionConservativeText UnmappedAction = "conservative-text"
)

// ApproxAction preserves the migration.approx_type_action configuration
// contract. CREATE TABLE generation is now SMT-owned and deliberately ignores
// this setting; it remains on FallbackChain while later DDL milestones retain
// their existing compatibility surface.
type ApproxAction string

const (
	// ApproxActionDeterministic is retained as a valid configuration value.
	ApproxActionDeterministic ApproxAction = "deterministic"

	// ApproxActionAIFallback is retained as a valid configuration value. It no
	// longer authorizes an AI-generated CREATE TABLE statement.
	ApproxActionAIFallback ApproxAction = "ai_fallback"
)

// FallbackChain is the decorator that routes between deterministic
// and AI mappers. The fallback is nil-safe: a chain with no AI mapper
// is functionally equivalent to the deterministic mapper alone, plus
// the unmapped-type-action behavior for Raw columns.
type FallbackChain struct {
	primary      *DeterministicMapper
	fallback     TypeMapper // typically *AITypeMapper; TypeMapper for testability
	action       UnmappedAction
	approxAction ApproxAction
}

// NewFallbackChain builds a chain. Pass nil for fallback when AI isn't
// configured — the chain still works, just without AI routing.
//
// approxAction remains normalized for compatibility with existing config and
// logs. It does not affect SMT-owned CREATE TABLE generation.
func NewFallbackChain(primary *DeterministicMapper, fallback TypeMapper, action UnmappedAction, approxAction ApproxAction) *FallbackChain {
	if action == "" {
		action = UnmappedActionFail
	}
	if approxAction == "" {
		if fallback != nil {
			approxAction = ApproxActionAIFallback
		} else {
			approxAction = ApproxActionDeterministic
		}
	}
	return &FallbackChain{
		primary:      primary,
		fallback:     fallback,
		action:       action,
		approxAction: approxAction,
	}
}

// MapType implements TypeMapper. Inspects the canonical kind first to
// decide routing: Raw types (vendor-specific, not in the catalog) go
// to AI when configured; everything else takes the deterministic path.
func (c *FallbackChain) MapType(info TypeInfo) string {
	col := typeInfoToTypemapColumn(info)
	canonical := typemap.ToCanonical(col, info.SourceDBType)

	if canonical.Kind == typemap.KindRaw {
		if c.fallback != nil {
			logging.Debug("typemap chain: routing Raw type %q to AI fallback", info.DataType)
			observability.RecordFallback(observability.SurfaceTypemap,
				fmt.Sprintf("%s:%s", info.SourceDBType, info.DataType))
			return c.fallback.MapType(info)
		}
		return c.handleUnmapped(info)
	}

	return typemap.FromCanonical(canonical, info.TargetDBType).SQLType
}

// CanMap implements TypeMapper. Returns true when either the primary
// or the fallback can handle the pair.
func (c *FallbackChain) CanMap(sourceDBType, targetDBType string) bool {
	if c.primary.CanMap(sourceDBType, targetDBType) {
		return true
	}
	if c.fallback != nil {
		return c.fallback.CanMap(sourceDBType, targetDBType)
	}
	return false
}

// SupportedTargets implements TypeMapper. Reports the union of the
// primary's targets and the fallback's targets.
func (c *FallbackChain) SupportedTargets() []string {
	seen := map[string]struct{}{}
	var out []string
	for _, t := range c.primary.SupportedTargets() {
		if _, ok := seen[t]; !ok {
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}
	if c.fallback != nil {
		for _, t := range c.fallback.SupportedTargets() {
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				out = append(out, t)
			}
		}
	}
	return out
}

// GenerateTableDDL implements TableTypeMapper through the SMT create boundary.
// It intentionally never delegates table DDL to the AI fallback: callers must
// receive SMT's deterministic SQL or its public unsupported-feature policy.
// Column-level MapType and later finalization DDL retain their established
// fallback behavior until their separate ownership milestones.
func (c *FallbackChain) GenerateTableDDL(ctx context.Context, req TableDDLRequest) (*TableDDLResponse, error) {
	return c.primary.GenerateTableDDL(ctx, req)
}

// GenerateFinalizationDDL implements FinalizationDDLMapper. Routes to
// AI fallback when the deterministic mapper returns ErrUnsupportedDDL
// (the sentinel for "vendor-specific feature outside the deterministic
// catalog"). Other errors propagate — they typically mean malformed
// input that AI can't recover either.
func (c *FallbackChain) GenerateFinalizationDDL(ctx context.Context, req FinalizationDDLRequest) (string, error) {
	sql, err := c.primary.GenerateFinalizationDDL(ctx, req)
	if err == nil {
		return sql, nil
	}
	if !errors.Is(err, ErrUnsupportedDDL) || c.fallback == nil {
		return "", err
	}
	finalMapper, ok := c.fallback.(FinalizationDDLMapper)
	if !ok {
		return "", fmt.Errorf("deterministic flagged unsupported DDL (%w) and AI fallback doesn't implement FinalizationDDLMapper", err)
	}
	logging.Debug("typemap chain: routing %s DDL to AI fallback (deterministic flagged unsupported)", req.Type)
	observability.RecordFallback(observability.SurfaceDDL, "finalization:"+string(req.Type))
	return finalMapper.GenerateFinalizationDDL(ctx, req)
}

// GenerateDropTableDDL implements TableDropDDLMapper. Always routed
// through the deterministic path — DROP TABLE is dialect-uniform and
// has no vendor-specific surface that benefits from AI.
func (c *FallbackChain) GenerateDropTableDDL(ctx context.Context, req DropTableDDLRequest) (string, error) {
	return c.primary.GenerateDropTableDDL(ctx, req)
}

// handleUnmapped implements the unmapped_type_action knob for column-
// level Raw types when no AI fallback is configured. Returns the
// SQL type string the chain emits in lieu of the missing mapping.
//
// The default (unknown action) case warns and falls back to fail
// semantics rather than silently emitting empty — without this guard,
// a config typo would silently produce invalid DDL with no visible
// cause (Copilot review on PR #192). Note: config validation
// (config.validate) also rejects unknown actions at load time, so
// this default branch should only fire if the chain is constructed
// programmatically with an unrecognized action string.
func (c *FallbackChain) handleUnmapped(info TypeInfo) string {
	switch c.action {
	case UnmappedActionConservativeText:
		return conservativeTextType(info.TargetDBType)
	case UnmappedActionSkip, UnmappedActionFail:
		// Both emit empty today — the TypeMapper interface can't carry
		// a "skip this column" signal. Downstream DDL emission will
		// fail visibly, which is "fail" semantics; future writer
		// changes can interpret empty as "skip" when the action is
		// UnmappedActionSkip.
		logging.Warn("typemap chain: no mapping for source type %q on target %s and no AI fallback configured (action=%s)",
			info.DataType, info.TargetDBType, c.action)
		return ""
	default:
		logging.Warn("typemap chain: unknown UnmappedAction %q — treating as fail; check config.migration.unmapped_type_action",
			c.action)
		return ""
	}
}

// conservativeTextType returns the target dialect's most-permissive
// text type. Used by UnmappedActionConservativeText as a last-resort
// fallback when neither deterministic nor AI can handle the column.
func conservativeTextType(targetDialect string) string {
	switch targetDialect {
	case typemap.DialectMSSQL:
		return "NVARCHAR(MAX)"
	case typemap.DialectMySQL:
		return "LONGTEXT"
	case typemap.DialectPostgres:
		return "TEXT"
	default:
		return "TEXT"
	}
}

// LogTypeMapperInit emits a debug log line identifying which type
// mapper is in use. Called from per-driver writer constructors at
// startup so a reviewer reading the log can see whether the
// deterministic-only path, AI-only path, or fallback chain is active
// without grepping the source.
//
// Exported (capitalized) so writers in sibling packages
// (now the generic catalog engine) can call it.
func LogTypeMapperInit(m TypeMapper) {
	switch t := m.(type) {
	case *FallbackChain:
		if t.fallback != nil {
			if ai, ok := t.fallback.(*AITypeMapper); ok {
				logging.Debug("type mapper: deterministic + AI fallback (provider=%s model=%s, cache=%d, approx=%s)",
					ai.ProviderName(), ai.Model(), ai.CacheSize(), t.approxAction)
				return
			}
			logging.Debug("type mapper: deterministic + non-AI fallback (approx=%s)", t.approxAction)
			return
		}
		logging.Debug("type mapper: deterministic only (action=%s, no AI fallback)", t.action)
	case *DeterministicMapper:
		logging.Debug("type mapper: deterministic only (raw, no chain)")
	case *AITypeMapper:
		logging.Debug("type mapper: AI only (provider=%s model=%s, cache=%d)",
			t.ProviderName(), t.Model(), t.CacheSize())
	default:
		logging.Debug("type mapper: unknown concrete type %T", m)
	}
}
