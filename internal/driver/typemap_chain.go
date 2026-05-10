// FallbackChain decorates the deterministic type mapper with the AI
// type mapper as a registered fallback for the cases the deterministic
// path can't handle:
//
//   - column-level: source UDT name doesn't appear in the canonical
//     catalog (KindRaw fallthrough — vendor-specific types like PG
//     inet/cidr/macaddr, MSSQL hierarchyid)
//   - table-level: GenerateTableDDL returned a non-nil error (e.g. an
//     unsupported source/target dialect that AI may still cover)
//   - finalization-level: GenerateFinalizationDDL returned the
//     ErrUnsupportedDDL sentinel (vendor index features — clustered,
//     covering, filtered)
//
// The chain implements all four type-mapper interfaces by satisfying
// each method with a "try deterministic, fall back to AI" decision.
// Most lossy translations stay on the deterministic path with an
// IsApproximate warning rather than invoking AI; the fallback is
// scoped narrowly to cases where the deterministic path cannot
// faithfully emit DDL.
//
// Part of #170 (AI-second epic #167).

package driver

import (
	"context"
	"errors"
	"fmt"

	"github.com/johndauphine/dmt/internal/logging"
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

// ApproxAction selects what the chain does at table-level when at least
// one column maps deterministically but with IsApproximate=true (a
// known-lossy mapping — e.g. PG INTERVAL → MSSQL NVARCHAR(255), PG
// ENUM → VARCHAR(255), JSONB → MySQL JSON). Mirrors the
// migration.approx_type_action config knob (#197).
type ApproxAction string

const (
	// ApproxActionDeterministic keeps the deterministic mapping and
	// emits a one-time INFO log per migration listing affected approx
	// columns so the user can decide whether to flip the knob.
	// NewFallbackChain selects this when no AI fallback is available
	// (no AI to route to); users with AI configured can also force
	// this explicitly.
	ApproxActionDeterministic ApproxAction = "deterministic"

	// ApproxActionAIFallback routes any table containing approx columns
	// through the AI fallback (same path as Raw-bearing tables). AI
	// sees the whole table and may produce better DDL — at the cost
	// of an AI call per affected table. NewFallbackChain selects this
	// by default when AI is configured (issue #209 — consistent with
	// how Raw / table-DDL-error / finalization-error paths default-on
	// when AI is available); users opt out via explicit
	// ApproxActionDeterministic.
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
// approxAction defaults to:
//   - ApproxActionAIFallback when an AI fallback is available (#209 —
//     consistent with the rest of the codebase: Raw / table-DDL-error /
//     finalization-error / error-diagnosis paths all default-on when
//     AI is configured. Configuring AI is an implicit opt-in to AI
//     features; users opt out by setting the knob explicitly.)
//   - ApproxActionDeterministic when no AI fallback is available
//     (no AI to route to → can't use it).
//
// Pass an explicit ApproxAction to override (still respected — users
// who want deterministic-on-AI-configured can set
// migration.approx_type_action: deterministic in their config).
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

// GenerateTableDDL implements TableTypeMapper. Two routing decisions
// matter here:
//
//  1. Pre-check: if any column maps to KindRaw, the deterministic
//     mapper would silently pass the source UDT name through verbatim
//     into the target DDL — emitting e.g. `[ip] INET` when PG inet
//     targets MSSQL, which is invalid MSSQL. Codex review on PR #170
//     caught this. Detect Raw columns up front and route the entire
//     table to AI fallback when configured (AI sees the whole table
//     and can make holistic decisions). When no AI is configured,
//     return an error naming the offending columns.
//
//  2. After the Raw-column gate, run the deterministic path. If
//     deterministic returns an error (typically unsupported
//     source/target combination), fall back to AI when configured;
//     propagate otherwise.
//
// Note: the conservative-text unmapped action is currently a no-op
// for the table-level path — substituting Raw column types in
// deterministic-generated DDL would require post-process string
// manipulation. Tracked as a follow-up; today, conservative-text
// only takes effect for the column-level MapType path.
func (c *FallbackChain) GenerateTableDDL(ctx context.Context, req TableDDLRequest) (*TableDDLResponse, error) {
	if rawCols := c.findRawColumns(req); len(rawCols) > 0 {
		if c.fallback != nil {
			if tableMapper, ok := c.fallback.(TableTypeMapper); ok {
				logging.Debug("typemap chain: routing GenerateTableDDL to AI fallback (Raw columns: %v)", rawCols)
				return tableMapper.GenerateTableDDL(ctx, req)
			}
		}
		return nil, fmt.Errorf("table %q has columns with unmapped (Raw) types %v and no AI fallback is configured (action=%s)",
			req.SourceTable.Name, rawCols, c.action)
	}

	// #197: optional routing for approximate (lossy-but-mapped) columns.
	// The Raw check above handles the "no mapping" case; this handles
	// the "mapping exists but lossy" case (e.g. PG INTERVAL → MSSQL
	// NVARCHAR(255)). Action selected per-chain in NewFallbackChain:
	// ai_fallback when AI is configured (#209 — implicit opt-in),
	// deterministic when AI isn't, with explicit overrides respected.
	// On the deterministic branch (or when ai_fallback was requested
	// but the AI mapper doesn't implement TableTypeMapper), log once
	// per table naming the approx columns.
	if approxCols := c.findApproximateColumns(req); len(approxCols) > 0 {
		if c.approxAction == ApproxActionAIFallback && c.fallback != nil {
			if tableMapper, ok := c.fallback.(TableTypeMapper); ok {
				logging.Debug("typemap chain: routing GenerateTableDDL to AI fallback (approx columns: %v)", approxCols)
				return tableMapper.GenerateTableDDL(ctx, req)
			}
		}
		// Telemetry-only path: deterministic stands but inform the
		// user. INFO not WARN — approx mappings are intentional
		// fidelity trade-offs, not errors. Once per table per migration;
		// the chain has no per-migration state so per-table is the
		// finest grain available without a bigger refactor.
		//
		// The message changes based on whether the user already opted
		// in: telling someone with ai_fallback set to "set the knob"
		// is misleading — they did set it, but AI wasn't available
		// (no provider configured, or the AI mapper doesn't implement
		// TableTypeMapper). Distinguish the two cases (Copilot review
		// on PR #208).
		if c.approxAction == ApproxActionAIFallback {
			logging.Info("typemap chain: table %q used %d approximate (lossy) deterministic mapping(s) for column(s) %v; "+
				"approx_type_action=ai_fallback was requested but no AI fallback is available (provider not configured or mapper doesn't support table-level routing) — used deterministic mapping",
				req.SourceTable.Name, len(approxCols), approxCols)
		} else {
			logging.Info("typemap chain: table %q used %d approximate (lossy) deterministic mapping(s) for column(s) %v; "+
				"set migration.approx_type_action: ai_fallback to route these tables through AI for potentially better DDL",
				req.SourceTable.Name, len(approxCols), approxCols)
		}
	}

	resp, err := c.primary.GenerateTableDDL(ctx, req)
	if err == nil {
		return resp, nil
	}
	if c.fallback == nil {
		return nil, err
	}
	tableMapper, ok := c.fallback.(TableTypeMapper)
	if !ok {
		return nil, fmt.Errorf("deterministic GenerateTableDDL failed (%w) and AI fallback doesn't implement TableTypeMapper", err)
	}
	logging.Debug("typemap chain: routing GenerateTableDDL to AI fallback after deterministic error: %v", err)
	return tableMapper.GenerateTableDDL(ctx, req)
}

// findRawColumns walks the source table's columns and returns the
// names of those whose canonical type is KindRaw (vendor-specific,
// not in the catalog). Used by GenerateTableDDL to detect cases where
// the deterministic path would silently emit invalid cross-dialect
// DDL via verbatim type-name passthrough.
func (c *FallbackChain) findRawColumns(req TableDDLRequest) []string {
	if req.SourceTable == nil {
		return nil
	}
	var raw []string
	for _, col := range req.SourceTable.Columns {
		canonical := typemap.ToCanonical(typemap.ColumnInfo{
			UDTName:  col.DataType,
			DataType: col.DataType,
		}, req.SourceDBType)
		if canonical.Kind == typemap.KindRaw {
			raw = append(raw, col.Name)
		}
	}
	return raw
}

// findApproximateColumns walks the source table's columns and returns
// the names of those whose deterministic mapping is non-Raw but
// IsApproximate (a known-lossy mapping like PG INTERVAL →
// NVARCHAR(255), JSONB → JSON, ENUM → VARCHAR(255)). Used by
// GenerateTableDDL to surface lossy mappings so the user can decide
// whether to enable AI fallback for the affected tables (#197).
//
// Skips Raw columns — those are handled by findRawColumns and routed
// before this gate. Calling this on a Raw-bearing table would double-
// classify those columns; the caller's order-of-checks avoids that.
//
// Populates the same ColumnInfo fields that the actual deterministic
// mapper sees (length/precision/scale) so the pre-scan's IsApproximate
// verdict matches what GenerateColumnDef would emit at DDL time. A
// shallow ColumnInfo would diverge for length-sensitive types — e.g. a
// `varchar(100)` column would be misclassified as nil-Length and emit
// LONGTEXT instead of VARCHAR(100), giving a different IsApproximate
// reading than the real mapper. Copilot review on PR #208.
func (c *FallbackChain) findApproximateColumns(req TableDDLRequest) []string {
	if req.SourceTable == nil {
		return nil
	}
	var approx []string
	for _, col := range req.SourceTable.Columns {
		colInfo := driverColumnToTypemapColumnInfo(col)
		canonical := typemap.ToCanonical(colInfo, req.SourceDBType)
		if canonical.Kind == typemap.KindRaw {
			continue
		}
		ddl := typemap.FromCanonical(canonical, req.TargetDBType)
		if ddl.IsApproximate {
			approx = append(approx, col.Name)
		}
	}
	return approx
}

// driverColumnToTypemapColumnInfo projects a driver.Column into the
// typemap.ColumnInfo shape used by ToCanonical. Mirrors the field
// population in typeInfoToTypemapColumn / driverColumnToDDL so any
// helper that needs to ask "what would the deterministic mapper do
// with this column?" gets the same answer those production paths do.
func driverColumnToTypemapColumnInfo(col Column) typemap.ColumnInfo {
	return typemap.ColumnInfo{
		UDTName:                col.DataType,
		DataType:               col.DataType,
		CharacterMaximumLength: nullableInt(col.MaxLength),
		NumericPrecision:       nullableInt(col.Precision),
		NumericScale:           nullableInt(col.Scale),
	}
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
// (driver/postgres, driver/mssql, driver/mysql) can call it.
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
