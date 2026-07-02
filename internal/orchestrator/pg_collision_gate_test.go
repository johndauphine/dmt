package orchestrator

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/source"
)

// collidingTables returns two tables whose names both sanitize to
// "order_items" under PostgreSQL identifier sanitization.
func collidingTables() []source.Table {
	return []source.Table{
		{Schema: "dbo", Name: "Order Items", Columns: []source.Column{{Name: "id"}}},
		{Schema: "dbo", Name: "Order-Items", Columns: []source.Column{{Name: "id"}}},
	}
}

// TestEnforcePGIdentifierCollisionGate_KeysOnCanonicalEngine pins #553's
// review finding: the gate must key on the canonical target engine name
// (targetPool.DBType()), not the raw config string, so PostgreSQL selected via
// the "pg"/"postgresql" aliases still triggers the gate — otherwise the
// sanitization/drop_recreate paths (which key on DBType()) run while the gate
// is skipped, reintroducing silent data loss.
func TestEnforcePGIdentifierCollisionGate_KeysOnCanonicalEngine(t *testing.T) {
	tests := []struct {
		name          string
		configType    string // raw config Target.Type (may be an alias)
		poolDBType    string // canonical name reported by the pool
		wantCollision bool
	}{
		{name: "canonical postgres fires", configType: "postgres", poolDBType: "postgres", wantCollision: true},
		{name: "pg alias still fires", configType: "pg", poolDBType: "postgres", wantCollision: true},
		{name: "postgresql alias still fires", configType: "postgresql", poolDBType: "postgres", wantCollision: true},
		{name: "non-postgres target is skipped", configType: "mysql", poolDBType: "mysql", wantCollision: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &Orchestrator{
				config:     &config.Config{Target: config.TargetConfig{Type: tt.configType}},
				targetPool: &targetModeTestPool{dbType: tt.poolDBType},
			}

			err := o.enforcePGIdentifierCollisionGate(collidingTables())
			if tt.wantCollision {
				if err == nil {
					t.Fatalf("expected a collision error for target %q (pool %q), got nil — gate skipped",
						tt.configType, tt.poolDBType)
				}
				if !strings.Contains(err.Error(), "order_items") {
					t.Fatalf("collision error should name the sanitized collision: %v", err)
				}
			} else if err != nil {
				t.Fatalf("non-postgres target %q must skip the gate, got: %v", tt.poolDBType, err)
			}
		})
	}
}

// TestEnforcePGIdentifierCollisionGate_NoCollisionPasses ensures a clean
// PostgreSQL table set is not falsely blocked.
func TestEnforcePGIdentifierCollisionGate_NoCollisionPasses(t *testing.T) {
	o := &Orchestrator{
		config:     &config.Config{Target: config.TargetConfig{Type: "pg"}},
		targetPool: &targetModeTestPool{dbType: "postgres"},
	}
	tables := []source.Table{
		{Schema: "dbo", Name: "orders", Columns: []source.Column{{Name: "id"}}},
		{Schema: "dbo", Name: "customers", Columns: []source.Column{{Name: "id"}}},
	}
	if err := o.enforcePGIdentifierCollisionGate(tables); err != nil {
		t.Fatalf("clean PostgreSQL table set must pass the gate, got: %v", err)
	}
}
