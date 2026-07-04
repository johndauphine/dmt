package config

import (
	"strings"
	"testing"
)

// TestValidate_DriftGate pins the drift_gate block's validation contract
// (#575): the section is optional; when present it must name exactly one of
// smt_config / smt_profile and a non-negative timeout.
func TestValidate_DriftGate(t *testing.T) {
	base := func() *Config {
		return &Config{
			Source: SourceConfig{
				Type: "postgres", Host: "src", Port: 5432, Database: "d",
				User: "u", Password: "p",
			},
			Target: TargetConfig{
				Type: "mssql", Host: "tgt", Port: 1433, Database: "d",
				User: "u", Password: "p",
			},
			Migration: MigrationConfig{TargetMode: "drop_recreate"},
		}
	}

	cases := []struct {
		name    string
		gate    *DriftGateConfig
		wantErr string // empty = expect valid
	}{
		{name: "absent section is valid", gate: nil},
		{name: "config path valid", gate: &DriftGateConfig{SMTConfig: "pair.yaml"}},
		{name: "profile valid", gate: &DriftGateConfig{SMTProfile: "prod"}},
		{
			name:    "neither locator rejected",
			gate:    &DriftGateConfig{},
			wantErr: "exactly one of smt_config or smt_profile",
		},
		{
			name:    "both locators rejected",
			gate:    &DriftGateConfig{SMTConfig: "pair.yaml", SMTProfile: "prod"},
			wantErr: "mutually exclusive",
		},
		{
			name:    "negative timeout rejected",
			gate:    &DriftGateConfig{SMTConfig: "pair.yaml", TimeoutSeconds: -1},
			wantErr: "timeout_seconds",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			cfg.Migration.DriftGate = tc.gate
			err := cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate() = nil, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validate() = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}
