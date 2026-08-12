package main

import (
	"github.com/urfave/cli/v2"

	"github.com/johndauphine/dmt/v5/internal/command"
	"github.com/johndauphine/dmt/v5/internal/orchestrator"
)

// setupObservability wires the Prometheus /metrics endpoint and OTLP
// trace exporter based on the --metrics-addr / --otel-endpoint flags
// (#229). The shared implementation lives in internal/command so TUI
// runs can start the same endpoints from /session keys (#445).
func setupObservability(c *cli.Context, orch *orchestrator.Orchestrator) func() {
	return command.SetupObservability(c.String("metrics-addr"), c.String("otel-endpoint"), orch)
}
