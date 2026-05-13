package main

import (
	"context"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

// setupObservability wires the Prometheus /metrics endpoint and OTLP
// trace exporter based on the --metrics-addr / --otel-endpoint flags (#229).
// Both are off by default and have zero overhead when disabled.
//
// The returned stop function tears everything down cleanly — close the
// metrics HTTP server, flush pending OTLP spans — and must be deferred
// by the caller so SIGTERM doesn't leak the listener port.
func setupObservability(c *cli.Context, orch *orchestrator.Orchestrator) func() {
	var stops []func()

	if addr := c.String("metrics-addr"); addr != "" {
		reg := observability.New()
		if err := reg.Start(addr); err != nil {
			// Failing the metrics endpoint shouldn't fail the migration —
			// the operator's SRE stack will notice the missing scrape and
			// the migration will continue to log normally.
			logging.Warn("metrics endpoint disabled: %v", err)
		} else {
			orch.SetMetrics(reg)
			stops = append(stops, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := reg.Stop(ctx); err != nil {
					logging.Warn("metrics server shutdown error: %v", err)
				}
			})
		}
	}

	if endpoint := c.String("otel-endpoint"); endpoint != "" {
		shutdown, err := observability.SetupTracer(context.Background(), endpoint)
		if err != nil {
			// Same posture as the metrics path — log and continue.
			logging.Warn("OTLP tracer disabled: %v", err)
		} else {
			stops = append(stops, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = shutdown(ctx)
			})
		}
	}

	return func() {
		// Tear down in reverse order so the tracer flushes before the
		// metrics server stops accepting scrapes (a final scrape may
		// arrive during shutdown; we want it served).
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
}
