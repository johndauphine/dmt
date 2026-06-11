// Observability wiring shared by the CLI and the TUI (#445): the
// Prometheus /metrics endpoint and OTLP trace exporter (#229). Both
// are off by default and have zero overhead when disabled.
package command

import (
	"context"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

// SetupObservability starts the metrics endpoint and OTLP exporter for
// one migration run. Failures are logged and skipped — observability
// must never fail the migration. The returned stop function tears
// everything down cleanly (close the metrics listener, flush pending
// spans) and must be deferred by the caller.
func SetupObservability(metricsAddr, otelEndpoint string, orch *orchestrator.Orchestrator) func() {
	var stops []func()

	if metricsAddr != "" {
		reg := observability.New()
		if err := reg.Start(metricsAddr); err != nil {
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

	if otelEndpoint != "" {
		shutdown, err := observability.SetupTracer(context.Background(), otelEndpoint)
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
