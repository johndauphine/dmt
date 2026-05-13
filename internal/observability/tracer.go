package observability

import (
	"context"
	"fmt"
	"net/url"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// SetupTracer installs a global OpenTelemetry tracer that exports spans
// to the given OTLP HTTP endpoint (#229). The endpoint should be a URL
// like "http://otel-collector:4318" — the OTLP HTTP exporter appends
// "/v1/traces" itself.
//
// Returns a shutdown function that flushes pending spans (5s typical
// timeout). Callers must defer it before the process exits — otherwise
// trailing spans never reach the collector.
//
// Failures don't propagate up beyond the returned error; the caller can
// log and continue without tracing rather than failing the migration.
func SetupTracer(ctx context.Context, endpoint string) (func(context.Context) error, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid OTLP endpoint %q: %w", endpoint, err)
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(u.Host),
	}
	// OTLP HTTP transport defaults to TLS; honor the scheme so plain http://
	// dev setups work without an extra flag.
	if u.Scheme == "http" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("dmt"),
		),
	)
	if err != nil {
		// resource.New with only ServiceName never fails in practice;
		// be defensive anyway so the tracer at least gets a default
		// resource instead of panicking.
		res = resource.Default()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// Tracer returns the dmt tracer for span creation. Use this everywhere
// you'd otherwise call otel.Tracer("dmt") so the instrumentation library
// name stays consistent across the codebase.
func Tracer() Tracer_t {
	return Tracer_t{t: otel.Tracer("dmt")}
}
