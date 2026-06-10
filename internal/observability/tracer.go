package observability

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"net"
	"net/url"
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
	if err != nil {
		return nil, fmt.Errorf("invalid OTLP endpoint %q: %w", endpoint, err)
	}
	// Avoid `%w` on a nil err — Copilot review caught this would produce
	// "...: %!w(<nil>)" and break errors.Unwrap. Emit a clean sentinel.
	if u.Host == "" {
		return nil, fmt.Errorf("invalid OTLP endpoint %q: missing host", endpoint)
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
func Tracer() TracerT {
	return TracerT{t: otel.Tracer("dmt")}
}

// TracerT wraps the OTel tracer with dmt-flavored helpers so call sites
// don't have to import OTel themselves.
type TracerT struct {
	t trace.Tracer
}

// Span is a thin alias for trace.Span. Returned by StartSpan; ends via
// span.End(). Use End()-defer-immediately-after-StartSpan pattern so
// errors don't escape without span lifecycle ending.
type Span = trace.Span

// StartSpan starts a new span named name as a child of any span already
// in ctx. Returns the new ctx (which downstream code should propagate
// so children attach correctly) and the span itself (call End() in a
// defer).
//
// kvs is an alternating key/value list — matches the logging.Event
// convention so the same attribute can be passed to both:
//
//	ctx, span := obs.Tracer().StartSpan(ctx, "phase.transfer", "run_id", runID)
//	defer span.End()
//
// Odd-length kvs is tolerated: the dangling value lands under "_dangling"
// (same as Event()).
func (t TracerT) StartSpan(ctx context.Context, name string, kvs ...any) (context.Context, Span) {
	if t.t == nil {
		// Should never happen — Tracer() always returns a valid OTel
		// tracer (no-op when the global provider isn't set). Be defensive
		// anyway so callers don't need nil checks.
		return ctx, noopSpan{}
	}
	ctx, span := t.t.Start(ctx, name)
	if len(kvs) > 0 {
		span.SetAttributes(kvsToAttrs(kvs)...)
	}
	return ctx, span
}

// AddEvent attaches a named event with optional key/value attributes to
// the span currently in ctx. Used for per-chunk milestones inside a
// table span (#229) — events are cheap and don't cause span explosion
// the way per-chunk spans would on large migrations.
func AddEvent(ctx context.Context, name string, kvs ...any) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	if len(kvs) > 0 {
		span.AddEvent(name, trace.WithAttributes(kvsToAttrs(kvs)...))
	} else {
		span.AddEvent(name)
	}
}

// RecordError marks the current span as failed with the supplied error.
// Use in error-handling paths so the trace makes it obvious which span
// faulted.
func RecordError(ctx context.Context, err error) {
	if err == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// kvsToAttrs converts the alternating-pair convention into OTel
// attributes. Matches logging.mergeAttrs() so the same call-site kvs list
// decorates logs AND spans identically — including non-string keys,
// which are stringified rather than silently dropped (Copilot review).
func kvsToAttrs(kvs []any) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(kvs)/2)
	for i := 0; i+1 < len(kvs); i += 2 {
		key, ok := kvs[i].(string)
		if !ok {
			// Mirror logging.mergeAttrs: stringify non-string keys so an
			// awkward call site still produces a visible attribute rather
			// than silently dropping its value.
			key = fmt.Sprintf("%v", kvs[i])
		}
		out = append(out, anyToAttr(key, kvs[i+1]))
	}
	if len(kvs)%2 == 1 {
		out = append(out, anyToAttr("_dangling", kvs[len(kvs)-1]))
	}
	return out
}

func anyToAttr(key string, val any) attribute.KeyValue {
	switch v := val.(type) {
	case string:
		return attribute.String(key, v)
	case bool:
		return attribute.Bool(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	default:
		return attribute.String(key, anyToString(v))
	}
}

// anyToString turns an arbitrary value into a useful string attribute.
// Important call sites pass errors (which implement Error() not String())
// and unstructured maps; fmt.Sprint handles both without silently dropping
// the value (Copilot review).
func anyToString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case error:
		return t.Error()
	case interface{ String() string }:
		return t.String()
	default:
		return fmt.Sprint(v)
	}
}

// noopSpan is the fallback returned when a tracer hasn't been initialized.
// All methods are no-ops — call sites can blindly call span.End() etc.
type noopSpan struct{ trace.Span }

func (noopSpan) End(...trace.SpanEndOption)              {}
func (noopSpan) AddEvent(string, ...trace.EventOption)   {}
func (noopSpan) IsRecording() bool                       { return false }
func (noopSpan) RecordError(error, ...trace.EventOption) {}
func (noopSpan) SetStatus(codes.Code, string)            {}
func (noopSpan) SetName(string)                          {}
func (noopSpan) SetAttributes(...attribute.KeyValue)     {}
func (noopSpan) SpanContext() trace.SpanContext          { return trace.SpanContext{} }
func (noopSpan) TracerProvider() trace.TracerProvider    { return nil }

// newListener creates a TCP listener for the metrics endpoint. Exists as
// a separate function so tests can use port 0 + the returned listener's
// Addr() to discover the bound port without racing on a hardcoded port.
func newListener(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}
