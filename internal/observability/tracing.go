// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"

	"github.com/randomvariable/zfs-csi/internal/observability/interceptors"
)

// TracingConfig configures the OTLP trace exporter.
type TracingConfig struct {
	// Endpoint is the OTLP gRPC endpoint (host:port). If empty, tracing is
	// disabled (no exporter registered) — the default for dev/CI.
	Endpoint string
	// ServiceName is the OTel resource service.name attribute.
	ServiceName string
}

// InitTracer initialises the global OpenTelemetry tracer provider with an OTLP
// gRPC exporter. It returns a shutdown function the caller MUST invoke on exit.
//
// If Endpoint is empty this is a no-op (tracing disabled) — no exporter, no
// spans exported. This keeps local dev/CI unburdened; set
// OTEL_EXPORTER_OTLP_ENDPOINT in production to enable.
func InitTracer(ctx context.Context, log logr.Logger, cfg TracingConfig) (func(context.Context) error, error) {
	if cfg.Endpoint == "" {
		log.Info("tracing disabled (no OTLP endpoint configured)")

		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(cfg.Endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("build trace resource: %w", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Info("tracing enabled", "endpoint", cfg.Endpoint, "service", cfg.ServiceName)

	return tp.Shutdown, nil
}

// GRPCServerOptions returns grpc.ServerOption values that install:
//   - the otelgrpc StatsHandler (propagates and creates spans for every CSI
//     RPC, emitting them to the configured OTLP exporter), and
//   - unary/stream interceptors that record our custom Prometheus metrics and
//     emit a structured log line per RPC.
//
// Pass the result to grpc.NewServer(...).
func GRPCServerOptions(log logr.Logger) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(interceptors.Unary(log)),
		grpc.StreamInterceptor(interceptors.Stream(log)),
	}
}
