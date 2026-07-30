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

// Package interceptors provides gRPC interceptors that record zfs_csi_*
// Prometheus metrics and emit structured logs for every CSI RPC. Tracing spans
// are handled separately by the otelgrpc StatsHandler installed alongside.
package interceptors

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/randomvariable/zfs-csi/internal/observability/metrics"
)

// statusLabel maps a gRPC status code to a coarse outcome label.
func statusLabel(err error) string {
	if err == nil {
		return "ok"
	}

	switch grpcstatus.Code(err) {
	case codes.OK:
		return "ok"
	case codes.DeadlineExceeded, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.Canceled:
		return "aborted"
	default:
		return "error"
	}
}

// Unary returns a unary server interceptor recording metrics + logs.
func Unary(log logr.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		elapsed := time.Since(start).Seconds()
		method := info.FullMethod
		outcome := statusLabel(err)

		metrics.CSIRPCTotal.WithLabelValues(method, outcome).Inc()
		metrics.CSIRPCDurationSeconds.WithLabelValues(method).Observe(elapsed)

		if err != nil {
			log.Error(err, "csi rpc", "method", method, "status", outcome, "duration_seconds", elapsed)
		} else {
			log.V(1).Info("csi rpc", "method", method, "status", outcome, "duration_seconds", elapsed)
		}

		return resp, err
	}
}

// Stream returns a stream server interceptor recording metrics + logs.
func Stream(log logr.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		err := handler(srv, ss)

		elapsed := time.Since(start).Seconds()
		method := info.FullMethod
		outcome := statusLabel(err)

		metrics.CSIRPCTotal.WithLabelValues(method, outcome).Inc()
		metrics.CSIRPCDurationSeconds.WithLabelValues(method).Observe(elapsed)

		if err != nil {
			log.Error(err, "csi stream rpc", "method", method, "status", outcome, "duration_seconds", elapsed)
		} else {
			log.V(1).Info("csi stream rpc", "method", method, "status", outcome, "duration_seconds", elapsed)
		}

		return err
	}
}
