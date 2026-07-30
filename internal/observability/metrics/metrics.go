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

// Package metrics defines the Prometheus collectors for the zfs-csi driver.
//
// Metrics follow Kubernetes SIG Instrumentation conventions: stable names with
// the zfs_csi_ prefix, registered on the controller-runtime Prometheus registry
// (already served at --metrics-bind-address).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metric namespace. Stable, prefixed, lowercase — per K8s instrumentation
// guidelines (avoid collisions, grep-friendly).
const namespace = "zfs_csi"

var (
	// CSIRPCTotal counts CSI gRPC RPCs by method and outcome.
	// Labels: method (CreateVolume, DeleteVolume, ...), status (ok|error|aborted).
	CSIRPCTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "rpc",
			Name:      "total",
			Help:      "Total number of CSI gRPC RPCs by method and status.",
		},
		[]string{"method", "status"},
	)

	// CSIRPCDurationSeconds histograms per-method RPC latency.
	CSIRPCDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "rpc",
			Name:      "duration_seconds",
			Help:      "CSI gRPC RPC latency in seconds, by method.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// ZFSOperationsTotal counts backend ZFS operations (create/destroy/expand/
	// clone/loadkey/unloadkey) by operation and outcome.
	ZFSOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "zfs",
			Name:      "operations_total",
			Help:      "Total ZFS backend operations by type and status.",
		},
		[]string{"operation", "status"},
	)

	// TransportOperationsTotal counts transport export/unexport ops by transport
	// (nvmet_tcp, nfs) and outcome.
	TransportOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "transport",
			Name:      "operations_total",
			Help:      "Total transport export/unexport operations by transport and status.",
		},
		[]string{"transport", "operation", "status"},
	)

	// CryptoOperationsTotal counts key-provider operations (generate/fetch/delete/
	// stage/shred) by outcome.
	CryptoOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "crypto",
			Name:      "operations_total",
			Help:      "Total key-provider (crypto) operations by type and status.",
		},
		[]string{"operation", "status"},
	)

	// VolumeStateGauge reports the count of Volume CRs observed in each state.
	// Label: state (Pending|Creating|Ready|Expanding|Deleting|Error).
	VolumeStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "volumes",
			Name:      "by_state",
			Help:      "Number of Volume CRs in each lifecycle state.",
		},
		[]string{"state"},
	)
)

func init() {
	// Register on the controller-runtime registry so all metrics are served at
	// the single --metrics-bind-address endpoint alongside controller_runtime_*
	// and workqueue metrics.
	metrics.Registry.MustRegister(
		CSIRPCTotal,
		CSIRPCDurationSeconds,
		ZFSOperationsTotal,
		TransportOperationsTotal,
		CryptoOperationsTotal,
		VolumeStateGauge,
	)
}
