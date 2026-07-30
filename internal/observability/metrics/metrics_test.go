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

package metrics

import (
	"testing"

	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// TestAllMetricsRegisteredOnControllerRegistry proves every zfs_csi_* collector
// is registered on the controller-runtime Prometheus registry (the one served at
// --metrics-bind-address). Prometheus Gather() does not emit empty CounterVec/
// HistogramVec/GaugeVec families, so each metric is materialized with a label
// set first, then asserted present by name in the gathered families.
func TestAllMetricsRegisteredOnControllerRegistry(t *testing.T) {
	t.Parallel()

	// Materialize one child per collector so the family is gatherable.
	CSIRPCTotal.WithLabelValues("CreateVolume", "ok").Inc()
	CSIRPCDurationSeconds.WithLabelValues("CreateVolume").Observe(0.01)
	ZFSOperationsTotal.WithLabelValues("create", "ok").Inc()
	TransportOperationsTotal.WithLabelValues("nvmet_tcp", "export", "ok").Inc()
	CryptoOperationsTotal.WithLabelValues("generate", "ok").Inc()
	VolumeStateGauge.WithLabelValues("Ready").Set(1)

	want := map[string]bool{
		"zfs_csi_rpc_total":                  false,
		"zfs_csi_rpc_duration_seconds":       false,
		"zfs_csi_zfs_operations_total":       false,
		"zfs_csi_transport_operations_total": false,
		"zfs_csi_crypto_operations_total":    false,
		"zfs_csi_volumes_by_state":           false,
	}

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather registry: %v", err)
	}

	for _, f := range families {
		if _, ok := want[f.GetName()]; ok {
			want[f.GetName()] = true
		}
	}

	for name, present := range want {
		if !present {
			t.Errorf("metric %s not registered on controller-runtime registry", name)
		}
	}
}
