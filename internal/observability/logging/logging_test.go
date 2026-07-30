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

package logging

import (
	"errors"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/randomvariable/zfs-csi/internal/observability/metrics"
)

// TestLogWithMetricCoupling proves the headline behaviour: when LogWith is
// given a Metric, calling OK()/Failed() both logs AND increments the registered
// Prometheus counter with the correct status label. This is the coupling that
// lets operational logging drive the zfs_csi_*_operations_total metrics from a
// single call site.
func TestLogWithMetricCoupling(t *testing.T) {
	t.Parallel()

	// Use the real registered counter so we also prove it is on the registry.
	counter := metrics.ZFSOperationsTotal

	op := LogWith(logr.Discard(), OpZFSCreate, KeyDataset, "tank/csi/test").
		Metric(counter, "create")

	op.OK()

	if got := counterValue(t, counter, "create", "ok"); got != 1 {
		t.Fatalf("after OK: zfs_csi_zfs_operations_total{create,ok} = %v, want 1", got)
	}

	op2 := LogWith(logr.Discard(), OpZFSCreate, KeyDataset, "tank/csi/test").
		Metric(counter, "create")
	op2.Failed(errors.New("boom"))

	if got := counterValue(t, counter, "create", "error"); got != 1 {
		t.Fatalf("after Failed: zfs_csi_zfs_operations_total{create,error} = %v, want 1", got)
	}
}

// counterValue reads a single labelled child of a CounterVec via the client_model dto.
func counterValue(t *testing.T, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()

	m := &dto.Metric{}
	observer, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("get counter metric: %v", err)
	}
	metric, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatal("counter observer does not expose a Prometheus metric")
	}
	if err := metric.Write(m); err != nil {
		t.Fatalf("write metric: %v", err)
	}

	if m.Counter == nil || m.Counter.Value == nil {
		return 0
	}

	return m.Counter.GetValue()
}

func TestLogCompletionRecordsBoundedDurationLabels(t *testing.T) {
	before := histogramCount(t, OperationDurationSeconds, OpZFSCreate, "ok")
	LogWith(logr.Discard(), OpZFSCreate).OK()
	if got := histogramCount(t, OperationDurationSeconds, OpZFSCreate, "ok"); got != before+1 {
		t.Fatalf("completed duration samples = %d, want %d", got, before+1)
	}

	before = histogramCount(t, OperationDurationSeconds, "other", "error")
	LogWith(logr.Discard(), "unregistered operation").Failed(errors.New("boom"))
	if got := histogramCount(t, OperationDurationSeconds, "other", "error"); got != before+1 {
		t.Fatalf("other duration samples = %d, want %d", got, before+1)
	}
}

func histogramCount(t *testing.T, vec *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()

	m := &dto.Metric{}
	observer, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("get histogram metric: %v", err)
	}
	metric, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatal("histogram observer does not expose a Prometheus metric")
	}
	if err := metric.Write(m); err != nil {
		t.Fatalf("write metric: %v", err)
	}

	if m.Histogram == nil {
		return 0
	}

	return m.Histogram.GetSampleCount()
}

// TestLogWithoutMetricDoesNotPanic proves OK/Failed are safe when no Metric attached.
func TestLogWithoutMetricDoesNotPanic(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked without metric: %v", r)
		}
	}()

	op := LogWith(logr.Discard(), OpZFSDestroy, KeyDataset, "x")
	op.OK()

	op2 := LogWith(logr.Discard(), OpZFSDestroy, KeyDataset, "x")
	op2.Failed(errors.New("e"))
}
