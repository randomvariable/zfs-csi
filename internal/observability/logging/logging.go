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

// Package logging provides LogWith: a builder for operation-scoped structured
// logging of side effects.
//
// Usage pattern — accumulate context up front, then push exactly one log entry
// on the outcome:
//
//	op := logging.LogWith(log, "zfs create", "dataset", dataset, "capacity", cap)
//	if err := r.ZFS.Create(ctx, opts); err != nil {
//	    op.Failed(err)            // error-level log with all accumulated keys
//	    return ...
//	}
//	op.OK()                        // info-level log with all accumulated keys
//
// Optional .Metric(counter, labels...) couples the operation to a Prometheus
// counter: OK() records status="ok", Failed(err) records status="error". This
// closes the gap between operational logging and the zfs_csi_*_operations_total
// metrics with a single call site.
package logging

import (
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
)

// OpLog accumulates key-value context for a single side-effecting operation and
// emits exactly one log entry when OK or Failed is called.
type OpLog struct {
	log     logr.Logger
	msg     string
	keys    []any
	counter *prometheus.CounterVec
	labels  []string // counter label values, excluding the trailing status
	started time.Time
}

// LogWith starts an operation log. The msg describes the operation (e.g.
// "zfs create", "transport export", "crypto-shred DEK"); keysAndValues are the
// structured context accumulated for the eventual log entry.
func LogWith(log logr.Logger, msg string, keysAndValues ...any) *OpLog {
	return &OpLog{log: log, msg: msg, keys: append([]any(nil), keysAndValues...), started: time.Now()}
}

// With adds more key-value context to the accumulated set. Returns the same
// OpLog so it can be chained at construction.
func (o *OpLog) With(keysAndValues ...any) *OpLog {
	o.keys = append(o.keys, keysAndValues...)

	return o
}

// Metric attaches a Prometheus counter that OK/Failed will record. labels are
// the counter's label values EXCLUDING the trailing "status" label, which
// OK/Failed append as "ok"/"error".
func (o *OpLog) Metric(c *prometheus.CounterVec, labels ...string) *OpLog {
	o.counter = c
	o.labels = append([]string(nil), labels...)

	return o
}

// OK pushes an info-level success log entry with all accumulated keys and, if a
// metric was attached, records status="ok".
func (o *OpLog) OK() {
	o.log.Info(o.msg, o.keys...)
	o.record("ok")
}

// Failed pushes an error-level failure log entry (carrying err) with all
// accumulated keys and, if a metric was attached, records status="error".
func (o *OpLog) Failed(err error) {
	o.log.Error(err, o.msg, o.keys...)
	o.record("error")
}

func (o *OpLog) record(status string) {
	OperationDurationSeconds.WithLabelValues(operationLabel(o.msg), status).Observe(time.Since(o.started).Seconds())

	if o.counter == nil {
		return
	}

	vals := make([]string, 0, len(o.labels)+1)
	vals = append(vals, o.labels...)
	vals = append(vals, status)
	o.counter.WithLabelValues(vals...).Inc()
}

// operationLabel rejects arbitrary log messages as Prometheus labels. The
// complete set is declared in keys.go; future ad-hoc logs land in "other".
func operationLabel(msg string) string {
	switch msg {
	case OpZFSCreate, OpZFSClone, OpZFSDestroy, OpZFSExpand, OpZFSShare,
		OpZFSSetProperty, OpZFSExists, OpZFSSnapshot, OpZFSDestroySnapshot,
		OpZFSLoadKey, OpZFSUnloadKey, OpZFSKeyStatus,
		OpTransportExport, OpTransportUnexport, OpTransportMapInitiator,
		OpTransportUnmapInitiator, OpTransportQuery, OpTransportForceDisconnect,
		OpCryptoFetch, OpCryptoStage, OpCryptoShred, OpCryptoDelete,
		OpCreateVolumeCR, OpPatchVolumeCR, OpDeleteVolumeCR, OpPatchVolumeStatus,
		OpPatchSnapshotStatus, OpCreateSnapshotCR, OpDeleteSnapshotCR,
		OpNFSMount, OpBlockAttach, OpBlockDetach, OpBlockFormat, OpBlockMount,
		OpBindMount, OpUnmountStaging, OpUnmountTarget, OpResize:
		return msg
	default:
		return "other"
	}
}
