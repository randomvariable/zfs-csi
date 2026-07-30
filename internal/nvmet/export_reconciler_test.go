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

package nvmet

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	obsevents "github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/testutil"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

type testDeps struct {
	crclient.Client
	export *fakeTransportServer
}

func newTestDeps(t *testing.T, objs ...crclient.Object) *testDeps {
	t.Helper()

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := nvmetv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	c := ctrlfake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&nvmetv1.NVMeExport{}).
		Build()

	return &testDeps{Client: c, export: newFakeTransportServer()}
}

func (d *testDeps) reconciler() *ExportReconciler {
	return &ExportReconciler{Client: d.Client, Log: logr.Discard(), Export: d.export}
}

type fakeTransportServer struct {
	exports          map[string]transport.TargetRef
	mapped           map[string]map[string]bool
	exportErr        error
	mappedErr        error
	unexportErr      error
	exportCalls      []transport.ExportOptions
	mapCalls         []string
	unmapCalls       []string
	unexported       []string
	forceDisconnects []string
}

func newFakeTransportServer() *fakeTransportServer {
	return &fakeTransportServer{
		exports: map[string]transport.TargetRef{},
		mapped:  map[string]map[string]bool{},
	}
}

func (f *fakeTransportServer) Export(_ context.Context, opts transport.ExportOptions) (transport.TargetRef, error) {
	f.exportCalls = append(f.exportCalls, opts)
	ref := transport.TargetRef{Kind: opts.Kind, TargetNQN: opts.TargetNQN, Portal: opts.Portal, NamespaceID: 1, DeviceGUID: opts.DeviceGUID}
	if f.exportErr != nil {
		return ref, f.exportErr
	}

	if f.exports[opts.TargetNQN].TargetNQN != "" {
		return ref, transport.ErrAlreadyExported
	}

	f.exports[opts.TargetNQN] = ref
	if f.mapped[opts.TargetNQN] == nil {
		f.mapped[opts.TargetNQN] = map[string]bool{}
	}

	return ref, nil
}

func (f *fakeTransportServer) Unexport(_ context.Context, ref transport.TargetRef) error {
	f.unexported = append(f.unexported, ref.TargetNQN)
	if f.unexportErr != nil {
		return f.unexportErr
	}
	delete(f.exports, ref.TargetNQN)
	delete(f.mapped, ref.TargetNQN)

	return nil
}

func (f *fakeTransportServer) MapInitiator(_ context.Context, ref transport.TargetRef, id string) error {
	f.mapCalls = append(f.mapCalls, id)
	if f.mapped[ref.TargetNQN] == nil {
		f.mapped[ref.TargetNQN] = map[string]bool{}
	}
	f.mapped[ref.TargetNQN][id] = true

	return nil
}

func (f *fakeTransportServer) UnmapInitiator(_ context.Context, ref transport.TargetRef, id string) error {
	f.unmapCalls = append(f.unmapCalls, id)
	delete(f.mapped[ref.TargetNQN], id)

	return nil
}

func (f *fakeTransportServer) MappedInitiators(_ context.Context, ref transport.TargetRef) ([]string, error) {
	if f.mappedErr != nil {
		return nil, f.mappedErr
	}

	out := []string{}
	for id := range f.mapped[ref.TargetNQN] {
		out = append(out, id)
	}

	return out, nil
}

func (f *fakeTransportServer) ForceDisconnect(_ context.Context, ref transport.TargetRef) error {
	f.forceDisconnects = append(f.forceDisconnects, ref.TargetNQN)

	return nil
}

func TestReconcileExportCreatesExportAndAdmitsInitiator(t *testing.T) {
	ctx := context.Background()
	export := testExport("export-a", []string{"nqn.worker1"})
	deps := newTestDeps(t, export)

	if _, err := deps.reconciler().Reconcile(ctx, requestFor(export)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(deps.export.exportCalls) != 1 {
		t.Fatalf("Export calls = %d, want 1", len(deps.export.exportCalls))
	}
	if got := deps.export.mapCalls; len(got) != 1 || got[0] != "nqn.worker1" {
		t.Fatalf("MapInitiator calls = %v, want [nqn.worker1]", got)
	}

	got := &nvmetv1.NVMeExport{}
	if err := deps.Get(ctx, crclient.ObjectKeyFromObject(export), got); err != nil {
		t.Fatalf("get NVMeExport: %v", err)
	}
	if got.Status.State != nvmetv1.NVMeExportStateReady {
		t.Fatalf("state = %q, want Ready", got.Status.State)
	}
	if got.Status.ObservedGeneration != got.Generation {
		t.Fatalf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
	}
	if len(got.Status.AdmittedInitiators) != 1 || got.Status.AdmittedInitiators[0] != "nqn.worker1" {
		t.Fatalf("admittedInitiators = %v, want [nqn.worker1]", got.Status.AdmittedInitiators)
	}
}

func TestReconcileExportFailureMarksReadyFalse(t *testing.T) {
	ctx := context.Background()
	export := testExport("export-fail", []string{"nqn.worker1"})
	deps := newTestDeps(t, export)
	deps.export.exportErr = errors.New("boom")

	if _, err := deps.reconciler().Reconcile(ctx, requestFor(export)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &nvmetv1.NVMeExport{}
	if err := deps.Get(ctx, crclient.ObjectKeyFromObject(export), got); err != nil {
		t.Fatalf("get NVMeExport: %v", err)
	}
	if got.Status.State != nvmetv1.NVMeExportStateError {
		t.Fatalf("state = %q, want Error", got.Status.State)
	}

	cond := findCondition(got.Status.Conditions, string(nvmetv1.NVMeExportConditionReady))
	if cond == nil {
		t.Fatal("Ready condition missing")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != "ExportFailed" {
		t.Fatalf("Ready condition = %s/%s, want False/ExportFailed", cond.Status, cond.Reason)
	}
	if cond.ObservedGeneration != got.Generation {
		t.Fatalf("Ready observedGeneration = %d, want %d", cond.ObservedGeneration, got.Generation)
	}
}

func TestReconcileExportRemovesStaleInitiator(t *testing.T) {
	ctx := context.Background()
	export := testExport("export-a", []string{"nqn.worker1"})
	deps := newTestDeps(t, export)
	deps.export.mapped[export.Spec.TargetNQN] = map[string]bool{"nqn.worker1": true, "nqn.stale": true}

	if _, err := deps.reconciler().Reconcile(ctx, requestFor(export)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := deps.export.unmapCalls; len(got) != 1 || got[0] != "nqn.stale" {
		t.Fatalf("UnmapInitiator calls = %v, want [nqn.stale]", got)
	}
}

func TestReconcileExportDeletionUnexportsImmediately(t *testing.T) {
	ctx := context.Background()
	now := metav1.NewTime(time.Now())
	export := testExport("export-a", []string{"nqn.worker1"})
	export.Finalizers = []string{nvmetv1.NVMeExportFinalizer}
	export.DeletionTimestamp = &now
	deps := newTestDeps(t, export)
	recorder := &testutil.Recorder{}
	reconciler := deps.reconciler()
	reconciler.Recorder = recorder

	// F7: deletion no longer waits for a liveness probe (the old drain-wait).
	// The desired state on a deleting CR is "no export", so reconcileDelete
	// unexports immediately and removes the finalizer, letting the CR be
	// collected.
	if _, err := reconciler.Reconcile(ctx, requestFor(export)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(deps.export.unexported) != 1 {
		t.Fatalf("Unexport calls = %v, want exactly one", deps.export.unexported)
	}

	// Finalizer removed -> the fake client collects the object.
	got := &nvmetv1.NVMeExport{}
	err := deps.Get(ctx, crclient.ObjectKeyFromObject(export), got)
	if err == nil && len(got.Finalizers) != 0 {
		t.Fatalf("finalizers = %v, want removed", got.Finalizers)
	}
	if got := len(recorder.Events()); got != 0 {
		t.Fatalf("successful deletion events = %d, want 0", got)
	}
}

func TestNVMeExportEvents_FailuresRecoveryAndSteadyState(t *testing.T) {
	ctx := context.Background()
	export := testExport("event-export", []string{"nqn.worker1"})
	deps := newTestDeps(t, export)
	recorder := &testutil.Recorder{}
	reconciler := deps.reconciler()
	reconciler.Recorder = recorder
	req := requestFor(export)

	deps.export.exportErr = errors.New("private NQN and device path")
	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("export failure reconcile: %v", err)
	}
	assertNVMeExportEvent(t, recorder.Events(), 0, obsevents.TypeWarning, obsevents.ReasonExportFailed, obsevents.ActionExporting)
	assertNoPrivateEventDetails(t, recorder.Events())

	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("repeated export failure reconcile: %v", err)
	}
	if got := len(recorder.Events()); got != 1 {
		t.Fatalf("events after identical export failure = %d, want 1", got)
	}

	deps.export.exportErr = nil
	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("recovery reconcile: %v", err)
	}
	assertNVMeExportEvent(t, recorder.Events(), 1, obsevents.TypeNormal, obsevents.ReasonExportReconciled, obsevents.ActionExporting)

	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("steady-state reconcile: %v", err)
	}
	if got := len(recorder.Events()); got != 2 {
		t.Fatalf("events after steady-state Ready reconcile = %d, want 2", got)
	}
}

func TestNVMeExportEvents_MappedInitiatorsFailure(t *testing.T) {
	ctx := context.Background()
	export := testExport("event-mapped", nil)
	deps := newTestDeps(t, export)
	deps.export.mappedErr = errors.New("initiator nqn.private unavailable")
	reconciler := deps.reconciler()
	recorder := &testutil.Recorder{}
	reconciler.Recorder = recorder

	if _, err := reconciler.Reconcile(ctx, requestFor(export)); err != nil {
		t.Fatalf("mapped initiators failure reconcile: %v", err)
	}
	assertNVMeExportEvent(t, recorder.Events(), 0, obsevents.TypeWarning, obsevents.ReasonMappedInitiatorsFailed, obsevents.ActionExporting)
	assertNoPrivateEventDetails(t, recorder.Events())
}

func TestNVMeExportEvents_UnexportFailureKeepsFinalizer(t *testing.T) {
	ctx := context.Background()
	now := metav1.NewTime(time.Now())
	export := testExport("event-unexport", nil)
	export.Finalizers = []string{nvmetv1.NVMeExportFinalizer}
	export.DeletionTimestamp = &now
	deps := newTestDeps(t, export)
	deps.export.unexportErr = errors.New("transport path failed")
	reconciler := deps.reconciler()
	recorder := &testutil.Recorder{}
	reconciler.Recorder = recorder

	if _, err := reconciler.Reconcile(ctx, requestFor(export)); err != nil {
		t.Fatalf("unexport failure reconcile: %v", err)
	}
	assertNVMeExportEvent(t, recorder.Events(), 0, obsevents.TypeWarning, obsevents.ReasonUnexportFailed, obsevents.ActionDeleting)

	got := &nvmetv1.NVMeExport{}
	if err := deps.Get(ctx, crclient.ObjectKeyFromObject(export), got); err != nil {
		t.Fatalf("get NVMeExport: %v", err)
	}
	if len(got.Finalizers) != 1 || got.Finalizers[0] != nvmetv1.NVMeExportFinalizer {
		t.Fatalf("finalizers = %v, want retained NVMeExport finalizer", got.Finalizers)
	}
}

func TestNVMeExportEvents_StatusPatchFailureAndNilRecorder(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "status patch failure", err: errors.New("status patch failed")},
		{name: "status conflict", err: apierrors.NewConflict(schema.GroupResource{Group: "nvmet.zfs-csi.io", Resource: "nvmeexports"}, "event-status-patch", errors.New("conflict"))},
	} {
		t.Run(tt.name+" emits nothing", func(t *testing.T) {
			export := testExport("event-status-patch", nil)
			deps := newTestDeps(t, export)
			deps.export.exportErr = errors.New("export failed")
			reconciler := deps.reconciler()
			reconciler.Client = statusPatchErrorClient{Client: deps.Client, err: tt.err}
			recorder := &testutil.Recorder{}
			reconciler.Recorder = recorder

			if _, err := reconciler.Reconcile(ctx, requestFor(export)); err == nil {
				t.Fatal("reconcile unexpectedly succeeded")
			}
			if got := len(recorder.Events()); got != 0 {
				t.Fatalf("events after failed status patch = %d, want 0", got)
			}
		})
	}

	t.Run("nil recorder is safe", func(t *testing.T) {
		export := testExport("event-nil-recorder", nil)
		deps := newTestDeps(t, export)
		deps.export.exportErr = errors.New("export failed")

		if _, err := deps.reconciler().Reconcile(ctx, requestFor(export)); err != nil {
			t.Fatalf("reconcile with nil recorder: %v", err)
		}
	})
}

func testExport(name string, initiators []string) *nvmetv1.NVMeExport {
	return &nvmetv1.NVMeExport{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nvmetv1.NVMeExportSpec{
			DevicePath:        "/dev/zvol/tank/csi/block/vol-a",
			TargetNQN:         "nqn.2026-07.uk.randomvariable:vol-a",
			Portal:            "server7:4420",
			DeviceGUID:        "11111111-2222-3333-4444-555555555555",
			NamespaceID:       1,
			AllowedInitiators: initiators,
		},
	}
}

func requestFor(obj crclient.Object) reconcile.Request {
	return reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(obj)}
}

func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}

	return nil
}

func assertNVMeExportEvent(t *testing.T, records []testutil.EventRecord, index int, eventType, reason, action string) {
	t.Helper()
	if len(records) <= index {
		t.Fatalf("events = %#v, missing index %d", records, index)
	}
	got := records[index]
	if got.Type != eventType || got.Reason != reason || got.Action != action {
		t.Fatalf("event[%d] = %#v, want %s/%s/%s", index, got, eventType, reason, action)
	}
}

func assertNoPrivateEventDetails(t *testing.T, records []testutil.EventRecord) {
	t.Helper()
	for _, record := range records {
		for _, private := range []string{"nqn.private", "device path", "transport path"} {
			if strings.Contains(record.Note, private) {
				t.Fatalf("event note leaks %q: %q", private, record.Note)
			}
		}
	}
}

type statusPatchErrorClient struct {
	crclient.Client
	err error
}

func (c statusPatchErrorClient) Status() crclient.StatusWriter {
	return statusPatchErrorWriter{SubResourceWriter: c.Client.Status(), err: c.err}
}

type statusPatchErrorWriter struct {
	crclient.SubResourceWriter
	err error
}

func (w statusPatchErrorWriter) Patch(context.Context, crclient.Object, crclient.Patch, ...crclient.SubResourcePatchOption) error {
	return w.err
}

var _ transport.Server = (*fakeTransportServer)(nil)
