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

package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/testutil"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func TestVolumeEvents_FailureRecoveryAndSteadyState(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	d.export.exportErr = errors.New("transport unavailable")

	vol := testEventVolume("event-recovery")
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: nn(vol.Name)}

	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("first reconcile unexpectedly succeeded")
	}
	assertEvent(t, recorder.Events(), 0, events.TypeWarning, events.ReasonExportFailed, events.ActionExporting)

	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("repeated failing reconcile unexpectedly succeeded")
	}
	if got := len(recorder.Events()); got != 1 {
		t.Fatalf("events after identical failure = %d, want 1", got)
	}

	d.export.exportErr = nil
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("recovery reconcile: %v", err)
	}
	assertEvent(t, recorder.Events(), 1, events.TypeNormal, events.ReasonExportRecovered, events.ActionProvisioning)

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("steady-state reconcile: %v", err)
	}
	if got := len(recorder.Events()); got != 2 {
		t.Fatalf("events after steady-state Ready reconcile = %d, want 2", got)
	}
}

func TestVolumeEvents_BackendHealthFailureRecoveryAndSteadyState(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	recorder := &testutil.Recorder{}
	r.Recorder = recorder

	vol := testEventVolume("event-backend-health")
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: nn(vol.Name)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	recorder = &testutil.Recorder{}
	r.Recorder = recorder
	d.export.exportErr = errors.New("configfs target unavailable")
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("health repair failure unexpectedly succeeded")
	}
	assertEvent(t, recorder.Events(), 0, events.TypeWarning, events.ReasonBackendUnhealthy, events.ActionHealthChecking)
	got := getVol(t, d, vol.Name)
	health := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy))
	if health == nil || health.Status != metav1.ConditionFalse || health.Message == "" {
		t.Fatalf("backend health = %#v, want persisted False with a message", health)
	}

	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("repeated health repair failure unexpectedly succeeded")
	}
	if got := len(recorder.Events()); got != 1 {
		t.Fatalf("events after identical backend failure = %d, want 1", got)
	}

	d.export.exportErr = nil
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("health repair recovery: %v", err)
	}
	assertEvent(t, recorder.Events(), 1, events.TypeNormal, events.ReasonBackendRecovered, events.ActionHealthChecking)
	got = getVol(t, d, vol.Name)
	health = findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy))
	if health == nil || health.Status != metav1.ConditionTrue {
		t.Fatalf("backend health = %#v, want persisted True", health)
	}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("healthy steady-state reconcile: %v", err)
	}
	if got := len(recorder.Events()); got != 2 {
		t.Fatalf("events after healthy steady state = %d, want 2", got)
	}
}

func TestVolumeEvents_BackendHealthRemainsAbnormalUntilRepairSucceeds(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	vol := testEventVolume("event-backend-health-repair")
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: nn(vol.Name)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	recorder = &testutil.Recorder{}
	r.Recorder = recorder

	d.export.exportErr = errors.New("configfs target unavailable")
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("target repair failure unexpectedly succeeded")
	}
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("repeated target repair failure unexpectedly succeeded")
	}
	if got := len(recorder.Events()); got != 1 {
		t.Fatalf("events after failed retries = %d, want 1", got)
	}

	d.export.exportErr = nil
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("target repair recovery: %v", err)
	}
	assertEvent(t, recorder.Events(), 1, events.TypeNormal, events.ReasonBackendRecovered, events.ActionHealthChecking)
}

func TestVolumeEvents_StatusPatchFailureSuppressesEvent(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	d.export.exportErr = errors.New("transport unavailable")
	r.Client = statusPatchErrorClient{Client: d.Client}

	vol := testEventVolume("event-status-failure")
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: nn(vol.Name)}); err == nil {
		t.Fatal("reconcile unexpectedly succeeded")
	}
	if got := len(recorder.Events()); got != 0 {
		t.Fatalf("events after failed status patch = %d, want 0", got)
	}
}

func TestVolumeEvents_ExpansionFailureAndSuccess(t *testing.T) {
	d := newTestDeps(t)
	backend := &expandErrorZFS{Backend: zfsfake.New().WithPool("tank", 1<<40), err: errors.New("expand failed")}
	d.useBackend(backend.Backend)
	r := d.reconciler()
	r.ZFS = backend
	recorder := &testutil.Recorder{}
	r.Recorder = recorder

	vol := testEventVolume("event-expand")
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: nn(vol.Name)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("create reconcile: %v", err)
	}
	if got := len(recorder.Events()); got != 1 {
		t.Fatalf("create events = %d, want 1", got)
	}
	setCapacity(t, d, vol.Name, 2<<30)

	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("expand failure unexpectedly succeeded")
	}
	assertEvent(t, recorder.Events(), 1, events.TypeWarning, events.ReasonExpansionFailed, events.ActionExpanding)

	// Preserve the existing retry route after an Error status, then exercise the
	// successful expansion transition from a Ready status.
	markReady(t, d, getVol(t, d, vol.Name))
	backend.err = nil
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("expansion recovery reconcile: %v", err)
	}
	assertEvent(t, recorder.Events(), 2, events.TypeNormal, events.ReasonVolumeExpanded, events.ActionExpanding)
}

func TestVolumeEvents_DeleteBlockedIsPersistedAndEmittedOnce(t *testing.T) {
	d, backend, r := depsWithRecordingZFS(t)
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	vol := createReadyBlock(t, d, "event-delete-blocked")
	backend.WithDataset(datasetPath(t, vol.Spec.VolumeID), zfs.KindBlock, false, zfs.KeyNone)

	cur := getVol(t, d, vol.Name)
	patch := crclient.MergeFrom(cur.DeepCopy())
	cur.Status.State = zfscsiv1.VolumeStateDeleting
	cur.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{NodeName: "worker-a", InitiatorID: "nqn.worker-a"}}
	if err := d.Status().Patch(context.Background(), cur, patch); err != nil {
		t.Fatal(err)
	}

	reconcileVol(t, r, vol.Name)
	assertEvent(t, recorder.Events(), 0, events.TypeWarning, events.ReasonDeleteBlockedInUse, events.ActionDeleting)
	got := getVol(t, d, vol.Name)
	ready := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionReady))
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != events.ReasonDeleteBlockedInUse {
		t.Fatalf("Ready condition = %#v, want False/%s", ready, events.ReasonDeleteBlockedInUse)
	}

	reconcileVol(t, r, vol.Name)
	if got := len(recorder.Events()); got != 1 {
		t.Fatalf("events after repeated blocked delete = %d, want 1", got)
	}
}

func TestVolumeEvents_DeleteFailureAndDestruction(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		d := newTestDeps(t)
		backend := &failDestroyZFS{Backend: zfsfake.New().WithPool("tank", 1<<40), err: errors.New("dependent clone")}
		d.useBackend(backend.Backend)
		r := d.reconciler()
		r.ZFS = backend
		recorder := &testutil.Recorder{}
		r.Recorder = recorder

		vol := testEventVolume("event-delete-failure")
		if err := d.Create(context.Background(), vol); err != nil {
			t.Fatal(err)
		}
		markDeleting(t, d, vol.Name)
		if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: nn(vol.Name)}); err == nil {
			t.Fatal("delete failure unexpectedly succeeded")
		}
		assertEvent(t, recorder.Events(), 0, events.TypeWarning, events.ReasonVolumeDeleteFailed, events.ActionDeleting)
	})

	t.Run("destruction", func(t *testing.T) {
		d, backend, r := depsWithRecordingZFS(t)
		recorder := &testutil.Recorder{}
		r.Recorder = recorder
		vol := testEventVolume("event-destroyed")
		if err := d.Create(context.Background(), vol); err != nil {
			t.Fatal(err)
		}
		backend.WithDataset(datasetPath(t, vol.Spec.VolumeID), zfs.KindBlock, false, zfs.KeyNone)
		markDeleting(t, d, vol.Name)

		if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: nn(vol.Name)}); err != nil {
			t.Fatalf("delete reconcile: %v", err)
		}
		assertEvent(t, recorder.Events(), 0, events.TypeNormal, events.ReasonVolumeDestroyed, events.ActionDeleting)
	})
}

func TestVolumeEvents_FenceOnlyAfterForceDisconnect(t *testing.T) {
	tests := []struct {
		name      string
		desired   []zfscsiv1.MappedInitiator
		live      []string
		wantEvent bool
	}{
		{
			name:      "replacement emits fence event",
			desired:   []zfscsiv1.MappedInitiator{{NodeName: "worker-b", InitiatorID: "nqn.worker-b"}},
			live:      []string{"nqn.worker-a"},
			wantEvent: true,
		},
		{
			name:    "scale down does not emit fence event",
			desired: []zfscsiv1.MappedInitiator{{NodeName: "worker-a", InitiatorID: "nqn.worker-a"}},
			live:    []string{"nqn.worker-a", "nqn.worker-b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, backend, r := depsWithRecordingZFS(t)
			recorder := &testutil.Recorder{}
			r.Recorder = recorder
			vol := createReadyBlock(t, d, "event-fence")
			backend.WithDataset(datasetPath(t, vol.Spec.VolumeID), zfs.KindBlock, false, zfs.KeyNone)

			cur := getVol(t, d, vol.Name)
			patch := crclient.MergeFrom(cur.DeepCopy())
			cur.Status.MappedInitiators = tt.desired
			if err := d.Status().Patch(context.Background(), cur, patch); err != nil {
				t.Fatal(err)
			}
			nqn := d.exportRefNQN("tank", "block", "event-fence")
			d.export.exports[nqn] = true
			d.export.mapped[nqn] = map[string]bool{}
			for _, initiator := range tt.live {
				d.export.mapped[nqn][initiator] = true
			}

			reconcileVol(t, r, vol.Name)
			if got := len(d.export.forceDisconnects); (got == 1) != tt.wantEvent {
				t.Fatalf("ForceDisconnect calls = %d, want event=%v", got, tt.wantEvent)
			}
			if tt.wantEvent {
				assertEvent(t, recorder.Events(), 0, events.TypeNormal, events.ReasonInitiatorFenced, events.ActionExporting)
			} else if got := len(recorder.Events()); got != 0 {
				t.Fatalf("events without ForceDisconnect = %#v, want none", recorder.Events())
			}
		})
	}
}

func TestVolumeEvents_NotesExcludeSensitiveReferences(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	recorder := &testutil.Recorder{}
	r.Recorder = recorder

	vol := testEventVolume("event-safe-note")
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	// Persisted details may identify the failing dependency, while the Event note
	// remains the fixed public text supplied by volumeWarningEvent.
	statusDetail := "fetch transit/secret-key for nqn.2026-01.example:secret"
	r.recordStatusWarning(context.Background(), vol, zfscsiv1.VolumeStateError, statusDetail, volumeWarningEvent{
		reason: events.ReasonVolumeCreateFailed, action: events.ActionProvisioning, publicNote: "volume encryption key is unavailable",
	})

	persisted := getVol(t, d, vol.Name)
	ready := findCondition(persisted.Status.Conditions, string(zfscsiv1.VolumeConditionReady))
	if ready == nil || ready.Message != statusDetail {
		t.Fatalf("persisted Ready detail = %#v, want %q", ready, statusDetail)
	}
	for _, event := range recorder.Events() {
		for _, forbidden := range []string{"transit/", "nqn.", "not-a-real-key"} {
			if strings.Contains(event.Note, forbidden) {
				t.Fatalf("event note leaks %q: %q", forbidden, event.Note)
			}
		}
	}
}

func testEventVolume(name string) *zfscsiv1.Volume {
	return &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30,
			VolumeID: "csi:tank:block:" + name, Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
}

func setCapacity(t *testing.T, d *testDeps, name string, capacity int64) {
	t.Helper()
	vol := getVol(t, d, name)
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Spec.Capacity = capacity
	if err := d.Patch(context.Background(), vol, patch); err != nil {
		t.Fatal(err)
	}
}

func markDeleting(t *testing.T, d *testDeps, name string) {
	t.Helper()
	vol := getVol(t, d, name)
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateDeleting
	if err := d.Status().Patch(context.Background(), vol, patch); err != nil {
		t.Fatal(err)
	}
}

func assertEvent(t *testing.T, records []testutil.EventRecord, index int, eventType, reason, action string) {
	t.Helper()
	if len(records) <= index {
		t.Fatalf("events = %#v, missing index %d", records, index)
	}
	got := records[index]
	if got.Type != eventType || got.Reason != reason || got.Action != action {
		t.Fatalf("event[%d] = %#v, want %s/%s/%s", index, got, eventType, reason, action)
	}
}

type statusPatchErrorClient struct {
	crclient.Client
}

func (c statusPatchErrorClient) Status() crclient.StatusWriter {
	return statusPatchErrorWriter{SubResourceWriter: c.Client.Status()}
}

type statusPatchErrorWriter struct {
	crclient.SubResourceWriter
}

func (statusPatchErrorWriter) Patch(context.Context, crclient.Object, crclient.Patch, ...crclient.SubResourcePatchOption) error {
	return errors.New("status patch failed")
}

type expandErrorZFS struct {
	*zfsfake.Backend
	err error
}

func (f *expandErrorZFS) Expand(ctx context.Context, name string, capacity int64) error {
	if f.err != nil {
		return f.err
	}

	return f.Backend.Expand(ctx, name, capacity)
}
