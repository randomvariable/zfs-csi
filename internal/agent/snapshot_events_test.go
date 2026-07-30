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
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/testutil"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func TestSnapshotEvents_MalformedIDsAreTransitionGated(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*zfscsiv1.Snapshot)
		reason string
	}{
		{"source volume", func(s *zfscsiv1.Snapshot) {
			s.Spec.SourceVolumeID = "invalid"
			s.Status.DatasetPath = "tank/csi/block/event-malformed@snap"
		}, events.ReasonSnapshotInvalidVolumeID},
		{"snapshot", func(s *zfscsiv1.Snapshot) { s.Spec.SnapshotID = "invalid" }, events.ReasonSnapshotInvalidSnapshotID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDeps(t)
			r := newSnapshotReconciler(d)
			recorder := &testutil.Recorder{}
			r.Recorder = recorder
			snap := testEventSnapshot("event-malformed")
			tt.mutate(snap)
			if err := d.Create(context.Background(), snap); err != nil {
				t.Fatal(err)
			}
			reconcileSnapshot(t, r, snap.Name)
			assertEvent(t, recorder.Events(), 0, events.TypeWarning, tt.reason, events.ActionProvisioning)
			reconcileSnapshot(t, r, snap.Name)
			if got := len(recorder.Events()); got != 1 {
				t.Fatalf("events after repeated malformed ID = %d, want 1", got)
			}
		})
	}
}

func TestSnapshotEvents_CreateFailureRecoveryAndSteadyState(t *testing.T) {
	d := newTestDeps(t)
	d.zfsb.WithDataset("tank/csi/block/event-recovery", zfs.KindBlock, false, zfs.KeyNone)
	backend := &snapshotErrorZFS{Backend: d.zfsb, snapshotErr: errors.New("tank/injected-dataset@snapshot-secret failed")}
	r := newSnapshotReconciler(d)
	r.ZFS = backend
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	snap := testEventSnapshot("event-recovery")
	if err := d.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}

	reconcileSnapshot(t, r, snap.Name)
	assertEvent(t, recorder.Events(), 0, events.TypeWarning, events.ReasonSnapshotCreateFailed, events.ActionProvisioning)
	assertSnapshotEventNotesPublic(t, recorder.Events())
	reconcileSnapshot(t, r, snap.Name)
	if got := len(recorder.Events()); got != 1 {
		t.Fatalf("events after repeated create failure = %d, want 1", got)
	}
	backend.snapshotErr = nil
	reconcileSnapshot(t, r, snap.Name)
	assertEvent(t, recorder.Events(), 1, events.TypeNormal, events.ReasonSnapshotReady, events.ActionProvisioning)
	reconcileSnapshot(t, r, snap.Name)
	if got := len(recorder.Events()); got != 2 {
		t.Fatalf("events after steady Ready reconcile = %d, want 2", got)
	}
}

func TestSnapshotEvents_StatusPatchFailureSuppressesEvent(t *testing.T) {
	d := newTestDeps(t)
	r := newSnapshotReconciler(d)
	r.Client = statusPatchErrorClient{Client: d.Client}
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	snap := testEventSnapshot("event-status-patch")
	snap.Spec.SourceVolumeID = "invalid"
	snap.Status.DatasetPath = "tank/csi/block/event-status-patch@snap"
	if err := d.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	reconcileSnapshot(t, r, snap.Name)
	if got := len(recorder.Events()); got != 0 {
		t.Fatalf("events after failed status patch = %d, want 0", got)
	}
}

func TestSnapshotEvents_DestroyFailureAndDeletingTransition(t *testing.T) {
	d := newTestDeps(t)
	backend := &snapshotErrorZFS{Backend: d.zfsb, destroyErr: errors.New("dependent clone")}
	r := newSnapshotReconciler(d)
	r.ZFS = backend
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	snap := testEventSnapshot("event-destroy")
	if err := d.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	setSnapshotDeleting(t, d, snap.Name)

	if result := reconcileSnapshot(t, r, snap.Name); result.RequeueAfter != 10*time.Second {
		t.Fatalf("destroy failure requeue = %s, want 10s", result.RequeueAfter)
	}
	assertEvent(t, recorder.Events(), 0, events.TypeWarning, events.ReasonSnapshotDestroyFailed, events.ActionDeleting)
	ready := findCondition(getSnapshot(t, d, snap.Name).Status.Conditions, string(zfscsiv1.SnapshotConditionReady))
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != events.ReasonSnapshotDestroyFailed {
		t.Fatalf("Ready condition = %#v, want False/%s", ready, events.ReasonSnapshotDestroyFailed)
	}
	reconcileSnapshot(t, r, snap.Name)
	if got := len(recorder.Events()); got != 1 {
		t.Fatalf("events after repeated destroy failure = %d, want 1", got)
	}

	backend.destroyErr = nil
	reconcileSnapshot(t, r, snap.Name)
	assertEvent(t, recorder.Events(), 1, events.TypeNormal, events.ReasonSnapshotDeleting, events.ActionDeleting)
	reconcileSnapshot(t, r, snap.Name)
	if got := len(recorder.Events()); got != 2 {
		t.Fatalf("events after repeated deletion = %d, want 2", got)
	}
}

func TestSnapshotEvents_DeletingFinalizerPath(t *testing.T) {
	t.Run("successful destroy emits before object deletion", func(t *testing.T) {
		d := newTestDeps(t)
		r := newSnapshotReconciler(d)
		recorder := &testutil.Recorder{}
		r.Recorder = recorder
		snap := testEventSnapshot("event-finalizer-success")
		snap.Finalizers = []string{zfscsiv1.SnapshotFinalizer}
		if err := d.Create(context.Background(), snap); err != nil {
			t.Fatal(err)
		}
		deleteSnapshot(t, d, snap.Name)

		reconcileSnapshot(t, r, snap.Name)
		assertEvent(t, recorder.Events(), 0, events.TypeNormal, events.ReasonSnapshotDeleting, events.ActionDeleting)
		deleted := &zfscsiv1.Snapshot{}
		if err := d.Get(context.Background(), nn(snap.Name), deleted); err == nil {
			t.Fatalf("snapshot remains after finalizer removal: %#v", deleted.Finalizers)
		}
	})

	t.Run("finalizer patch failure emits once before retry", func(t *testing.T) {
		d := newTestDeps(t)
		r := newSnapshotReconciler(d)
		r.Client = finalizerPatchErrorClient{Client: d.Client}
		recorder := &testutil.Recorder{}
		r.Recorder = recorder
		snap := testEventSnapshot("event-finalizer-failure")
		snap.Finalizers = []string{zfscsiv1.SnapshotFinalizer}
		if err := d.Create(context.Background(), snap); err != nil {
			t.Fatal(err)
		}
		deleteSnapshot(t, d, snap.Name)

		if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: nn(snap.Name)}); err == nil {
			t.Fatal("reconcile unexpectedly succeeded when finalizer patch failed")
		}
		assertEvent(t, recorder.Events(), 0, events.TypeNormal, events.ReasonSnapshotDeleting, events.ActionDeleting)
		current := getSnapshot(t, d, snap.Name)
		if !hasFinalizer(current.Finalizers, zfscsiv1.SnapshotFinalizer) {
			t.Fatal("finalizer was removed despite failed patch")
		}

		if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: nn(snap.Name)}); err == nil {
			t.Fatal("retry unexpectedly succeeded when finalizer patch failed")
		}
		if got := len(recorder.Events()); got != 1 {
			t.Fatalf("events after finalizer patch retry = %d, want 1", got)
		}
	})
}

func TestSnapshotEvents_NilRecorderIsSafe(t *testing.T) {
	d := newTestDeps(t)
	r := newSnapshotReconciler(d)
	snap := testEventSnapshot("event-nil-recorder")
	if err := d.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	reconcileSnapshot(t, r, snap.Name)
}

func assertSnapshotEventNotesPublic(t *testing.T, records []testutil.EventRecord) {
	t.Helper()
	for _, event := range records {
		for _, forbidden := range []string{"tank/", "injected-dataset", "snapshot-secret"} {
			if strings.Contains(event.Note, forbidden) {
				t.Fatalf("event note leaks %q: %q", forbidden, event.Note)
			}
		}
	}
}

func newSnapshotReconciler(d *testDeps) *SnapshotReconciler {
	guid, _ := d.zfsb.PoolGUID(context.Background(), "tank")
	if wrapped, ok := d.Client.(poolIdentityClient); ok {
		wrapped.identities["tank"] = guid
	}
	return &SnapshotReconciler{Client: d.Client, ZFS: d.zfsb, NodeName: "storage-a"}
}

func testPoolGUID(t *testing.T, d *testDeps) string {
	t.Helper()
	guid, err := d.zfsb.PoolGUID(t.Context(), "tank")
	if err != nil {
		t.Fatal(err)
	}
	return guid
}

func TestSnapshotWrongOwnerDoesNotMutateBackendOrFinalizer(t *testing.T) {
	d := newTestDeps(t)
	d.zfsb.WithDatasetCapacity("tank/csi/block/source", zfs.KindBlock, 1<<30, false, zfs.KeyNone)
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "wrong-owner", Finalizers: []string{zfscsiv1.SnapshotFinalizer}}, Spec: zfscsiv1.SnapshotSpec{
		VolumeRef: "source", SourceVolumeID: "csi:tank:block:source", SnapName: "snap",
		SnapshotID: "csi:tank:block:source@snap", OwnerNode: "storage-b",
	}}
	if err := d.Create(t.Context(), snap); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete(t.Context(), snap); err != nil {
		t.Fatal(err)
	}
	r := newSnapshotReconciler(d)
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Snapshot{}
	if err := d.Get(t.Context(), types.NamespacedName{Name: snap.Name}, got); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(got.Finalizers, zfscsiv1.SnapshotFinalizer) {
		t.Fatal("wrong owner removed snapshot finalizer")
	}
}

func TestSnapshotReconcilerRejectsImportedVolumeByProvenance(t *testing.T) {
	d := newTestDeps(t)
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "import-existing"}, Spec: zfscsiv1.VolumeSpec{
		Provenance: zfscsiv1.VolumeProvenanceImported, VolumeID: "csi:tank:block:ordinary-handle", Pool: "tank", PoolGUID: testPoolGUID(t, d), Type: zfscsiv1.VolumeTypeBlock, OwnerNode: "storage-a",
	}}
	if err := d.Create(context.Background(), volume); err != nil {
		t.Fatal(err)
	}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "import-snapshot"}, Spec: zfscsiv1.SnapshotSpec{
		VolumeRef: volume.Name, SourceVolumeID: volume.Spec.VolumeID, SnapName: "snap", SnapshotID: volume.Spec.VolumeID + "@snap", OwnerNode: "storage-a",
	}}
	if err := d.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	r := newSnapshotReconciler(d)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Snapshot{}
	if err := d.Get(context.Background(), types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CurrentState() != zfscsiv1.SnapshotStateError {
		t.Fatalf("state=%q", got.Status.CurrentState())
	}
}

func TestSnapshotReconcilerAllowsDynamicImportPrefixedVolume(t *testing.T) {
	d := newTestDeps(t)
	const dataset = "tank/csi/block/import-dynamic"
	d.zfsb.WithDatasetCapacity(dataset, zfs.KindBlock, 1<<30, false, zfs.KeyNone)
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "import-dynamic"}, Spec: zfscsiv1.VolumeSpec{
		Provenance: zfscsiv1.VolumeProvenanceDynamic, VolumeID: "csi:tank:block:import-dynamic", Pool: "tank", PoolGUID: testPoolGUID(t, d), Type: zfscsiv1.VolumeTypeBlock, OwnerNode: "storage-a",
	}}
	if err := d.Create(context.Background(), volume); err != nil {
		t.Fatal(err)
	}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "dynamic-snapshot"}, Spec: zfscsiv1.SnapshotSpec{
		VolumeRef: volume.Name, SourceVolumeID: volume.Spec.VolumeID, SnapName: "snap", SnapshotID: volume.Spec.VolumeID + "@snap", OwnerNode: "storage-a",
	}}
	if err := d.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	r := newSnapshotReconciler(d)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Snapshot{}
	if err := d.Get(context.Background(), types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CurrentState() != zfscsiv1.SnapshotStateReady {
		t.Fatalf("state=%q, want Ready", got.Status.CurrentState())
	}
}

func TestSnapshotReconcilerDeletesImportedSnapshotBeforeProvenanceRejection(t *testing.T) {
	d := newTestDeps(t)
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "imported"}, Spec: zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, VolumeID: "csi:tank:block:opaque", Pool: "tank", PoolGUID: testPoolGUID(t, d), Type: zfscsiv1.VolumeTypeBlock, OwnerNode: "storage-a"}}
	if err := d.Create(context.Background(), volume); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithDatasetCapacity("tank/csi/block/opaque", zfs.KindBlock, 1<<30, false, zfs.KeyNone)
	if err := d.zfsb.Snapshot(context.Background(), "tank/csi/block/opaque", "snap"); err != nil {
		t.Fatal(err)
	}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "imported-delete", Finalizers: []string{zfscsiv1.SnapshotFinalizer}}, Spec: zfscsiv1.SnapshotSpec{VolumeRef: volume.Name, SourceVolumeID: volume.Spec.VolumeID, SnapName: "snap", SnapshotID: volume.Spec.VolumeID + "@snap", OwnerNode: "storage-a"}, Status: zfscsiv1.SnapshotStatus{State: zfscsiv1.SnapshotStateDeleting, DatasetPath: "tank/csi/block/opaque@snap"}}
	if err := d.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	r := newSnapshotReconciler(d)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Snapshot{}
	err := d.Get(context.Background(), types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}, got)
	if err == nil && slices.Contains(got.Finalizers, zfscsiv1.SnapshotFinalizer) {
		t.Fatalf("snapshot finalizer was not removed: %#v", got.Finalizers)
	}
}

func TestSnapshotReconcilerMalformedDeletingSnapshotRetainsFinalizerUntilPoolIdentityVerified(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*zfsfake.Backend)
		want   string
	}{
		{name: "missing pool", mutate: func(backend *zfsfake.Backend) { backend.RemovePool("tank") }, want: "read pool"},
		{name: "mismatched pool", mutate: func(backend *zfsfake.Backend) { backend.ReplacePool("tank", 1<<40, "2", "ONLINE") }, want: "GUID mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t)
			snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "malformed-delete", Finalizers: []string{zfscsiv1.SnapshotFinalizer}}, Spec: zfscsiv1.SnapshotSpec{
				PoolGUID: testPoolGUID(t, d), VolumeRef: "source", SourceVolumeID: "malformed", SnapshotID: "malformed", OwnerNode: "storage-a",
			}, Status: zfscsiv1.SnapshotStatus{State: zfscsiv1.SnapshotStateDeleting, DatasetPath: "tank/csi/block/source@snap"}}
			if err := d.Create(t.Context(), snap); err != nil {
				t.Fatal(err)
			}
			tc.mutate(d.zfsb)
			r := newSnapshotReconciler(d)
			if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("reconcile error=%v, want %q", err, tc.want)
			}
			got := getSnapshot(t, d, snap.Name)
			if !slices.Contains(got.Finalizers, zfscsiv1.SnapshotFinalizer) {
				t.Fatal("pool identity failure removed malformed Snapshot finalizer")
			}
			if !reflect.DeepEqual(got.Status, snap.Status) {
				t.Fatalf("pool identity failure mutated status: got=%#v want=%#v", got.Status, snap.Status)
			}
		})
	}
}

func TestSnapshotSourcePoolGUIDMismatchFailsBeforeMutation(t *testing.T) {
	d := newTestDeps(t)
	const dataset = "tank/csi/block/source-guid-mismatch"
	d.zfsb.WithDatasetCapacity(dataset, zfs.KindBlock, 1<<30, false, zfs.KeyNone)
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "source-guid-mismatch"}, Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", PoolGUID: "2", VolumeID: "csi:tank:block:source-guid-mismatch", Type: zfscsiv1.VolumeTypeBlock, OwnerNode: "storage-a",
	}}
	if err := d.Create(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "source-guid-mismatch"}, Spec: zfscsiv1.SnapshotSpec{
		PoolGUID: testPoolGUID(t, d), VolumeRef: source.Name, SourceVolumeID: source.Spec.VolumeID, SnapName: "snap",
		SnapshotID: source.Spec.VolumeID + "@snap", OwnerNode: "storage-a",
	}}
	if err := d.Create(t.Context(), snap); err != nil {
		t.Fatal(err)
	}
	r := newSnapshotReconciler(d)
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err == nil || !strings.Contains(err.Error(), "does not match Snapshot pool GUID") {
		t.Fatalf("reconcile error=%v, want source pool GUID mismatch", err)
	}
	if snapshots, err := d.zfsb.ListSnapshots(t.Context(), dataset); err != nil || len(snapshots) != 0 {
		t.Fatalf("backend snapshots=%v err=%v, want none", snapshots, err)
	}
	got := getSnapshot(t, d, snap.Name)
	if !reflect.DeepEqual(got.Status, snap.Status) || !reflect.DeepEqual(got.Finalizers, snap.Finalizers) {
		t.Fatalf("source identity mismatch mutated Snapshot: got=%#v want=%#v", got, snap)
	}
}

func TestSnapshotSourceOwnerMismatchFailsBeforeMutation(t *testing.T) {
	d := newTestDeps(t)
	const dataset = "tank/csi/block/source-owner-mismatch"
	d.zfsb.WithDatasetCapacity(dataset, zfs.KindBlock, 1<<30, false, zfs.KeyNone)
	guid := testPoolGUID(t, d)
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "source-owner-mismatch"}, Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", PoolGUID: guid, VolumeID: "csi:tank:block:source-owner-mismatch", Type: zfscsiv1.VolumeTypeBlock, OwnerNode: "storage-b",
	}}
	if err := d.Create(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "source-owner-mismatch", Finalizers: []string{zfscsiv1.SnapshotFinalizer}}, Spec: zfscsiv1.SnapshotSpec{
		PoolGUID: guid, VolumeRef: source.Name, SourceVolumeID: source.Spec.VolumeID, SnapName: "snap",
		SnapshotID: source.Spec.VolumeID + "@snap", OwnerNode: "storage-a",
	}}
	if err := d.Create(t.Context(), snap); err != nil {
		t.Fatal(err)
	}
	recorder := &testutil.Recorder{}
	r := newSnapshotReconciler(d)
	r.Recorder = recorder
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err == nil || !strings.Contains(err.Error(), "does not match Snapshot owner node") {
		t.Fatalf("reconcile error=%v, want source owner mismatch", err)
	}
	if snapshots, err := d.zfsb.ListSnapshots(t.Context(), dataset); err != nil || len(snapshots) != 0 {
		t.Fatalf("backend snapshots=%v err=%v, want none", snapshots, err)
	}
	got := getSnapshot(t, d, snap.Name)
	if !reflect.DeepEqual(got.Status, snap.Status) || !reflect.DeepEqual(got.Finalizers, snap.Finalizers) {
		t.Fatalf("source owner mismatch mutated Snapshot: got=%#v want=%#v", got, snap)
	}
	if events := recorder.Events(); len(events) != 0 {
		t.Fatalf("source owner mismatch emitted events: %#v", events)
	}
}

func TestSnapshotReguidReplacementRejectsMutation(t *testing.T) {
	d := newTestDeps(t)
	r := newSnapshotReconciler(d)
	snap := testEventSnapshot("reguid-snapshot")
	if err := d.Create(t.Context(), snap); err != nil {
		t.Fatal(err)
	}
	d.zfsb.ReplacePool("tank", 1<<40, "2", "ONLINE")
	before := &zfscsiv1.Snapshot{}
	if err := d.Get(t.Context(), types.NamespacedName{Name: snap.Name}, before); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err == nil || !strings.Contains(err.Error(), "GUID mismatch") {
		t.Fatalf("reconcile after reguid error=%v, want GUID mismatch", err)
	}
	after := &zfscsiv1.Snapshot{}
	if err := d.Get(t.Context(), types.NamespacedName{Name: snap.Name}, after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Status, after.Status) || !reflect.DeepEqual(before.Finalizers, after.Finalizers) {
		t.Fatalf("reguid mismatch mutated snapshot: before=%#v after=%#v", before, after)
	}
}

func TestSnapshotReconcilerEmptyVolumeRefFailsWithoutRequeue(t *testing.T) {
	d := newTestDeps(t)
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "empty-ref"}, Spec: zfscsiv1.SnapshotSpec{SourceVolumeID: "csi:tank:block:source", SnapName: "snap", SnapshotID: "csi:tank:block:source@snap", OwnerNode: "storage-a"}}
	if err := d.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	r := newSnapshotReconciler(d)
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result=%#v, want no requeue", result)
	}
	got := &zfscsiv1.Snapshot{}
	if err := d.Get(context.Background(), types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CurrentState() != zfscsiv1.SnapshotStateError {
		t.Fatalf("state=%q, want Error", got.Status.CurrentState())
	}
}

func testEventSnapshot(name string) *zfscsiv1.Snapshot {
	volumeID := "csi:tank:block:" + name
	return &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: zfscsiv1.SnapshotSpec{
		VolumeRef: volumeID, SourceVolumeID: volumeID, SnapName: "snap", SnapshotID: volumeID + "@snap", OwnerNode: "storage-a",
	}}
}

func reconcileSnapshot(t *testing.T, r *SnapshotReconciler, name string) reconcile.Result {
	t.Helper()
	snap := &zfscsiv1.Snapshot{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: name}, snap); err == nil && snap.Spec.VolumeRef != "" {
		source := &zfscsiv1.Volume{}
		key := types.NamespacedName{Name: snap.Spec.VolumeRef}
		if err := r.Get(t.Context(), key, source); apierrors.IsNotFound(err) {
			source = &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: snap.Spec.VolumeRef}, Spec: zfscsiv1.VolumeSpec{
				Provenance: zfscsiv1.VolumeProvenanceDynamic, VolumeID: snap.Spec.SourceVolumeID,
				Pool: "tank", PoolGUID: snap.Spec.PoolGUID, Type: zfscsiv1.VolumeTypeBlock, OwnerNode: snap.Spec.OwnerNode,
			}}
			if err := r.Create(t.Context(), source); err != nil {
				t.Fatal(err)
			}
		}
	}
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: nn(name)})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func getSnapshot(t *testing.T, d *testDeps, name string) *zfscsiv1.Snapshot {
	t.Helper()
	snap := &zfscsiv1.Snapshot{}
	if err := d.Get(context.Background(), nn(name), snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

func setSnapshotDeleting(t *testing.T, d *testDeps, name string) {
	t.Helper()
	snap := getSnapshot(t, d, name)
	patch := crclient.MergeFrom(snap.DeepCopy())
	snap.Status.State = zfscsiv1.SnapshotStateDeleting
	if err := d.Status().Patch(context.Background(), snap, patch); err != nil {
		t.Fatal(err)
	}
}

func deleteSnapshot(t *testing.T, d *testDeps, name string) {
	t.Helper()
	if err := d.Delete(context.Background(), getSnapshot(t, d, name)); err != nil {
		t.Fatal(err)
	}
}

type finalizerPatchErrorClient struct {
	crclient.Client
}

func (finalizerPatchErrorClient) Patch(context.Context, crclient.Object, crclient.Patch, ...crclient.PatchOption) error {
	return errors.New("finalizer patch failed")
}

type snapshotErrorZFS struct {
	zfs.Backend
	snapshotErr error
	destroyErr  error
}

func (f *snapshotErrorZFS) Snapshot(ctx context.Context, dataset, snapName string) error {
	if f.snapshotErr != nil {
		return f.snapshotErr
	}
	return f.Backend.Snapshot(ctx, dataset, snapName)
}

func (f *snapshotErrorZFS) DestroySnapshot(ctx context.Context, dataset, snapName string) error {
	if f.destroyErr != nil {
		return f.destroyErr
	}
	return f.Backend.DestroySnapshot(ctx, dataset, snapName)
}
