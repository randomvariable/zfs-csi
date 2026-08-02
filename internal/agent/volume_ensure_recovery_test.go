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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func nn(name string) apimachinerytypes.NamespacedName {
	return apimachinerytypes.NamespacedName{Name: name}
}

func getVol(t *testing.T, d *testDeps, name string) *zfscsiv1.Volume {
	t.Helper()
	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), nn(name), got); err != nil {
		t.Fatal(err)
	}

	return got
}

// markReady patches a freshly-created Volume to Ready state.
func markReady(t *testing.T, d *testDeps, vol *zfscsiv1.Volume) {
	t.Helper()
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	vol.Status.ObservedGeneration = vol.Generation
	if err := d.Client.Status().Patch(context.Background(), vol, patch); err != nil {
		t.Fatal(err)
	}
}

func createReadyBlock(t *testing.T, d *testDeps, name string) *zfscsiv1.Volume {
	t.Helper()
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Transport: zfscsiv1.TransportNVMeTCP,
			Capacity: 1 << 30, VolName: name, VolumeID: "csi:tank:block:" + name,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	got := getVol(t, d, name)
	markReady(t, d, got)

	return got
}

func createReadyFilesystem(t *testing.T, d *testDeps, name string) *zfscsiv1.Volume {
	t.Helper()
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
			Capacity: 1 << 30, VolName: name, VolumeID: "csi:tank:filesystem:" + name,
			NFSExportCIDRs: []string{"10.0.0.0/24"}, NFSExportAccessMode: "rw",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	got := getVol(t, d, name)
	markReady(t, d, got)

	return got
}

func datasetPath(t *testing.T, volID string) string {
	t.Helper()
	p, err := naming.ParseVolID(volID)
	if err != nil {
		t.Fatalf("parse volID %q: %v", volID, err)
	}

	return p.DatasetPath()
}

func reconcileVol(t *testing.T, r *VolumeReconciler, name string) reconcile.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: nn(name)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	return res
}

// depsWithRecordingZFS builds testDeps wired to a recordingZFS over a fresh
// fake backend with the given pool. The tank root dataset (/tank) is seeded so
// the explicit-NFSv4-root derivation can resolve it.
func depsWithRecordingZFS(t *testing.T) (*testDeps, *recordingZFS, *VolumeReconciler) {
	t.Helper()
	d := newTestDeps(t)
	rz := &recordingZFS{Backend: zfsfake.New().WithPool("tank", 1<<40)}
	rz.WithDataset("tank", zfs.KindFilesystem, false, zfs.KeyNone)
	rz.WithExportPath("tank", "/tank")
	rz.WithMounted("tank", true)
	d.useBackend(rz.Backend)
	r := d.reconciler()
	r.ZFS = rz

	return d, rz, r
}

// F3/B1: a Ready filesystem volume is re-shared on every ensure pass
// (unconditional idempotent re-export — the reboot-recovery contract). The old
// design gated on Get().ExportPath, but the real libzfs backend never populates
// that field, so the gate was inert in production and Share ran unconditionally
// anyway. The correct contract is: Share is ALWAYS called and MUST be idempotent
// (Backend.Share guards zfs_mount with zfs_is_mounted and re-runs the sharenfs
// changelist). This test locks that Share is invoked on a steady-state pass; the
// idempotency of Share itself is covered in the libzfs backend.
func TestEnsure_FilesystemReSharesEveryPass(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyFilesystem(t, d, "fs-shared")
	rz.WithDataset(datasetPath(t, vol.Spec.VolumeID), zfs.KindFilesystem, true, zfs.KeyNone)

	// Two passes: Share is called each time (idempotent re-ensure), and neither
	// pass flips the volume out of Ready.
	reconcileVol(t, r, vol.Name)
	reconcileVol(t, r, vol.Name)

	if len(rz.shareImportedCalls) != 2 {
		t.Fatalf("ShareImported called %d times over two ensure passes; want 2 (unconditional idempotent re-share)", len(rz.shareImportedCalls))
	}
	if got := getVol(t, d, vol.Name); got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("state = %q, want Ready (idempotent re-share must not flap)", got.Status.State)
	}
}

func TestEnsure_FilesystemUsesMetadataPreservingShare(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyFilesystem(t, d, "fs-preserve-root")
	rz.WithDataset(datasetPath(t, vol.Spec.VolumeID), zfs.KindFilesystem, true, zfs.KeyNone)

	reconcileVol(t, r, vol.Name)

	if len(rz.shareImportedCalls) != 1 {
		t.Fatalf("ShareImported called %d times, want 1", len(rz.shareImportedCalls))
	}
	if len(rz.shareCalls) != 0 {
		t.Fatalf("Share calls = %+v, want none (steady-state ensure must preserve root metadata)", rz.shareCalls)
	}
}

// F3: a Ready filesystem volume whose export was wiped (reboot) is re-shared on
// the ensure pass.
func TestEnsure_FilesystemUnshared_ReShares(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyFilesystem(t, d, "fs-wiped")
	rz.WithDataset(datasetPath(t, vol.Spec.VolumeID), zfs.KindFilesystem, false, zfs.KeyNone)

	reconcileVol(t, r, vol.Name)

	if len(rz.shareImportedCalls) != 1 {
		t.Fatalf("ShareImported called %d times on an unshared dataset; want 1", len(rz.shareImportedCalls))
	}
}

func TestEnsure_FilesystemHealsNFSRootStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		root string
	}{
		{name: "missing"},
		{name: "divergent", root: "/wrong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, rz, r := depsWithRecordingZFS(t)
			vol := createReadyFilesystem(t, d, "fs-root-"+tc.name)
			dataset := datasetPath(t, vol.Spec.VolumeID)
			rz.WithDataset(dataset, zfs.KindFilesystem, true, zfs.KeyNone)
			path := "/tank/" + tc.name
			rz.WithExportPath(dataset, path)
			before := getVol(t, d, vol.Name)
			patch := crclient.MergeFrom(before.DeepCopy())
			before.Status.ExportPath = path
			before.Status.NFSRootPath = tc.root
			if err := d.Client.Status().Patch(t.Context(), before, patch); err != nil {
				t.Fatal(err)
			}

			reconcileVol(t, r, vol.Name)
			got := getVol(t, d, vol.Name)
			if got.Status.ExportPath != path {
				t.Fatalf("export path changed = %q, want %q", got.Status.ExportPath, path)
			}
			if got.Status.NFSRootPath != "/tank" {
				t.Fatalf("root = %q, want /tank", got.Status.NFSRootPath)
			}
			if got.Status.State != zfscsiv1.VolumeStateReady {
				t.Fatalf("state = %q, want Ready", got.Status.State)
			}
		})
	}
}

func TestEnsure_FilesystemCorrectNFSStatusIsNoOp(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyFilesystem(t, d, "fs-root-correct")
	dataset := datasetPath(t, vol.Spec.VolumeID)
	path := "/tank/correct"
	rz.WithDataset(dataset, zfs.KindFilesystem, true, zfs.KeyNone)
	rz.WithExportPath(dataset, path)
	before := getVol(t, d, vol.Name)
	patch := crclient.MergeFrom(before.DeepCopy())
	before.Status.ExportPath = path
	before.Status.NFSRootPath = "/tank"
	before.Status.ObservedGeneration = before.Generation
	before.Status.ActualCapacity = before.Spec.Capacity
	if err := d.Client.Status().Patch(t.Context(), before, patch); err != nil {
		t.Fatal(err)
	}
	before = getVol(t, d, vol.Name)
	resourceVersion := before.ResourceVersion
	sharesBefore := len(rz.shareImportedCalls)

	reconcileVol(t, r, vol.Name)

	after := getVol(t, d, vol.Name)
	if got := len(rz.shareImportedCalls); got != sharesBefore+1 {
		t.Fatalf("Share calls = %d, want %d; ensure path was not reached", got, sharesBefore+1)
	}
	if after.ResourceVersion != resourceVersion {
		t.Fatalf("resourceVersion changed %q -> %q despite correct status", resourceVersion, after.ResourceVersion)
	}
	if after.Status.ExportPath != path || after.Status.NFSRootPath != "/tank" || after.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("status drifted: %+v", after.Status)
	}
}

// F4/B3: dataset gone + pool NOT imported must NOT recreate and must hold Ready.
func TestEnsure_DatasetGonePoolImported_FlipsPending(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyBlock(t, d, "blk-gone")
	// dataset absent, pool present (default WithPool tank).
	_ = rz

	reconcileVol(t, r, vol.Name)

	if got := getVol(t, d, vol.Name); got.Status.State != zfscsiv1.VolumeStatePending {
		t.Fatalf("state = %q, want Pending (dataset gone, pool imported)", got.Status.State)
	}
}

func TestEnsure_DatasetGonePoolImported_PreservesReadyCondition(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyBlock(t, d, "blk-gone-ready")
	_ = rz
	cur := getVol(t, d, vol.Name)
	patch := crclient.MergeFrom(cur.DeepCopy())
	cur.Status.Conditions = []metav1.Condition{{
		Type: string(zfscsiv1.VolumeConditionReady), Status: metav1.ConditionTrue,
		Reason: "VolumeReady", Message: "still preserve this condition", LastTransitionTime: metav1.Now(),
	}}
	if err := d.Client.Status().Patch(context.Background(), cur, patch); err != nil {
		t.Fatal(err)
	}

	reconcileVol(t, r, vol.Name)
	got := getVol(t, d, vol.Name)
	if ready := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionReady)); ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready = %#v, want preserved True condition", ready)
	}
	if health := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy)); health == nil || health.Status != metav1.ConditionFalse {
		t.Fatalf("BackendHealthy = %#v, want False", health)
	}
}

// F4: Exists error preserves state and uses the workqueue's rate-limited backoff.
func TestEnsure_ExistsError_PreservesState(t *testing.T) {
	d := newTestDeps(t)
	base := zfsfake.New().WithPool("tank", 1<<40)
	d.useBackend(base)
	r := d.reconciler()
	r.ZFS = &existsErrZFS{Backend: base}

	vol := createReadyBlock(t, d, "blk-existserr")

	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}})
	if err == nil {
		t.Fatal("expected error on Exists failure")
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("expected rate-limited error backoff, got fixed requeue %v", res.RequeueAfter)
	}
	if got := getVol(t, d, vol.Name); got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("state = %q, want Ready preserved on Exists error", got.Status.State)
	}
}

// F1/B2: fence fires only for single-writer replacement; never multi-node.
func TestEnsure_FenceOnlyForSingleWriterReplacement(t *testing.T) {
	tests := []struct {
		name      string
		desired   []zfscsiv1.MappedInitiator
		live      []string
		wantFence bool
	}{
		{
			name:      "single-writer failover [A]->[B] fences",
			desired:   []zfscsiv1.MappedInitiator{{NodeName: "b", InitiatorID: "nqn.b"}},
			live:      []string{"nqn.a"},
			wantFence: true,
		},
		{
			name:      "multi-node scale-down does not fence",
			desired:   []zfscsiv1.MappedInitiator{{NodeName: "b", InitiatorID: "nqn.b"}, {NodeName: "c", InitiatorID: "nqn.c"}},
			live:      []string{"nqn.a", "nqn.b", "nqn.c"},
			wantFence: false,
		},
		{
			// ROX/RWX scaled [A,B]->[A]: one orphan unmapped (B) and len(desired)==1,
			// but the survivor A was already live -> subset shrink, must NOT fence.
			name:      "multi-node scale-down to one survivor does not fence",
			desired:   []zfscsiv1.MappedInitiator{{NodeName: "a", InitiatorID: "nqn.a"}},
			live:      []string{"nqn.a", "nqn.b"},
			wantFence: false,
		},
		{
			name:      "steady-state single writer, no orphan, no fence",
			desired:   []zfscsiv1.MappedInitiator{{NodeName: "b", InitiatorID: "nqn.b"}},
			live:      []string{"nqn.b"},
			wantFence: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, rz, r := depsWithRecordingZFS(t)
			vol := createReadyBlock(t, d, "blk-fence")
			rz.WithDataset(datasetPath(t, vol.Spec.VolumeID), zfs.KindBlock, false, zfs.KeyNone)

			cur := getVol(t, d, vol.Name)
			patch := crclient.MergeFrom(cur.DeepCopy())
			cur.Status.MappedInitiators = tt.desired
			if err := d.Client.Status().Patch(context.Background(), cur, patch); err != nil {
				t.Fatal(err)
			}

			p, _ := naming.ParseVolID(vol.Spec.VolumeID)
			nqn, _ := naming.TargetNQN(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)
			d.export.exports[nqn] = true
			d.export.mapped[nqn] = map[string]bool{}
			for _, l := range tt.live {
				d.export.mapped[nqn][l] = true
			}

			reconcileVol(t, r, vol.Name)

			fenced := len(d.export.forceDisconnects) > 0
			if fenced != tt.wantFence {
				t.Fatalf("fence fired = %v, want %v (forceDisconnects=%v)", fenced, tt.wantFence, d.export.forceDisconnects)
			}
		})
	}
}

func TestEnsure_MapFailureFencesReplacementAndPersistsUnhealthy(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyBlock(t, d, "blk-map-fail-fence")
	rz.WithDataset(datasetPath(t, vol.Spec.VolumeID), zfs.KindBlock, false, zfs.KeyNone)
	cur := getVol(t, d, vol.Name)
	patch := crclient.MergeFrom(cur.DeepCopy())
	cur.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{NodeName: "b", InitiatorID: "nqn.b"}}
	if err := d.Client.Status().Patch(context.Background(), cur, patch); err != nil {
		t.Fatal(err)
	}
	p, _ := naming.ParseVolID(vol.Spec.VolumeID)
	nqn, _ := naming.TargetNQN(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)
	d.export.exports[nqn] = true
	d.export.mapped[nqn] = map[string]bool{"nqn.a": true}
	d.export.mapErr = errors.New("map B failed")

	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: nn(vol.Name)})
	if err == nil {
		t.Fatal("expected MapInitiator error")
	}
	if len(d.export.forceDisconnects) != 1 || d.export.forceDisconnects[0] != nqn {
		t.Fatalf("ForceDisconnect calls = %v, want [%q]", d.export.forceDisconnects, nqn)
	}
	got := getVol(t, d, vol.Name)
	if health := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy)); health == nil || health.Status != metav1.ConditionFalse {
		t.Fatalf("BackendHealthy = %#v, want False", health)
	}
}

// F6: reconcileDelete refuses (requeue, no destroy) while still mapped.
func TestReconcileDelete_InUseGuard(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyBlock(t, d, "blk-inuse")
	ds := datasetPath(t, vol.Spec.VolumeID)
	rz.WithDataset(ds, zfs.KindBlock, false, zfs.KeyNone)

	cur := getVol(t, d, vol.Name)
	patch := crclient.MergeFrom(cur.DeepCopy())
	cur.Status.State = zfscsiv1.VolumeStateDeleting
	cur.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{NodeName: "a", InitiatorID: "nqn.a"}}
	if err := d.Client.Status().Patch(context.Background(), cur, patch); err != nil {
		t.Fatal(err)
	}

	res := reconcileVol(t, r, vol.Name)
	if res.Priority == nil {
		t.Fatal("expected low-priority requeue for in-use delete")
	}
	if len(d.export.forceDisconnects) != 0 {
		t.Fatal("in-use delete must not fence")
	}
	if ok, _ := rz.Exists(context.Background(), ds); !ok {
		t.Fatal("dataset destroyed despite in-use guard")
	}
}

// F6: the force annotation overrides the guard and lets delete proceed.
func TestReconcileDelete_ForceAnnotationOverrides(t *testing.T) {
	d, rz, r := depsWithRecordingZFS(t)
	vol := createReadyBlock(t, d, "blk-force")
	ds := datasetPath(t, vol.Spec.VolumeID)
	rz.WithDataset(ds, zfs.KindBlock, false, zfs.KeyNone)

	cur := getVol(t, d, vol.Name)
	patch := crclient.MergeFrom(cur.DeepCopy())
	cur.Annotations = map[string]string{zfscsiv1.ForceDeleteAnnotation: "true"}
	if err := d.Patch(context.Background(), cur, patch); err != nil {
		t.Fatal(err)
	}
	cur = getVol(t, d, vol.Name)
	patch = crclient.MergeFrom(cur.DeepCopy())
	cur.Status.State = zfscsiv1.VolumeStateDeleting
	cur.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{NodeName: "a", InitiatorID: "nqn.a"}}
	if err := d.Client.Status().Patch(context.Background(), cur, patch); err != nil {
		t.Fatal(err)
	}

	reconcileVol(t, r, vol.Name)

	if ok, _ := rz.Exists(context.Background(), ds); ok {
		t.Fatal("dataset survived despite force-delete annotation")
	}
}

// existsErrZFS forces Exists to return an error.
type existsErrZFS struct {
	*zfsfake.Backend
}

func (e *existsErrZFS) Exists(_ context.Context, _ string) (bool, error) {
	return false, context.DeadlineExceeded
}
