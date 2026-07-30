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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func newImportReconciler(t *testing.T, backend *zfsfake.Backend, objects ...runtime.Object) *VolumeImportReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := zfscsiv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	enabled := true
	now := time.Now()
	guid, err := backend.PoolGUID(t.Context(), "tank")
	if err != nil {
		guid = "1"
	}
	observed := metav1.NewTime(now)
	objects = append(objects, &zfscsiv1.StorageNode{ObjectMeta: metav1.ObjectMeta{Name: "storage-a", Generation: 1}, Spec: zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: []string{guid}, Enabled: &enabled, NetworkDomain: "workers"}, Status: zfscsiv1.StorageNodeStatus{ObservedGeneration: 1, LastObservedTime: &observed, ReachableFrom: []string{"workers"}, Endpoints: []zfscsiv1.StorageNodeEndpoint{{Protocol: zfscsiv1.StorageProtocolNFS, Host: "storage-a", Port: 2049}, {Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: "storage-a", Port: 4420}}, Conditions: []metav1.Condition{{Type: zfscsiv1.StorageNodeConditionReady, Status: metav1.ConditionTrue}}, Pools: []zfscsiv1.StorageNodePoolStatus{{GUID: guid, Name: "tank", FreeBytes: 1, Ready: true}}}})
	client := ctrlfake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).WithStatusSubresource(&zfscsiv1.VolumeImport{}, &zfscsiv1.Volume{}).Build()
	return &VolumeImportReconciler{Client: client, Log: logr.Discard(), ZFS: backend, NodeName: "storage-a", ProbeFormat: func(context.Context, string) (string, error) { return "", nil }}
}

func importFixture(name, path string, kind zfscsiv1.VolumeType) *zfscsiv1.VolumeImport {
	return &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: zfscsiv1.VolumeImportSpec{
		Pool: "tank", BackendPath: path, Type: kind, Capacity: 1 << 30, OwnerNode: "storage-a", Transport: zfscsiv1.TransportNVMeTCP,
		NFSExportCIDRs: []string{"10.42.0.0/16"}, NFSExportAccessMode: "rw", DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain,
	}}
}

func reconcileImport(t *testing.T, r *VolumeImportReconciler, name string) *zfscsiv1.VolumeImport {
	t.Helper()
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestVolumeImportMaterializesRetainedVolumeAtArbitraryPath(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/database", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	imp := importFixture("database", "tank/apps/database", zfscsiv1.VolumeTypeBlock)
	r := newImportReconciler(t, backend, imp)
	var got *zfscsiv1.VolumeImport
	for range 4 {
		got = reconcileImport(t, r, imp.Name)
		if got.Status.State == zfscsiv1.VolumeImportStateReady {
			break
		}
	}
	wantID := naming.ImportID(imp.Spec.BackendPath)
	wantHandle, err := naming.EncodeVolID(imp.Spec.Pool, zfs.KindBlock, wantID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.VolumeHandle != wantHandle || got.Status.VolumeRef != wantID || got.Status.VolumeRef == imp.Name {
		t.Fatalf("unexpected status: %#v", got.Status)
	}
	vol := &zfscsiv1.Volume{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: got.Status.VolumeRef}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.BackendPath != "tank/apps/database" || vol.Spec.Provenance != zfscsiv1.VolumeProvenanceImported || vol.Spec.DeletionPolicy != zfscsiv1.VolumeDeletionPolicyRetain {
		t.Fatalf("unexpected imported Volume: %#v", vol.Spec)
	}
	if len(vol.OwnerReferences) != 0 {
		t.Fatalf("imported Volume has ownerReferences: %#v", vol.OwnerReferences)
	}
	if vol.Annotations[volumeImportAnnotation] != volumeImportClaim(imp) || !slices.Contains(vol.Finalizers, zfscsiv1.VolumeFinalizer) {
		t.Fatalf("materialized Volume metadata = %#v", vol.ObjectMeta)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: imp.Name}}); err != nil {
		t.Fatalf("idempotent reconcile: %v", err)
	}
}

func TestVolumeImportDuplicateBackendPathHasSingleWinner(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/shared", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	first := importFixture("a-first", "tank/apps/shared", zfscsiv1.VolumeTypeBlock)
	second := importFixture("b-second", "tank/apps/shared", zfscsiv1.VolumeTypeBlock)
	r := newImportReconciler(t, backend, first, second)
	loser := reconcileImport(t, r, second.Name)
	if loser.Status.State != zfscsiv1.VolumeImportStateFailed {
		t.Fatalf("loser state=%q, want Failed", loser.Status.State)
	}
	winner := reconcileImport(t, r, first.Name)
	if winner.Status.VolumeRef == "" {
		t.Fatalf("winner status=%#v", winner.Status)
	}
	vol := &zfscsiv1.Volume{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: winner.Status.VolumeRef}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Annotations[volumeImportAnnotation] != volumeImportClaim(first) {
		t.Fatalf("claim owner=%q", vol.Annotations[volumeImportAnnotation])
	}
}

func TestVolumeImportEarlierBackendClaimantWinsClusterWide(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/cluster-shared", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	first := importFixture("first", "tank/apps/cluster-shared", zfscsiv1.VolumeTypeBlock)
	second := importFixture("second", "tank/apps/cluster-shared", zfscsiv1.VolumeTypeBlock)
	first.CreationTimestamp = metav1.NewTime(time.Unix(1, 0))
	second.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
	r := newImportReconciler(t, backend, first, second)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: first.Name}}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := r.Get(ctx, types.NamespacedName{Name: first.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.VolumeRef == "" {
		t.Fatalf("winner status=%#v", got.Status)
	}
}

func TestVolumeImportNamespaceIsNotPartOfIdentity(t *testing.T) {
	imp := importFixture("wrong-namespace", "tank/apps/existing", zfscsiv1.VolumeTypeBlock)
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity(imp.Spec.BackendPath, zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	r := newImportReconciler(t, backend, imp)
	got := reconcileImport(t, r, imp.Name)
	if got.Status.VolumeRef == "" {
		t.Fatalf("cluster-scoped import did not materialize: %#v", got.Status)
	}
}

func TestVolumeImportFailsWhenMaterializedVolumeFails(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/failing", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	imp := importFixture("failing", "tank/apps/failing", zfscsiv1.VolumeTypeBlock)
	id := naming.ImportID(imp.Spec.BackendPath)
	handle, _ := naming.EncodeVolID("tank", zfs.KindBlock, id)
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: id, Annotations: map[string]string{volumeImportAnnotation: volumeImportClaim(imp)}, Finalizers: []string{zfscsiv1.VolumeFinalizer}},
		Spec:       zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, BackendPath: imp.Spec.BackendPath, DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain, Pool: "tank", Capacity: 2 << 30, Type: zfscsiv1.VolumeTypeBlock, OwnerNode: "storage-a", VolumeID: handle},
		Status:     zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateError},
	}
	r := newImportReconciler(t, backend, imp, vol)
	if got := reconcileImport(t, r, imp.Name); got.Status.State != zfscsiv1.VolumeImportStateFailed {
		t.Fatalf("state=%q, want Failed", got.Status.State)
	}
}

func TestVolumeImportFailsInsteadOfRecreatingDeletedMaterializedVolume(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/deleted", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	imp := importFixture("deleted", "tank/apps/deleted", zfscsiv1.VolumeTypeBlock)
	imp.Status.VolumeRef = naming.ImportID(imp.Spec.BackendPath)
	imp.Status.VolumeHandle, _ = naming.EncodeVolID("tank", zfs.KindBlock, imp.Status.VolumeRef)
	r := newImportReconciler(t, backend, imp)
	got := reconcileImport(t, r, imp.Name)
	if got.Status.State != zfscsiv1.VolumeImportStateFailed {
		t.Fatalf("state=%q, want Failed", got.Status.State)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := r.List(context.Background(), volumes); err != nil {
		t.Fatal(err)
	}
	if len(volumes.Items) != 0 {
		t.Fatalf("recreated %d Volumes", len(volumes.Items))
	}
}

func TestVolumeImportValidationFailures(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		kind     zfscsiv1.VolumeType
		seedKind zfs.VolumeKind
		capacity int64
		key      zfs.KeyLocality
	}{
		{name: "managed-subtree", path: "tank/csi/block/existing", kind: zfscsiv1.VolumeTypeBlock, seedKind: zfs.KindBlock, capacity: 2 << 30},
		{name: "wrong-kind", path: "tank/apps/wrong", kind: zfscsiv1.VolumeTypeFilesystem, seedKind: zfs.KindBlock, capacity: 2 << 30},
		{name: "undersized", path: "tank/apps/small", kind: zfscsiv1.VolumeTypeBlock, seedKind: zfs.KindBlock, capacity: 1},
		{name: "encrypted", path: "tank/apps/secret", kind: zfscsiv1.VolumeTypeBlock, seedKind: zfs.KindBlock, capacity: 2 << 30, key: zfs.KeyAvailable},
		{name: "zero-refquota", path: "tank/apps/unlimited", kind: zfscsiv1.VolumeTypeFilesystem, seedKind: zfs.KindFilesystem, capacity: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity(tt.path, tt.seedKind, tt.capacity, false, tt.key)
			imp := importFixture(tt.name, tt.path, tt.kind)
			r := newImportReconciler(t, backend, imp)
			if got := reconcileImport(t, r, imp.Name); got.Status.State != zfscsiv1.VolumeImportStateFailed {
				t.Fatalf("state=%q, want Failed", got.Status.State)
			}
			vols := &zfscsiv1.VolumeList{}
			if err := r.List(context.Background(), vols); err != nil {
				t.Fatal(err)
			}
			if len(vols.Items) != 0 {
				t.Fatalf("validation materialized %d Volumes", len(vols.Items))
			}
		})
	}
}

func TestVolumeImportRejectsBlockFormatMismatch(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/formatted", zfs.KindBlock, 2<<30, false, zfs.KeyNone).WithFormat("tank/apps/formatted", "xfs")
	imp := importFixture("format-mismatch", "tank/apps/formatted", zfscsiv1.VolumeTypeBlock)
	imp.Spec.FsType = "ext4"
	r := newImportReconciler(t, backend, imp)
	if got := reconcileImport(t, r, imp.Name); got.Status.State != zfscsiv1.VolumeImportStateFailed {
		t.Fatalf("state=%q", got.Status.State)
	}
}

func TestVolumeImportConflictingObjectRejected(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/conflict", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	imp := importFixture("conflict", "tank/apps/conflict", zfscsiv1.VolumeTypeBlock)
	id := naming.ImportID(imp.Spec.BackendPath)
	handle, _ := naming.EncodeVolID("tank", zfs.KindBlock, id)
	conflict := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: id}, Spec: zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, BackendPath: "tank/apps/different", Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, OwnerNode: "storage-a", VolumeID: handle}}
	r := newImportReconciler(t, backend, imp, conflict)
	if got := reconcileImport(t, r, imp.Name); got.Status.State != zfscsiv1.VolumeImportStateFailed {
		t.Fatalf("state=%q", got.Status.State)
	}
}

func TestVolumeImportRecreationRejectsChangedMaterializedIntent(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/recreated", zfs.KindFilesystem, 2<<30, false, zfs.KeyNone)
	original := importFixture("original", "tank/apps/recreated", zfscsiv1.VolumeTypeFilesystem)
	original.Spec.Transport = ""
	r := newImportReconciler(t, backend, original)
	first := reconcileImport(t, r, original.Name)
	if first.Status.VolumeRef == "" {
		t.Fatalf("first status=%#v", first.Status)
	}
	if err := r.Delete(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	recreated := importFixture(original.Name, original.Spec.BackendPath, zfscsiv1.VolumeTypeFilesystem)
	recreated.Spec.Transport = ""
	recreated.Spec.NFSExportCIDRs = []string{"10.99.0.0/16"}
	if err := r.Create(context.Background(), recreated); err != nil {
		t.Fatal(err)
	}
	got := reconcileImport(t, r, recreated.Name)
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.State != zfscsiv1.VolumeImportStateFailed || ready == nil || ready.Reason != "VolumeConflict" || !strings.Contains(ready.Message, "nfsExportCIDR") {
		t.Fatalf("status=%#v", got.Status)
	}
}

func TestVolumeImportRecreationReusesIdenticalMaterializedIntent(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/recreated-identical", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	original := importFixture("same-name", "tank/apps/recreated-identical", zfscsiv1.VolumeTypeBlock)
	r := newImportReconciler(t, backend, original)
	first := reconcileImport(t, r, original.Name)
	if err := r.Delete(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	recreated := original.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = ""
	recreated.CreationTimestamp = metav1.Time{}
	recreated.Status = zfscsiv1.VolumeImportStatus{}
	if err := r.Create(context.Background(), recreated); err != nil {
		t.Fatal(err)
	}
	got := reconcileImport(t, r, recreated.Name)
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.State == zfscsiv1.VolumeImportStateFailed || (ready != nil && ready.Reason == "VolumeConflict") {
		t.Fatalf("status=%#v", got.Status)
	}
}

func TestImportedVolumeFormatDeclarationMatrix(t *testing.T) {
	tests := []struct {
		name, materialized, declared string
		wantMismatch                 bool
	}{
		{name: "raw block survives volume xfs default", materialized: "", declared: ""},
		{name: "declared xfs remains xfs", materialized: "xfs", declared: "xfs"},
		{name: "raw changed to xfs", materialized: "", declared: "xfs", wantMismatch: true},
		{name: "xfs changed to raw", materialized: "xfs", declared: "", wantMismatch: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imp := importFixture("format-intent", "tank/apps/format-intent", zfscsiv1.VolumeTypeBlock)
			imp.Spec.FsType = tt.declared
			path := imp.Spec.BackendPath
			handle, _ := naming.EncodeVolID("tank", zfs.KindBlock, naming.ImportID(path))
			vol := &zfscsiv1.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: naming.ImportID(path), Annotations: map[string]string{volumeImportAnnotation: volumeImportClaim(imp)}},
				Spec:       materializedImportIntent(imp, path, handle, "1", "workers"),
			}
			vol.Spec.ImportFsTypeDeclaration = tt.materialized
			vol.Spec.FsType = "xfs" // Kubernetes applies this default for both declarations.
			mismatches := importedVolumeIntentMismatches(vol, imp, path, handle, "1", "workers")
			gotMismatch := slices.Contains(mismatches, "importFsTypeDeclaration")
			if gotMismatch != tt.wantMismatch {
				t.Fatalf("mismatches=%v, want declaration mismatch=%t", mismatches, tt.wantMismatch)
			}
			if slices.Contains(mismatches, "fsType") {
				t.Fatalf("misleading fsType diagnostic: %v", mismatches)
			}
		})
	}
}

func TestImportIDRetainsHashWithin63Characters(t *testing.T) {
	idA := naming.ImportID("tank/" + strings.Repeat("a", 100) + "/same-readable-leaf")
	idB := naming.ImportID("tank/other/same-readable-leaf")
	if len(idA) > 63 || idA == idB || !strings.HasPrefix(idA, "import-") {
		t.Fatalf("bad import ids: %q %q", idA, idB)
	}
}

func TestVolumeImportRejectsInvalidFilesystemExportPathsBeforeMaterialization(t *testing.T) {
	for _, exportPath := range []string{"", "legacy", "none", "relative/path", "/tank/../etc", "/tank/apps/"} {
		t.Run(strings.ReplaceAll(exportPath, "/", "_"), func(t *testing.T) {
			backendPath := "tank/apps/invalid-" + strings.NewReplacer("/", "-", ".", "-").Replace(exportPath)
			backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity(backendPath, zfs.KindFilesystem, 2<<30, false, zfs.KeyNone).WithExportPath(backendPath, exportPath)
			imp := importFixture("invalid-export", backendPath, zfscsiv1.VolumeTypeFilesystem)
			imp.Spec.Transport = ""
			r := newImportReconciler(t, backend, imp)
			got := reconcileImport(t, r, imp.Name)
			if got.Status.State != zfscsiv1.VolumeImportStateFailed {
				t.Fatalf("state=%q, want Failed", got.Status.State)
			}
			volumes := &zfscsiv1.VolumeList{}
			if err := r.List(context.Background(), volumes); err != nil {
				t.Fatal(err)
			}
			if len(volumes.Items) != 0 {
				t.Fatalf("materialized %d Volumes", len(volumes.Items))
			}
			share, err := backend.GetProperty(context.Background(), backendPath, "sharenfs")
			if err != nil || share != "" {
				t.Fatalf("backend share mutated to %q, err=%v", share, err)
			}
		})
	}
}

func TestVolumeImportWrongOwnerDoesNotInspectOrMaterialize(t *testing.T) {
	imp := importFixture("wrong-owner", "tank/apps/data", zfscsiv1.VolumeTypeBlock)
	imp.Spec.OwnerNode = "storage-b"
	r := newImportReconciler(t, zfsfake.New(), imp)
	got := reconcileImport(t, r, imp.Name)
	if got.Status.State != "" {
		t.Fatalf("wrong owner changed status: %#v", got.Status)
	}
}

func TestVolumeImportAbsentPoolStaysPendingWithoutMaterializing(t *testing.T) {
	imp := importFixture("pool-absent", "tank/apps/data", zfscsiv1.VolumeTypeBlock)
	r := newImportReconciler(t, zfsfake.New(), imp)
	got := reconcileImport(t, r, imp.Name)
	if got.Status.State != zfscsiv1.VolumeImportStatePending {
		t.Fatalf("state=%q, want Pending", got.Status.State)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := r.List(context.Background(), volumes); err != nil {
		t.Fatal(err)
	}
	if len(volumes.Items) != 0 {
		t.Fatalf("materialized %d volumes", len(volumes.Items))
	}
}

func TestVolumeImportFilesystemRequiresNFSExportCIDRs(t *testing.T) {
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity("tank/apps/fs", zfs.KindFilesystem, 2<<30, false, zfs.KeyNone)
	imp := importFixture("missing-cidr", "tank/apps/fs", zfscsiv1.VolumeTypeFilesystem)
	imp.Spec.NFSExportCIDRs = nil
	r := newImportReconciler(t, backend, imp)
	if got := reconcileImport(t, r, imp.Name); got.Status.State != zfscsiv1.VolumeImportStateFailed {
		t.Fatalf("state=%q", got.Status.State)
	}
}

func TestVolumeImportRejectsInvalidNFSIntentBeforeMaterialization(t *testing.T) {
	for name, mutate := range map[string]func(*zfscsiv1.VolumeImport){
		"unmasked CIDR": func(imp *zfscsiv1.VolumeImport) { imp.Spec.NFSExportCIDRs = []string{"10.42.0.7/24"} },
		"mapped IPv6":   func(imp *zfscsiv1.VolumeImport) { imp.Spec.NFSExportCIDRs = []string{"::ffff:192.0.2.0/120"} },
		"bracketed IPv6": func(imp *zfscsiv1.VolumeImport) {
			imp.Spec.NFSExportCIDRs = []string{"[2001:db8::]/64"}
		},
		"zoned IPv6": func(imp *zfscsiv1.VolumeImport) { imp.Spec.NFSExportCIDRs = []string{"fe80::%eth0/64"} },
		"option injection": func(imp *zfscsiv1.VolumeImport) {
			imp.Spec.NFSExportCIDRs = []string{"rw=@0.0.0.0/0"}
		},
		"invalid access mode": func(imp *zfscsiv1.VolumeImport) { imp.Spec.NFSExportAccessMode = "rw,no_root_squash" },
	} {
		t.Run(name, func(t *testing.T) {
			backendPath := "tank/apps/invalid-" + strings.ReplaceAll(name, " ", "-")
			backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity(backendPath, zfs.KindFilesystem, 2<<30, false, zfs.KeyNone)
			imp := importFixture("invalid-"+strings.ReplaceAll(name, " ", "-"), backendPath, zfscsiv1.VolumeTypeFilesystem)
			mutate(imp)
			r := newImportReconciler(t, backend, imp)
			got := reconcileImport(t, r, imp.Name)
			ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
			if got.Status.State != zfscsiv1.VolumeImportStateFailed || ready == nil || ready.Reason != "InvalidNFSExportIntent" || !strings.Contains(ready.Message, "invalid NFS export intent") {
				t.Fatalf("status=%#v", got.Status)
			}
			volumes := &zfscsiv1.VolumeList{}
			if err := r.List(t.Context(), volumes); err != nil {
				t.Fatal(err)
			}
			if len(volumes.Items) != 0 {
				t.Fatalf("materialized %d Volumes", len(volumes.Items))
			}
			if share, err := backend.GetProperty(t.Context(), backendPath, "sharenfs"); err != nil || share != "" {
				t.Fatalf("backend share=%q err=%v", share, err)
			}
		})
	}
}

func TestVolumeImportMaterializesCanonicalNFSIntent(t *testing.T) {
	backendPath := "tank/apps/canonical-cidrs"
	backend := zfsfake.New().WithPool("tank", 1<<40).WithDatasetCapacity(backendPath, zfs.KindFilesystem, 2<<30, false, zfs.KeyNone).WithExportPath(backendPath, "/srv/canonical-cidrs")
	imp := importFixture("canonical-cidrs", backendPath, zfscsiv1.VolumeTypeFilesystem)
	imp.Spec.NFSExportCIDRs = []string{" 2001:db8::/64 ", "10.42.0.0/16", "10.42.0.0/16"}
	imp.Spec.NFSExportAccessMode = ""
	r := newImportReconciler(t, backend, imp)
	got := reconcileImport(t, r, imp.Name)
	vol := &zfscsiv1.Volume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: got.Status.VolumeRef}, vol); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(vol.Spec.NFSExportCIDRs, []string{"10.42.0.0/16", "2001:db8::/64"}) || vol.Spec.NFSExportAccessMode != "rw" {
		t.Fatalf("materialized NFS intent=%q/%q", vol.Spec.NFSExportCIDRs, vol.Spec.NFSExportAccessMode)
	}
	if !cidrSetsEqual(vol.Spec.NFSExportCIDRs, imp.Spec.NFSExportCIDRs) {
		t.Fatal("canonical materialized intent differs from equivalent import set")
	}
}
