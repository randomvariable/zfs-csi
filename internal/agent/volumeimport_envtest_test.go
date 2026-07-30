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

//go:build envtest

package agent

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
	testenv "github.com/randomvariable/zfs-csi/internal/testutil/envtest"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func envtestImportBackend(t *testing.T, h *testenv.Harness) *zfsfake.Backend {
	t.Helper()
	backend := zfsfake.New().WithPool("tank", 1<<40)
	guid, err := backend.PoolGUID(t.Context(), "tank")
	if err != nil {
		t.Fatal(err)
	}
	testenv.EnsureStorageNode(t, h, testenv.DefaultOwnerNode, guid)
	return backend
}

func TestEnvtestVolumeImportMaterializesDecoupledRetainedVolume(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)
	backend := envtestImportBackend(t, h).WithDatasetCapacity("tank/apps/envtest", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	r := &VolumeImportReconciler{Client: h.Client, Log: logr.Discard(), ZFS: backend, NodeName: testenv.DefaultOwnerNode, ProbeFormat: func(context.Context, string) (string, error) { return "", nil }}
	imp := &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "envtest-import"}, Spec: zfscsiv1.VolumeImportSpec{Pool: "tank", BackendPath: "tank/apps/envtest", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode, Transport: zfscsiv1.TransportNVMeTCP, DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain}}
	if err := h.Client.Create(ctx, imp); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: imp.Name, Namespace: imp.Namespace}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.VolumeRef == "" || got.Status.VolumeHandle == "" {
		t.Fatalf("status = %#v", got.Status)
	}
	wantRef := naming.ImportID(imp.Spec.BackendPath)
	if got.Status.VolumeRef != wantRef || got.Status.VolumeRef == imp.Name {
		t.Fatalf("materialized identity=%q, want %q", got.Status.VolumeRef, wantRef)
	}
	vol := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: got.Status.VolumeRef}, vol); err != nil {
		t.Fatal(err)
	}
	if len(vol.OwnerReferences) != 0 || vol.Spec.BackendPath != imp.Spec.BackendPath ||
		vol.Spec.Provenance != zfscsiv1.VolumeProvenanceImported || vol.Spec.DeletionPolicy != zfscsiv1.VolumeDeletionPolicyRetain ||
		vol.Annotations[volumeImportAnnotation] != volumeImportClaim(imp) {
		t.Fatalf("materialized Volume = %#v", vol)
	}
	if err := h.Client.Delete(ctx, got); err != nil {
		t.Fatal(err)
	}
	stillThere := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, stillThere); err != nil {
		t.Fatalf("Volume cascaded with VolumeImport: %v", err)
	}
}

func TestEnvtestVolumeImportHasClusterIdentity(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)
	backend := envtestImportBackend(t, h).WithDatasetCapacity("tank/apps/existing", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	r := &VolumeImportReconciler{Client: h.Client, Log: logr.Discard(), ZFS: backend, NodeName: testenv.DefaultOwnerNode, ProbeFormat: func(context.Context, string) (string, error) { return "", nil }}
	imp := &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "cluster-import"}, Spec: zfscsiv1.VolumeImportSpec{Pool: "tank", BackendPath: "tank/apps/existing", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode, Transport: zfscsiv1.TransportNVMeTCP}}
	if err := h.Client.Create(ctx, imp); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: imp.Name, Namespace: imp.Namespace}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.State != zfscsiv1.VolumeImportStatePending || ready == nil || ready.Reason != "WaitingForVolume" {
		t.Fatalf("status=%#v", got.Status)
	}
}

func TestEnvtestVolumeImportEarlierClusterClaimantBlocksMaterialization(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)
	backend := envtestImportBackend(t, h).WithDatasetCapacity("tank/apps/claimed", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	earlier := &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "earlier"}, Spec: zfscsiv1.VolumeImportSpec{Pool: "tank", BackendPath: "tank/apps/claimed", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode}}
	if err := h.Client.Create(ctx, earlier); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	current := earlier.DeepCopy()
	current.ResourceVersion = ""
	current.UID = ""
	current.Name = "current"
	current.CreationTimestamp = metav1.Time{}
	if err := h.Client.Create(ctx, current); err != nil {
		t.Fatal(err)
	}
	r := &VolumeImportReconciler{Client: h.Client, Log: logr.Discard(), ZFS: backend, NodeName: testenv.DefaultOwnerNode, ProbeFormat: func(context.Context, string) (string, error) { return "", nil }}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: current.Name}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.State != zfscsiv1.VolumeImportStateFailed || ready == nil || ready.Reason != "BackendConflict" {
		t.Fatalf("status=%#v", got.Status)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := h.Client.List(ctx, volumes); err != nil {
		t.Fatal(err)
	}
	if len(volumes.Items) != 0 {
		t.Fatalf("materialized %d Volumes", len(volumes.Items))
	}
}

func TestEnvtestRecreatedVolumeImportRejectsChangedIntentAndAllowsIdenticalIntent(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)
	backend := envtestImportBackend(t, h).WithDatasetCapacity("tank/apps/recreate-envtest", zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	r := &VolumeImportReconciler{Client: h.Client, Log: logr.Discard(), ZFS: backend, NodeName: testenv.DefaultOwnerNode, ProbeFormat: func(context.Context, string) (string, error) { return "", nil }}
	original := &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "recreate"}, Spec: zfscsiv1.VolumeImportSpec{Pool: "tank", BackendPath: "tank/apps/recreate-envtest", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode, Transport: zfscsiv1.TransportNVMeTCP, DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain}}
	if err := h.Client.Create(ctx, original); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: original.Name, Namespace: original.Namespace}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := h.Client.Delete(ctx, original); err != nil {
		t.Fatal(err)
	}
	eventuallyImportDeleted(t, ctx, h.Client, req.NamespacedName)

	identical := original.DeepCopy()
	identical.ResourceVersion = ""
	identical.UID = ""
	identical.CreationTimestamp = metav1.Time{}
	identical.Status = zfscsiv1.VolumeImportStatus{}
	if err := h.Client.Create(ctx, identical); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.State == zfscsiv1.VolumeImportStateFailed {
		t.Fatalf("identical recreation status=%#v", got.Status)
	}
	if err := h.Client.Delete(ctx, got); err != nil {
		t.Fatal(err)
	}
	eventuallyImportDeleted(t, ctx, h.Client, req.NamespacedName)

	changed := original.DeepCopy()
	changed.ResourceVersion = ""
	changed.UID = ""
	changed.CreationTimestamp = metav1.Time{}
	changed.Status = zfscsiv1.VolumeImportStatus{}
	changed.Spec.NFSExportCIDRs = []string{"10.99.0.0/16"}
	if err := h.Client.Create(ctx, changed); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.State != zfscsiv1.VolumeImportStateFailed || ready == nil || ready.Reason != "VolumeConflict" || !strings.Contains(ready.Message, "nfsExportCIDR") {
		t.Fatalf("changed recreation status=%#v", got.Status)
	}
}

func TestEnvtestFilesystemImportRejectsInvalidExportPathWithoutMaterializing(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)
	backendPath := "tank/apps/invalid-export-envtest"
	backend := envtestImportBackend(t, h).WithDatasetCapacity(backendPath, zfs.KindFilesystem, 2<<30, false, zfs.KeyNone).WithExportPath(backendPath, "legacy")
	r := &VolumeImportReconciler{Client: h.Client, Log: logr.Discard(), ZFS: backend, NodeName: testenv.DefaultOwnerNode}
	imp := &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "invalid-export"}, Spec: zfscsiv1.VolumeImportSpec{Pool: "tank", BackendPath: backendPath, Type: zfscsiv1.VolumeTypeFilesystem, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode, NFSExportCIDRs: []string{"10.42.0.0/16"}, NFSExportAccessMode: "rw", DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain}}
	if err := h.Client.Create(ctx, imp); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: imp.Name, Namespace: imp.Namespace}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.State != zfscsiv1.VolumeImportStateFailed || ready == nil || ready.Reason != "InvalidExportPath" {
		t.Fatalf("status=%#v", got.Status)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := h.Client.List(ctx, volumes); err != nil {
		t.Fatal(err)
	}
	if len(volumes.Items) != 0 {
		t.Fatalf("materialized %d Volumes", len(volumes.Items))
	}
	share, err := backend.GetProperty(ctx, backendPath, "sharenfs")
	if err != nil || share != "" {
		t.Fatalf("backend share mutated to %q, err=%v", share, err)
	}
}

func TestEnvtestFilesystemImportRejectsInvalidNFSIntentWithoutMaterializing(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)
	backendPath := "tank/apps/invalid-nfs-intent-envtest"
	backend := envtestImportBackend(t, h).WithDatasetCapacity(backendPath, zfs.KindFilesystem, 2<<30, false, zfs.KeyNone).WithExportPath(backendPath, "/srv/invalid-nfs-intent")
	r := &VolumeImportReconciler{Client: h.Client, Log: logr.Discard(), ZFS: backend, NodeName: testenv.DefaultOwnerNode}
	imp := &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "invalid-nfs-intent"}, Spec: zfscsiv1.VolumeImportSpec{Pool: "tank", BackendPath: backendPath, Type: zfscsiv1.VolumeTypeFilesystem, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode, NFSExportCIDRs: []string{"10.42.0.7/24"}, NFSExportAccessMode: "rw", DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain}}
	if err := h.Client.Create(ctx, imp); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: imp.Name}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.State != zfscsiv1.VolumeImportStateFailed || ready == nil || ready.Reason != "InvalidNFSExportIntent" || !strings.Contains(ready.Message, "canonical IPv4 or IPv6 prefix") {
		t.Fatalf("status=%#v", got.Status)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := h.Client.List(ctx, volumes); err != nil {
		t.Fatal(err)
	}
	if len(volumes.Items) != 0 {
		t.Fatalf("materialized %d Volumes", len(volumes.Items))
	}
	share, err := backend.GetProperty(ctx, backendPath, "sharenfs")
	if err != nil || share != "" {
		t.Fatalf("backend share mutated to %q, err=%v", share, err)
	}

	canonicalPath := "tank/apps/canonical-nfs-intent-envtest"
	backend.WithDatasetCapacity(canonicalPath, zfs.KindFilesystem, 2<<30, false, zfs.KeyNone).WithExportPath(canonicalPath, "/srv/canonical-nfs-intent")
	canonical := &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "canonical-nfs-intent"}, Spec: zfscsiv1.VolumeImportSpec{Pool: "tank", BackendPath: canonicalPath, Type: zfscsiv1.VolumeTypeFilesystem, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode, NFSExportCIDRs: []string{"2001:db8::/64", "10.42.0.0/16"}, NFSExportAccessMode: "rw", DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain}}
	if err := h.Client.Create(ctx, canonical); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: canonical.Name}}); err != nil {
		t.Fatal(err)
	}
	volume := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: naming.ImportID(canonicalPath)}, volume); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(volume.Spec.NFSExportCIDRs, []string{"10.42.0.0/16", "2001:db8::/64"}) || volume.Spec.NFSExportAccessMode != "rw" {
		t.Fatalf("materialized NFS intent=%q/%q", volume.Spec.NFSExportCIDRs, volume.Spec.NFSExportAccessMode)
	}
}

func TestEnvtestFilesystemImportRemainsReadyAcrossAPIDefaultingAndReconcile(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)
	backendPath := "tank/apps/filesystem-ready-envtest"
	backend := envtestImportBackend(t, h).WithDatasetCapacity(backendPath, zfs.KindFilesystem, 2<<30, false, zfs.KeyNone).WithExportPath(backendPath, "/srv/imported/filesystem-ready")
	r := &VolumeImportReconciler{Client: h.Client, Log: logr.Discard(), ZFS: backend, NodeName: testenv.DefaultOwnerNode}
	imp := &zfscsiv1.VolumeImport{ObjectMeta: metav1.ObjectMeta{Name: "filesystem-ready"}, Spec: zfscsiv1.VolumeImportSpec{Pool: "tank", BackendPath: backendPath, Type: zfscsiv1.VolumeTypeFilesystem, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode, NFSExportCIDRs: []string{"10.42.0.0/16"}, NFSExportAccessMode: "rw", DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain}}
	if err := h.Client.Create(ctx, imp); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: imp.Name, Namespace: imp.Namespace}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.VolumeImport{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: got.Status.VolumeRef}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.FsType != "xfs" || vol.Spec.ImportFsTypeDeclaration != "" {
		t.Fatalf("materialized defaults: fsType=%q import declaration=%q", vol.Spec.FsType, vol.Spec.ImportFsTypeDeclaration)
	}
	before := vol.DeepCopy()
	vol.Status.State = zfscsiv1.VolumeStateReady
	vol.Status.ObservedGeneration = vol.Generation
	if err := h.Client.Status().Patch(ctx, vol, crclient.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatal(err)
		}
		if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
			t.Fatal(err)
		}
		ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
		if got.Status.State != zfscsiv1.VolumeImportStateReady || ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason == "VolumeConflict" {
			t.Fatalf("reconcile status=%#v", got.Status)
		}
	}
}

func eventuallyImportDeleted(t *testing.T, ctx context.Context, client crclient.Client, key types.NamespacedName) {
	t.Helper()
	for range 50 {
		if err := client.Get(ctx, key, &zfscsiv1.VolumeImport{}); apierrors.IsNotFound(err) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("VolumeImport %s was not deleted", key)
}
