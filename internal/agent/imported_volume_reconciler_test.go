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
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

func TestImportedVolumeUsesBackendPathAndRetainsOnDelete(t *testing.T) {
	d := newTestDeps(t)
	const backendPath = "tank/apps/existing-zvol"
	d.zfsb.WithDatasetCapacity(backendPath, zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	id := naming.ImportID(backendPath)
	handle, err := naming.EncodeVolID("tank", zfs.KindBlock, id)
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: id, Finalizers: []string{zfscsiv1.VolumeFinalizer}}, Spec: zfscsiv1.VolumeSpec{
		Provenance: zfscsiv1.VolumeProvenanceImported, BackendPath: backendPath, DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain,
		Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 2 << 30, OwnerNode: "storage-a", VolumeID: handle, VolName: id, Transport: zfscsiv1.TransportNVMeTCP,
	}}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	r := d.reconciler()
	r.NodeName = "storage-a"
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: id}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.DatasetPath != backendPath || got.Status.ZvolPath != "/dev/zvol/"+backendPath {
		t.Fatalf("status used wrong backend: %#v", got.Status)
	}
	dynamicPath := "tank/csi/block/" + id
	if exists, _ := d.zfsb.Exists(context.Background(), dynamicPath); exists {
		t.Fatalf("created handle-derived backend %q", dynamicPath)
	}
	if err := d.Delete(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if exists, err := d.zfsb.Exists(context.Background(), backendPath); err != nil || !exists {
		t.Fatalf("retained backend exists=%v err=%v", exists, err)
	}
}

func TestImportedTLSVolumeRetainsExportedPortalAtReady(t *testing.T) {
	d := newTestDeps(t)
	const backendPath = "tank/apps/existing-tls-zvol"
	d.zfsb.WithDatasetCapacity(backendPath, zfs.KindBlock, 2<<30, false, zfs.KeyNone)
	d.export.returnedPortal = "server7:4421"
	id := naming.ImportID(backendPath)
	handle, err := naming.EncodeVolID("tank", zfs.KindBlock, id)
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: id, Finalizers: []string{zfscsiv1.VolumeFinalizer}}, Spec: zfscsiv1.VolumeSpec{
		Provenance: zfscsiv1.VolumeProvenanceImported, BackendPath: backendPath, DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain,
		Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 2 << 30, OwnerNode: "storage-a", VolumeID: handle, VolName: id,
		Transport: zfscsiv1.TransportNVMeTCP, NVMeTLSEnabled: true,
	}}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	r := d.reconciler()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: id}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("state=%q, want Ready", got.Status.State)
	}
	if got.Status.Portal != "server7:4421" || got.Status.PortalHost != "server7" || got.Status.PortalPort != 4421 {
		t.Fatalf("target endpoint = portal %q host %q port %d, want server7:4421/server7/4421", got.Status.Portal, got.Status.PortalHost, got.Status.PortalPort)
	}
}

func TestImportedFilesystemDeleteUnsharesWithoutUnmountOrDestroy(t *testing.T) {
	d := newTestDeps(t)
	const backendPath = "tank/apps/existing-fs"
	d.zfsb.WithDatasetCapacity(backendPath, zfs.KindFilesystem, 2<<30, true, zfs.KeyNone).WithRootMetadata(backendPath, 1001, 2002, 0o750)
	id := naming.ImportID(backendPath)
	handle, _ := naming.EncodeVolID("tank", zfs.KindFilesystem, id)
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: id, Finalizers: []string{zfscsiv1.VolumeFinalizer}}, Spec: zfscsiv1.VolumeSpec{
		Provenance: zfscsiv1.VolumeProvenanceImported, BackendPath: backendPath, DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain,
		Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem, Capacity: 2 << 30, OwnerNode: "storage-a", VolumeID: handle, VolName: id,
		NFSExportCIDRs: []string{"10.42.0.0/16"}, NFSExportAccessMode: "rw",
	}}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	r := d.reconciler()
	r.NodeName = "storage-a"
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: id}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	info, err := d.zfsb.Get(context.Background(), backendPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.ExportPath != "/"+backendPath {
		t.Fatalf("observable mount path changed: %q", info.ExportPath)
	}
	share, _ := d.zfsb.GetProperty(context.Background(), backendPath, "sharenfs")
	if share != "off" {
		t.Fatalf("sharenfs=%q, want off", share)
	}
	uid, gid, mode, ok := d.zfsb.RootMetadata(backendPath)
	if !ok || uid != 1001 || gid != 2002 || mode != 0o750 {
		t.Fatalf("root metadata changed: uid=%d gid=%d mode=%#o", uid, gid, mode)
	}
}

func TestImportedVolumeWrongOwnerDoesNothing(t *testing.T) {
	d := newTestDeps(t)
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "import-test"}, Spec: zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, BackendPath: "tank/apps/missing", Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1, OwnerNode: "storage-b", VolumeID: "csi:tank:block:import-test"}}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	r := d.reconciler()
	r.NodeName = "storage-a"
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.State != "" {
		t.Fatalf("wrong owner changed state to %q", got.Status.State)
	}
}

func TestDynamicVolumeWrongOwnerDoesNothing(t *testing.T) {
	for _, kind := range []zfscsiv1.VolumeType{zfscsiv1.VolumeTypeBlock, zfscsiv1.VolumeTypeFilesystem} {
		t.Run(string(kind), func(t *testing.T) {
			d := newTestDeps(t)
			name := "dynamic-wrong-owner-" + string(kind)
			vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: name, Finalizers: []string{zfscsiv1.VolumeFinalizer}}, Spec: zfscsiv1.VolumeSpec{
				Pool: "tank", Type: kind, Capacity: 1, OwnerNode: "node-b", VolName: name,
				VolumeID: "csi:tank:" + string(kind) + ":" + name, Transport: zfscsiv1.TransportNVMeTCP,
				NFSExportCIDRs: []string{"10.42.0.0/16"}, NFSExportAccessMode: "rw",
			}}
			if err := d.Create(t.Context(), vol); err != nil {
				t.Fatal(err)
			}
			r := d.reconciler()
			r.NodeName = "node-a"

			if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: vol.Name}}); err != nil {
				t.Fatal(err)
			}
			got := &zfscsiv1.Volume{}
			if err := d.Get(t.Context(), types.NamespacedName{Name: vol.Name}, got); err != nil {
				t.Fatal(err)
			}
			if got.Status.State != "" || len(got.Finalizers) != 1 || len(d.export.exports) != 0 {
				t.Fatalf("wrong owner mutated state=%q finalizers=%v exports=%v", got.Status.State, got.Finalizers, d.export.exports)
			}

			if err := d.Delete(t.Context(), got); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: vol.Name}}); err != nil {
				t.Fatal(err)
			}
			deleting := &zfscsiv1.Volume{}
			if err := d.Get(t.Context(), types.NamespacedName{Name: vol.Name}, deleting); err != nil {
				t.Fatal(err)
			}
			if len(deleting.Finalizers) != 1 {
				t.Fatalf("wrong owner removed finalizer: %v", deleting.Finalizers)
			}
			owner := d.reconciler()
			owner.NodeName = "node-b"
			if _, err := owner.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: vol.Name}}); err != nil {
				t.Fatal(err)
			}
			if err := d.Get(t.Context(), types.NamespacedName{Name: vol.Name}, &zfscsiv1.Volume{}); !apierrors.IsNotFound(err) {
				t.Fatalf("owner did not complete deletion: %v", err)
			}
		})
	}
}
