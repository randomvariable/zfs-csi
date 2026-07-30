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
	"strings"
	"testing"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	testenv "github.com/randomvariable/zfs-csi/internal/testutil/envtest"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func TestEnvtestVolumeFinalizerLifecycle(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	zfsb := zfsfake.New().WithPool("tank", 1<<40)
	guid, _ := zfsb.PoolGUID(ctx, "tank")
	keys := &recKeyProvider{fetch: map[string][]byte{"transit/test": bytes32(1)}, del: map[string]bool{}}
	r := &VolumeReconciler{
		Client:   h.Client,
		Scheme:   h.Client.Scheme(),
		Log:      logr.Discard(),
		ZFS:      zfsb,
		Export:   newFakeTransportServer(),
		Keys:     keys,
		Stager:   &nopStager{},
		Portal:   "server7:4420",
		NodeName: testenv.DefaultOwnerNode,
	}

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "finalizer-vol",
			Finalizers: []string{zfscsiv1.VolumeFinalizer},
		},
		Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
			Pool:             "tank",
			PoolGUID:         guid,
			VolName:          "finalizer-vol",
			Type:             zfscsiv1.VolumeTypeBlock,
			Capacity:         1 << 30,
			VolumeID:         "csi:tank:block:finalizer-vol",
			Transport:        zfscsiv1.TransportNVMeTCP,
			EncryptionKeyRef: "transit/test",
		}),
	}
	if err := h.Client.Create(ctx, vol); err != nil {
		t.Fatalf("create Volume: %v", err)
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "finalizer-vol"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile create: %v", err)
	}
	if exists, err := zfsb.Exists(ctx, "tank/csi/block/finalizer-vol"); err != nil || !exists {
		t.Fatalf("dataset after create exists=%v err=%v", exists, err)
	}

	if err := h.Client.Delete(ctx, vol); err != nil {
		t.Fatalf("delete Volume: %v", err)
	}
	deleting := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, req.NamespacedName, deleting); err != nil {
		t.Fatalf("get deleting Volume: %v", err)
	}
	if deleting.DeletionTimestamp.IsZero() {
		t.Fatal("delete did not set deletionTimestamp while finalizer is present")
	}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}
	if exists, err := zfsb.Exists(ctx, "tank/csi/block/finalizer-vol"); err != nil || exists {
		t.Fatalf("dataset after delete exists=%v err=%v", exists, err)
	}
	if !keys.del["transit/test"] {
		t.Fatal("encrypted volume DEK was not deleted")
	}

	gone := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, req.NamespacedName, gone); !apierrors.IsNotFound(err) {
		t.Fatalf("Volume still exists after finalizer removal: %v", err)
	}
}

func TestEnvtestSnapshotFinalizerLifecycle(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	zfsb := zfsfake.New().WithPool("tank", 1<<40)
	guid, _ := zfsb.PoolGUID(ctx, "tank")
	if err := zfsb.Create(ctx, zfs.CreateOptions{Name: "tank/csi/block/source-vol", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
		t.Fatalf("create source dataset: %v", err)
	}
	r := &SnapshotReconciler{Client: h.Client, Log: logr.Discard(), ZFS: zfsb, NodeName: testenv.DefaultOwnerNode}
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "source-vol"}, Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
		Pool: "tank", PoolGUID: guid, VolName: "source-vol", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, OwnerNode: testenv.DefaultOwnerNode,
		VolumeID: "csi:tank:block:source-vol", Transport: zfscsiv1.TransportNVMeTCP,
	})}
	if err := h.Client.Create(ctx, source); err != nil {
		t.Fatalf("create source Volume: %v", err)
	}

	snap := &zfscsiv1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "finalizer-snap",
			Finalizers: []string{zfscsiv1.SnapshotFinalizer},
		},
		Spec: zfscsiv1.SnapshotSpec{
			VolumeRef:      "source-vol",
			SourceVolumeID: "csi:tank:block:source-vol",
			SnapName:       "snap-a",
			SnapshotID:     "csi:tank:block:source-vol@snap-a",
			OwnerNode:      testenv.DefaultOwnerNode,
			PoolGUID:       guid,
		},
	}
	if err := h.Client.Create(ctx, snap); err != nil {
		t.Fatalf("create Snapshot: %v", err)
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "finalizer-snap"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile snapshot create: %v", err)
	}
	if snaps, err := zfsb.ListSnapshots(ctx, "tank/csi/block/source-vol"); err != nil || len(snaps) != 1 || snaps[0] != "snap-a" {
		t.Fatalf("snapshots after create = %v err=%v", snaps, err)
	}

	if err := h.Client.Delete(ctx, snap); err != nil {
		t.Fatalf("delete Snapshot: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile snapshot delete: %v", err)
	}
	if snaps, err := zfsb.ListSnapshots(ctx, "tank/csi/block/source-vol"); err != nil || len(snaps) != 0 {
		t.Fatalf("snapshots after delete = %v err=%v", snaps, err)
	}

	gone := &zfscsiv1.Snapshot{}
	if err := h.Client.Get(ctx, req.NamespacedName, gone); !apierrors.IsNotFound(err) {
		t.Fatalf("Snapshot still exists after finalizer removal: %v", err)
	}
}

func TestEnvtestSnapshotSourcePoolGUIDMismatchFailsBeforeMutation(t *testing.T) {
	ctx := t.Context()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	zfsb := zfsfake.New().WithPoolIdentity("tank", 1<<40, "1", "ONLINE")
	if err := zfsb.Create(ctx, zfs.CreateOptions{Name: "tank/csi/block/source-guid-mismatch", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	r := &SnapshotReconciler{Client: h.Client, Log: logr.Discard(), ZFS: zfsb, NodeName: testenv.DefaultOwnerNode}
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "source-guid-mismatch"}, Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
		Pool: "tank", PoolGUID: "2", VolName: "source-guid-mismatch", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30,
		OwnerNode: testenv.DefaultOwnerNode, VolumeID: "csi:tank:block:source-guid-mismatch", Transport: zfscsiv1.TransportNVMeTCP,
	})}
	if err := h.Client.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "source-guid-mismatch"}, Spec: zfscsiv1.SnapshotSpec{
		PoolGUID: "1", VolumeRef: source.Name, SourceVolumeID: source.Spec.VolumeID, SnapName: "snap",
		SnapshotID: source.Spec.VolumeID + "@snap", OwnerNode: testenv.DefaultOwnerNode,
	}}
	if err := h.Client.Create(ctx, snap); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}
	if _, err := r.Reconcile(ctx, req); err == nil || !strings.Contains(err.Error(), "does not match Snapshot pool GUID") {
		t.Fatalf("reconcile error=%v, want source pool GUID mismatch", err)
	}
	if snapshots, err := zfsb.ListSnapshots(ctx, "tank/csi/block/source-guid-mismatch"); err != nil || len(snapshots) != 0 {
		t.Fatalf("backend snapshots=%v err=%v, want none", snapshots, err)
	}
	got := &zfscsiv1.Snapshot{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CurrentState() != "" || len(got.Finalizers) != 0 {
		t.Fatalf("source identity mismatch mutated Snapshot: %#v", got)
	}
}

func TestEnvtestSnapshotSourceOwnerMismatchFailsBeforeMutation(t *testing.T) {
	ctx := t.Context()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	zfsb := zfsfake.New().WithPoolIdentity("tank", 1<<40, "1", "ONLINE")
	const dataset = "tank/csi/block/source-owner-mismatch"
	if err := zfsb.Create(ctx, zfs.CreateOptions{Name: dataset, Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	r := &SnapshotReconciler{Client: h.Client, Log: logr.Discard(), ZFS: zfsb, NodeName: testenv.DefaultOwnerNode}
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "source-owner-mismatch"}, Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
		Pool: "tank", PoolGUID: "1", VolName: "source-owner-mismatch", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30,
		OwnerNode: "storage-b", VolumeID: "csi:tank:block:source-owner-mismatch", Transport: zfscsiv1.TransportNVMeTCP,
	})}
	if err := h.Client.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "source-owner-mismatch", Finalizers: []string{zfscsiv1.SnapshotFinalizer}}, Spec: zfscsiv1.SnapshotSpec{
		PoolGUID: "1", VolumeRef: source.Name, SourceVolumeID: source.Spec.VolumeID, SnapName: "snap",
		SnapshotID: source.Spec.VolumeID + "@snap", OwnerNode: testenv.DefaultOwnerNode,
	}}
	if err := h.Client.Create(ctx, snap); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}
	if _, err := r.Reconcile(ctx, req); err == nil || !strings.Contains(err.Error(), "does not match Snapshot owner node") {
		t.Fatalf("reconcile error=%v, want source owner mismatch", err)
	}
	if snapshots, err := zfsb.ListSnapshots(ctx, dataset); err != nil || len(snapshots) != 0 {
		t.Fatalf("backend snapshots=%v err=%v, want none", snapshots, err)
	}
	got := &zfscsiv1.Snapshot{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CurrentState() != "" || !hasFinalizer(got.Finalizers, zfscsiv1.SnapshotFinalizer) {
		t.Fatalf("source owner mismatch mutated Snapshot: %#v", got)
	}
}

func TestEnvtestMalformedDeletingSnapshotMissingPoolRetainsFinalizer(t *testing.T) {
	ctx := t.Context()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	r := &SnapshotReconciler{Client: h.Client, Log: logr.Discard(), ZFS: zfsfake.New(), NodeName: testenv.DefaultOwnerNode}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "malformed-delete", Finalizers: []string{zfscsiv1.SnapshotFinalizer}}, Spec: zfscsiv1.SnapshotSpec{
		PoolGUID: "1", VolumeRef: "source", SourceVolumeID: "malformed", SnapName: "snap", SnapshotID: "malformed", OwnerNode: testenv.DefaultOwnerNode,
	}, Status: zfscsiv1.SnapshotStatus{State: zfscsiv1.SnapshotStateDeleting, DatasetPath: "tank/csi/block/source@snap"}}
	if err := h.Client.Create(ctx, snap); err != nil {
		t.Fatal(err)
	}
	snap.Status = zfscsiv1.SnapshotStatus{State: zfscsiv1.SnapshotStateDeleting, DatasetPath: "tank/csi/block/source@snap"}
	if err := h.Client.Status().Update(ctx, snap); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}
	if _, err := r.Reconcile(ctx, req); err == nil || !strings.Contains(err.Error(), "read pool") {
		t.Fatalf("reconcile error=%v, want missing pool error", err)
	}
	got := &zfscsiv1.Snapshot{}
	if err := h.Client.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if !hasFinalizer(got.Finalizers, zfscsiv1.SnapshotFinalizer) || got.Status.CurrentState() != zfscsiv1.SnapshotStateDeleting {
		t.Fatalf("missing pool mutated malformed deleting Snapshot: %#v", got)
	}
}
