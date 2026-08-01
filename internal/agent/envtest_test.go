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
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/crypto"
	"github.com/randomvariable/zfs-csi/internal/naming"
	internalEvents "github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/testutil"
	testenv "github.com/randomvariable/zfs-csi/internal/testutil/envtest"
	"github.com/randomvariable/zfs-csi/internal/transport"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func TestEnvtestVolumeRequiresOwnerNode(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "missing-owner"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", VolName: "missing-owner", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:missing-owner", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := h.Client.Create(ctx, vol); err == nil || !strings.Contains(err.Error(), "spec.ownerNode") {
		t.Fatalf("create Volume without ownerNode error = %v, want spec.ownerNode validation error", err)
	}

	immutable := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "immutable-owner"}, Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
		Pool: "tank", VolName: "immutable-owner", Type: zfscsiv1.VolumeTypeBlock,
		Capacity: 1 << 20, VolumeID: "csi:tank:block:immutable-owner", Transport: zfscsiv1.TransportNVMeTCP,
	})}
	immutable.Spec.OwnerNode = "node-a"
	if err := h.Client.Create(ctx, immutable); err != nil {
		t.Fatal(err)
	}
	immutable.Spec.OwnerNode = "node-b"
	if err := h.Client.Update(ctx, immutable); err == nil || !strings.Contains(err.Error(), "ownerNode is immutable") {
		t.Fatalf("update ownerNode error = %v, want immutable validation error", err)
	}

	// ZFS fixes a zvol's volblocksize at creation and a clone inherits it from its
	// origin, so every capacity the driver aligned against this value would become
	// an illegal volsize if it could be edited after the fact.
	blockSized := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "immutable-blocksize"}, Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
		Pool: "tank", VolName: "immutable-blocksize", Type: zfscsiv1.VolumeTypeBlock,
		Capacity: 1 << 20, VolumeID: "csi:tank:block:immutable-blocksize", Transport: zfscsiv1.TransportNVMeTCP,
		VolBlockSize: zfs.DefaultVolBlockSizeValue,
	})}
	if err := h.Client.Create(ctx, blockSized); err != nil {
		t.Fatal(err)
	}
	blockSized.Spec.VolBlockSize = "128k"
	if err := h.Client.Update(ctx, blockSized); err == nil || !strings.Contains(err.Error(), "volBlockSize is immutable") {
		t.Fatalf("update volBlockSize error = %v, want immutable validation error", err)
	}

	clusterIdentity := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "cluster-identity", Namespace: "ignored"}, Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
		Pool: "tank", VolName: "cluster-identity", Type: zfscsiv1.VolumeTypeBlock,
		Capacity: 1 << 20, VolumeID: "csi:tank:block:cluster-identity", Transport: zfscsiv1.TransportNVMeTCP,
	})}
	if err := h.Client.Create(ctx, clusterIdentity); err != nil {
		t.Fatalf("create Volume with ignored namespace: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: clusterIdentity.Name}, got); err != nil {
		t.Fatalf("get cluster Volume by name: %v", err)
	}
	if got.Namespace != "" {
		t.Fatalf("persisted namespace=%q, want empty", got.Namespace)
	}

	enabled := true
	storageNode := &zfscsiv1.StorageNode{ObjectMeta: metav1.ObjectMeta{Name: "inventory-node"}, Spec: zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: []string{"1"}, Enabled: &enabled, NetworkDomain: "workers"}}
	if err := h.Client.Create(ctx, storageNode); err != nil {
		t.Fatal(err)
	}
	storageNode.Status.ObservedGeneration = storageNode.Generation
	capacityObservedAt := metav1.Now()
	storageNode.Status.Pools = []zfscsiv1.StorageNodePoolStatus{{GUID: "1", Name: "tank", FreeBytes: 1, CapacityObservedAt: capacityObservedAt, Ready: true}}
	if err := h.Client.Status().Update(ctx, storageNode); err != nil {
		t.Fatal(err)
	}
	storageNode.Spec.NetworkDomain = "changed"
	storageNode.Status.Pools = nil
	if err := h.Client.Status().Update(ctx, storageNode); err != nil {
		t.Fatal(err)
	}
	observed := &zfscsiv1.StorageNode{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: storageNode.Name}, observed); err != nil {
		t.Fatal(err)
	}
	if observed.Spec.NetworkDomain != "workers" {
		t.Fatalf("status update changed spec.networkDomain=%q", observed.Spec.NetworkDomain)
	}
	observed.Spec.AuthoritativePoolGUIDs = []string{"2"}
	if err := h.Client.Update(ctx, observed); err == nil || !strings.Contains(err.Error(), "authoritativePoolGUIDs is immutable") {
		t.Fatalf("update authoritativePoolGUIDs error=%v, want immutable error", err)
	}
	for _, tc := range []struct {
		name  string
		guids []string
	}{
		{name: "required"},
		{name: "zero", guids: []string{"0"}},
		{name: "leading-zero", guids: []string{"01"}},
		{name: "overflow", guids: []string{"18446744073709551616"}},
		{name: "duplicate", guids: []string{"1", "1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := &zfscsiv1.StorageNode{ObjectMeta: metav1.ObjectMeta{Name: "invalid-" + tc.name}, Spec: zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: tc.guids, NetworkDomain: "workers"}}
			if err := h.Client.Create(ctx, invalid); err == nil {
				t.Fatalf("created StorageNode with invalid authoritativePoolGUIDs %v", tc.guids)
			}
		})
	}
}

func TestEnvtestSnapshotOwnerNodeIsImmutable(t *testing.T) {
	ctx := t.Context()
	h := testenv.Start(t)
	defer h.Stop(t)

	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "immutable-snapshot-owner"}, Spec: testenv.SnapshotSpec(zfscsiv1.SnapshotSpec{
		VolumeRef: "source", SourceVolumeID: "csi:tank:block:source", SnapName: "snap",
		SnapshotID: "csi:tank:block:source@snap", OwnerNode: "node-a",
	})}
	if err := h.Client.Create(ctx, snap); err != nil {
		t.Fatal(err)
	}
	snap.Spec.OwnerNode = "node-b"
	if err := h.Client.Update(ctx, snap); err == nil || !strings.Contains(err.Error(), "ownerNode is immutable") {
		t.Fatalf("update Snapshot ownerNode error = %v, want immutable validation error", err)
	}
}

func TestEnvtestVolumeReconcilerCreateStatusSubresource(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	zfsb := zfsfake.New().WithPool("tank", 1<<40)
	guid, _ := zfsb.PoolGUID(ctx, "tank")
	export := newFakeTransportServer()
	r := &VolumeReconciler{
		Client:   h.Client,
		Scheme:   h.Client.Scheme(),
		Log:      logr.Discard(),
		ZFS:      zfsb,
		Export:   export,
		Keys:     &recKeyProvider{fetch: map[string][]byte{}, del: map[string]bool{}},
		Stager:   &nopStager{},
		Portal:   "server7:4420",
		NodeName: testenv.DefaultOwnerNode,
	}
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "ev1"},
		Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
			Pool:      "tank",
			PoolGUID:  guid,
			VolName:   "csi-tank-block-ev1",
			Type:      zfscsiv1.VolumeTypeBlock,
			Capacity:  1 << 30,
			VolumeID:  "csi:tank:block:ev1",
			Transport: zfscsiv1.TransportNVMeTCP,
		}),
	}
	if err := h.Client.Create(ctx, vol); err != nil {
		t.Fatalf("create Volume: %v", err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "ev1"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: "ev1"}, got); err != nil {
		t.Fatalf("get Volume: %v", err)
	}
	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("state = %q, want Ready", got.Status.State)
	}
	exists, err := zfsb.Exists(ctx, "tank/csi/block/ev1")
	if err != nil || !exists {
		t.Fatalf("dataset exists=%v err=%v", exists, err)
	}
	nqn, err := naming.TargetNQN(vol.Spec.OwnerNode, vol.Spec.PoolGUID, zfs.KindBlock, "ev1")
	if err != nil {
		t.Fatalf("target nqn: %v", err)
	}
	if !export.exports[nqn] {
		t.Fatal("export not created")
	}
}

func TestEnvtestSnapshotReconcilerCreatePersistsReadyDetails(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	const dataset = "tank/csi/block/snapshot-ready"
	zfsb := zfsfake.New().WithPool("tank", 1<<40)
	guid, _ := zfsb.PoolGUID(ctx, "tank")
	zfsb.WithDataset(dataset, zfs.KindBlock, false, zfs.KeyNone)
	r := &SnapshotReconciler{Client: h.Client, Log: logr.Discard(), ZFS: zfsb, NodeName: testenv.DefaultOwnerNode}
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "snapshot-ready"}, Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
		Pool: "tank", PoolGUID: guid, VolName: "snapshot-ready", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1, OwnerNode: testenv.DefaultOwnerNode,
		VolumeID: "csi:tank:block:snapshot-ready", Transport: zfscsiv1.TransportNVMeTCP,
	})}
	if err := h.Client.Create(ctx, source); err != nil {
		t.Fatalf("create source Volume: %v", err)
	}
	snap := &zfscsiv1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshot-ready"},
		Spec: zfscsiv1.SnapshotSpec{
			VolumeRef:      source.Name,
			SourceVolumeID: "csi:tank:block:snapshot-ready",
			SnapName:       "ready",
			SnapshotID:     "csi:tank:block:snapshot-ready@ready",
			OwnerNode:      testenv.DefaultOwnerNode,
			PoolGUID:       guid,
		},
	}
	if err := h.Client.Create(ctx, snap); err != nil {
		t.Fatalf("create Snapshot: %v", err)
	}

	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: snap.Name}}); err != nil {
		t.Fatalf("reconcile after finalizer: %v", err)
	}

	got := &zfscsiv1.Snapshot{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: snap.Name, Namespace: snap.Namespace}, got); err != nil {
		t.Fatalf("get Snapshot: %v", err)
	}
	if got.Status.DatasetPath != dataset+"@ready" {
		t.Fatalf("snapshot status=%#v, want datasetPath %q", got.Status, dataset+"@ready")
	}
	if got.Status.CreatedAt <= 0 {
		t.Fatalf("createdAt = %d, want positive unix timestamp", got.Status.CreatedAt)
	}
}

func TestEnvtestEnsure_TargetRepairPersistsHealthTransitions(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	zfsb := zfsfake.New().WithPool("tank", 1<<40)
	guid, _ := zfsb.PoolGUID(ctx, "tank")
	if err := zfsb.Create(ctx, zfs.CreateOptions{Name: "tank/csi/block/health", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	export := newFakeTransportServer()
	r := &VolumeReconciler{
		Client: h.Client, Scheme: h.Client.Scheme(), Log: logr.Discard(), ZFS: zfsb, Export: export,
		Keys:   &recKeyProvider{fetch: map[string][]byte{}, del: map[string]bool{}},
		Stager: &nopStager{}, Portal: "server7:4420",
		NodeName: testenv.DefaultOwnerNode,
	}
	recorder := &testutil.Recorder{}
	r.Recorder = recorder
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "health"},
		Spec:       testenv.VolumeSpec(zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: guid, VolName: "csi-tank-block-health", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, VolumeID: "csi:tank:block:health", Transport: zfscsiv1.TransportNVMeTCP}),
	}
	if err := h.Client.Create(ctx, vol); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	if err := h.Client.Status().Patch(ctx, vol, patch); err != nil {
		t.Fatal(err)
	}

	export.exportErr = errors.New("configfs target unavailable")
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}}); err == nil {
		t.Fatal("target repair unexpectedly succeeded")
	}
	got := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	health := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy))
	if health == nil || health.Status != metav1.ConditionFalse || health.Reason != internalEvents.ReasonBackendUnhealthy {
		t.Fatalf("persisted unhealthy condition = %#v", health)
	}
	assertEvent(t, recorder.Events(), 0, internalEvents.TypeWarning, internalEvents.ReasonBackendUnhealthy, internalEvents.ActionHealthChecking)

	export.exportErr = nil
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}}); err != nil {
		t.Fatalf("target repair recovery: %v", err)
	}
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}}); err != nil {
		t.Fatalf("verified target recovery: %v", err)
	}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	health = findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy))
	if health == nil || health.Status != metav1.ConditionTrue || health.Reason != internalEvents.ReasonBackendRecovered {
		t.Fatalf("persisted recovered condition = %#v", health)
	}
	assertEvent(t, recorder.Events(), 1, internalEvents.TypeNormal, internalEvents.ReasonBackendRecovered, internalEvents.ActionHealthChecking)
	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("repair changed lifecycle state to %q", got.Status.State)
	}
}

func TestEnvtestStatusSubresourceRejectsSpecUpdateStatusMutation(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "status-only"},
		Spec:       testenv.VolumeSpec(zfscsiv1.VolumeSpec{Pool: "tank", VolName: "csi-status-only", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 20, VolumeID: "csi:tank:block:status-only"}),
	}
	if err := h.Client.Create(ctx, vol); err != nil {
		t.Fatalf("create Volume: %v", err)
	}
	patch := client.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	if err := h.Client.Patch(ctx, vol, patch); err != nil {
		t.Fatalf("spec-subresource patch: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: "status-only"}, got); err != nil {
		t.Fatalf("get Volume: %v", err)
	}
	if got.Status.State != "" {
		t.Fatalf("main resource patch mutated status=%q; status subresource is not protecting status", got.Status.State)
	}
}

// TestEnvtestEnsure_DatasetGonePoolImported_PreservesConditions proves the
// missing-dataset transition against a real apiserver. Status conditions are an
// RFC 7386-replaced array, so the fresh retry baseline must retain Ready while
// adding BackendHealthy=False.
func TestEnvtestEnsure_DatasetGonePoolImported_PreservesConditions(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	// Pool imported (tank present) but the dataset never created -> Exists=false.
	zfsb := zfsfake.New().WithPool("tank", 1<<40)
	guid, _ := zfsb.PoolGUID(ctx, "tank")
	r := &VolumeReconciler{
		Client: h.Client, Scheme: h.Client.Scheme(), Log: logr.Discard(), ZFS: zfsb,
		Export: newFakeTransportServer(),
		Keys:   &recKeyProvider{fetch: map[string][]byte{}, del: map[string]bool{}},
		Stager: &nopStager{}, Portal: "server7:4420",
		NodeName: testenv.DefaultOwnerNode,
	}

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "gone"},
		Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: guid, VolName: "csi-tank-block-gone", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:gone", Transport: zfscsiv1.TransportNVMeTCP,
		}),
	}
	if err := h.Client.Create(ctx, vol); err != nil {
		t.Fatalf("create Volume: %v", err)
	}
	// Drive it to Ready via the status subresource.
	patch := client.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	vol.Status.Conditions = []metav1.Condition{{
		Type: string(zfscsiv1.VolumeConditionReady), Status: metav1.ConditionTrue,
		Reason: "VolumeReady", Message: "concurrent Ready state", LastTransitionTime: metav1.Now(),
	}}
	if err := h.Client.Status().Patch(ctx, vol, patch); err != nil {
		t.Fatalf("seed Ready: %v", err)
	}

	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "gone"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Read back through the API: the persisted state must be Pending.
	got := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: "gone"}, got); err != nil {
		t.Fatalf("get Volume: %v", err)
	}
	if got.Status.State != zfscsiv1.VolumeStatePending {
		t.Fatalf("persisted state = %q, want Pending", got.Status.State)
	}
	if ready := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionReady)); ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready = %#v, want preserved True condition", ready)
	}
	if health := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy)); health == nil || health.Status != metav1.ConditionFalse {
		t.Fatalf("BackendHealthy = %#v, want False", health)
	}
}

// TestEnvtestEnsure_MissingPoolFailsClosedWithoutMutatingReady proves against a
// real apiserver that unverifiable pool identity returns an error for
// controller-runtime rate limiting and leaves persisted Ready status untouched.
func TestEnvtestEnsure_MissingPoolFailsClosedWithoutMutatingReady(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	// Pool NOT imported: no WithPool call, so PoolNames is empty.
	zfsb := zfsfake.New()
	r := &VolumeReconciler{
		Client: h.Client, Scheme: h.Client.Scheme(), Log: logr.Discard(), ZFS: zfsb,
		Export: newFakeTransportServer(),
		Keys:   &recKeyProvider{fetch: map[string][]byte{}, del: map[string]bool{}},
		Stager: &nopStager{}, Portal: "server7:4420",
		NodeName: testenv.DefaultOwnerNode,
	}

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "nopool"},
		Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
			Pool: "tank", VolName: "csi-tank-block-nopool", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:nopool", Transport: zfscsiv1.TransportNVMeTCP,
		}),
	}
	if err := h.Client.Create(ctx, vol); err != nil {
		t.Fatalf("create Volume: %v", err)
	}
	patch := client.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	if err := h.Client.Status().Patch(ctx, vol, patch); err != nil {
		t.Fatalf("seed Ready: %v", err)
	}

	res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: "nopool"}})
	if err == nil || !errors.Is(err, zfs.ErrPoolNotFound) {
		t.Fatalf("reconcile error=%v, want wrapped ErrPoolNotFound", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("result=%#v, want controller-runtime error backoff", res)
	}

	got := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: "nopool"}, got); err != nil {
		t.Fatalf("get Volume: %v", err)
	}
	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("persisted state = %q, want Ready (unimported pool must not recreate/flip)", got.Status.State)
	}
}

// compile-time checks reused by envtest.
var (
	_ transport.Server   = (*fakeTransportServer)(nil)
	_ crypto.KeyProvider = (*recKeyProvider)(nil)
)
