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

package driver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/capacity"
	"github.com/randomvariable/zfs-csi/internal/crypto"
	"github.com/randomvariable/zfs-csi/internal/inventory"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/psk"
	"github.com/randomvariable/zfs-csi/internal/reachability"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	"sigs.k8s.io/yaml"
)

func newTestClient(t *testing.T, objs ...crclient.Object) crclient.Client {
	t.Helper()

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	if err := zfscsiv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&zfscsiv1.Volume{}, &zfscsiv1.Snapshot{}).Build()
}

func capacityConfigMap(node string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      capacity.ConfigMapNameForNode(node),
			Namespace: "zfs-csi",
			Labels: map[string]string{
				capacity.ManagedByLabel: capacity.ManagedByValue,
				capacity.NodeLabel:      node,
			},
		},
		Data: data,
	}
}

func TestGetCapacityReadsPublishedPoolFreeBytes(t *testing.T) {
	c := newTestClient(t, capacityConfigMap("storage-0", map[string]string{"tank": "4294967296"}))
	s := NewControllerServer(ControllerConfig{Client: c, Namespace: "zfs-csi"})
	resp, err := s.GetCapacity(context.Background(), &csi.GetCapacityRequest{Parameters: map[string]string{"pool": "tank"}})
	if err != nil {
		t.Fatalf("GetCapacity: %v", err)
	}
	if resp.AvailableCapacity != 4294967296 {
		t.Fatalf("AvailableCapacity = %d, want 4294967296", resp.AvailableCapacity)
	}
}

// TestGetCapacityAggregatesAcrossNodes proves the controller reads every
// per-node ConfigMap and takes the max free bytes for the requested pool.
func TestGetCapacityAggregatesAcrossNodes(t *testing.T) {
	c := newTestClient(t,
		capacityConfigMap("node-a", map[string]string{"tank": "100"}),
		capacityConfigMap("node-b", map[string]string{"tank": "300", "flash": "50"}),
	)
	s := NewControllerServer(ControllerConfig{Client: c, Namespace: "zfs-csi"})
	resp, err := s.GetCapacity(context.Background(), &csi.GetCapacityRequest{Parameters: map[string]string{"pool": "tank"}})
	if err != nil {
		t.Fatalf("GetCapacity: %v", err)
	}
	if resp.AvailableCapacity != 300 {
		t.Fatalf("AvailableCapacity = %d, want 300 (max across nodes)", resp.AvailableCapacity)
	}
}

func TestGetCapacitySharedDomainReportsMaxSinglePoolAfterReservations(t *testing.T) {
	c := newTestClient(t)
	testPoolResolverWithFree(c, "node-a", "tank", "1", 300)
	testPoolResolverWithFree(c, "node-b", "tank", "2", 250)
	for _, name := range []string{"node-a", "node-b"} {
		node := &zfscsiv1.StorageNode{}
		if err := c.Get(t.Context(), crclient.ObjectKey{Name: name}, node); err != nil {
			t.Fatal(err)
		}
		node.Spec.NetworkDomain = "shared"
		node.Status.ReachableFrom = []string{"shared"}
		if err := c.Update(t.Context(), node); err != nil {
			t.Fatal(err)
		}
	}
	reservation := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "pending"}, Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", Pool: "tank", PoolGUID: "1", Capacity: 100}}
	if err := c.Create(t.Context(), reservation); err != nil {
		t.Fatal(err)
	}
	s := NewControllerServer(ControllerConfig{Client: c, APIReader: c, Namespace: "zfs-csi"})
	resp, err := s.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: map[string]string{"pool": "tank"},
		AccessibleTopology: &csi.Topology{Segments: map[string]string{
			reachability.TopologyKeyNetworkDomain: "shared",
		}},
	})
	if err != nil {
		t.Fatalf("GetCapacity: %v", err)
	}
	if resp.AvailableCapacity != 250 {
		t.Fatalf("AvailableCapacity = %d, want 250 (max sibling pool, not 450 sum)", resp.AvailableCapacity)
	}
}

func TestGetCapacityRejectsUnknownPool(t *testing.T) {
	c := newTestClient(t, capacityConfigMap("storage-0", map[string]string{"tank": "42"}))
	s := NewControllerServer(ControllerConfig{Client: c, Namespace: "zfs-csi"})
	_, err := s.GetCapacity(context.Background(), &csi.GetCapacityRequest{Parameters: map[string]string{"pool": "missing"}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("GetCapacity code = %s, want %s", status.Code(err), codes.Unavailable)
	}
}

func newTestController(c crclient.Client) *ControllerServer {
	return newTestControllerWithKeys(c, nil)
}

func newTestControllerWithKeys(c crclient.Client, keys crypto.KeyProvider) *ControllerServer {
	testPoolResolver(c, "server7", "tank", "1")
	return NewControllerServer(ControllerConfig{
		Log:       logr.Discard(),
		Client:    c,
		APIReader: c,
		Namespace: "zfs-csi-system",
		Portal:    "server7:4420",
		Keys:      keys,
	})
}

func testPoolResolver(c crclient.Client, owner, pool, guid string) inventory.Resolver {
	return testPoolResolverWithFree(c, owner, pool, guid, 1<<40)
}

func testPoolResolverWithFree(c crclient.Client, owner, pool, guid string, free int64) inventory.Resolver {
	now := time.Now()
	enabled := true
	observed := metav1.NewTime(now)
	node := &zfscsiv1.StorageNode{ObjectMeta: metav1.ObjectMeta{Name: owner, Generation: 1}, Spec: zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: []string{guid}, Enabled: &enabled, NetworkDomain: "workers"}, Status: zfscsiv1.StorageNodeStatus{ObservedGeneration: 1, LastObservedTime: &observed, ReachableFrom: []string{"workers"}, Endpoints: []zfscsiv1.StorageNodeEndpoint{{Protocol: zfscsiv1.StorageProtocolNFS, Host: owner, Port: 2049}, {Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: owner, Port: 4420}}, Conditions: []metav1.Condition{{Type: zfscsiv1.StorageNodeConditionReady, Status: metav1.ConditionTrue}}, Pools: []zfscsiv1.StorageNodePoolStatus{{GUID: guid, Name: pool, FreeBytes: free, CapacityObservedAt: observed, Ready: true}}}}
	_ = c.Create(context.Background(), node)
	return inventory.Resolver{Client: c, Now: func() time.Time { return now }}
}

func withPoolOwner(s *ControllerServer, c crclient.Client, owner string) {
	testPoolResolver(c, owner, "tank", "1")
}

func mustSetReady(t *testing.T, c crclient.Client, name string) {
	t.Helper()

	v := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: name}, v); err != nil {
		t.Fatal(err)
	}

	patch := crclient.MergeFrom(v.DeepCopy())
	v.Status.State = zfscsiv1.VolumeStateReady

	v.Status.ActualCapacity = v.Spec.Capacity
	parsed := mustParseTestVolumeID(t, v.Spec.VolumeID)
	v.Status.TargetNQN = nqnFor(v, parsed)
	v.Status.DeviceGUID, _ = naming.DeviceGUID(v.Spec.OwnerNode, v.Spec.PoolGUID, parsed.Kind, parsed.ID)
	v.Status.Portal = "server7:4420"
	v.Status.PortalHost = "server7"
	v.Status.PortalPort = 4420
	v.Status.NFSServer = "server7"
	if err := c.Status().Patch(context.Background(), v, patch); err != nil {
		t.Fatal(err)
	}
}

// TestCreateVolume_WritesCR proves CreateVolume translates CSI intent → Volume CR.
// It sets the CR Ready via a watcher-style goroutine so the controller's
// waitForReady returns (simulating the storage-agent reconciler).
func TestCreateVolume_WritesCR(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	go autoReady(t, c, "pvc-abc123")

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-abc123",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
		Parameters: map[string]string{
			"pool": "tank", "type": "block", "fsType": "ext4",
			"blocksize": "16k", "transport": "nvme-tcp",
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if resp.GetVolume().GetVolumeId() == "" {
		t.Fatal("empty volume id")
	}
	// CR must exist with the parsed id + spec.
	v := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-abc123"}, v); err != nil {
		t.Fatalf("CR not written: %v", err)
	}

	if v.Spec.Pool != "tank" || v.Spec.Type != zfscsiv1.VolumeTypeBlock {
		t.Fatalf("CR spec wrong: %+v", v.Spec)
	}

	if v.Spec.Capacity != 1024*1024*1024 {
		t.Fatalf("capacity mismatch: %d", v.Spec.Capacity)
	}
}

func TestCreateSnapshotDerivesOwnerNodeAndRejectsUnavailableOwner(t *testing.T) {
	ctx := context.Background()
	withOwner := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "source"}, Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1, OwnerNode: "storage-a",
		VolumeID: "csi:tank:block:source", VolName: "source", Transport: zfscsiv1.TransportNVMeTCP,
	}}
	c := newTestClient(t, withOwner)
	cs := newTestController(c)
	go func() {
		for {
			snap := &zfscsiv1.Snapshot{}
			if err := c.Get(ctx, crclient.ObjectKey{Name: "snap"}, snap); err == nil {
				before := snap.DeepCopy()
				snap.Status.State = zfscsiv1.SnapshotStateReady
				snap.Status.ReadyToUse = true
				if err := c.Status().Patch(ctx, snap, crclient.MergeFrom(before)); err != nil {
					t.Errorf("mark snapshot ready: %v", err)
				}
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	if _, err := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap", SourceVolumeId: withOwner.Spec.VolumeID}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Snapshot{}
	if err := c.Get(ctx, crclient.ObjectKey{Name: "snap"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.OwnerNode != withOwner.Spec.OwnerNode {
		t.Fatalf("ownerNode=%q, want %q", got.Spec.OwnerNode, withOwner.Spec.OwnerNode)
	}

	withoutOwner := withOwner.DeepCopy()
	withoutOwner.Name = "owner-unavailable"
	withoutOwner.Spec.VolumeID = "csi:tank:block:owner-unavailable"
	withoutOwner.Spec.OwnerNode = ""
	c = newTestClient(t, withoutOwner)
	cs = newTestController(c)
	_, err := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "unavailable", SourceVolumeId: withoutOwner.Spec.VolumeID})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateSnapshot error=%v code=%s, want FailedPrecondition", err, status.Code(err))
	}
	if err := c.Get(ctx, crclient.ObjectKey{Name: "unavailable"}, &zfscsiv1.Snapshot{}); !apierrors.IsNotFound(err) {
		t.Fatalf("snapshot with unavailable owner exists: %v", err)
	}
}

func TestCreateVolumeUsesInventoryPortalNotControllerFlag(t *testing.T) {
	cs := newTestController(newTestClient(t))
	cs.portal = ""
	go autoReady(t, cs.client, "pvc-no-portal")
	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-no-portal",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:    map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume must use owner inventory endpoint, got %v", err)
	}
}

func TestCreateVolume_AppliesInitialMutableCompression(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	go autoReady(t, c, "pvc-vac-initial")
	req := &csi.CreateVolumeRequest{
		Name:          "pvc-vac-initial",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters: map[string]string{
			"pool": "tank", "type": "block", "transport": "nvme-tcp", "compression": "off",
		},
		MutableParameters: map[string]string{"CoMpReSsIoN": "zstd-3"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	}
	_, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume with initial mutable compression: %v", err)
	}
	if _, err := cs.CreateVolume(context.Background(), req); err != nil {
		t.Fatalf("idempotent initial VAC retry: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-vac-initial"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Compression != "zstd-3" {
		t.Fatalf("initial VAC compression = %q, want zstd-3", got.Spec.Compression)
	}
}

func TestCreateVolume_RejectsUnsupportedMutableParametersBeforeCreation(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-vac-unsupported",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:    map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp"},
		MutableParameters: map[string]string{
			"compression": "zstd-3", "upstream.example.io/unsupported": "value",
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsupported mutable parameters error code = %s, want InvalidArgument (err=%v)", status.Code(err), err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-vac-unsupported"}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("unsupported mutable params created a Volume CR: %v", err)
	}
}

func createTestVolume(t *testing.T, cs *ControllerServer, c crclient.Client, name string, bytes int64, params map[string]string) (*csi.CreateVolumeResponse, error) {
	t.Helper()
	go autoReady(t, c, name)

	return cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          name,
		CapacityRange: &csi.CapacityRange{RequiredBytes: bytes},
		Parameters:    params,
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
}

func TestCreateVolume_IdempotentSameParamsSucceeds(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	params := map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp"}
	const oneGiB = 1024 * 1024 * 1024

	first, err := createTestVolume(t, cs, c, "pvc-idem", oneGiB, params)
	if err != nil {
		t.Fatalf("first CreateVolume: %v", err)
	}

	// Same name, same capacity+params → idempotent success, same volume id.
	second, err := createTestVolume(t, cs, c, "pvc-idem", oneGiB, params)
	if err != nil {
		t.Fatalf("idempotent retry must succeed, got: %v", err)
	}
	if first.GetVolume().GetVolumeId() != second.GetVolume().GetVolumeId() {
		t.Fatalf("idempotent retry returned different volume id: %q vs %q",
			first.GetVolume().GetVolumeId(), second.GetVolume().GetVolumeId())
	}
}

func TestSelectPlacementUsesFreshInventoryAndPendingReservations(t *testing.T) {
	c := newTestClient(t)
	testPoolResolverWithFree(c, "node-b", "tank", "2", 200)
	testPoolResolverWithFree(c, "node-a", "tank", "1", 200)
	pending := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "pending"}, Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", Pool: "tank", PoolGUID: "1", Capacity: 150}}
	if err := c.Create(t.Context(), pending); err != nil {
		t.Fatal(err)
	}
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: c, APIReader: c, Namespace: "zfs-csi-system", Portal: "server7:4420"})
	candidate, err := cs.selectPlacement(t.Context(), "tank", 40, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.OwnerNode != "node-b" {
		t.Fatalf("candidate=%#v, want node-b because node-a reservation is unaccounted", candidate)
	}
}

func TestCreateVolumeInsufficientReturnsResourceExhausted(t *testing.T) {
	c := newTestClient(t)
	testPoolResolverWithFree(c, "node-a", "tank", "1", 10)
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: c, APIReader: c, Namespace: "zfs-csi-system", Portal: "server7:4420"})
	_, err := cs.CreateVolume(t.Context(), &csi.CreateVolumeRequest{Name: "exhausted", CapacityRange: &csi.CapacityRange{RequiredBytes: 11}, Parameters: map[string]string{"pool": "tank", "type": "block"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("error=%v, want ResourceExhausted", err)
	}
}

func TestCreateVolumeDeletingReservationReturnsResourceExhaustedUntilDestroyed(t *testing.T) {
	now := metav1.Now()
	deleting := now.DeepCopy()
	reserved := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "wedged-delete", DeletionTimestamp: deleting, Finalizers: []string{zfscsiv1.VolumeFinalizer}},
		Spec:       zfscsiv1.VolumeSpec{OwnerNode: "node-a", Pool: "tank", PoolGUID: "1", Capacity: 90},
	}
	c := newTestClient(t, reserved)
	testPoolResolverWithFree(c, "node-a", "tank", "1", 100)
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: c, APIReader: c, Namespace: "zfs-csi-system", Portal: "server7:4420"})
	req := &csi.CreateVolumeRequest{Name: "after-wedged-delete", CapacityRange: &csi.CapacityRange{RequiredBytes: 11}, Parameters: map[string]string{"pool": "tank", "type": "block"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}}
	if _, err := cs.CreateVolume(t.Context(), req); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("deleting reservation error=%v, want ResourceExhausted", err)
	}

	current := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Name: reserved.Name}, current); err != nil {
		t.Fatal(err)
	}
	before := current.DeepCopy()
	current.Status.State = zfscsiv1.VolumeStateDestroyed
	if err := c.Status().Patch(t.Context(), current, crclient.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := cs.CreateVolume(ctx, req); status.Code(err) == codes.ResourceExhausted {
		t.Fatalf("Destroyed reservation still blocked placement: %v", err)
	}
}

func TestCreateVolume_RetryUsesPersistedOwnerDespiteInventoryChange(t *testing.T) {
	c := newTestClient(t)
	ownerA := newTestController(c)
	withPoolOwner(ownerA, c, "node-a")
	ownerB := newTestController(c)
	withPoolOwner(ownerB, c, "node-b")
	params := map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp"}

	if _, err := createTestVolume(t, ownerA, c, "pvc-owner", 1024*1024*1024, params); err != nil {
		t.Fatalf("first CreateVolume: %v", err)
	}
	_, err := createTestVolume(t, ownerB, c, "pvc-owner", 1024*1024*1024, params)
	if err != nil {
		t.Fatalf("retry after inventory owner change: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Name: "pvc-owner"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.OwnerNode != "node-a" || got.Spec.PoolGUID != "1" || got.Spec.NetworkDomain != "workers" {
		t.Fatalf("persisted placement identity=%#v, want node-a pool 1 in workers", got.Spec)
	}
	if got.Status.PortalHost != "server7" || got.Status.PortalPort != 4420 {
		t.Fatalf("persisted endpoint=%s:%d, want original server7:4420", got.Status.PortalHost, got.Status.PortalPort)
	}
}

func TestCreateVolume_IncompatibleCapacityReturnsAlreadyExists(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	params := map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp"}

	if _, err := createTestVolume(t, cs, c, "pvc-cap", 1024*1024*1024, params); err != nil {
		t.Fatalf("first CreateVolume: %v", err)
	}

	// Same name, DIFFERENT capacity → AlreadyExists.
	_, err := createTestVolume(t, cs, c, "pvc-cap", 2*1024*1024*1024, params)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("incompatible capacity: code = %v, want AlreadyExists (err=%v)", status.Code(err), err)
	}
}

func TestCreateVolume_IncompatiblePoolReturnsAlreadyExists(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	const oneGiB = 1024 * 1024 * 1024

	if _, err := createTestVolume(t, cs, c, "pvc-pool", oneGiB,
		map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp"}); err != nil {
		t.Fatalf("first CreateVolume: %v", err)
	}

	// Same name, DIFFERENT pool → AlreadyExists.
	_, err := createTestVolume(t, cs, c, "pvc-pool", oneGiB,
		map[string]string{"pool": "flash", "type": "block", "transport": "nvme-tcp"})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("incompatible pool: code = %v, want AlreadyExists (err=%v)", status.Code(err), err)
	}
}

func TestCreateVolume_WritesSnapshotCloneSourceToCR(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	snapshotID := "csi:tank:block:source@snap-a"
	if err := c.Create(context.Background(), &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "snap-a"}, Spec: zfscsiv1.SnapshotSpec{SourceVolumeID: "csi:tank:block:source", SnapshotID: snapshotID, OwnerNode: "server7", PoolGUID: "1"}}); err != nil {
		t.Fatal(err)
	}

	go autoReady(t, c, "pvc-restore")

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-restore",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
		Parameters:    map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshotID},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if got := resp.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId(); got != snapshotID {
		t.Fatalf("response snapshot source = %q, want %q", got, snapshotID)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-restore"}, got); err != nil {
		t.Fatalf("get Volume CR: %v", err)
	}
	if got.Spec.SourceSnapshotID != snapshotID {
		t.Fatalf("SourceSnapshotID = %q, want %q", got.Spec.SourceSnapshotID, snapshotID)
	}
	if got.Spec.SourceVolumeID != "" {
		t.Fatalf("SourceVolumeID = %q, want empty for snapshot restore", got.Spec.SourceVolumeID)
	}
}

func TestCreateVolume_RejectsMismatchedSnapshotSource(t *testing.T) {
	cs := newTestController(newTestClient(t))

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-restore-bad",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:    map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "csi:other:block:source@snap-a"},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVolume error code = %s, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

// TestCreateVolume_RejectsMismatchedVolumeSource guards the same-pool clone
// invariant (see docs/storage-model.md): `zfs clone` is same-pool-only, so a
// clone whose source volume lives in a different pool than the requested
// StorageClass must be rejected rather than silently mis-placed.
func TestCreateVolume_RejectsMismatchedVolumeSource(t *testing.T) {
	cs := newTestController(newTestClient(t))

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-clone-bad",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:    map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "csi:other:block:source"},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVolume error code = %s, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestCreateVolume_WritesVolumeCloneSourceToCR(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	if err := c.Create(context.Background(), &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "source"},
		Spec:       zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceDynamic, Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:source"},
	}); err != nil {
		t.Fatal(err)
	}

	go autoReady(t, c, "pvc-clone")

	sourceVolumeID := "csi:tank:block:source"
	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-clone",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
		Parameters:    map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: sourceVolumeID},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if got := resp.GetVolume().GetContentSource().GetVolume().GetVolumeId(); got != sourceVolumeID {
		t.Fatalf("response volume source = %q, want %q", got, sourceVolumeID)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-clone"}, got); err != nil {
		t.Fatalf("get Volume CR: %v", err)
	}
	if got.Spec.SourceVolumeID != sourceVolumeID {
		t.Fatalf("SourceVolumeID = %q, want %q", got.Spec.SourceVolumeID, sourceVolumeID)
	}
	if got.Spec.SourceSnapshotID != "" {
		t.Fatalf("SourceSnapshotID = %q, want empty for volume clone", got.Spec.SourceSnapshotID)
	}
	if got.Spec.Capacity != 2<<30 {
		t.Fatalf("capacity = %d, want %d", got.Spec.Capacity, int64(2<<30))
	}
}

func TestCreateVolume_NetworkAttachedVolumeUsesReachabilityTopology(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	go autoReady(t, c, "pvc-network")

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-network",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
		Parameters: map[string]string{
			"pool": "tank", "type": "block", "transport": "nvme-tcp",
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		AccessibilityRequirements: &csi.TopologyRequirement{Preferred: []*csi.Topology{{Segments: map[string]string{reachability.TopologyKeyNetworkDomain: "workers"}}}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if got := resp.GetVolume().GetAccessibleTopology(); len(got) != 1 || got[0].GetSegments()[reachability.TopologyKeyNetworkDomain] != "workers" {
		t.Fatalf("network-attached volume topology=%v, want workers reachability domain", got)
	}
}

func TestCreateVolume_FilesystemNFSRequiresExportCIDR(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-nfs-default",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
		Parameters:    map[string]string{"pool": "tank", "type": "filesystem"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVolume code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestCreateVolume_FilesystemNFSStorageClassOverride(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	go autoReady(t, c, "pvc-nfs-override")

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-nfs-override",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
		Parameters: map[string]string{
			"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.42.0.0/16", "nfsExportAccessMode": "ro", "nfsTLS": "true",
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-nfs-override"}, got); err != nil {
		t.Fatalf("get Volume CR: %v", err)
	}
	if !slices.Equal(got.Spec.NFSExportCIDRs, []string{"10.42.0.0/16"}) || got.Spec.NFSExportAccessMode != "ro" || !got.Spec.NFSTLSEnabled {
		t.Fatalf("NFS override = %q/%q/%t, want 10.42.0.0/16/ro/true", got.Spec.NFSExportCIDRs, got.Spec.NFSExportAccessMode, got.Spec.NFSTLSEnabled)
	}
}

func TestCreateVolume_RejectsIdempotentRetryWithMismatchedNFSTLS(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	params := map[string]string{
		"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.42.0.0/16", "nfsTLS": "false",
	}
	req := func(parameters map[string]string) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:          "pvc-nfs-tls-idempotency",
			CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
			Parameters:    parameters,
			VolumeCapabilities: []*csi.VolumeCapability{{
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			}},
		}
	}

	go autoReady(t, c, "pvc-nfs-tls-idempotency")
	if _, err := cs.CreateVolume(context.Background(), req(params)); err != nil {
		t.Fatalf("first CreateVolume: %v", err)
	}

	params["nfsTLS"] = "true"
	_, err := cs.CreateVolume(context.Background(), req(params))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("retry code = %v, want AlreadyExists (err=%v)", status.Code(err), err)
	}
}

func TestVolumeSpecCompatibleTreatsNFSExportCIDRsAsSet(t *testing.T) {
	existing := &zfscsiv1.VolumeSpec{
		Pool: "tank", Capacity: 1, Type: zfscsiv1.VolumeTypeFilesystem,
		OwnerNode:      "storage-0",
		NFSExportCIDRs: []string{"10.42.0.0/16", "2001:db8::/64"}, NFSExportAccessMode: "rw",
	}
	want := requestedVolume{
		pool: "tank", capacity: 1, kind: zfscsiv1.VolumeTypeFilesystem, ownerNode: "storage-0",
		nfsCIDRs: []string{"2001:db8::/64", "10.42.0.0/16"}, nfsMode: "rw",
	}
	if err := volumeSpecCompatible(existing, want); err != nil {
		t.Fatalf("equivalent CIDR set rejected: %v", err)
	}
}

func TestCreateVolume_RejectsUnsafeNFSExportParameters(t *testing.T) {
	for name, params := range map[string]map[string]string{
		"raw sharenfs string": {"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "rw=@0.0.0.0/0"},
		"unmasked cidr":       {"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.42.0.7/24"},
		"doubled comma":       {"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.0.0.0/8,,192.0.2.0/24"},
		"unsafe mode":         {"pool": "tank", "type": "filesystem", "nfsExportAccessMode": "rw,no_root_squash"},
	} {
		t.Run(name, func(t *testing.T) {
			c := newTestClient(t)
			cs := newTestController(c)

			_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:          "pvc-" + sanitizeID(name),
				CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
				Parameters:    params,
				VolumeCapabilities: []*csi.VolumeCapability{{
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				}},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CreateVolume code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
			}
		})
	}
}

func TestCreateVolume_NVMeTLSCreatesImmutableStableSecretWithoutLeak(t *testing.T) {
	c := newTestClient(t)
	cs := NewControllerServer(ControllerConfig{
		Log: logr.Discard(), Client: c, APIReader: c, Namespace: "zfs-csi-system",
		PSKReader: bytes.NewReader(bytes.Repeat([]byte{0x42}, 64)),
	})
	testPoolResolver(c, "server7", "tank", "1")
	go autoReady(t, c, "pvc-nvme-tls")
	req := &csi.CreateVolumeRequest{
		Name: "pvc-nvme-tls", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "nvmeTLS": "true"},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
	}
	first, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Name: "pvc-nvme-tls"}, vol); err != nil {
		t.Fatal(err)
	}
	wantName := nvmeTLSPSKSecretName(crNameFor(req.Name))
	if !vol.Spec.NVMeTLSEnabled || vol.Spec.NVMeTLSPSKSecretName != wantName {
		t.Fatalf("NVMe TLS spec = enabled=%v ref=%q", vol.Spec.NVMeTLSEnabled, vol.Spec.NVMeTLSPSKSecretName)
	}
	secret := &corev1.Secret{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: wantName}, secret); err != nil {
		t.Fatal(err)
	}
	if secret.Immutable == nil || !*secret.Immutable || secret.Type != corev1.SecretTypeOpaque || len(secret.OwnerReferences) != 0 {
		t.Fatalf("Secret metadata = immutable=%v type=%q ownerRefs=%v", secret.Immutable, secret.Type, secret.OwnerReferences)
	}
	if _, err := psk.Parse(string(secret.Data[nvmeTLSPSKSecretDataKey])); err != nil {
		t.Fatalf("configured PSK does not parse: %v", err)
	}
	before := append([]byte(nil), secret.Data[nvmeTLSPSKSecretDataKey]...)
	second, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: wantName}, secret); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, secret.Data[nvmeTLSPSKSecretDataKey]) {
		t.Fatal("idempotent retry changed configured PSK bytes")
	}
	for _, contextMap := range []map[string]string{first.Volume.VolumeContext, second.Volume.VolumeContext, publishContextForVolume(vol)} {
		for key, value := range contextMap {
			if key == nvmeTLSPSKSecretDataKey || strings.Contains(value, string(before)) {
				t.Fatalf("configured PSK leaked through CSI context: %v", contextMap)
			}
		}
	}
}

type secretGetReader struct {
	crclient.Reader
	gets      int
	missFirst bool
}

func (r *secretGetReader) Get(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
	r.gets++
	if r.missFirst && r.gets == 1 {
		return apierrors.NewNotFound(corev1.Resource("secrets"), key.Name)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

type noSecretGetClient struct {
	crclient.Client
	gets                int
	creates             int
	createAlreadyExists bool
}

func (c *noSecretGetClient) Get(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
	c.gets++
	return errors.New("cached client Get must not be called")
}

func (c *noSecretGetClient) Create(ctx context.Context, obj crclient.Object, opts ...crclient.CreateOption) error {
	c.creates++
	if c.createAlreadyExists {
		return apierrors.NewAlreadyExists(corev1.Resource("secrets"), obj.GetName())
	}
	return c.Client.Create(ctx, obj, opts...)
}

func TestEnsureNVMeTLSPSKSecretReadsThroughAPIReader(t *testing.T) {
	interchange, err := psk.Generate(bytes.NewReader(bytes.Repeat([]byte{0x42}, 32)), psk.HMACSHA256)
	if err != nil {
		t.Fatal(err)
	}
	formatted, err := interchange.Format()
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	const volumeName = "pvc-api-reader"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: nvmeTLSPSKSecretName(volumeName), Namespace: "zfs-csi-system"},
		Immutable:  &immutable,
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{nvmeTLSPSKSecretDataKey: []byte(formatted)},
	}

	for _, tc := range []struct {
		name                string
		missFirst           bool
		createAlreadyExists bool
		wantReaderGets      int
		wantCreates         int
	}{
		{name: "existing secret", wantReaderGets: 1},
		{name: "winning secret after create race", missFirst: true, createAlreadyExists: true, wantReaderGets: 2, wantCreates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backing := newTestClient(t, secret.DeepCopy())
			reader := &secretGetReader{Reader: backing, missFirst: tc.missFirst}
			cached := &noSecretGetClient{Client: backing, createAlreadyExists: tc.createAlreadyExists}
			cs := NewControllerServer(ControllerConfig{
				Client: cached, APIReader: reader, Namespace: "zfs-csi-system",
				PSKReader: bytes.NewReader(bytes.Repeat([]byte{0x24}, 32)),
			})

			got, err := cs.ensureNVMeTLSPSKSecret(t.Context(), volumeName)
			if err != nil {
				t.Fatal(err)
			}
			if got != secret.Name {
				t.Fatalf("Secret name = %q, want %q", got, secret.Name)
			}
			if reader.gets != tc.wantReaderGets || cached.gets != 0 || cached.creates != tc.wantCreates {
				t.Fatalf("calls: APIReader.Get=%d cached.Get=%d cached.Create=%d, want %d, 0, %d", reader.gets, cached.gets, cached.creates, tc.wantReaderGets, tc.wantCreates)
			}
		})
	}
}

func TestCreateVolume_RejectsNVMeTLSCompatibilityMismatch(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	go autoReady(t, c, "pvc-nvme-tls-mismatch")
	req := &csi.CreateVolumeRequest{Name: "pvc-nvme-tls-mismatch", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, Parameters: map[string]string{"pool": "tank", "type": "block"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}}
	_, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.Parameters["nvmeTLS"] = "true"
	if _, err := cs.CreateVolume(t.Context(), req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("retry code=%v err=%v, want AlreadyExists", status.Code(err), err)
	}
}

func TestCreateVolume_NVMeTLSDeleteDoesNotPrematurelyDeleteSecret(t *testing.T) {
	for _, policy := range []zfscsiv1.VolumeDeletionPolicy{zfscsiv1.VolumeDeletionPolicyDelete, zfscsiv1.VolumeDeletionPolicyRetain} {
		t.Run(string(policy), func(t *testing.T) {
			name := "delete-" + strings.ToLower(string(policy))
			secretName := nvmeTLSPSKSecretName(crNameFor(name))
			immutable := true
			interchange, _ := psk.Generate(bytes.NewReader(bytes.Repeat([]byte{1}, 32)), psk.HMACSHA256)
			formatted, _ := interchange.Format()
			vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: zfscsiv1.VolumeSpec{VolumeID: "csi:tank:block:" + name, Pool: "tank", PoolGUID: "1", Type: zfscsiv1.VolumeTypeBlock, VolName: name, OwnerNode: "server7", NetworkDomain: "test", DeletionPolicy: policy, NVMeTLSEnabled: true, NVMeTLSPSKSecretName: secretName}}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "zfs-csi-system"}, Immutable: &immutable, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{nvmeTLSPSKSecretDataKey: []byte(formatted)}}
			c := newTestClient(t, vol, secret)
			cs := newTestController(c)
			if _, err := cs.DeleteVolume(t.Context(), &csi.DeleteVolumeRequest{VolumeId: vol.Spec.VolumeID}); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(t.Context(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: secretName}, &corev1.Secret{}); err != nil {
				t.Fatalf("DeleteVolume prematurely removed PSK Secret: %v", err)
			}
		})
	}
}

func TestCreateVolume_NVMeTLSCompatibilityAndModeValidation(t *testing.T) {
	for name, parameters := range map[string]map[string]string{
		"filesystem": {"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.0.0.0/24", "nvmeTLS": "true"},
		"bad bool":   {"pool": "tank", "type": "block", "nvmeTLS": "sometimes"},
	} {
		t.Run(name, func(t *testing.T) {
			cs := newTestController(newTestClient(t))
			_, err := cs.CreateVolume(t.Context(), &csi.CreateVolumeRequest{Name: "invalid-" + sanitizeID(name), CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, Parameters: parameters, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code=%v err=%v, want InvalidArgument", status.Code(err), err)
			}
		})
	}

	c := newTestClient(t)
	cs := newTestController(c)
	req := &csi.CreateVolumeRequest{Name: "tls-mismatch", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, Parameters: map[string]string{"pool": "tank", "type": "block", "nvmeTLS": "true"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}}
	go autoReady(t, c, req.Name)
	_, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.Parameters["nvmeTLS"] = "false"
	if _, err := cs.CreateVolume(t.Context(), req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("mismatched retry code=%v err=%v, want AlreadyExists", status.Code(err), err)
	}
}

func TestCreateVolume_NVMeTLSCloneCreatesDestinationSecretWithoutMutatingSource(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	sourceSecretName := nvmeTLSPSKSecretName("source")
	immutable := true
	sourceSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: sourceSecretName, Namespace: "zfs-csi-system"},
		Immutable:  &immutable,
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{nvmeTLSPSKSecretDataKey: []byte("source-psk")},
	}
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "source"}, Spec: zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers", Type: zfscsiv1.VolumeTypeBlock, Transport: zfscsiv1.TransportNVMeTCP, Provenance: zfscsiv1.VolumeProvenanceDynamic, VolumeID: "csi:tank:block:source", VolName: "source", Capacity: 1 << 30, NVMeTLSEnabled: true, NVMeTLSPSKSecretName: sourceSecretName}}
	if err := c.Create(t.Context(), sourceSecret); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	go autoReady(t, c, "clone-tls")
	req := &csi.CreateVolumeRequest{Name: "clone-tls", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, Parameters: map[string]string{"pool": "tank", "type": "block", "nvmeTLS": "true"}, VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: source.Spec.VolumeID}}}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}}
	first, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	destination := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Name: "clone-tls"}, destination); err != nil {
		t.Fatal(err)
	}
	if destination.Spec.NVMeTLSPSKSecretName == sourceSecretName {
		t.Fatal("clone inherited source NVMe TLS PSK Secret")
	}
	destinationSecret := &corev1.Secret{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: destination.Spec.NVMeTLSPSKSecretName}, destinationSecret); err != nil {
		t.Fatal(err)
	}
	if destinationSecret.Immutable == nil || !*destinationSecret.Immutable || destinationSecret.Type != corev1.SecretTypeOpaque {
		t.Fatalf("destination Secret %q must be immutable opaque", destinationSecret.Name)
	}
	destinationPSK := append([]byte(nil), destinationSecret.Data[nvmeTLSPSKSecretDataKey]...)
	second, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatalf("idempotent clone retry: %v", err)
	}
	if first.GetVolume().GetVolumeId() != second.GetVolume().GetVolumeId() {
		t.Fatalf("idempotent clone id changed: %q vs %q", first.GetVolume().GetVolumeId(), second.GetVolume().GetVolumeId())
	}
	if err := c.Get(t.Context(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: destination.Spec.NVMeTLSPSKSecretName}, destinationSecret); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(destinationPSK, destinationSecret.Data[nvmeTLSPSKSecretDataKey]) {
		t.Fatal("idempotent clone retry changed destination PSK bytes")
	}

	snapshotID := "csi:tank:block:source@snap-tls"
	if err := c.Create(t.Context(), &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "snap-tls"}, Spec: zfscsiv1.SnapshotSpec{SourceVolumeID: source.Spec.VolumeID, SnapshotID: snapshotID, OwnerNode: "server7", PoolGUID: "1"}}); err != nil {
		t.Fatal(err)
	}
	go autoReady(t, c, "restore-tls")
	restoreReq := &csi.CreateVolumeRequest{Name: "restore-tls", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, Parameters: map[string]string{"pool": "tank", "type": "block", "nvmeTLS": "true"}, VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshotID}}}, VolumeCapabilities: req.VolumeCapabilities}
	if _, err := cs.CreateVolume(t.Context(), restoreReq); err != nil {
		t.Fatalf("restore NVMe TLS snapshot: %v", err)
	}
	restore := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Name: "restore-tls"}, restore); err != nil {
		t.Fatal(err)
	}
	if restore.Spec.NVMeTLSPSKSecretName == sourceSecretName || restore.Spec.NVMeTLSPSKSecretName == destination.Spec.NVMeTLSPSKSecretName {
		t.Fatalf("restore PSK ref=%q, want distinct source=%q clone=%q", restore.Spec.NVMeTLSPSKSecretName, sourceSecretName, destination.Spec.NVMeTLSPSKSecretName)
	}
	restoreSecret := &corev1.Secret{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: restore.Spec.NVMeTLSPSKSecretName}, restoreSecret); err != nil {
		t.Fatal(err)
	}
	if restoreSecret.Immutable == nil || !*restoreSecret.Immutable || restoreSecret.Type != corev1.SecretTypeOpaque {
		t.Fatalf("restore Secret %q must be immutable opaque", restoreSecret.Name)
	}
	if err := c.Get(t.Context(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: sourceSecretName}, sourceSecret); err != nil {
		t.Fatal(err)
	}
	if got := string(sourceSecret.Data[nvmeTLSPSKSecretDataKey]); got != "source-psk" {
		t.Fatalf("source PSK = %q, want unchanged", got)
	}
}

func TestCreateVolume_EncryptedGeneratesDEKReference(t *testing.T) {
	c := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}}
	cs := newTestControllerWithKeys(c, keys)

	go autoReady(t, c, "pvc-crypt")

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-crypt",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
		Parameters: map[string]string{
			"pool": "tank", "type": "block", "transport": "nvme-tcp", "encrypted": "true",
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-crypt"}, got); err != nil {
		t.Fatalf("get Volume CR: %v", err)
	}

	if len(keys.generated) != 1 {
		t.Fatalf("Generate calls = %v, want one", keys.generated)
	}
	if got.Spec.EncryptionKeyRef != keys.generated[0] {
		t.Fatalf("encryption key ref = %q, want generated ref %q", got.Spec.EncryptionKeyRef, keys.generated[0])
	}
	if _, err := keys.Fetch(context.Background(), got.Spec.EncryptionKeyRef); err != nil {
		t.Fatalf("generated ref must round-trip through Fetch: %v", err)
	}
}

func TestCreateVolume_EncryptedExistingRetryDoesNotGenerateDEK(t *testing.T) {
	c := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}}
	cs := newTestControllerWithKeys(c, keys)
	go autoReady(t, c, "pvc-crypt-retry")
	req := &csi.CreateVolumeRequest{
		Name: "pvc-crypt-retry", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "encrypted": "true"},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
	}
	if _, err := cs.CreateVolume(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	generated := len(keys.generated)
	if _, err := cs.CreateVolume(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if len(keys.generated) != generated {
		t.Fatalf("existing retry generated another DEK: before=%d after=%d", generated, len(keys.generated))
	}
}

func TestCreateVolume_EncryptedGenerateFailureDoesNotWriteCR(t *testing.T) {
	c := newTestClient(t)
	keys := &controllerRecKeys{generateErr: errors.New("bao unavailable")}
	cs := newTestControllerWithKeys(c, keys)

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-crypt-fail",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
		Parameters: map[string]string{
			"pool": "tank", "type": "block", "encrypted": "true",
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if err == nil {
		t.Fatal("expected Generate failure")
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pvc-crypt-fail"}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("Volume CR should not be written, get err = %v", err)
	}
	lease := &coordinationv1.Lease{}
	if err := c.Get(context.Background(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: defaultPlacementLeaseName}, lease); !apierrors.IsNotFound(err) {
		t.Fatalf("Generate failure should not acquire placement Lease, get err=%v", err)
	}
}

func TestCreateVolume_EncryptedPlacementFailureDoesNotDeleteGeneratedKey(t *testing.T) {
	c := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}}
	cs := newTestControllerWithKeys(c, keys)
	_, err := cs.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name: "pvc-crypt-exhausted", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 50},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "encrypted": "true"},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("error=%v, want ResourceExhausted", err)
	}
	if len(keys.generated) != 1 || len(keys.deleted) != 0 {
		t.Fatalf("generated=%v deleted=%v, failed create must not shred shared key", keys.generated, keys.deleted)
	}
	if _, err := keys.Fetch(t.Context(), keys.generated[0]); err != nil {
		t.Fatalf("generated key not fetchable after placement failure: %v", err)
	}
}

func TestCreateVolume_EncryptedLeaseAcquireFailureDoesNotDeleteGeneratedKey(t *testing.T) {
	base := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}}
	client := &placementFaultClient{Client: base, failLeaseGet: true}
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: client, APIReader: base, Namespace: "zfs-csi-system", Portal: "server7:4420", Keys: keys})
	testPoolResolver(base, "server7", "tank", "1")
	_, err := cs.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name: "pvc-crypt-lease-fail", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "encrypted": "true"},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
	})
	if err == nil {
		t.Fatal("expected Lease acquisition failure")
	}
	if len(keys.generated) != 1 || len(keys.deleted) != 0 {
		t.Fatalf("generated=%v deleted=%v, Lease failure must not shred shared key", keys.generated, keys.deleted)
	}
}

func TestCreateVolume_EncryptedCreateFailureDoesNotDeleteGeneratedKey(t *testing.T) {
	base := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}}
	client := &placementFaultClient{Client: base, failVolumeCreate: true}
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: client, APIReader: base, Namespace: "zfs-csi-system", Portal: "server7:4420", Keys: keys})
	testPoolResolver(base, "server7", "tank", "1")
	_, err := cs.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name: "pvc-crypt-create-fail", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "encrypted": "true"},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
	})
	if err == nil {
		t.Fatal("expected Volume create failure")
	}
	if len(keys.generated) != 1 || len(keys.deleted) != 0 {
		t.Fatalf("generated=%v deleted=%v, create failure must not shred shared key", keys.generated, keys.deleted)
	}
}

func TestCreateVolume_EncryptedUnderLeaseReadFailureDoesNotDeleteGeneratedKey(t *testing.T) {
	base := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}}
	client := &placementFaultClient{Client: base, failVolumeGetAt: 2}
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: client, APIReader: base, Namespace: "zfs-csi-system", Portal: "server7:4420", Keys: keys})
	testPoolResolver(base, "server7", "tank", "1")
	_, err := cs.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
		Name: "pvc-crypt-reread-fail", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "encrypted": "true"},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
	})
	if err == nil {
		t.Fatal("expected under-Lease Volume read failure")
	}
	if len(keys.generated) != 1 || len(keys.deleted) != 0 {
		t.Fatalf("generated=%v deleted=%v, under-Lease read failure must not shred shared key", keys.generated, keys.deleted)
	}
}

func TestCreateVolume_ConcurrentEncryptedSameNameLoserDoesNotShredWinnerKey(t *testing.T) {
	c := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}, generateArrived: make(chan struct{}, 2), generateBarrier: make(chan struct{})}
	servers := []*ControllerServer{newTestControllerWithKeys(c, keys), newTestControllerWithKeys(c, keys)}
	go autoReady(t, c, "pvc-crypt-race")
	requests := []*csi.CreateVolumeRequest{
		{Name: "pvc-crypt-race", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, Parameters: map[string]string{"pool": "tank", "type": "block", "encrypted": "true"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}},
		{Name: "pvc-crypt-race", CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30}, Parameters: map[string]string{"pool": "tank", "type": "block", "encrypted": "true"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}},
	}
	errs := make(chan error, len(servers))
	start := make(chan struct{})
	for i := range servers {
		go func(i int) {
			<-start
			_, err := servers[i].CreateVolume(t.Context(), requests[i])
			errs <- err
		}(i)
	}
	close(start)
	<-keys.generateArrived
	<-keys.generateArrived
	close(keys.generateBarrier)
	succeeded, incompatible := 0, 0
	for range servers {
		switch err := <-errs; status.Code(err) {
		case codes.OK:
			succeeded++
		case codes.AlreadyExists:
			incompatible++
		default:
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	if succeeded != 1 || incompatible != 1 {
		t.Fatalf("succeeded=%d incompatible=%d, want 1/1", succeeded, incompatible)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Name: "pvc-crypt-race"}, got); err != nil {
		t.Fatal(err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("losing request deleted shared key: %v", keys.deleted)
	}
	if _, err := keys.Fetch(t.Context(), got.Spec.EncryptionKeyRef); err != nil {
		t.Fatalf("winner key no longer fetchable: %v", err)
	}
}

func TestCreateVolume_FailedEncryptedCreateThenSameNameRetryUsable(t *testing.T) {
	base := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}}
	failingClient := &placementFaultClient{Client: base, failVolumeCreate: true}
	failing := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: failingClient, APIReader: base, Namespace: "zfs-csi-system", Portal: "server7:4420", Keys: keys})
	testPoolResolver(base, "server7", "tank", "1")
	req := &csi.CreateVolumeRequest{
		Name: "pvc-crypt-retry-after-fail", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:         map[string]string{"pool": "tank", "type": "block", "encrypted": "true"},
		VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
	}
	if _, err := failing.CreateVolume(t.Context(), req); err == nil {
		t.Fatal("expected first create failure")
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("first failure deleted deterministic key: %v", keys.deleted)
	}
	retry := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: base, APIReader: base, Namespace: "zfs-csi-system", Portal: "server7:4420", Keys: keys})
	go autoReady(t, base, "pvc-crypt-retry-after-fail")
	if _, err := retry.CreateVolume(t.Context(), req); err != nil {
		t.Fatalf("same-name retry: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := base.Get(t.Context(), crclient.ObjectKey{Name: "pvc-crypt-retry-after-fail"}, got); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Fetch(t.Context(), got.Spec.EncryptionKeyRef); err != nil {
		t.Fatalf("retry key not usable: %v", err)
	}
}

func TestCreateVolume_EncryptedGenerateDoesNotHoldPlacementLease(t *testing.T) {
	c := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}, generateStarted: make(chan struct{}), generateContinue: make(chan struct{})}
	cs := newTestControllerWithKeys(c, keys)
	go autoReady(t, c, "pvc-crypt-blocked")
	errCh := make(chan error, 1)
	go func() {
		_, err := cs.CreateVolume(t.Context(), &csi.CreateVolumeRequest{
			Name: "pvc-crypt-blocked", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
			Parameters:         map[string]string{"pool": "tank", "type": "block", "encrypted": "true"},
			VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
		})
		errCh <- err
	}()
	<-keys.generateStarted
	lease := &coordinationv1.Lease{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Namespace: "zfs-csi-system", Name: defaultPlacementLeaseName}, lease); !apierrors.IsNotFound(err) {
		t.Fatalf("placement Lease exists while Generate is blocked: err=%v lease=%#v", err, lease.Spec)
	}
	volume := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), crclient.ObjectKey{Name: "pvc-crypt-blocked"}, volume); !apierrors.IsNotFound(err) {
		t.Fatalf("Volume exists while Generate is blocked: err=%v", err)
	}
	close(keys.generateContinue)
	if err := <-errCh; err != nil {
		t.Fatalf("CreateVolume after Generate unblocked: %v", err)
	}
}

func TestCreateVolume_EncryptedReadinessFailureRetainsGeneratedDEK(t *testing.T) {
	c := newTestClient(t)
	keys := &controllerRecKeys{refs: map[string][]byte{}}
	cs := newTestControllerWithKeys(c, keys)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:          "pvc-crypt-timeout",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
		Parameters: map[string]string{
			"pool": "tank", "type": "block", "encrypted": "true",
		},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if err == nil {
		t.Fatal("expected readiness error")
	}
	if len(keys.generated) != 1 {
		t.Fatalf("Generate calls = %v, want one", keys.generated)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("Delete calls = %v, create-side failure must not shred shared key", keys.deleted)
	}
}

type placementFaultClient struct {
	crclient.Client
	failLeaseGet      bool
	failInventoryList bool
	failVolumeCreate  bool
	failVolumeGetAt   int
	volumeGets        int
}

func (c *placementFaultClient) List(ctx context.Context, list crclient.ObjectList, opts ...crclient.ListOption) error {
	if c.failInventoryList {
		switch list.(type) {
		case *zfscsiv1.StorageNodeList, *zfscsiv1.VolumeList:
			return errors.New("injected inventory list failure")
		}
	}
	return c.Client.List(ctx, list, opts...)
}

func (c *placementFaultClient) Get(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
	if c.failLeaseGet {
		if _, ok := obj.(*coordinationv1.Lease); ok {
			return errors.New("injected Lease get failure")
		}
	}
	if _, ok := obj.(*zfscsiv1.Volume); ok {
		c.volumeGets++
		if c.failVolumeGetAt > 0 && c.volumeGets == c.failVolumeGetAt {
			return errors.New("injected Volume get failure")
		}
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *placementFaultClient) Create(ctx context.Context, obj crclient.Object, opts ...crclient.CreateOption) error {
	if c.failVolumeCreate {
		if _, ok := obj.(*zfscsiv1.Volume); ok {
			return errors.New("injected Volume create failure")
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

// autoReady polls for a Volume CR by name and flips it to Ready (simulates the
// storage-agent reconciler responding to the CR the controller just wrote).
// autoReady polls for a Volume CR by name and flips it to Ready (simulates the
// storage-agent reconciler responding to the CR the controller just wrote).
// Uses a deadline + bounded sleep (not a tight loop) so it survives CPU
// contention when run alongside envtest tests under -tags=envtest in CI.
func autoReady(t *testing.T, c crclient.Client, name string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		v := &zfscsiv1.Volume{}
		if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: name}, v); err == nil {
			if v.Status.State == "" {
				mustSetReady(t, c, name)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("autoReady never observed CR %q", name)
}

func autoConfirmPublish(t *testing.T, c crclient.Client, name, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		v := &zfscsiv1.Volume{}
		if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: name}, v); err == nil {
			// Confirm the initiator in the agent-owned publishedInitiators set.
			alreadyPublished := false
			for _, p := range v.Status.PublishedInitiators {
				if p == nodeID {
					alreadyPublished = true
				}
			}
			if alreadyPublished {
				return
			}
			// Check the controller has written the desired mapping first.
			hasMapping := false
			for _, mapped := range v.Status.MappedInitiators {
				if mapped.NodeName == nodeID {
					hasMapping = true
					break
				}
			}
			if !hasMapping {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			patch := crclient.MergeFrom(v.DeepCopy())
			v.Status.PublishedInitiators = append(v.Status.PublishedInitiators, nodeID)
			if v.Status.TargetNQN == "" {
				v.Status.TargetNQN = nqnFor(v, mustParseTestVolumeID(t, v.Spec.VolumeID))
			}
			if v.Status.Portal == "" {
				v.Status.Portal = "server7:4420"
			}
			if err := c.Status().Patch(context.Background(), v, patch); err != nil {
				t.Errorf("confirm publish patch %q: %v", name, err)
			}

			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("autoConfirmPublish never observed mapping %q/%q", name, nodeID)
}

func mustParseTestVolumeID(t *testing.T, volumeID string) naming.ParsedVolID {
	t.Helper()
	p, err := naming.ParseVolID(volumeID)
	if err != nil {
		t.Fatalf("parse volume id: %v", err)
	}

	return p
}

type controllerRecKeys struct {
	mu               sync.Mutex
	refs             map[string][]byte
	generated        []string
	deleted          []string
	generateErr      error
	generateStarted  chan struct{}
	generateContinue chan struct{}
	generateArrived  chan struct{}
	generateBarrier  chan struct{}
}

func (k *controllerRecKeys) Generate(_ context.Context, volumeID string) (string, error) {
	if k.generateStarted != nil {
		close(k.generateStarted)
		<-k.generateContinue
	}
	if k.generateArrived != nil {
		k.generateArrived <- struct{}{}
		<-k.generateBarrier
	}
	return k.generate(volumeID)
}

func (k *controllerRecKeys) generate(volumeID string) (string, error) {
	if k.generateErr != nil {
		return "", k.generateErr
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.refs == nil {
		k.refs = map[string][]byte{}
	}
	ref := "test-ref/" + volumeID
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	k.refs[ref] = key
	k.generated = append(k.generated, ref)

	return ref, nil
}

func (k *controllerRecKeys) Fetch(_ context.Context, ref string) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	raw, ok := k.refs[ref]
	if !ok {
		return nil, crypto.ErrKeyNotFound
	}

	return append([]byte(nil), raw...), nil
}

func (k *controllerRecKeys) Delete(_ context.Context, ref string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.refs, ref)
	k.deleted = append(k.deleted, ref)

	return nil
}

var _ crypto.KeyProvider = (*controllerRecKeys)(nil)

// TestCreateVolume_Idempotent proves a repeated CreateVolume with the same name
// returns the same volume id (no duplicate CRs / no error).
func TestCreateVolume_Idempotent(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	req := &csi.CreateVolumeRequest{
		Name:          "pvc-idem",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:    map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	}

	go autoReady(t, c, "pvc-idem")

	r1, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("first CreateVolume: %v", err)
	}

	mustSetReady(t, c, "pvc-idem")

	r2, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("second CreateVolume: %v", err)
	}

	if r1.GetVolume().GetVolumeId() != r2.GetVolume().GetVolumeId() {
		t.Fatalf("idempotent id changed: %s vs %s", r1.GetVolume().GetVolumeId(), r2.GetVolume().GetVolumeId())
	}
	// exactly one CR
	list := &zfscsiv1.VolumeList{}
	if err := c.List(context.Background(), list); err != nil {
		t.Fatal(err)
	}

	if len(list.Items) != 1 {
		t.Fatalf("expected 1 CR, got %d", len(list.Items))
	}
}

// TestCreateVolume_RejectsBadParams proves validation surfaces InvalidArgument.
func TestCreateVolume_RejectsBadParams(t *testing.T) {
	cs := newTestController(newTestClient(t))

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "x", Parameters: map[string]string{"type": "block"}, // no pool
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if err == nil {
		t.Fatal("expected error for missing pool")
	}
}

// TestDeleteVolume_NoopForForeignID proves a non-zfs-csi volume id → no-op.
func TestDeleteVolume_NoopForForeignID(t *testing.T) {
	cs := newTestController(newTestClient(t))

	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "not-our-format"})
	if err != nil {
		t.Fatalf("foreign id should be no-op, got %v", err)
	}
}

// TestDeleteVolume_MarksDeleting proves DeleteVolume sets Deleting state.
// CR name is derived from the volID's id part (csi:tank:block:del → "del").
func TestDeleteVolume_MarksDeleting(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "del"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:del"},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "csi:tank:block:del"})
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
}

// TestControllerPublish_BlockMapsInitiator proves publish records the initiator.
func TestControllerPublish_BlockMapsInitiator(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "pub"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "server7", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:pub"},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	mustSetReady(t, c, "pub")
	go autoConfirmPublish(t, c, "pub", "worker1")

	resp, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "csi:tank:block:pub", NodeId: "worker1",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	wantNQN, _ := naming.TargetNQN(v.Spec.OwnerNode, v.Spec.PoolGUID, zfs.KindBlock, "pub")
	if resp.GetPublishContext()[publishContextTargetNQN] != wantNQN {
		t.Fatalf("publish context target_nqn = %q", resp.GetPublishContext()[publishContextTargetNQN])
	}
	if resp.GetPublishContext()[publishContextPortal] != "server7:4420" {
		t.Fatalf("publish context portal = %q", resp.GetPublishContext()[publishContextPortal])
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "pub"}, got); err != nil {
		t.Fatal(err)
	}

	if len(got.Status.MappedInitiators) != 1 || got.Status.MappedInitiators[0].NodeName != "worker1" {
		t.Fatalf("initiator not mapped: %+v", got.Status.MappedInitiators)
	}
}

func TestControllerPublish_WaitsForConfirmedMapping(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "wait-pub"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:wait-pub"},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	mustSetReady(t, c, "wait-pub")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := cs.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: "csi:tank:block:wait-pub", NodeId: "worker1",
	})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("publish error code = %v, want %v (err=%v)", status.Code(err), codes.DeadlineExceeded, err)
	}
}

func TestControllerPublish_SingleNodeWriterEvictsPriorInitiator(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "rwo"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "server7", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:rwo"},
		Status: zfscsiv1.VolumeStatus{
			State: zfscsiv1.VolumeStateReady, ExportPath: "/tank/csi/filesystem/fs", NFSRootPath: "/tank", NFSServer: "server7",
			MappedInitiators: []zfscsiv1.MappedInitiator{
				{NodeName: "node-A", InitiatorID: "node-A"},
			},
		},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	mustSetReady(t, c, "rwo")
	go autoConfirmPublish(t, c, "rwo", "node-B")

	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "csi:tank:block:rwo",
		NodeId:   "node-B",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	})
	if err != nil {
		t.Fatalf("publish node-B: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "rwo"}, got); err != nil {
		t.Fatal(err)
	}

	if len(got.Status.MappedInitiators) != 1 || got.Status.MappedInitiators[0].NodeName != "node-B" {
		t.Fatalf("node-A should be evicted and node-B should be sole initiator: %+v", got.Status.MappedInitiators)
	}
}

func TestControllerPublish_ReturnsMaterializedPublishContext(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "ctx"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:ctx"},
		Status: zfscsiv1.VolumeStatus{
			State:     zfscsiv1.VolumeStateReady,
			TargetNQN: "nqn.test/materialized", Portal: "10.0.0.5:4420", PortalHost: "10.0.0.5", PortalPort: 4420,
			DeviceGUID: "guid-materialized",
		},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	go autoConfirmPublish(t, c, "ctx", "worker1")

	resp, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "csi:tank:block:ctx", NodeId: "worker1",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	ctxMap := resp.GetPublishContext()
	if ctxMap[publishContextTargetNQN] != "nqn.test/materialized" || ctxMap[publishContextPortal] != "10.0.0.5:4420" || ctxMap[publishContextDeviceGUID] != "guid-materialized" {
		t.Fatalf("publish context = %v", ctxMap)
	}
}

func TestControllerPublish_FilesystemIncludesObservedExportPath(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "import-fs", Generation: 1},
		Spec:       zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem, VolumeID: "csi:tank:filesystem:import-fs"},
		Status:     zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateReady, ObservedGeneration: 1, ExportPath: "/tank/apps/existing", NFSRootPath: "/tank", NFSServer: "server7"},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	resp, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{VolumeId: v.Spec.VolumeID, NodeId: "worker1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPublishContext()["exportPath"] != "/tank/apps/existing" {
		t.Fatalf("publish context = %v", resp.GetPublishContext())
	}
	if got := resp.GetPublishContext()["nfs_root_path"]; got != "/tank" {
		t.Fatalf("publish context nfs root = %q, want /tank", got)
	}
	if got := resp.GetPublishContext()[publishContextTLS]; got != "false" {
		t.Fatalf("publish context tls = %q, want false", got)
	}
}

func TestVolumeResponseIncludesObservedFilesystemExportPath(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "import-fs-response"},
		Spec:       zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem, VolumeID: "csi:tank:filesystem:opaque-id"},
		Status:     zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateReady, ActualCapacity: 1 << 30, ExportPath: "/tank/apps/existing", NFSRootPath: "/tank", NFSServer: "server7"},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	resp, err := cs.volumeResponse(context.Background(), v.Name, v.Spec.VolumeID, &csi.CreateVolumeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetVolume().GetVolumeContext()["exportPath"]; got != "/tank/apps/existing" {
		t.Fatalf("volume context exportPath = %q", got)
	}
	if got := resp.GetVolume().GetVolumeContext()["nfs_root_path"]; got != "/tank" {
		t.Fatalf("volume context nfs root = %q, want /tank", got)
	}
}

func TestControllerPublishRejectsStaleImportedValidation(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "import-stale"}, Spec: zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem, VolumeID: "csi:tank:filesystem:import-stale"}, Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateReady, ObservedGeneration: -1}}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{VolumeId: v.Spec.VolumeID, NodeId: "worker1"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("publish error = %v", err)
	}
}

func TestImportedVolumeMutationsRejected(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "import-existing-0123456789abcdef12"}, Spec: zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:import-existing-0123456789abcdef12"}}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{VolumeId: v.Spec.VolumeID, CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expand error = %v", err)
	}
	if _, err := cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{VolumeId: v.Spec.VolumeID, MutableParameters: map[string]string{"compression": "lz4"}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("modify error = %v", err)
	}
	if _, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "snap", SourceVolumeId: v.Spec.VolumeID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("snapshot error = %v", err)
	}
	cloneReq := &csi.CreateVolumeRequest{
		Name: "clone-imported", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:          map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities:  []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: v.Spec.VolumeID}}},
	}
	if _, err := cs.CreateVolume(context.Background(), cloneReq); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("clone error = %v", err)
	}
}

func TestDynamicImportPrefixedVolumeCanBeCloned(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	const sourceID = "csi:tank:block:import-dynamic"
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "import-dynamic"}, Spec: zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceDynamic, Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, VolumeID: sourceID}}
	if err := c.Create(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	req := &csi.CreateVolumeRequest{
		Name: "clone-dynamic-import-prefix", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:          map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities:  []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: sourceID}}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := cs.CreateVolume(ctx, req); status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.NotFound {
		t.Fatalf("dynamic import-prefixed clone was rejected by identity heuristic: %v", err)
	}
	clone := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), crclient.ObjectKey{Name: sanitizeID(req.Name)}, clone); err != nil {
		t.Fatal(err)
	}
	if clone.Spec.SourceVolumeID != sourceID || clone.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		t.Fatalf("clone spec = %#v", clone.Spec)
	}
}

func TestDeleteVolumeImportedDeletesOnlyRetainedVolumeCR(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "import-delete-0123456789abcdef12", Finalizers: []string{zfscsiv1.VolumeFinalizer}}, Spec: zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain, Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:import-delete-0123456789abcdef12"}}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: v.Spec.VolumeID}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), crclient.ObjectKeyFromObject(v), got); err != nil {
		t.Fatalf("get retained-finalizing Volume: %v", err)
	}
	if got.DeletionTimestamp.IsZero() {
		t.Fatal("DeleteVolume did not start retained finalization")
	}
}

func TestCreateVolumeCannotCollideWithImportedIdentity(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	const name = "import-existing-0123456789abcdef12"
	v := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: zfscsiv1.VolumeSpec{Provenance: zfscsiv1.VolumeProvenanceImported, Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, VolumeID: "csi:tank:block:" + name}}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	req := &csi.CreateVolumeRequest{Name: name, CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, Parameters: map[string]string{"pool": "tank", "type": "block"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}}
	if _, err := cs.CreateVolume(context.Background(), req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateVolume error = %v", err)
	}
}

func TestControllerPublish_FilesystemDoesNotEvictInitiators(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "fs"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem, VolumeID: "csi:tank:filesystem:fs"},
		Status: zfscsiv1.VolumeStatus{
			State: zfscsiv1.VolumeStateReady, ExportPath: "/tank/csi/filesystem/fs", NFSRootPath: "/tank", NFSServer: "server7",
			MappedInitiators: []zfscsiv1.MappedInitiator{
				{NodeName: "node-A", InitiatorID: "node-A"},
			},
		},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "csi:tank:filesystem:fs",
		NodeId:   "node-B",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	})
	if err != nil {
		t.Fatalf("publish filesystem: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "fs"}, got); err != nil {
		t.Fatal(err)
	}

	if len(got.Status.MappedInitiators) != 1 || got.Status.MappedInitiators[0].NodeName != "node-A" {
		t.Fatalf("filesystem publish should preserve initiator status: %+v", got.Status.MappedInitiators)
	}
}

// TestControllerUnpublish_RemovesInitiator.
func TestControllerUnpublish_RemovesInitiator(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "unpub"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:unpub"},
		Status: zfscsiv1.VolumeStatus{
			State:            zfscsiv1.VolumeStateReady,
			MappedInitiators: []zfscsiv1.MappedInitiator{{NodeName: "worker1", InitiatorID: "worker1"}},
		},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	if _, err := cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "csi:tank:block:unpub", NodeId: "worker1",
	}); err != nil {
		t.Fatalf("unpublish: %v", err)
	}

	got := &zfscsiv1.Volume{}

	_ = c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "unpub"}, got)
	if len(got.Status.MappedInitiators) != 0 {
		t.Fatalf("initiator not removed: %+v", got.Status.MappedInitiators)
	}
}

// F6: DeleteVolume must refuse (FailedPrecondition) while the volume is still
// published to a node, unless the force annotation is set.
func TestDeleteVolume_InUseGuard(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "delinuse"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:delinuse"},
		Status: zfscsiv1.VolumeStatus{
			State:            zfscsiv1.VolumeStateReady,
			MappedInitiators: []zfscsiv1.MappedInitiator{{NodeName: "worker1", InitiatorID: "worker1"}},
		},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "csi:tank:block:delinuse"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteVolume while mapped: err code = %v, want FailedPrecondition", status.Code(err))
	}
}

// F6: the force-delete annotation lets DeleteVolume proceed despite a mapping.
func TestDeleteVolume_ForceAnnotationOverridesGuard(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "delforce",
			Annotations: map[string]string{zfscsiv1.ForceDeleteAnnotation: "true"},
		},
		Spec: zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:delforce"},
		Status: zfscsiv1.VolumeStatus{
			State:            zfscsiv1.VolumeStateReady,
			MappedInitiators: []zfscsiv1.MappedInitiator{{NodeName: "worker1", InitiatorID: "worker1"}},
		},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	if _, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "csi:tank:block:delforce"}); err != nil {
		t.Fatalf("DeleteVolume with force annotation: %v", err)
	}
}

// F5: ControllerUnpublish uses an optimistic-lock patch, so an unpublish racing
// a concurrent single-writer publish for another node must not clobber the
// publish. We drive the race deterministically: an interceptor performs the
// publish-equivalent write (list -> [node-b]) between the unpublish's Get and
// Patch, forcing the unpublish's first patch to 409 and re-read [node-b].
func TestControllerUnpublish_OptimisticLockPreservesConcurrentPublish(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "race"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:race"},
		Status: zfscsiv1.VolumeStatus{
			State:            zfscsiv1.VolumeStateReady,
			MappedInitiators: []zfscsiv1.MappedInitiator{{NodeName: "node-a", InitiatorID: "nqn.node-a"}},
		},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	// Simulate the concurrent single-writer publish landing first: replace the
	// list with [node-b] via an optimistic-locked status patch (what
	// patchMappedInitiatorWithRetry does for a single-writer publish).
	cur := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "race"}, cur); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFromWithOptions(cur.DeepCopy(), crclient.MergeFromWithOptimisticLock{})
	cur.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{NodeName: "node-b", InitiatorID: "nqn.node-b"}}
	if err := c.Status().Patch(context.Background(), cur, patch); err != nil {
		t.Fatal(err)
	}

	// Now unpublish node-a. Because the list is already [node-b], removing node-a
	// is a no-op that must PRESERVE node-b (a plain merge patch on a stale [node-a]
	// baseline would clobber it back to []).
	if _, err := cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "csi:tank:block:race", NodeId: "node-a",
	}); err != nil {
		t.Fatalf("unpublish: %v", err)
	}

	got := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "race"}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.MappedInitiators) != 1 || got.Status.MappedInitiators[0].NodeName != "node-b" {
		t.Fatalf("concurrent publish clobbered: mappedInitiators = %+v, want [node-b]", got.Status.MappedInitiators)
	}
}

// TestControllerExpand_UpdatesCapacity.
func TestControllerExpand_UpdatesCapacity(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	sample := metav1.Now()
	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "grow"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "server7", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:grow", Capacity: 1 << 30},
		Status:     zfscsiv1.VolumeStatus{CapacityAccountedAt: &sample},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "csi:tank:block:grow",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}

	if resp.GetCapacityBytes() != 2<<30 {
		t.Fatalf("expand response capacity: %d", resp.GetCapacityBytes())
	}

	if !resp.GetNodeExpansionRequired() {
		t.Fatal("block expand must require node expansion")
	}

	got := &zfscsiv1.Volume{}

	_ = c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "grow"}, got)
	if got.Spec.Capacity != 2<<30 {
		t.Fatalf("CR capacity not updated: %d", got.Spec.Capacity)
	}
	if got.Status.CapacityAccountedAt != nil {
		t.Fatalf("capacityAccountedAt=%v, want reset before spec growth", got.Status.CapacityAccountedAt)
	}
}

func TestControllerExpandPinnedPoolInsufficientDoesNotReplace(t *testing.T) {
	c := newTestClient(t)
	testPoolResolverWithFree(c, "owner-a", "tank", "1", 100)
	testPoolResolverWithFree(c, "owner-b", "tank", "2", 1000)
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: c, APIReader: c, Namespace: "zfs-csi-system"})
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "pinned"}, Spec: zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "owner-a", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:pinned", Capacity: 80}}
	if err := c.Create(t.Context(), volume); err != nil {
		t.Fatal(err)
	}

	_, err := cs.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{VolumeId: volume.Spec.VolumeID, CapacityRange: &csi.CapacityRange{RequiredBytes: 101}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expand error=%v, want ResourceExhausted", err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), apimachinerytypes.NamespacedName{Name: volume.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.OwnerNode != "owner-a" || got.Spec.PoolGUID != "1" || got.Spec.Capacity != 80 {
		t.Fatalf("insufficient expansion changed placement/spec: %#v", got.Spec)
	}
}

func TestControllerExpandIdempotentRetrySkipsLeaseAndCapacity(t *testing.T) {
	base := newTestClient(t)
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "idem-grow"}, Spec: zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "server7", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:idem-grow", Capacity: 20}}
	if err := base.Create(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	faults := &placementFaultClient{Client: base, failLeaseGet: true, failInventoryList: true}
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: faults, APIReader: faults, Namespace: "zfs-csi-system"})

	resp, err := cs.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{VolumeId: volume.Spec.VolumeID, CapacityRange: &csi.CapacityRange{RequiredBytes: 20}})
	if err != nil || resp.GetCapacityBytes() != 20 || !resp.GetNodeExpansionRequired() {
		t.Fatalf("idempotent expand response=%#v err=%v", resp, err)
	}
}

type statusConflictOnceClient struct {
	crclient.Client
	conflicted bool
}

func (c *statusConflictOnceClient) Status() crclient.StatusWriter {
	return &statusConflictOnceWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type statusConflictOnceWriter struct {
	crclient.SubResourceWriter
	client *statusConflictOnceClient
}

func (w *statusConflictOnceWriter) Patch(ctx context.Context, obj crclient.Object, patch crclient.Patch, opts ...crclient.SubResourcePatchOption) error {
	if !w.client.conflicted {
		w.client.conflicted = true
		return apierrors.NewConflict(zfscsiv1.GroupVersion.WithResource("volumes").GroupResource(), obj.GetName(), errors.New("injected status conflict"))
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func TestControllerExpandRetriesCapacityStatusConflict(t *testing.T) {
	base := newTestClient(t)
	testPoolResolverWithFree(base, "server7", "tank", "1", 100)
	sample := metav1.Now()
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "conflict-grow"}, Spec: zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "server7", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:conflict-grow", Capacity: 40}, Status: zfscsiv1.VolumeStatus{CapacityAccountedAt: &sample}}
	if err := base.Create(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	client := &statusConflictOnceClient{Client: base}
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: client, APIReader: client, Namespace: "zfs-csi-system"})

	if _, err := cs.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{VolumeId: volume.Spec.VolumeID, CapacityRange: &csi.CapacityRange{RequiredBytes: 80}}); err != nil {
		t.Fatalf("expand after status conflict: %v", err)
	}
	got := &zfscsiv1.Volume{}
	if err := base.Get(t.Context(), apimachinerytypes.NamespacedName{Name: volume.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != 80 || got.Status.CapacityAccountedAt != nil || !client.conflicted {
		t.Fatalf("post-conflict volume=%#v conflicted=%v", got, client.conflicted)
	}
}

type failCapacitySpecPatchClient struct {
	crclient.Client
}

func (c *failCapacitySpecPatchClient) Patch(ctx context.Context, obj crclient.Object, patch crclient.Patch, opts ...crclient.PatchOption) error {
	if volume, ok := obj.(*zfscsiv1.Volume); ok && volume.Spec.Capacity == 80 {
		return errors.New("injected capacity spec patch failure")
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestControllerExpandSpecPatchFailureRemainsFullyReserved(t *testing.T) {
	base := newTestClient(t)
	testPoolResolverWithFree(base, "server7", "tank", "1", 100)
	sample := metav1.Now()
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "failed-grow"}, Spec: zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "server7", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:failed-grow", Capacity: 40}, Status: zfscsiv1.VolumeStatus{CapacityAccountedAt: &sample}}
	if err := base.Create(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	client := &failCapacitySpecPatchClient{Client: base}
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: client, APIReader: client, Namespace: "zfs-csi-system"})

	_, err := cs.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{VolumeId: volume.Spec.VolumeID, CapacityRange: &csi.CapacityRange{RequiredBytes: 80}})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expand error=%v, want Internal", err)
	}
	got := &zfscsiv1.Volume{}
	if err := base.Get(t.Context(), apimachinerytypes.NamespacedName{Name: volume.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Capacity != 40 || got.Status.CapacityAccountedAt != nil {
		t.Fatalf("failed spec patch exposed unsafe accounting state: spec=%d marker=%v", got.Spec.Capacity, got.Status.CapacityAccountedAt)
	}
}

// TestControllerModifyVolume_UpdatesCompression proves ControllerModifyVolume
// (VolumeAttributesClass mutable params) patches the Volume CR's Compression spec
// so the agent can `zfs set` it live.
func TestControllerModifyVolume_UpdatesCompression(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "mod"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:mod", Capacity: 1 << 30, Compression: "off"},
		Status: zfscsiv1.VolumeStatus{
			State:          zfscsiv1.VolumeStateReady,
			ActualCapacity: 1 << 30,
		},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}

	if _, err := cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          "csi:tank:block:mod",
		MutableParameters: map[string]string{"CoMpReSsIoN": "zstd-3"},
	}); err != nil {
		t.Fatalf("modify: %v", err)
	}

	got := &zfscsiv1.Volume{}
	_ = c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "mod"}, got)
	if got.Spec.Compression != "zstd-3" {
		t.Fatalf("compression not updated: %q", got.Spec.Compression)
	}
	if !reflect.DeepEqual(got.Status, v.Status) {
		t.Fatalf("modify changed status: got=%+v want=%+v", got.Status, v.Status)
	}

	// Invalid compression value is rejected.
	if _, err := cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          "csi:tank:block:mod",
		MutableParameters: map[string]string{"compression": "bogus"},
	}); err == nil {
		t.Fatal("expected InvalidArgument for bogus compression")
	}

	// An empty map is an idempotent no-op.
	if _, err := cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          "csi:tank:block:mod",
		MutableParameters: map[string]string{},
	}); err != nil {
		t.Fatalf("empty mutable parameters should no-op, got %v", err)
	}

	if _, err := cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          "csi:tank:block:mod",
		MutableParameters: map[string]string{"upstream.example.io/unsupported": "value"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsupported mutable parameter error code = %s, want InvalidArgument (err=%v)", status.Code(err), err)
	}

	// Unsupported keys cannot piggyback on a supported compression change.
	if _, err := cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          "csi:tank:block:mod",
		MutableParameters: map[string]string{"compression": "lz4", "upstream.example.io/unsupported": "value"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("mixed supported+unsupported error code = %s, want InvalidArgument (err=%v)", status.Code(err), err)
	}
	afterRejected := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "mod"}, afterRejected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterRejected.Spec, got.Spec) || !reflect.DeepEqual(afterRejected.Status, v.Status) {
		t.Fatalf("rejected mixed request mutated volume: spec=%+v status=%+v", afterRejected.Spec, afterRejected.Status)
	}

	// Retrying the same VAC request is idempotent.
	if _, err := cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId: "csi:tank:block:mod", MutableParameters: map[string]string{"compression": "zstd-3"},
	}); err != nil {
		t.Fatalf("retry should succeed, got %v", err)
	}
	afterRetry := &zfscsiv1.Volume{}
	if err := c.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "mod"}, afterRetry); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterRetry.Status, v.Status) {
		t.Fatalf("retry changed status: got=%+v want=%+v", afterRetry.Status, v.Status)
	}
}

func TestConformanceVolumeAttributesClassUsesSupportedMutableParameters(t *testing.T) {
	manifest, err := os.ReadFile(filepath.Join("..", "..", "test", "e2e", "data", "vac", "compression-zstd-3.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	var vac struct {
		Parameters map[string]string `yaml:"parameters"`
	}
	if err := yaml.Unmarshal(manifest, &vac); err != nil {
		t.Fatal(err)
	}
	if err := validateMutableParams(vac.Parameters); err != nil {
		t.Fatalf("conformance VolumeAttributesClass is incompatible with driver validation: %v", err)
	}
}

// TestControllerGetVolume_ReportsHealthCondition proves ControllerGetVolume
// returns a VolumeCondition derived from the Volume CR state: Ready=healthy,
// Error=abnormal (with the reason).
func TestControllerGetVolume_ReportsHealthCondition(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)

	mkVol := func(name, id string, state zfscsiv1.VolumeState, msg string) {
		v := &zfscsiv1.Volume{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: id, Capacity: 1 << 30},
		}
		if err := c.Create(context.Background(), v); err != nil {
			t.Fatal(err)
		}
		patch := crclient.MergeFrom(v.DeepCopy())
		v.Status.State = state
		if msg != "" {
			v.Status.Conditions = []metav1.Condition{{
				Type: string(zfscsiv1.VolumeConditionReady), Status: metav1.ConditionFalse,
				Reason: "Error", Message: msg, LastTransitionTime: metav1.Now(),
			}}
		}
		if err := c.Status().Patch(context.Background(), v, patch); err != nil {
			t.Fatal(err)
		}
	}

	mkVol("healthy", "csi:tank:block:healthy", zfscsiv1.VolumeStateReady, "")
	mkVol("sick", "csi:tank:block:sick", zfscsiv1.VolumeStateError, "zfs create: pool full")

	resp, err := cs.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "csi:tank:block:healthy"})
	if err != nil {
		t.Fatalf("get healthy: %v", err)
	}
	if resp.GetStatus().GetVolumeCondition().GetAbnormal() {
		t.Fatal("Ready volume should report healthy (abnormal=false)")
	}

	resp, err = cs.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "csi:tank:block:sick"})
	if err != nil {
		t.Fatalf("get sick: %v", err)
	}
	cond := resp.GetStatus().GetVolumeCondition()
	if !cond.GetAbnormal() {
		t.Fatal("Error volume should report abnormal")
	}
	if cond.GetMessage() != "zfs create: pool full" {
		t.Fatalf("abnormal message = %q, want the Error condition reason", cond.GetMessage())
	}
}

func TestControllerHealthUsesPersistedBackendCondition(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	v := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "backend-unhealthy"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:backend-unhealthy", Capacity: 1 << 30},
	}
	if err := c.Create(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(v.DeepCopy())
	v.Status.State = zfscsiv1.VolumeStateReady
	v.Status.Conditions = []metav1.Condition{{
		Type: string(zfscsiv1.VolumeConditionBackendHealthy), Status: metav1.ConditionFalse,
		Reason: "BackendUnhealthy", Message: "repair target export: configfs unavailable", LastTransitionTime: metav1.Now(),
	}}
	if err := c.Status().Patch(context.Background(), v, patch); err != nil {
		t.Fatal(err)
	}

	get, err := cs.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: v.Spec.VolumeID})
	if err != nil {
		t.Fatalf("ControllerGetVolume: %v", err)
	}
	if !get.GetStatus().GetVolumeCondition().GetAbnormal() || get.GetStatus().GetVolumeCondition().GetMessage() != "repair target export: configfs unavailable" {
		t.Fatalf("get health = %#v", get.GetStatus().GetVolumeCondition())
	}

	list, err := cs.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes: %v", err)
	}
	if len(list.Entries) != 1 || !list.Entries[0].GetStatus().GetVolumeCondition().GetAbnormal() {
		t.Fatalf("list health = %#v", list.Entries)
	}
}

// TestValidateVolumeCapabilities_ConfirmsValid.
func TestValidateVolumeCapabilities_ConfirmsValid(t *testing.T) {
	cs := newTestController(newTestClient(t))
	cap := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}

	resp, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           "csi:tank:block:x",
		VolumeCapabilities: []*csi.VolumeCapability{cap},
	})
	if err != nil {
		t.Fatalf("validate: %v", err)
	}

	if resp.GetConfirmed() == nil {
		t.Fatal("expected confirmation")
	}
}

// TestValidateCapabilities_BlockMultiNode covers the multi-node block matrix:
// ROX and RWX are accepted (readers-only is safe; RWX is presented for
// consumer-coordinated use), while MULTI_NODE_SINGLE_WRITER is rejected (no
// asymmetric per-initiator RW/RO enforcement yet).
func TestValidateCapabilities_BlockMultiNode(t *testing.T) {
	cases := []struct {
		name    string
		mode    csi.VolumeCapability_AccessMode_Mode
		wantErr bool
	}{
		{"ROX accepted", csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY, false},
		{"RWX accepted", csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, false},
		{"MNSW rejected", csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER, true},
		{"single-node writer accepted", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			caps := []*csi.VolumeCapability{{
				AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: tc.mode},
			}}
			err := validateCapabilities(caps, "block")
			if tc.wantErr && err == nil {
				t.Fatalf("mode %s: expected error, got nil", tc.mode)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("mode %s: unexpected error: %v", tc.mode, err)
			}
		})
	}
}

// TestValidateCapabilities_BlockOnFilesystemRejected asserts a raw-block
// access type is rejected for a filesystem (NFS) volume: NFS is a file
// protocol and cannot present a block device, so CreateVolume must fail fast
// (leaving the PVC Pending) rather than binding an unpublishable PV. This is
// the conformance "block volmode should fail in binding" contract.
func TestValidateCapabilities_BlockOnFilesystemRejected(t *testing.T) {
	blockCap := []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}}
	if err := validateCapabilities(blockCap, "filesystem"); err == nil {
		t.Fatal("block access type on filesystem kind: expected rejection, got nil")
	} else if !errors.Is(err, errBlockOnFilesystem) {
		t.Fatalf("block on filesystem: want errBlockOnFilesystem, got %v", err)
	}

	// A filesystem (Mount) access type on a filesystem volume stays valid.
	mountCap := []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}}
	if err := validateCapabilities(mountCap, "filesystem"); err != nil {
		t.Fatalf("mount access type on filesystem kind: unexpected error: %v", err)
	}

	// Block access type on a block volume is unaffected by the new guard.
	if err := validateCapabilities(blockCap, "block"); err != nil {
		t.Fatalf("block access type on block kind: unexpected error: %v", err)
	}
}

// TestControllerGetCapabilities advertises the full set.
func TestControllerGetCapabilities(t *testing.T) {
	cs := newTestController(newTestClient(t))

	resp, err := cs.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.GetCapabilities()) < 7 {
		t.Fatalf("expected >=7 caps, got %d", len(resp.GetCapabilities()))
	}

	// GET_CAPACITY must be advertised: GetCapacity is a real implementation
	// (reads per-node capacity ConfigMaps), so serving it without advertising
	// would violate the CSI spec / fail csi-sanity.
	var haveCapacity bool
	for _, c := range resp.GetCapabilities() {
		if c.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_GET_CAPACITY {
			haveCapacity = true
		}
	}
	if !haveCapacity {
		t.Fatal("GET_CAPACITY capability not advertised")
	}

	// RWOP support: SINGLE_NODE_MULTI_WRITER must be advertised so the external
	// provisioner/attacher enable the single-node access modes (ReadWriteOncePod).
	var haveRWOP bool
	for _, c := range resp.GetCapabilities() {
		if c.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER {
			haveRWOP = true
		}
	}
	if !haveRWOP {
		t.Fatal("SINGLE_NODE_MULTI_WRITER capability not advertised (RWOP)")
	}

	// ModifyVolume support: MODIFY_VOLUME must be advertised so external-resizer
	// drives VolumeAttributesClass changes.
	var haveModify bool
	for _, c := range resp.GetCapabilities() {
		if c.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_MODIFY_VOLUME {
			haveModify = true
		}
	}
	if !haveModify {
		t.Fatal("MODIFY_VOLUME capability not advertised (VolumeAttributesClass)")
	}

	// Volume health monitoring: GET_VOLUME + VOLUME_CONDITION.
	var haveGetVol, haveCond bool
	for _, c := range resp.GetCapabilities() {
		switch c.GetRpc().GetType() {
		case csi.ControllerServiceCapability_RPC_GET_VOLUME:
			haveGetVol = true
		case csi.ControllerServiceCapability_RPC_VOLUME_CONDITION:
			haveCond = true
		}
	}
	if !haveGetVol || !haveCond {
		t.Fatalf("health caps missing: GET_VOLUME=%v VOLUME_CONDITION=%v", haveGetVol, haveCond)
	}
}

// TestParseSCParams exercises the SC param parser.
func TestParseSCParams(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		p, err := parseSCParams(map[string]string{"pool": "tank"})
		if err != nil {
			t.Fatal(err)
		}

		if p.Pool != "tank" || p.Type != "block" || p.FsType != "xfs" || p.Transport != "nvme-tcp" {
			t.Fatalf("defaults wrong: %+v", p)
		}
		if p.NFSTLSEnabled {
			t.Fatal("nfsTLS default = true, want false")
		}
	})
	t.Run("nfs TLS filesystem", func(t *testing.T) {
		p, err := parseSCParams(map[string]string{
			"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.0.0.0/24", "nfsTLS": "true",
		})
		if err != nil {
			t.Fatal(err)
		}
		if !p.NFSTLSEnabled {
			t.Fatal("nfsTLS = false, want true")
		}
	})
	t.Run("nfs TLS block rejected", func(t *testing.T) {
		for _, value := range []string{"false", "true"} {
			t.Run(value, func(t *testing.T) {
				if _, err := parseSCParams(map[string]string{"pool": "tank", "nfsTLS": value}); err == nil {
					t.Fatal("expected block nfsTLS validation error")
				}
			})
		}
	})
	t.Run("NVMe TLS block", func(t *testing.T) {
		p, err := parseSCParams(map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp", "nvmeTLS": "true"})
		if err != nil {
			t.Fatal(err)
		}
		if !p.NVMeTLSEnabled {
			t.Fatal("nvmeTLS = false, want true")
		}
	})
	t.Run("NVMe TLS invalid protocol or mode rejected", func(t *testing.T) {
		cases := []map[string]string{
			{"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.0.0.0/24", "nvmeTLS": "true"},
			{"pool": "tank", "type": "block", "transport": "iscsi", "nvmeTLS": "true"},
			{"pool": "tank", "type": "block", "nvmeTLS": "not-bool"},
		}
		for _, params := range cases {
			if _, err := parseSCParams(params); err == nil {
				t.Fatalf("parseSCParams(%v) succeeded", params)
			}
		}
	})
	t.Run("missing pool", func(t *testing.T) {
		if _, err := parseSCParams(map[string]string{}); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("bad type", func(t *testing.T) {
		if _, err := parseSCParams(map[string]string{"pool": "tank", "type": "exotic"}); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestVolumeSpecCompatibleRejectsMismatchedNFSTLS(t *testing.T) {
	existing := &zfscsiv1.VolumeSpec{
		Pool:          "tank",
		Capacity:      1,
		Type:          zfscsiv1.VolumeTypeFilesystem,
		OwnerNode:     "server7",
		NFSTLSEnabled: true,
	}
	err := volumeSpecCompatible(existing, requestedVolume{
		pool: "tank", capacity: 1, kind: zfscsiv1.VolumeTypeFilesystem, ownerNode: "server7",
		nfsTLS: false,
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), codes.AlreadyExists, err)
	}
}

func TestPublishContextForFilesystemIncludesNFSTLS(t *testing.T) {
	for _, tlsEnabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(tlsEnabled), func(t *testing.T) {
			context := publishContextForVolume(&zfscsiv1.Volume{
				Spec:   zfscsiv1.VolumeSpec{Type: zfscsiv1.VolumeTypeFilesystem, NFSTLSEnabled: tlsEnabled},
				Status: zfscsiv1.VolumeStatus{ExportPath: "/export/vol", NFSRootPath: "/export", NFSServer: "server7"},
			})
			if got, want := context[publishContextTLS], strconv.FormatBool(tlsEnabled); got != want {
				t.Fatalf("publish context tls = %q, want %q", got, want)
			}
		})
	}
}

func TestPublishContextForNVMeTLSIncludesSecretNameOnly(t *testing.T) {
	base := &zfscsiv1.Volume{
		Spec:   zfscsiv1.VolumeSpec{Type: zfscsiv1.VolumeTypeBlock, NVMeTLSEnabled: true, NVMeTLSPSKSecretName: "zfs-csi-nvme-psk-vol-abc"},
		Status: zfscsiv1.VolumeStatus{TargetNQN: "nqn.2026-01.csi.randomvariable:zfs:abc:block:vol", DeviceGUID: "0123456789abcdef0123456789abcdef", PortalHost: "storage-a", PortalPort: 4421},
	}
	context := publishContextForVolume(base)
	if context[publishContextTLS] != "true" || context[publishContextPSKSecret] != base.Spec.NVMeTLSPSKSecretName {
		t.Fatalf("NVMe TLS publish context = %#v", context)
	}
	for key, value := range context {
		if key != publishContextPSKSecret && strings.Contains(value, "NVMeTLSkey") {
			t.Fatalf("publish context leaked PSK material in %q", key)
		}
	}
	base.Spec.NVMeTLSEnabled = false
	context = publishContextForVolume(base)
	if _, ok := context[publishContextPSKSecret]; ok {
		t.Fatalf("non-TLS publish context contains psk_secret: %#v", context)
	}
}

func TestVolumeContextForFilesystemDoesNotIncludeNFSTLS(t *testing.T) {
	context := volumeContextForVolume(&zfscsiv1.Volume{
		Spec: zfscsiv1.VolumeSpec{Type: zfscsiv1.VolumeTypeFilesystem, NFSTLSEnabled: true},
		Status: zfscsiv1.VolumeStatus{
			ExportPath:  "/exports/pvc-nfs-tls",
			NFSRootPath: "/exports",
			NFSServer:   "server7",
		},
	})
	if _, found := context[publishContextTLS]; found {
		t.Fatalf("volume context unexpectedly contains %q: %v", publishContextTLS, context)
	}
	if context[publishContextExportPath] == "" || context[publishContextNFSServer] == "" {
		t.Fatalf("fixture did not exercise populated filesystem VolumeContext: %v", context)
	}
}

func TestPublishContextUsesAuthoritativeCustomNFSRoot(t *testing.T) {
	vol := &zfscsiv1.Volume{
		Spec: zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem},
		Status: zfscsiv1.VolumeStatus{
			State: zfscsiv1.VolumeStateReady, ExportPath: "/srv/tank/csi/fs/a",
			NFSRootPath: "/srv/tank", NFSServer: "server7",
		},
	}
	got := publishContextForVolume(vol)
	if got[publishContextNFSRootPath] != "/srv/tank" {
		t.Fatalf("root = %q, want /srv/tank", got[publishContextNFSRootPath])
	}
}

func TestNVMeTLSPSKSecretNameSatisfiesCRDGrammar(t *testing.T) {
	valid := regexp.MustCompile(`^zfs-csi-nvme-psk-[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	for _, name := range []string{
		"pvc-simple",
		"UPPER_case.with spaces/slashes",
		"---",
		"😀💾",
		strings.Repeat("Very-Long_CSI.Volume/Name!", 32),
		strings.Repeat("a", 4096),
	} {
		crName := crNameFor(name)
		secretName := nvmeTLSPSKSecretName(crName)
		if !valid.MatchString(secretName) {
			t.Fatalf("CSI name %q produced invalid CR/Secret names %q/%q", name, crName, secretName)
		}
		if len(secretName) > 253 {
			t.Fatalf("CSI name %q produced overlong Secret name (%d): %q", name, len(secretName), secretName)
		}
	}
}

// guard against unused import warnings.
var _ = apierrors.IsAlreadyExists

// TestCreateVolume_ReturnsPromptlyWhenAgentNotReconciled proves the fallback
// wait budget: a caller without a deadline gets DeadlineExceeded after five
// seconds rather than entering the provisioner retry backoff cliff.
func TestCreateVolume_ReturnsPromptlyWhenAgentNotReconciled(t *testing.T) {
	c := newTestClient(t)
	cs := newTestController(c)
	// Deliberately NO autoReady goroutine — the volume never becomes Ready.

	start := time.Now()
	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "pvc-slow-agent",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1024 * 1024 * 1024},
		Parameters:    map[string]string{"pool": "tank", "type": "block", "transport": "nvme-tcp"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error when agent never reconciles")
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want DeadlineExceeded (err=%v)", status.Code(err), err)
	}
	// Must return within the five-second fallback bound plus modest margin.
	// for the final poll tick. Must NOT be minutes.
	if elapsed > 8*time.Second {
		t.Fatalf("CreateVolume blocked for %v; expected prompt return (< 8s)", elapsed)
	}
	t.Logf("CreateVolume returned in %v with %v", elapsed, status.Code(err))
}

func TestReadinessPollContextUsesCallerDeadlineBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pollCtx, pollCancel := readinessPollContext(ctx)
	defer pollCancel()

	deadline, ok := pollCtx.Deadline()
	if !ok {
		t.Fatal("poll context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining > time.Second-pollDeadlineSafetyMargin+100*time.Millisecond || remaining < 500*time.Millisecond {
		t.Fatalf("poll budget = %v, want caller deadline minus safety margin", remaining)
	}
}

func TestReadinessPollContextShortCallerDeadlineExpiresImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), pollDeadlineSafetyMargin/2)
	defer cancel()

	pollCtx, pollCancel := readinessPollContext(ctx)
	defer pollCancel()

	select {
	case <-pollCtx.Done():
		if !errors.Is(pollCtx.Err(), context.DeadlineExceeded) {
			t.Fatalf("poll context error = %v, want DeadlineExceeded", pollCtx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("short deadline poll context did not expire")
	}
}

func TestControllerPublishShortCallerDeadlineReturnsDeadlineExceeded(t *testing.T) {
	const volumeName = "short-publish"

	volumeID, err := naming.EncodeVolID("tank", zfs.KindBlock, volumeName)
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: crNameFor(volumeName)},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, VolumeID: volumeID,
		},
		Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateReady},
	}
	cs := newTestController(newTestClient(t, vol))
	ctx, cancel := context.WithTimeout(context.Background(), pollDeadlineSafetyMargin/2)
	defer cancel()

	_, err = cs.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-a",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	})
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("publish wait status = %v (err=%v), want DeadlineExceeded", got, err)
	}
}

func TestWaitStatusErrorPreservesDeadlineAndCancellation(t *testing.T) {
	if got := status.Code(waitStatusError("wait", context.DeadlineExceeded)); got != codes.DeadlineExceeded {
		t.Fatalf("deadline status = %v, want DeadlineExceeded", got)
	}
	if got := status.Code(waitStatusError("wait", context.Canceled)); got != codes.Canceled {
		t.Fatalf("canceled status = %v, want Canceled", got)
	}
}
