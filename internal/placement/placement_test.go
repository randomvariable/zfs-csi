// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

func TestSelectDeterministicCapacityAndTieBreak(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	nodes := []zfscsiv1.StorageNode{
		storageNode("node-b", "2", "tank", 200, now),
		storageNode("node-a", "1", "tank", 200, now),
		storageNode("node-c", "3", "tank", 300, now),
	}
	candidate, err := Select(nodes, nil, "tank", 100, now, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.OwnerNode != "node-c" || candidate.Available != 200 {
		t.Fatalf("candidate=%#v, want node-c with 200 post-reservation", candidate)
	}
	nodes[2].Status.Pools[0].FreeBytes = 200
	candidate, err = Select(nodes, nil, "tank", 100, now, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.OwnerNode != "node-a" {
		t.Fatalf("tie candidate=%#v, want node-a", candidate)
	}
}

func TestSelectSharedDomainDeterministicAndPinnedOwner(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	nodes := []zfscsiv1.StorageNode{
		storageNode("node-b", "2", "tank", 200, now),
		storageNode("node-a", "1", "tank", 200, now),
	}
	for i := range nodes {
		nodes[i].Spec.NetworkDomain = "shared"
		nodes[i].Status.ReachableFrom = []string{"shared"}
	}

	candidate, err := Select(nodes, nil, "tank", 100, now, "", "", "shared")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.OwnerNode != "node-a" || candidate.NetworkDomain != "shared" {
		t.Fatalf("shared-domain candidate=%#v, want deterministic node-a", candidate)
	}

	candidate, err = Select(nodes, nil, "tank", 100, now, "node-b", "2", "shared")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.OwnerNode != "node-b" || candidate.PoolGUID != "2" {
		t.Fatalf("pinned shared-domain candidate=%#v, want node-b pool 2", candidate)
	}
}

func TestSelectSharedDomainReservationFallsBackToSibling(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	nodes := []zfscsiv1.StorageNode{
		storageNode("node-a", "1", "tank", 200, now),
		storageNode("node-b", "2", "tank", 180, now),
	}
	for i := range nodes {
		nodes[i].Spec.NetworkDomain = "shared"
		nodes[i].Status.ReachableFrom = []string{"shared"}
	}
	volumes := []zfscsiv1.Volume{{Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", PoolGUID: "1", Capacity: 150}}}

	candidate, err := Select(nodes, volumes, "tank", 100, now, "", "", "shared")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.OwnerNode != "node-b" || candidate.Available != 80 {
		t.Fatalf("shared-domain fallback candidate=%#v, want node-b with 80 bytes", candidate)
	}
}

func TestSelectFiltersAndReservationFormulaUsesSameClockMarker(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	sample := metav1.NewTime(now.Add(-time.Minute))
	node := storageNode("node-a", "1", "tank", 100, now)
	node.Status.Pools[0].CapacityObservedAt = sample
	accounted := sample.DeepCopy()
	otherClock := metav1.NewTime(now.Add(24 * time.Hour))
	volumes := []zfscsiv1.Volume{
		{Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", PoolGUID: "1", Capacity: 60}, Status: zfscsiv1.VolumeStatus{CapacityAccountedAt: accounted}},
		{Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", PoolGUID: "1", Capacity: 50}, Status: zfscsiv1.VolumeStatus{CapacityAccountedAt: &otherClock}},
	}
	if _, err := Select([]zfscsiv1.StorageNode{node}, volumes, "tank", 51, now, "", ""); err == nil {
		t.Fatal("stale/future marker did not reserve capacity")
	}
	if candidate, err := Select([]zfscsiv1.StorageNode{node}, volumes, "tank", 50, now, "", ""); err != nil || candidate.Available != 0 {
		t.Fatalf("candidate=%#v err=%v, want exactly 50 reserved and 50 available", candidate, err)
	}
}

func TestSelectCapacityExpansionReservationSemantics(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	sample := metav1.NewTime(now)
	node := storageNode("node-a", "1", "tank", 100, now)
	volume := zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "grow"}, Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", PoolGUID: "1", Capacity: 60}, Status: zfscsiv1.VolumeStatus{CapacityAccountedAt: sample.DeepCopy()}}
	request := CapacityRequest{RequestedCapacity: 90, ExistingCapacity: 60}

	candidate, err := SelectCapacity([]zfscsiv1.StorageNode{node}, []zfscsiv1.Volume{volume}, "tank", request, now, "node-a", "1")
	if err != nil || candidate.Available != 70 {
		t.Fatalf("accounted expansion candidate=%#v err=%v, want 70 observed-free bytes remaining", candidate, err)
	}

	volume.Status.CapacityAccountedAt = nil
	candidate, err = SelectCapacity([]zfscsiv1.StorageNode{node}, []zfscsiv1.Volume{volume}, "tank", request, now, "node-a", "1")
	if err != nil || candidate.Available != 10 {
		t.Fatalf("reserved expansion candidate=%#v err=%v, want old reservation plus growth with 10 bytes remaining", candidate, err)
	}
	if _, err := SelectCapacity([]zfscsiv1.StorageNode{node}, []zfscsiv1.Volume{volume}, "tank", CapacityRequest{RequestedCapacity: 101, ExistingCapacity: 60}, now, "node-a", "1"); err == nil {
		t.Fatal("expansion exceeded pinned pool capacity")
	}
}

func TestSelectCapacityReservationArithmeticFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	node := storageNode("node-a", "1", "tank", 10, now)
	volumes := []zfscsiv1.Volume{{Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", PoolGUID: "1", Capacity: 20}}}
	if _, err := Select([]zfscsiv1.StorageNode{node}, volumes, "tank", 0, now, "node-a", "1"); err == nil {
		t.Fatal("reservation larger than inventory wrapped into available capacity")
	}
	if _, err := SelectCapacity([]zfscsiv1.StorageNode{node}, nil, "tank", CapacityRequest{RequestedCapacity: 1, ExistingCapacity: 2}, now, "node-a", "1"); err == nil {
		t.Fatal("negative growth request accepted")
	}
}

func TestSelectRejectsStaleNotReadyInvalidAndWrongPool(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	stale := storageNode("stale", "1", "tank", 100, now.Add(-2*time.Hour))
	notReady := storageNode("not-ready", "2", "tank", 100, now)
	notReady.Status.Conditions[0].Status = metav1.ConditionFalse
	invalid := storageNode("invalid", "not-guid", "tank", 100, now)
	wrongPool := storageNode("wrong", "3", "flash", 100, now)
	if _, err := Select([]zfscsiv1.StorageNode{stale, notReady, invalid, wrongPool}, nil, "tank", 1, now, "", ""); err == nil {
		t.Fatal("ineligible inventory produced candidate")
	}
}

func TestAccountedSampleRequiresKnownMaterialization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	node := storageNode("node-a", "1", "tank", 100, now)
	if got := AccountedSample(&node, "node-a", "1", false); got != nil {
		t.Fatalf("unmaterialized dataset received marker %s", got)
	}
	if got := AccountedSample(&node, "node-a", "1", true); got == nil || !got.Time.Equal(now) {
		t.Fatalf("materialized marker=%v, want %s", got, now)
	}
}

func TestDeletingVolumeReservesUntilDestroyed(t *testing.T) {
	now := time.Now().UTC()
	node := storageNode("node-a", "1", "tank", 100, now)
	deleting := metav1.NewTime(now)
	volumes := []zfscsiv1.Volume{{
		ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &deleting, Finalizers: []string{zfscsiv1.VolumeFinalizer}},
		Spec:       zfscsiv1.VolumeSpec{OwnerNode: "node-a", PoolGUID: "1", Capacity: 90},
	}}
	if _, err := Select([]zfscsiv1.StorageNode{node}, volumes, "tank", 11, now, "", ""); err == nil {
		t.Fatal("deleting but non-Destroyed Volume did not reserve capacity")
	}
	volumes[0].Status.State = zfscsiv1.VolumeStateDestroyed
	if candidate, err := Select([]zfscsiv1.StorageNode{node}, volumes, "tank", 100, now, "", ""); err != nil || candidate.Available != 0 {
		t.Fatalf("candidate=%#v err=%v, Destroyed Volume should release reservation", candidate, err)
	}
}

func storageNode(name, guid, pool string, free int64, observed time.Time) zfscsiv1.StorageNode {
	enabled := true
	t := metav1.NewTime(observed)
	return zfscsiv1.StorageNode{ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 1}, Spec: zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: []string{guid}, Enabled: &enabled, NetworkDomain: "workers"}, Status: zfscsiv1.StorageNodeStatus{
		ObservedGeneration: 1, LastObservedTime: &t, Conditions: []metav1.Condition{{Type: zfscsiv1.StorageNodeConditionReady, Status: metav1.ConditionTrue}},
		ReachableFrom: []string{"workers"}, Endpoints: []zfscsiv1.StorageNodeEndpoint{{Protocol: zfscsiv1.StorageProtocolNFS, Host: name, Port: 2049}, {Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: name, Port: 4420}},
		Pools: []zfscsiv1.StorageNodePoolStatus{{GUID: guid, Name: pool, FreeBytes: free, Ready: true, CapacityObservedAt: t}},
	}}
}
