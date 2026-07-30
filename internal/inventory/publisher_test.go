// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package inventory

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func TestEligibleFreshnessBoundaries(t *testing.T) {
	now := time.Unix(1000, 0)
	ready := func(observed time.Time) *zfscsiv1.StorageNode {
		return &zfscsiv1.StorageNode{ObjectMeta: metav1.ObjectMeta{Generation: 2}, Spec: zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: []string{"1"}, NetworkDomain: "workers"}, Status: zfscsiv1.StorageNodeStatus{
			ObservedGeneration: 2, LastObservedTime: &metav1.Time{Time: observed},
			Conditions: []metav1.Condition{{Type: zfscsiv1.StorageNodeConditionReady, Status: metav1.ConditionTrue}},
		}}
	}
	for name, tc := range map[string]struct {
		observed time.Time
		want     bool
	}{
		"exact timeout":    {now.Add(-FreshnessTimeout), true},
		"past timeout":     {now.Add(-FreshnessTimeout - time.Nanosecond), false},
		"future tolerated": {now.Add(MaxFutureSkew), true},
		"future stale":     {now.Add(MaxFutureSkew + time.Nanosecond), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := Eligible(ready(tc.observed), now); got != tc.want {
				t.Fatalf("Eligible=%v want %v", got, tc.want)
			}
		})
	}
}

func TestPublisherPublishesCompleteInventory(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	uid := types.UID("uid-a")
	node := readyNode("node-a", uid)
	sn := storageNode("node-a", []string{"10", "20"}, true)
	c := testClient(t, node, sn)
	z := zfsfake.New().WithPoolIdentity("tank", 42, "20", "ONLINE").WithPoolIdentity("archive", 7, "10", "ONLINE")
	p := testPublisher(c, z, "node-a")
	p.Now = func() time.Time { return now }
	if err := p.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.StorageNode{}
	if err := c.Get(ctx, types.NamespacedName{Name: sn.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.NetworkDomain != "workers" || len(got.Status.Pools) != 2 {
		t.Fatalf("spec/status=%#v/%#v", got.Spec, got.Status)
	}
	if got.Status.Pools[0].GUID != "10" || !got.Status.Pools[0].Ready || got.Status.Pools[1].GUID != "20" || !got.Status.Pools[1].Ready {
		t.Fatalf("pools=%#v", got.Status.Pools)
	}
	for _, pool := range got.Status.Pools {
		if !pool.CapacityObservedAt.Time.Equal(now) {
			t.Fatalf("pool %s capacityObservedAt=%s, want %s", pool.GUID, pool.CapacityObservedAt.Time, now)
		}
	}
	if !Eligible(got, now) {
		t.Fatal("fresh successful inventory not eligible")
	}
}

func TestPublisherReadsExpectedOwnerNodeThroughUncachedReader(t *testing.T) {
	ctx := t.Context()
	sn := storageNode("node-a", []string{"1"}, true)
	writeClient := testClient(t, readyNode("node-a", "stale-cache"), sn)
	readerClient := testClient(t, readyNode("node-a", "api-reader"))
	reader := &countingReader{Reader: readerClient}
	p := testPublisher(writeClient, zfsfake.New().WithPoolIdentity("tank", 1, "1", "ONLINE"), "node-a")
	p.NodeReader = reader

	if err := p.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if reader.gets != 1 || len(reader.keys) != 1 || reader.keys[0].Name != "node-a" {
		t.Fatalf("Node API-reader GETs = %d keys=%v, want exactly one GET for node-a", reader.gets, reader.keys)
	}
}

func TestPublisherNodeReaderFailureFailsClosed(t *testing.T) {
	sn := storageNode("node-a", []string{"1"}, true)
	c := testClient(t, readyNode("node-a", "cached"), sn)
	reader := &countingReader{Reader: testClient(t)}
	p := testPublisher(c, zfsfake.New().WithPoolIdentity("tank", 1, "1", "ONLINE"), "node-a")
	p.NodeReader = reader

	if err := p.Publish(t.Context()); err == nil || !strings.Contains(err.Error(), "Kubernetes Node does not exist") {
		t.Fatalf("Publish error = %v, want missing uncached Node failure", err)
	}
	if reader.gets != 1 {
		t.Fatalf("Node API-reader GETs = %d, want 1", reader.gets)
	}
}

func TestPublisherRequiresUncachedNodeReader(t *testing.T) {
	sn := storageNode("node-a", []string{"1"}, true)
	c := testClient(t, readyNode("node-a", "cached"), sn)
	p := testPublisher(c, zfsfake.New().WithPoolIdentity("tank", 1, "1", "ONLINE"), "node-a")
	p.NodeReader = nil

	if err := p.Publish(t.Context()); err == nil || !strings.Contains(err.Error(), "uncached Kubernetes Node reader is not configured") {
		t.Fatalf("Publish error = %v, want missing uncached reader failure", err)
	}
}

func TestPublisherCanonicalizesReachableFrom(t *testing.T) {
	ctx := t.Context()
	sn := storageNode("node-a", []string{"1"}, true)
	c := testClient(t, readyNode("node-a", "uid"), sn)
	p := testPublisher(c, zfsfake.New().WithPoolIdentity("tank", 1, "1", "ONLINE"), "node-a")
	p.ReachableFrom = []string{"workers-b", "workers", "workers-b"}
	if err := p.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.StorageNode{}
	if err := c.Get(ctx, types.NamespacedName{Name: sn.Name}, got); err != nil {
		t.Fatal(err)
	}
	want := []string{"workers", "workers-b"}
	if !reflect.DeepEqual(got.Status.ReachableFrom, want) {
		t.Fatalf("reachableFrom = %v, want %v", got.Status.ReachableFrom, want)
	}
}

func TestPublisherRejectsInvalidReachableFrom(t *testing.T) {
	for _, test := range []struct {
		name      string
		domains   []string
		wantError string
	}{
		{name: "empty", domains: nil, wantError: "non-empty"},
		{name: "invalid", domains: []string{"bad/value", "workers"}, wantError: "invalid"},
		{name: "missing owner domain", domains: []string{"workers-b"}, wantError: "must include"},
	} {
		t.Run(test.name, func(t *testing.T) {
			sn := storageNode("node-a", []string{"1"}, true)
			c := testClient(t, readyNode("node-a", "uid"), sn)
			p := testPublisher(c, zfsfake.New().WithPoolIdentity("tank", 1, "1", "ONLINE"), "node-a")
			p.ReachableFrom = test.domains
			if err := p.Publish(t.Context()); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Publish error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestPublisherStampsReadyVolumeAfterPoolObservation(t *testing.T) {
	ctx := t.Context()
	now := time.Unix(1000, 0)
	sn := storageNode("node-a", []string{"1"}, true)
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "ready"}, Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", Pool: "tank", PoolGUID: "1", Capacity: 10}, Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateReady, ActualCapacity: 10}}
	c := testClient(t, readyNode("node-a", "uid"), sn, volume)
	p := testPublisher(c, zfsfake.New().WithPoolIdentity("tank", 90, "1", "ONLINE"), "node-a")
	p.Now = func() time.Time { return now }
	if err := p.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(ctx, types.NamespacedName{Name: volume.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CapacityAccountedAt == nil || !got.Status.CapacityAccountedAt.Time.Equal(now) {
		t.Fatalf("capacityAccountedAt=%v, want %s", got.Status.CapacityAccountedAt, now)
	}
}

func TestPublisherDoesNotAccountVolumeMaterializedDuringSample(t *testing.T) {
	enabled := true
	node := readyNode("node-a", "uid")
	storage := storageNode("node-a", []string{"1"}, enabled)
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "raced"}, Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", Pool: "tank", PoolGUID: "1", Capacity: 10}}
	c := testClient(t, node, storage, volume)
	publisher := testPublisher(c, zfsfake.New().WithPoolIdentity("tank", 100, "1", "ONLINE"), "node-a")
	if err := publisher.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volume.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CapacityAccountedAt != nil {
		t.Fatalf("capacityAccountedAt=%v, volume was not materialized before sample", got.Status.CapacityAccountedAt)
	}
}

func TestPublisherDoesNotStampVolumeExpandedDuringSample(t *testing.T) {
	ctx := t.Context()
	now := time.Unix(1000, 0)
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "grow"}, Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", Pool: "tank", PoolGUID: "1", Capacity: 20}, Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateReady}}
	c := testClient(t, volume)
	p := &Publisher{Client: c, NodeName: "node-a"}
	pools := []zfscsiv1.StorageNodePoolStatus{{GUID: "1", CapacityObservedAt: metav1.NewTime(now)}}

	if err := p.StampCapacityAccounted(ctx, pools, map[string]int64{volume.Name: 10}); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.Volume{}
	if err := c.Get(ctx, types.NamespacedName{Name: volume.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.CapacityAccountedAt != nil {
		t.Fatalf("capacityAccountedAt=%v, grown spec was not represented by pool sample", got.Status.CapacityAccountedAt)
	}
}

func TestPublisherDoesNotAccountGrowthBeforeBackendCapacity(t *testing.T) {
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "growing"}, Spec: zfscsiv1.VolumeSpec{OwnerNode: "node-a", Pool: "tank", PoolGUID: "1", Capacity: 20}, Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateReady, ActualCapacity: 10}}
	c := testClient(t, volume)
	p := &Publisher{Client: c, NodeName: "node-a"}

	materialized, err := p.materializedVolumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := materialized[volume.Name]; ok {
		t.Fatal("backend smaller than grown spec was eligible for capacity stamp")
	}
}

func TestPublisherNodeRecreationWithNewUIDAndMatchingPoolsSucceeds(t *testing.T) {
	ctx := context.Background()
	sn := storageNode("node-a", []string{"1"}, true)
	c := testClient(t, readyNode("node-a", "actual"), sn)
	p := testPublisher(c, zfsfake.New().WithPoolIdentity("tank", 1, "1", "ONLINE"), "node-a")
	if err := p.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	got := &zfscsiv1.StorageNode{}
	if err := c.Get(ctx, types.NamespacedName{Name: sn.Name}, got); err != nil {
		t.Fatal(err)
	}
	if !Eligible(got, time.Now()) {
		t.Fatalf("matching stable pool identity not eligible after Node recreation: %#v", got.Status)
	}
}

func TestPublisherSameLogicalNameDifferentPoolIdentityFailsClosed(t *testing.T) {
	ctx := context.Background()
	sn := storageNode("node-a", []string{"1"}, true)
	c := testClient(t, readyNode("node-a", "replacement-node-uid"), sn)
	p := &Publisher{Client: c, NodeReader: c, ZFS: zfsfake.New().WithPoolIdentity("tank", 1, "2", "ONLINE"), NodeName: "node-a", Log: logr.Discard()}
	if err := p.Publish(ctx); err == nil {
		t.Fatal("expected stable pool identity mismatch")
	}
	got := &zfscsiv1.StorageNode{}
	if err := c.Get(ctx, types.NamespacedName{Name: "node-a"}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Pools) != 0 || Eligible(got, time.Now()) {
		t.Fatalf("mismatched host published placement inventory: %#v", got.Status)
	}
}

func TestPublisherExtraOrMissingPoolFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name          string
		authoritative []string
		backend       *zfsfake.Backend
	}{
		{name: "extra", authoritative: []string{"1"}, backend: zfsfake.New().WithPoolIdentity("tank", 1, "1", "ONLINE").WithPoolIdentity("extra", 1, "2", "ONLINE")},
		{name: "missing", authoritative: []string{"1", "2"}, backend: zfsfake.New().WithPoolIdentity("tank", 1, "1", "ONLINE")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := testClient(t, readyNode("node-a", "uid"), storageNode("node-a", tc.authoritative, true))
			p := &Publisher{Client: c, NodeReader: c, ZFS: tc.backend, NodeName: "node-a", Log: logr.Discard()}
			if err := p.Publish(ctx); err == nil {
				t.Fatal("expected complete identity mismatch")
			}
			got := &zfscsiv1.StorageNode{}
			if err := c.Get(ctx, types.NamespacedName{Name: "node-a"}, got); err != nil {
				t.Fatal(err)
			}
			if len(got.Status.Pools) != 0 {
				t.Fatalf("partial identity published: %#v", got.Status.Pools)
			}
		})
	}
}

func TestPublisherMissingStorageNodeDoesNotCreate(t *testing.T) {
	c := testClient(t, readyNode("node-a", "uid"))
	p := &Publisher{Client: c, ZFS: zfsfake.New().WithPool("tank", 1), NodeName: "node-a", Log: logr.Discard()}
	if err := p.Publish(context.Background()); err == nil {
		t.Fatal("expected missing StorageNode")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &zfscsiv1.StorageNode{}); err == nil {
		t.Fatal("publisher created StorageNode")
	}
}

func TestPublisherDiscoveryFailureClearsEntireInventory(t *testing.T) {
	sn := storageNode("node-a", []string{"1"}, true)
	sn.Status.Pools = []zfscsiv1.StorageNodePoolStatus{{GUID: "1", Name: "old", FreeBytes: 1, Ready: true}}
	c := testClient(t, readyNode("node-a", "uid"), sn)
	z := zfsfake.New().WithPoolIdentity("tank", 1, "0", "ONLINE")
	p := &Publisher{Client: c, NodeReader: c, ZFS: z, NodeName: "node-a", Log: logr.Discard()}
	if err := p.Publish(context.Background()); err == nil {
		t.Fatal("expected discovery failure")
	}
	got := &zfscsiv1.StorageNode{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "node-a"}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Pools) != 0 || len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Status != metav1.ConditionFalse {
		t.Fatalf("status=%#v", got.Status)
	}
}

func TestPublisherFailureClearsPoolsAndPreservesSpec(t *testing.T) {
	for _, tc := range []struct {
		name    string
		node    *corev1.Node
		enabled bool
	}{
		{name: "disabled", node: readyNode("node-a", "uid"), enabled: false},
		{name: "node not ready", node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "uid"}}, enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sn := storageNode("node-a", []string{"1"}, tc.enabled)
			sn.Status.Pools = []zfscsiv1.StorageNodePoolStatus{{GUID: "1", Name: "old", FreeBytes: 1, Ready: true}}
			c := testClient(t, tc.node, sn)
			p := &Publisher{Client: c, NodeReader: c, ZFS: zfsfake.New().WithPool("tank", 1), NodeName: "node-a", Log: logr.Discard()}
			if err := p.Publish(context.Background()); err == nil {
				t.Fatal("expected not ready")
			}
			got := &zfscsiv1.StorageNode{}
			if err := c.Get(context.Background(), types.NamespacedName{Name: "node-a"}, got); err != nil {
				t.Fatal(err)
			}
			if len(got.Status.Pools) != 0 || got.Spec.NetworkDomain != "workers" {
				t.Fatalf("got=%#v", got)
			}
		})
	}
}

func testClient(t *testing.T, objects ...runtime.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = zfscsiv1.AddToScheme(s)
	objs := make([]runtime.Object, 0, len(objects))
	objs = append(objs, objects...)
	c := ctrlfake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objs...).WithStatusSubresource(&zfscsiv1.StorageNode{}, &zfscsiv1.Volume{}).Build()
	return c
}

type countingReader struct {
	client.Reader
	gets int
	keys []client.ObjectKey
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.gets++
	r.keys = append(r.keys, key)
	return r.Reader.Get(ctx, key, obj, opts...)
}

func readyNode(name string, uid types.UID) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
}
func storageNode(name string, poolGUIDs []string, enabled bool) *zfscsiv1.StorageNode {
	return &zfscsiv1.StorageNode{ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 1}, Spec: zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: poolGUIDs, Enabled: &enabled, NetworkDomain: "workers"}}
}

func testPublisher(client client.Client, backend *zfsfake.Backend, name string) *Publisher {
	return &Publisher{Client: client, NodeReader: client, ZFS: backend, NodeName: name, Log: logr.Discard(), ReachableFrom: []string{"workers"}, Endpoints: []zfscsiv1.StorageNodeEndpoint{{Protocol: zfscsiv1.StorageProtocolNFS, Host: name, Port: 2049}, {Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: name, Port: 4420}}}
}
