// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package inventory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/reachability"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const (
	PublishInterval  = 30 * time.Second
	FreshnessTimeout = 90 * time.Second
	MaxFutureSkew    = 30 * time.Second
)

// NodeReader is the complete Kubernetes Node API surface needed by Publisher.
// Production wiring must use an uncached reader so nodes/get RBAC is sufficient.
type NodeReader interface {
	Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
}

// Publisher writes complete StorageNode observations for exactly one Node.
type Publisher struct {
	Client        client.Client
	NodeReader    NodeReader
	ZFS           zfs.Backend
	NodeName      string
	Interval      time.Duration
	Log           logr.Logger
	Now           func() time.Time
	ReachableFrom []string
	Endpoints     []zfscsiv1.StorageNodeEndpoint
}

// StampCapacityAccounted marks materialized owner Volumes with the same sample
// identifier as the newly published pool observation. It runs only after the
// pool status write; interruption therefore leaves reservations conservatively
// double-counted until a later complete sample.
func (p *Publisher) StampCapacityAccounted(ctx context.Context, pools []zfscsiv1.StorageNodePoolStatus, materialized map[string]int64) error {
	volumes := &zfscsiv1.VolumeList{}
	if err := p.Client.List(ctx, volumes); err != nil {
		return fmt.Errorf("list Volumes for capacity accounting: %w", err)
	}
	samples := make(map[string]metav1.Time, len(pools))
	for _, pool := range pools {
		samples[pool.GUID] = pool.CapacityObservedAt
	}
	for i := range volumes.Items {
		volume := &volumes.Items[i]
		observedCapacity, existedBeforeSample := materialized[volume.Name]
		if !existedBeforeSample || volume.Spec.OwnerNode != p.NodeName || volume.Spec.Capacity != observedCapacity {
			continue
		}
		sample, ok := samples[volume.Spec.PoolGUID]
		if !ok || sample.IsZero() {
			continue
		}
		key := client.ObjectKeyFromObject(volume)
		if err := wait.ExponentialBackoffWithContext(ctx, wait.Backoff{Duration: 10 * time.Millisecond, Factor: 2, Steps: 6, Cap: 200 * time.Millisecond}, func(ctx context.Context) (bool, error) {
			current := &zfscsiv1.Volume{}
			if err := p.Client.Get(ctx, key, current); err != nil {
				return false, err
			}
			if current.Spec.Capacity != observedCapacity || current.Spec.OwnerNode != p.NodeName || current.Spec.PoolGUID != volume.Spec.PoolGUID {
				return true, nil
			}
			before := current.DeepCopy()
			current.Status.CapacityAccountedAt = sample.DeepCopy()
			if err := p.Client.Status().Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				if apierrors.IsConflict(err) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		}); err != nil {
			return fmt.Errorf("stamp Volume %q capacity accounting: %w", volume.Name, err)
		}
	}
	return nil
}

func (p *Publisher) materializedVolumes(ctx context.Context) (map[string]int64, error) {
	volumes := &zfscsiv1.VolumeList{}
	if err := p.Client.List(ctx, volumes); err != nil {
		return nil, fmt.Errorf("list Volumes before capacity sample: %w", err)
	}
	materialized := make(map[string]int64)
	for i := range volumes.Items {
		volume := &volumes.Items[i]
		if volume.Spec.OwnerNode == p.NodeName && volume.Status.State == zfscsiv1.VolumeStateReady && volume.Status.ActualCapacity >= volume.Spec.Capacity {
			materialized[volume.Name] = volume.Spec.Capacity
		}
	}
	return materialized, nil
}

func (p *Publisher) Start(ctx context.Context) error {
	interval := p.Interval
	if interval <= 0 {
		interval = PublishInterval
	}
	p.publishAndLog(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.publishAndLog(ctx)
		}
	}
}

func (p *Publisher) publishAndLog(ctx context.Context) {
	if err := p.Publish(ctx); err != nil && ctx.Err() == nil {
		p.Log.Error(err, "publish StorageNode inventory", "node", p.NodeName)
	}
}

// Publish performs one complete observation. Missing StorageNode and UID
// mismatch never write status because operator intent cannot be fabricated or
// trusted, respectively.
func (p *Publisher) Publish(ctx context.Context) error {
	storageNode := &zfscsiv1.StorageNode{}
	if err := p.Client.Get(ctx, types.NamespacedName{Name: p.NodeName}, storageNode); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("StorageNode %q not found", p.NodeName)
		}
		return fmt.Errorf("get StorageNode %q: %w", p.NodeName, err)
	}
	if p.NodeReader == nil {
		return p.writeFailure(ctx, storageNode, "NodeReaderMissing", "uncached Kubernetes Node reader is not configured")
	}
	node := &corev1.Node{}
	if err := p.NodeReader.Get(ctx, types.NamespacedName{Name: p.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return p.writeFailure(ctx, storageNode, "NodeNotFound", "Kubernetes Node does not exist")
		}
		return p.writeFailure(ctx, storageNode, "NodeReadFailed", err.Error())
	}
	if err := zfscsiv1.ValidateAuthoritativePoolGUIDs(storageNode.Spec.AuthoritativePoolGUIDs); err != nil {
		return p.writeFailure(ctx, storageNode, "InvalidAuthoritativeIdentity", err.Error())
	}
	if !storageNode.IsEnabled() {
		return p.writeFailure(ctx, storageNode, "Disabled", "StorageNode is disabled")
	}
	if !nodeReady(node) {
		return p.writeFailure(ctx, storageNode, "NodeNotReady", "Kubernetes Node is not Ready")
	}
	reachableFrom, err := reachability.CanonicalReachableFrom(storageNode.Spec.NetworkDomain, p.ReachableFrom)
	if err != nil {
		return p.writeFailure(ctx, storageNode, "ReachabilityInvalid", err.Error())
	}
	storageNode.Status.ReachableFrom = reachableFrom
	storageNode.Status.Endpoints = append([]zfscsiv1.StorageNodeEndpoint(nil), p.Endpoints...)
	if err := reachability.ValidateStorageNodeEndpoints(storageNode); err != nil {
		return p.writeFailure(ctx, storageNode, "EndpointInvalid", err.Error())
	}
	materialized, err := p.materializedVolumes(ctx)
	if err != nil {
		return p.writeFailure(ctx, storageNode, "CapacityAccountingFailed", err.Error())
	}
	pools, err := p.discover(ctx)
	if err != nil {
		return p.writeFailure(ctx, storageNode, "DiscoveryFailed", err.Error())
	}
	if err := verifyAuthoritativeIdentity(storageNode.Spec.AuthoritativePoolGUIDs, pools); err != nil {
		return p.writeFailure(ctx, storageNode, "PoolIdentityMismatch", err.Error())
	}
	now := time.Now()
	if p.Now != nil {
		now = p.Now()
	}
	for i := range pools {
		pools[i].CapacityObservedAt = metav1.NewTime(now)
	}
	if err := p.writeStatus(ctx, storageNode, pools, metav1.ConditionTrue, "ObservationSucceeded", "storage inventory observed"); err != nil {
		return err
	}
	return p.StampCapacityAccounted(ctx, pools, materialized)
}

func verifyAuthoritativeIdentity(authoritative []string, pools []zfscsiv1.StorageNodePoolStatus) error {
	observed := make(map[string]struct{}, len(pools))
	for _, pool := range pools {
		if !pool.Ready {
			return fmt.Errorf("authoritative identity cannot be established: pool %q (GUID %s) is not ONLINE", pool.Name, pool.GUID)
		}
		observed[pool.GUID] = struct{}{}
	}
	if len(observed) != len(authoritative) {
		return fmt.Errorf("complete ONLINE pool GUID set does not match authoritativePoolGUIDs")
	}
	for _, guid := range authoritative {
		if _, exists := observed[guid]; !exists {
			return fmt.Errorf("complete ONLINE pool GUID set does not match authoritativePoolGUIDs")
		}
	}
	return nil
}

func nodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (p *Publisher) discover(ctx context.Context) ([]zfscsiv1.StorageNodePoolStatus, error) {
	names, err := p.ZFS.PoolNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("list pools: %w", err)
	}
	pools := make([]zfscsiv1.StorageNodePoolStatus, 0, len(names))
	seen := map[string]struct{}{}
	for _, name := range names {
		guid, err := p.ZFS.PoolGUID(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("read pool %q GUID: %w", name, err)
		}
		parsed, err := strconv.ParseUint(guid, 10, 64)
		if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != guid {
			return nil, fmt.Errorf("pool %q has invalid canonical GUID %q", name, guid)
		}
		if _, duplicate := seen[guid]; duplicate {
			return nil, fmt.Errorf("duplicate pool GUID %q", guid)
		}
		seen[guid] = struct{}{}
		health, err := p.ZFS.PoolHealth(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("read pool %q health: %w", name, err)
		}
		free, err := p.ZFS.PoolFreeBytes(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("read pool %q free bytes: %w", name, err)
		}
		if free < 0 {
			return nil, fmt.Errorf("pool %q has negative free bytes", name)
		}
		pools = append(pools, zfscsiv1.StorageNodePoolStatus{GUID: guid, Name: name, FreeBytes: free, Ready: strings.EqualFold(health, "ONLINE")})
	}
	sort.Slice(pools, func(i, j int) bool {
		left, _ := strconv.ParseUint(pools[i].GUID, 10, 64)
		right, _ := strconv.ParseUint(pools[j].GUID, 10, 64)
		return left < right
	})
	return pools, nil
}

func (p *Publisher) writeFailure(ctx context.Context, node *zfscsiv1.StorageNode, reason, message string) error {
	if err := p.writeStatus(ctx, node, nil, metav1.ConditionFalse, reason, message); err != nil {
		return err
	}
	return fmt.Errorf("StorageNode %q not ready: %s", node.Name, message)
}

func (p *Publisher) writeStatus(ctx context.Context, original *zfscsiv1.StorageNode, pools []zfscsiv1.StorageNodePoolStatus, conditionStatus metav1.ConditionStatus, reason, message string) error {
	now := time.Now()
	if p.Now != nil {
		now = p.Now()
	}
	var reachableFrom []string
	if canonical, err := reachability.CanonicalReachableFrom(original.Spec.NetworkDomain, p.ReachableFrom); err == nil {
		reachableFrom = canonical
	}
	return wait.ExponentialBackoffWithContext(ctx, wait.Backoff{Duration: 10 * time.Millisecond, Factor: 2, Steps: 5}, func(ctx context.Context) (bool, error) {
		current := &zfscsiv1.StorageNode{}
		if err := p.Client.Get(ctx, types.NamespacedName{Name: original.Name}, current); err != nil {
			return false, err
		}
		if !stringSetsEqual(current.Spec.AuthoritativePoolGUIDs, original.Spec.AuthoritativePoolGUIDs) {
			return false, fmt.Errorf("StorageNode authoritativePoolGUIDs changed while publishing")
		}
		current.Status = zfscsiv1.StorageNodeStatus{
			ObservedGeneration: current.Generation,
			LastObservedTime:   &metav1.Time{Time: now},
			ReachableFrom:      append([]string(nil), reachableFrom...),
			Endpoints:          append([]zfscsiv1.StorageNodeEndpoint(nil), p.Endpoints...),
			Pools:              pools,
			Conditions:         []metav1.Condition{{Type: zfscsiv1.StorageNodeConditionReady, Status: conditionStatus, Reason: reason, Message: message, ObservedGeneration: current.Generation, LastTransitionTime: metav1.NewTime(now)}},
		}
		if err := p.Client.Status().Update(ctx, current); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
}

func stringSetsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	set := make(map[string]struct{}, len(left))
	for _, value := range left {
		set[value] = struct{}{}
	}
	for _, value := range right {
		if _, exists := set[value]; !exists {
			return false
		}
	}
	return true
}

// Eligible reports whether inventory is complete, current and enabled.
func Eligible(node *zfscsiv1.StorageNode, now time.Time) bool {
	if node == nil || !node.IsEnabled() || node.Status.ObservedGeneration != node.Generation || node.Status.LastObservedTime == nil {
		return false
	}
	if !meta.IsStatusConditionTrue(node.Status.Conditions, zfscsiv1.StorageNodeConditionReady) {
		return false
	}
	age := now.Sub(node.Status.LastObservedTime.Time)
	if age < 0 {
		if -age > MaxFutureSkew {
			return false
		}
		age = 0
	}
	return age <= FreshnessTimeout
}
