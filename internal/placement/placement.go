// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

// Package placement selects storage ownership from durable inventory and
// CR-backed capacity reservations.
package placement

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/inventory"
	"github.com/randomvariable/zfs-csi/internal/reachability"
)

// Candidate is one eligible owner-qualified pool and its remaining capacity.
type Candidate struct {
	OwnerNode     string
	PoolName      string
	PoolGUID      string
	Available     int64
	NetworkDomain string
	ReachableFrom []string
	Endpoints     []zfscsiv1.StorageNodeEndpoint
}

// CapacityRequest describes a placement reservation. ExistingCapacity is the
// selected volume's current reservation when checking an in-place expansion.
type CapacityRequest struct {
	RequestedCapacity int64
	ExistingCapacity  int64
}

func (r CapacityRequest) growth() (int64, error) {
	if r.RequestedCapacity < 0 || r.ExistingCapacity < 0 || r.RequestedCapacity < r.ExistingCapacity {
		return 0, fmt.Errorf("invalid capacity request: requested=%d existing=%d", r.RequestedCapacity, r.ExistingCapacity)
	}
	return r.RequestedCapacity - r.ExistingCapacity, nil
}

// Select deterministically chooses maximum post-reservation free capacity,
// then owner name and numeric pool GUID as stable tie-breakers.
func Select(nodes []zfscsiv1.StorageNode, volumes []zfscsiv1.Volume, poolName string, requested int64, now time.Time, pinnedOwner, pinnedGUID string, domainOrder ...string) (Candidate, error) {
	return SelectCapacity(nodes, volumes, poolName, CapacityRequest{RequestedCapacity: requested}, now, pinnedOwner, pinnedGUID, domainOrder...)
}

// SelectCapacity applies reservation accounting for create or in-place growth.
// Expansion replaces its volume's old unaccounted reservation with the new full
// reservation, so only growth delta is required from currently available space.
func SelectCapacity(nodes []zfscsiv1.StorageNode, volumes []zfscsiv1.Volume, poolName string, request CapacityRequest, now time.Time, pinnedOwner, pinnedGUID string, domainOrder ...string) (Candidate, error) {
	growth, err := request.growth()
	if err != nil {
		return Candidate{}, err
	}
	candidates := make([]Candidate, 0)
	for i := range nodes {
		node := &nodes[i]
		if !inventory.Eligible(node, now) || (pinnedOwner != "" && node.Name != pinnedOwner) {
			continue
		}
		if err := reachability.ValidateStorageNodeEndpoints(node); err != nil {
			continue
		}
		selectedDomain := firstReachableDomain(node.Status.ReachableFrom, domainOrder)
		if len(domainOrder) > 0 && selectedDomain == "" {
			continue
		}
		for _, pool := range node.Status.Pools {
			if pool.Name != poolName || !pool.Ready || pool.CapacityObservedAt.IsZero() || (pinnedGUID != "" && pool.GUID != pinnedGUID) || !authoritative(node.Spec.AuthoritativePoolGUIDs, pool.GUID) {
				continue
			}
			if _, err := canonicalGUID(pool.GUID); err != nil {
				continue
			}
			available := pool.FreeBytes
			for j := range volumes {
				volume := &volumes[j]
				if volume.Spec.OwnerNode != node.Name || volume.Spec.PoolGUID != pool.GUID || terminal(volume.Status.State) {
					continue
				}
				if volume.Status.CapacityAccountedAt == nil || !volume.Status.CapacityAccountedAt.Time.Equal(pool.CapacityObservedAt.Time) {
					available = reserve(available, volume.Spec.Capacity)
				}
			}
			additional := growth
			if available >= additional {
				candidates = append(candidates, Candidate{OwnerNode: node.Name, PoolName: pool.Name, PoolGUID: pool.GUID, Available: available - additional, NetworkDomain: selectedDomain, ReachableFrom: slices.Clone(node.Status.ReachableFrom), Endpoints: slices.Clone(node.Status.Endpoints)})
			}
		}
	}
	if len(candidates) == 0 {
		return Candidate{}, fmt.Errorf("no fresh eligible pool %q has capacity for %d requested bytes (%d growth bytes)", poolName, request.RequestedCapacity, growth)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if len(domainOrder) > 0 {
			left := slices.Index(domainOrder, candidates[i].NetworkDomain)
			right := slices.Index(domainOrder, candidates[j].NetworkDomain)
			if left != right {
				return left < right
			}
		}
		if candidates[i].Available != candidates[j].Available {
			return candidates[i].Available > candidates[j].Available
		}
		if candidates[i].OwnerNode != candidates[j].OwnerNode {
			return candidates[i].OwnerNode < candidates[j].OwnerNode
		}
		left, _ := canonicalGUID(candidates[i].PoolGUID)
		right, _ := canonicalGUID(candidates[j].PoolGUID)
		return left < right
	})
	return candidates[0], nil
}

func reserve(available, capacity int64) int64 {
	if available < 0 || capacity < 0 || capacity > available {
		return -1
	}
	return available - capacity
}

func firstReachableDomain(reachable, order []string) string {
	for _, domain := range order {
		if slices.Contains(reachable, domain) {
			return domain
		}
	}
	if len(order) > 0 || len(reachable) == 0 {
		return ""
	}
	copy := slices.Clone(reachable)
	slices.Sort(copy)
	return copy[0]
}

// AccountedSample returns the pool sample marker only when the dataset is
// known materialized in the same owner observation. A marker is never inferred
// from API timestamps or from readiness alone.
func AccountedSample(node *zfscsiv1.StorageNode, ownerNode, poolGUID string, datasetExists bool) *metav1.Time {
	if node == nil || !datasetExists || node.Name != ownerNode {
		return nil
	}
	for _, pool := range node.Status.Pools {
		if pool.GUID == poolGUID && !pool.CapacityObservedAt.IsZero() {
			return pool.CapacityObservedAt.DeepCopy()
		}
	}
	return nil
}

func terminal(state zfscsiv1.VolumeState) bool { return state == zfscsiv1.VolumeStateDestroyed }

func authoritative(values []string, guid string) bool {
	for _, value := range values {
		if value == guid {
			return true
		}
	}
	return false
}

func canonicalGUID(guid string) (uint64, error) {
	value, err := strconv.ParseUint(guid, 10, 64)
	if err != nil || value == 0 || strconv.FormatUint(value, 10) != guid {
		return 0, fmt.Errorf("invalid pool GUID %q", guid)
	}
	return value, nil
}
