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

package capacity

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const (
	// ConfigMapPrefix is the name prefix for per-node capacity ConfigMaps. Each
	// storage-agent instance owns exactly one ConfigMap named
	// "<prefix><node>", so concurrent agents on different nodes never contend
	// on a single object (H2). The controller aggregates across all of them.
	ConfigMapPrefix = "zfs-csi-capacity-"

	// ManagedByLabel/ManagedByValue mark ConfigMaps the controller aggregates.
	ManagedByLabel = "zfs.csi.randomvariable.co.uk/capacity"
	ManagedByValue = "true"

	// NodeLabel records which storage node published a capacity ConfigMap.
	NodeLabel = "zfs.csi.randomvariable.co.uk/node"
)

// ConfigMapNameForNode returns the per-node capacity ConfigMap name.
func ConfigMapNameForNode(node string) string {
	return ConfigMapPrefix + node
}

// Publisher periodically writes the free bytes of every imported ZFS pool on
// its node to a per-node ConfigMap. A separate object per node means N storage
// agents never overwrite each other's data (leaderless, DaemonSet-safe).
type Publisher struct {
	Client    client.Client
	ZFS       zfs.Backend
	Namespace string
	// Node is the name of the node this agent runs on; it scopes the ConfigMap.
	Node     string
	Interval time.Duration
	Log      logr.Logger
}

func (p *Publisher) Start(ctx context.Context) error {
	interval := p.Interval
	if interval <= 0 {
		interval = 30 * time.Second
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
	if err := p.publish(ctx); err != nil && ctx.Err() == nil {
		p.Log.Error(err, "publish ZFS pool capacity")
	}
}

// publish records raw pool FREE bytes per pool. This is a COARSE scheduler gate:
// it ignores per-StorageClass overhead (compression ratio, refreservation,
// encryption, blocksize), so every SC on the same pool reports identical
// capacity. That over-estimates provisionable bytes but is the standard CSI
// contract for AvailableCapacity without MaximumVolumeSize — the scheduler uses
// it only to avoid obviously-full nodes, and CreateVolume still enforces the
// real limit. Good enough for network-attached pools; revisit if per-SC
// accounting is ever required.
func (p *Publisher) publish(ctx context.Context) error {
	pools, err := p.ZFS.PoolNames(ctx)
	if err != nil {
		return fmt.Errorf("list pools for capacity: %w", err)
	}
	data := make(map[string]string, len(pools))
	for _, pool := range pools {
		free, err := p.ZFS.PoolFreeBytes(ctx, pool)
		if err != nil {
			return fmt.Errorf("read free capacity for pool %s: %w", pool, err)
		}
		data[pool] = strconv.FormatInt(free, 10)
	}

	labels := map[string]string{
		ManagedByLabel: ManagedByValue,
		NodeLabel:      p.Node,
	}
	key := types.NamespacedName{Namespace: p.Namespace, Name: ConfigMapNameForNode(p.Node)}
	current := &corev1.ConfigMap{}
	err = p.Client.Get(ctx, key, current)
	if apierrors.IsNotFound(err) {
		return p.Client.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: labels},
			Data:       data,
		})
	}
	if err != nil {
		return fmt.Errorf("get capacity ConfigMap: %w", err)
	}
	current.Labels = labels
	current.Data = data
	if err := p.Client.Update(ctx, current); err != nil {
		return fmt.Errorf("update capacity ConfigMap: %w", err)
	}
	return nil
}
