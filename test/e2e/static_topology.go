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

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/zfs-csi/internal/reachability"
	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

func reconcileStaticConsumerTopology(ctx context.Context, c client.Client, workers []e2econfig.ConsumerWorker) error {
	selected, domains := map[string]struct{}{}, map[string]struct{}{}
	for _, worker := range workers {
		if len(worker.NodeNames) != worker.Replicas || len(worker.NodeNames) == 0 {
			return fmt.Errorf("static consumer group %q must name all %d Nodes", worker.Name, worker.Replicas)
		}
		domains[worker.NetworkDomain] = struct{}{}
		for _, name := range worker.NodeNames {
			selected[name] = struct{}{}
		}
	}
	if len(domains) != 1 {
		return fmt.Errorf("static consumers must use exactly one network domain, got %d", len(domains))
	}
	var domain string
	for domain = range domains {
	}

	nodes := &corev1.NodeList{}
	csiNodes := &storagev1.CSINodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return fmt.Errorf("list static Nodes: %w", err)
	}
	if err := c.List(ctx, csiNodes); err != nil {
		return fmt.Errorf("list static CSINodes: %w", err)
	}
	nodeDomains := make(map[string]string, len(nodes.Items))
	for i := range nodes.Items {
		nodeDomains[nodes.Items[i].Name] = nodes.Items[i].Labels[reachability.TopologyKeyNetworkDomain]
	}
	advertises := map[string]bool{}
	for i := range csiNodes.Items {
		for _, driver := range csiNodes.Items[i].Spec.Drivers {
			if driver.Name == zfsCSIProvisioner && slices.Contains(driver.TopologyKeys, reachability.TopologyKeyNetworkDomain) {
				advertises[csiNodes.Items[i].Name] = true
			}
		}
	}
	for name := range selected {
		got, exists := nodeDomains[name]
		if !exists {
			return fmt.Errorf("configured static consumer Node %q does not exist", name)
		}
		if got != domain {
			return fmt.Errorf("static consumer Node %q has topology domain %q, want %q", name, got, domain)
		}
		if !advertises[name] {
			return fmt.Errorf("static consumer Node %q does not advertise zfs-csi topology key %q", name, reachability.TopologyKeyNetworkDomain)
		}
	}

	for i := range nodes.Items {
		node := &nodes.Items[i]
		if _, ok := selected[node.Name]; ok || advertises[node.Name] || node.Labels[reachability.TopologyKeyNetworkDomain] != domain {
			continue
		}
		base := node.DeepCopy()
		delete(node.Labels, reachability.TopologyKeyNetworkDomain)
		if err := c.Patch(ctx, node, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("remove false zfs-csi topology from Node %q: %w", node.Name, err)
		}
	}
	return nil
}
