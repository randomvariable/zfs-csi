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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/randomvariable/zfs-csi/internal/reachability"
	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

func TestReconcileStaticConsumerTopologyPreflight(t *testing.T) {
	worker := e2econfig.ConsumerWorker{Name: "workers", NodeNames: []string{"worker-a"}, Replicas: 1, NetworkDomain: "fabric-a"}
	for _, tc := range []struct {
		name    string
		workers []e2econfig.ConsumerWorker
		drivers []storagev1.CSINodeDriver
		domain  string
		want    string
	}{
		{name: "missing driver", workers: []e2econfig.ConsumerWorker{worker}, domain: "fabric-a", want: "does not advertise"},
		{name: "missing topology key", workers: []e2econfig.ConsumerWorker{worker}, drivers: []storagev1.CSINodeDriver{{Name: zfsCSIProvisioner}}, domain: "fabric-a", want: "does not advertise"},
		{name: "mismatched selected domain", workers: []e2econfig.ConsumerWorker{worker}, drivers: []storagev1.CSINodeDriver{{Name: zfsCSIProvisioner, TopologyKeys: []string{reachability.TopologyKeyNetworkDomain}}}, domain: "fabric-b", want: "want \"fabric-a\""},
		{name: "multiple domains", workers: []e2econfig.ConsumerWorker{worker, e2econfig.ConsumerWorker{Name: "other", NodeNames: []string{"worker-b"}, Replicas: 1, NetworkDomain: "fabric-b"}}, want: "exactly one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Labels: map[string]string{reachability.TopologyKeyNetworkDomain: tc.domain}}},
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-b"}},
				&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Spec: storagev1.CSINodeSpec{Drivers: tc.drivers}},
			).Build()
			if err := reconcileStaticConsumerTopology(context.Background(), c, tc.workers); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestReconcileStaticConsumerTopologyCleanup(t *testing.T) {
	driver := storagev1.CSINodeDriver{Name: zfsCSIProvisioner, TopologyKeys: []string{reachability.TopologyKeyNetworkDomain}}
	objects := []client.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "selected", Labels: map[string]string{reachability.TopologyKeyNetworkDomain: "fabric-a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "false", Labels: map[string]string{reachability.TopologyKeyNetworkDomain: "fabric-a", "keep": "yes"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "other-plugin", Labels: map[string]string{reachability.TopologyKeyNetworkDomain: "fabric-a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Labels: map[string]string{reachability.TopologyKeyNetworkDomain: "fabric-b"}}},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "selected"}, Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{driver}}},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "other-plugin"}, Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{driver}}},
	}
	c := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(objects...).Build()
	worker := e2econfig.ConsumerWorker{Name: "workers", NodeNames: []string{"selected"}, Replicas: 1, NetworkDomain: "fabric-a"}
	if err := reconcileStaticConsumerTopology(context.Background(), c, []e2econfig.ConsumerWorker{worker}); err != nil {
		t.Fatal(err)
	}
	for name, domain := range map[string]string{"selected": "fabric-a", "false": "", "other-plugin": "fabric-a", "foreign": "fabric-b"} {
		node := &corev1.Node{}
		if err := c.Get(context.Background(), client.ObjectKey{Name: name}, node); err != nil {
			t.Fatal(err)
		}
		if node.Labels[reachability.TopologyKeyNetworkDomain] != domain || name == "false" && node.Labels["keep"] != "yes" {
			t.Errorf("Node %q labels = %#v", name, node.Labels)
		}
	}
}
