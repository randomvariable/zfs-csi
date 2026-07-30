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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

func TestPublisherWritesPoolFreeBytes(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Publisher{Client: c, ZFS: zfsfake.New().WithPool("tank", 42), Namespace: "zfs-csi", Node: "storage-0"}
	if err := p.publish(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "zfs-csi", Name: ConfigMapNameForNode("storage-0")}, cm); err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	if got := cm.Data["tank"]; got != "42" {
		t.Fatalf("tank capacity = %q, want 42", got)
	}
	if cm.Labels[ManagedByLabel] != ManagedByValue || cm.Labels[NodeLabel] != "storage-0" {
		t.Fatalf("labels = %v, want managed-by + node=storage-0", cm.Labels)
	}
}

// TestPublishersOnSeparateNodesDoNotContend proves the per-node scoping fix:
// two agents on different nodes each own their own ConfigMap, so neither erases
// the other's pool data.
func TestPublishersOnSeparateNodesDoNotContend(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	a := &Publisher{Client: c, ZFS: zfsfake.New().WithPool("tankA", 10), Namespace: "zfs-csi", Node: "node-a"}
	b := &Publisher{Client: c, ZFS: zfsfake.New().WithPool("tankB", 20), Namespace: "zfs-csi", Node: "node-b"}
	if err := a.publish(context.Background()); err != nil {
		t.Fatalf("publish node-a: %v", err)
	}
	if err := b.publish(context.Background()); err != nil {
		t.Fatalf("publish node-b: %v", err)
	}

	list := &corev1.ConfigMapList{}
	if err := c.List(context.Background(), list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("ConfigMap count = %d, want 2 (one per node)", len(list.Items))
	}
}
