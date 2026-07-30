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
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"
)

const testDriverManifest = `StorageClass:
  FromExistingClassName: zfs-tank-nfs
DriverInfo:
  Name: zfs.csi.randomvariable.co.uk-nfs
  Capabilities:
    persistence: true
    RWX: true
`

// rewriteTestDriverStorageClass must inject top-level NodeSelectors (merged by
// the upstream suite into every test pod's node selection) so cross-node tests
// only schedule onto nodes that run the node-plugin DaemonSet. Without the pin
// the multi-node tests place pods on plugin-less nodes and time out mounting.
func TestRewriteTestDriverStorageClassInjectsNodeSelectors(t *testing.T) {
	selectors := map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": "workers-a"}
	out, err := rewriteTestDriverStorageClass([]byte(testDriverManifest), nil, selectors)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal rewritten: %v", err)
	}
	got, ok := doc["NodeSelectors"].(map[string]any)
	if !ok {
		t.Fatalf("NodeSelectors missing or wrong type: %#v", doc["NodeSelectors"])
	}
	if got["zfs-csi.randomvariable.co.uk/consumer-group"] != "workers-a" {
		t.Fatalf("consumer-group selector = %v, want workers-a", got)
	}
	// SC reference untouched when no rename applies.
	sc := doc["StorageClass"].(map[string]any)
	if sc["FromExistingClassName"] != "zfs-tank-nfs" {
		t.Fatalf("SC renamed unexpectedly: %v", sc["FromExistingClassName"])
	}
}

func TestRewriteTestDriverStorageClassRenamesAndKeepsSelectorsOff(t *testing.T) {
	renames := map[string]string{"zfs-tank-nfs": "custom-nfs"}
	out, err := rewriteTestDriverStorageClass([]byte(testDriverManifest), renames, nil)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal rewritten: %v", err)
	}
	sc := doc["StorageClass"].(map[string]any)
	if sc["FromExistingClassName"] != "custom-nfs" {
		t.Fatalf("SC = %v, want custom-nfs", sc["FromExistingClassName"])
	}
	if _, present := doc["NodeSelectors"]; present {
		t.Fatalf("NodeSelectors must be absent when no selectors supplied: %#v", doc["NodeSelectors"])
	}
}

func TestConstrainStaticStorageClassesPatchesOnlyHelmOwnedZFSClasses(t *testing.T) {
	scheme := newSchemeForTest(t)
	owned := &storagev1.StorageClass{
		ObjectMeta:  helmStorageClassObjectMeta("zfs-owned"),
		Provisioner: zfsCSIProvisioner,
	}
	compatible := &storagev1.StorageClass{
		ObjectMeta:  helmStorageClassObjectMeta("zfs-compatible"),
		Provisioner: zfsCSIProvisioner,
		AllowedTopologies: []corev1.TopologySelectorTerm{{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{
			Key: e2eNetworkDomainLabelKey, Values: []string{"fabric-b", "fabric-a"},
		}}}},
	}
	unrelated := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}, Provisioner: "example.csi.invalid"}
	nonHelm := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "zfs-not-helm"}, Provisioner: zfsCSIProvisioner}
	otherNamespace := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "zfs-other-namespace", Annotations: map[string]string{
			helmReleaseNameAnnotation: zfsCSIHelmReleaseName, helmReleaseNamespaceAnnotation: "other",
		}, Labels: map[string]string{helmManagedByLabel: helmManagedByValue}},
		Provisioner: zfsCSIProvisioner,
	}
	annotationsOnly := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "zfs-annotations-only", Annotations: helmStorageClassAnnotations()},
		Provisioner: zfsCSIProvisioner,
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owned, compatible, unrelated, nonHelm, otherNamespace, annotationsOnly).Build()

	if err := constrainStaticStorageClasses(context.Background(), c, []string{"fabric-b", "fabric-a", "fabric-a"}); err != nil {
		t.Fatal(err)
	}
	got := &storagev1.StorageClass{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: owned.Name}, got); err != nil {
		t.Fatal(err)
	}
	want := []string{"fabric-a", "fabric-b"}
	if len(got.AllowedTopologies) != 1 || got.AllowedTopologies[0].MatchLabelExpressions[0].Key != e2eNetworkDomainLabelKey || !slices.Equal(got.AllowedTopologies[0].MatchLabelExpressions[0].Values, want) {
		t.Fatalf("owned StorageClass AllowedTopologies = %#v, want domains %v", got.AllowedTopologies, want)
	}
	got = &storagev1.StorageClass{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: compatible.Name}, got); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.AllowedTopologies[0].MatchLabelExpressions[0].Values, []string{"fabric-b", "fabric-a"}) {
		t.Fatalf("compatible AllowedTopologies changed: %#v", got.AllowedTopologies)
	}
	for _, name := range []string{unrelated.Name, nonHelm.Name, otherNamespace.Name, annotationsOnly.Name} {
		got = &storagev1.StorageClass{}
		if err := c.Get(context.Background(), client.ObjectKey{Name: name}, got); err != nil {
			t.Fatal(err)
		}
		if got.AllowedTopologies != nil {
			t.Fatalf("StorageClass %q unexpectedly changed: %#v", name, got.AllowedTopologies)
		}
	}
}

func TestConstrainStaticStorageClassesRejectsConflictBeforeWrites(t *testing.T) {
	scheme := newSchemeForTest(t)
	owned := func(name string, topologies []corev1.TopologySelectorTerm) *storagev1.StorageClass {
		return &storagev1.StorageClass{
			ObjectMeta:        helmStorageClassObjectMeta(name),
			Provisioner:       zfsCSIProvisioner,
			AllowedTopologies: topologies,
		}
	}
	empty := owned("a-empty", nil)
	conflicting := owned("z-conflicting", []corev1.TopologySelectorTerm{{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{Key: "other.example/domain", Values: []string{"wrong"}}}}})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(empty, conflicting).Build()

	err := constrainStaticStorageClasses(context.Background(), c, []string{"fabric-a"})
	if err == nil || !strings.Contains(err.Error(), "conflicting AllowedTopologies") {
		t.Fatalf("expected conflicting AllowedTopologies error, got %v", err)
	}
	got := &storagev1.StorageClass{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: empty.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.AllowedTopologies != nil {
		t.Fatalf("StorageClass patched before conflict validation: %#v", got.AllowedTopologies)
	}
}

func helmStorageClassAnnotations() map[string]string {
	return map[string]string{
		helmReleaseNameAnnotation:      zfsCSIHelmReleaseName,
		helmReleaseNamespaceAnnotation: zfsCSINamespace,
	}
}

func helmStorageClassObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name: name, Annotations: helmStorageClassAnnotations(),
		Labels: map[string]string{helmManagedByLabel: helmManagedByValue},
	}
}
