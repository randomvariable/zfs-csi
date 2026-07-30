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
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDriverImageHelmValues(t *testing.T) {
	repository, tag, digest, err := driverImageHelmValues("registry.example:5000/zfs-csi:dev")
	if err != nil || repository != "registry.example:5000/zfs-csi" || tag != "dev" || digest != "" {
		t.Fatalf("tag image values = %q, %q, %q, %v", repository, tag, digest, err)
	}
	const digestImage = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	repository, tag, digest, err = driverImageHelmValues("registry.example/zfs-csi@" + digestImage)
	if err != nil || repository != "registry.example/zfs-csi" || tag != "" || digest != digestImage {
		t.Fatalf("digest image values = %q, %q, %q, %v", repository, tag, digest, err)
	}
}

func TestHashFilesAndRunMetadata(t *testing.T) {
	dir := t.TempDir()
	testDriver := filepath.Join(dir, "testdriver.yaml")
	if err := os.WriteFile(testDriver, []byte("driver: zfs-csi\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	hashes, err := hashFiles([]string{testDriver})
	if err != nil || len(hashes[testDriver]) != 64 {
		t.Fatalf("hashFiles = %v, %v", hashes, err)
	}
	metadata, err := newRunMetadata("run", "cluster", "registry.example/zfs-csi:dev", []string{testDriver}, storageNode{}, 7, dir)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.TestDriverSHA256[testDriver] != hashes[testDriver] || metadata.GinkgoSeed != 7 {
		t.Fatalf("metadata missing immutable evidence: %#v", metadata)
	}
}

func TestWriteRunMetadataDoesNotSerializeSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.json")
	metadata := e2eRunMetadata{DriverImage: "registry.example/zfs-csi:dev", Environment: map[string]string{"AWS_REGION": "us-east-1"}}
	if err := writeRunMetadata(path, metadata); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "AWS_SECRET_ACCESS_KEY") || strings.Contains(string(body), "token") {
		t.Fatalf("metadata serialized secret-shaped field: %s", body)
	}
}

func TestCaptureKubernetesContainerLogsWritesCurrentAndAvailablePrevious(t *testing.T) {
	dir := t.TempDir()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "zfs-csi", Name: "node/0"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "init"}},
			Containers:     []corev1.Container{{Name: "driver"}},
		},
	}
	workload := fake.NewClientBuilder().WithObjects(pod).Build()
	runner := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		container := args[7]
		previous := args[len(args)-1] == "--previous=true"
		if previous && container == "init" {
			return []byte("no previous logs"), fmt.Errorf("not found")
		}
		if previous {
			return []byte("previous " + container), nil
		}
		return []byte("current " + container), nil
	}

	captureKubernetesContainerLogs(context.Background(), dir, "kubeconfig", workload, runner)
	logDir := filepath.Join(dir, "kubernetes-logs")
	for name, want := range map[string]string{
		"zfs-csi__node_0__driver__current.log":  "current driver",
		"zfs-csi__node_0__driver__previous.log": "previous driver",
		"zfs-csi__node_0__init__current.log":    "current init",
	} {
		body, err := os.ReadFile(filepath.Join(logDir, name))
		if err != nil || string(body) != want {
			t.Errorf("artifact %s = %q, %v; want %q", name, body, err, want)
		}
	}
	previous, err := os.ReadFile(filepath.Join(logDir, "zfs-csi__node_0__init__previous.log"))
	if err != nil || string(previous) != "no previous logs" {
		t.Fatalf("partial previous log = %q, %v", previous, err)
	}
}

func TestCaptureKubernetesContainerLogsRecordsFailureWithoutReturningIt(t *testing.T) {
	dir := t.TempDir()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "broken"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}}}
	workload := fake.NewClientBuilder().WithObjects(pod).Build()
	captureKubernetesContainerLogs(context.Background(), dir, "kubeconfig", workload, func(context.Context, string, ...string) ([]byte, error) {
		return []byte("partial output"), fmt.Errorf("exit 1")
	})
	body, err := os.ReadFile(filepath.Join(dir, "kubernetes-logs", "collection-errors.log"))
	if err != nil || !strings.Contains(string(body), "default__broken__app current: exit 1") || !strings.Contains(string(body), "partial output") {
		t.Fatalf("collection failure artifact = %q, %v", body, err)
	}
}

func TestConformanceInputCarriesPostRunDiagnosticHook(t *testing.T) {
	called := false
	input := conformanceInput{AfterRun: func(context.Context) error {
		called = true
		return nil
	}}
	if input.AfterRun == nil {
		t.Fatal("conformance post-run diagnostic hook is nil")
	}
	if err := input.AfterRun(context.Background()); err != nil || !called {
		t.Fatalf("post-run diagnostic hook = %v, called=%t", err, called)
	}
}

func TestChartOverridesContainInstallEvidence(t *testing.T) {
	const digestImage = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	overrides := chartOverrides("registry.example/zfs-csi@"+digestImage, storageNode{Name: "storage-0", PortalHost: "10.0.0.9", NFSServer: "10.0.0.9"})
	for _, key := range []string{"image.repository", "image.tag", "image.digest", "storageNode.name", "network.portalHost", "network.nfsServer", "storageClasses.tankNVMe.pool"} {
		if _, ok := overrides[key]; !ok {
			t.Errorf("override %q missing", key)
		}
	}
	if overrides["image.repository"] != "registry.example/zfs-csi" || overrides["image.digest"] != digestImage || overrides["image.tag"] != "" {
		t.Fatalf("digest image metadata = %#v", overrides)
	}

	overrides = chartOverrides("registry.example/zfs-csi:dev", storageNode{})
	if overrides["image.repository"] != "registry.example/zfs-csi" || overrides["image.tag"] != "dev" || overrides["image.digest"] != "" {
		t.Fatalf("tag image metadata = %#v", overrides)
	}
	if overrides["image.pullPolicy"] != "Always" {
		t.Fatalf("mutable E2E image pull policy = %q, want Always", overrides["image.pullPolicy"])
	}
	if _, ok := overrides["image.reference"]; ok {
		t.Fatal("metadata recorded non-chart image.reference")
	}
}

func TestChartOverridesAlwaysRecordProviderNFSExportCIDRs(t *testing.T) {
	overrides := chartOverridesForImageValues("registry.example/zfs-csi", "dev", "", storageNode{})
	if got := overrides["storageClasses.tankNFS.nfsExportCIDRs"]; got == "" {
		t.Fatalf("metadata omitted provider NFS CIDR: %#v", overrides)
	}
	if got := overrides["storageClasses.tankNFS.enabled"]; got != "true" {
		t.Fatalf("NFS StorageClass enabled = %q, want true", got)
	}
}
