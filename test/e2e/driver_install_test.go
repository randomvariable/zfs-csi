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
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWaitForDriverReadyDefaultStackDoesNotRequireNVMetController(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(readyDriverWorkloads(false)...).Build()
	if err := waitForDriverReady(context.Background(), c); err != nil {
		t.Fatalf("default stack readiness: %v", err)
	}
}

func TestStorageOwnerTolerationsIncludeConfiguredStaticOwnerTaints(t *testing.T) {
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "static")
	t.Setenv("E2E_NON_BLOCKING_TAINTS", "zfs.csi.randomvariable.co.uk/storage,node-role.kubernetes.io/nas")

	tolerations := storageOwnerTolerations()
	seen := make(map[string]corev1.Toleration, len(tolerations))
	for _, toleration := range tolerations {
		seen[toleration.Key] = toleration
	}
	for _, key := range []string{
		"zfs.csi.randomvariable.co.uk/storage",
		"node-role.kubernetes.io/nas",
	} {
		toleration, ok := seen[key]
		if !ok {
			t.Fatalf("storage owner tolerations missing configured taint %q: %#v", key, tolerations)
		}
		if toleration.Effect != corev1.TaintEffectNoSchedule {
			t.Errorf("toleration %q effect = %q, want NoSchedule", key, toleration.Effect)
		}
	}
}

func TestWaitForDriverReadyRequiresEnabledNVMetController(t *testing.T) {
	objects := readyDriverWorkloads(true)
	nvmet := objects[len(objects)-1].(*appsv1.DaemonSet)
	nvmet.Status.NumberReady = 0
	c := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(objects...).Build()

	err := waitForDriverReady(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "nvmet-controller") {
		t.Fatalf("enabled unready nvmet-controller error = %v", err)
	}

	patch := client.MergeFrom(nvmet.DeepCopy())
	nvmet.Status.NumberReady = 1
	if err := c.Status().Patch(context.Background(), nvmet, patch); err != nil {
		t.Fatalf("update nvmet status: %v", err)
	}
	if err := waitForDriverReady(context.Background(), c); err != nil {
		t.Fatalf("enabled ready nvmet-controller: %v", err)
	}
}

func TestActiveDriverWorkloadsUsesDefaultAndEnabledNVMetSets(t *testing.T) {
	for _, test := range []struct {
		name         string
		includeNVMet bool
		want         int
	}{
		{name: "default", want: 3},
		{name: "nvmet enabled", includeNVMet: true, want: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(readyDriverWorkloads(test.includeNVMet)...).Build()
			workloads, err := activeDriverWorkloads(context.Background(), c)
			if err != nil || len(workloads) != test.want {
				t.Fatalf("active workloads = %d, %v; want %d", len(workloads), err, test.want)
			}
		})
	}
}

func TestCollectDriverDiagnosticsIncludesStorageSurfacesAndContainerLogs(t *testing.T) {
	scheme := newSchemeForTest(t)
	objects := readyDriverWorkloads(false)
	objects = append(
		objects,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "node-a", Namespace: zfsCSINamespace},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "driver"},
				{Name: "nfs-stage"},
			}},
		},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-a", Namespace: "conformance-a"}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-a"}},
		&storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "attachment-a"}},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "zfs-tank-nvme"}, Provisioner: "zfs.csi.randomvariable.co.uk"},
	)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()

	var commands []string
	runner := func(_ context.Context, name string, args ...string) ([]byte, error) {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return []byte("container log"), nil
	}
	got := collectDriverDiagnosticsWithRunner(context.Background(), c, "/tmp/workload.conf", runner)
	for _, want := range []string{
		"## persistent volume claims", "claim-a",
		"## persistent volumes", "pv-a",
		"## volume attachments", "attachment-a",
		"## CSI nodes", "worker-a",
		"## storage classes", "zfs-tank-nvme",
		"## driver deployments", "zfs-csi-controller",
		"## driver daemonsets", "zfs-csi-node",
		"## volume snapshots", "## driver logs", "container=nfs-stage", "container log",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("diagnostics missing %q\n%s", want, got)
		}
	}
	if len(commands) != 2 {
		t.Fatalf("log commands = %d, want 2: %v", len(commands), commands)
	}
	for _, command := range commands {
		if !strings.Contains(command, "--kubeconfig /tmp/workload.conf") || !strings.Contains(command, "--timestamps=true --tail=-1") {
			t.Errorf("unexpected log command: %s", command)
		}
	}
}

func TestCollectDriverDiagnosticsRedactsWorkloadSecrets(t *testing.T) {
	const (
		openBaoToken = "openbao-root-token"
		password     = "database-password"
		apiKey       = "api-key-value"
		secretName   = "controller-credentials"
		secretKey    = "access-token"
		pullSecret   = "private-registry-credentials"
	)
	secretEnv := []corev1.EnvVar{
		{Name: "OPENBAO_TOKEN", Value: openBaoToken},
		{Name: "DATABASE_PASSWORD", Value: password},
		{Name: "API_KEY", Value: apiKey},
		{Name: "LOG_LEVEL", Value: "debug"},
		{Name: "FROM_SECRET", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: secretName}, Key: secretKey,
		}}},
	}
	objects := readyDriverWorkloads(false)
	deployment := objects[0].(*appsv1.Deployment)
	deployment.Spec.Template.Spec.Containers = []corev1.Container{{Name: "controller", Env: secretEnv}}
	deployment.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: pullSecret}}
	daemonSet := objects[1].(*appsv1.DaemonSet)
	daemonSet.Spec.Template.Spec.Containers = []corev1.Container{{Name: "storage", Env: secretEnv}}

	c := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(objects...).Build()
	got := collectDriverDiagnosticsWithRunner(context.Background(), c, "", nil)
	for _, raw := range []string{openBaoToken, password, apiKey, secretName, secretKey, pullSecret} {
		if strings.Contains(got, raw) {
			t.Errorf("diagnostics leaked %q\n%s", raw, got)
		}
	}
	for _, want := range []string{
		"zfs-csi-controller", "zfs-csi-storage", "controller", "storage",
		"OPENBAO_TOKEN", "DATABASE_PASSWORD", "API_KEY", "FROM_SECRET",
		"LOG_LEVEL", "debug", diagnosticRedaction,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("diagnostics missing useful field %q\n%s", want, got)
		}
	}
	if gotCount := strings.Count(got, openBaoToken); gotCount != 0 {
		t.Fatalf("known raw token appears %d times", gotCount)
	}
	if deployment.Spec.Template.Spec.Containers[0].Env[0].Value != openBaoToken {
		t.Fatal("diagnostic redaction mutated the source Deployment")
	}
}

func TestObjectListYAMLRedactsSecretObjects(t *testing.T) {
	rawData := []byte("raw-secret-data")
	list := &corev1.SecretList{Items: []corev1.Secret{{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials"},
		Data:       map[string][]byte{"token": rawData},
		StringData: map[string]string{"password": "raw-string-data"},
	}}}
	got := objectListYAML(list)
	for _, raw := range []string{"raw-secret-data", "cmF3LXNlY3JldC1kYXRh", "raw-string-data"} {
		if strings.Contains(got, raw) {
			t.Errorf("secret diagnostics leaked %q\n%s", raw, got)
		}
	}
	if !strings.Contains(got, "credentials") || !strings.Contains(got, diagnosticRedaction) {
		t.Fatalf("secret metadata/redaction marker missing:\n%s", got)
	}
}

func readyDriverWorkloads(includeNVMet bool) []client.Object {
	objects := []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-controller", Namespace: zfsCSINamespace}, Status: appsv1.DeploymentStatus{ReadyReplicas: 1}},
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-storage", Namespace: zfsCSINamespace}, Status: appsv1.DaemonSetStatus{NumberReady: 1}},
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-node", Namespace: zfsCSINamespace}, Status: appsv1.DaemonSetStatus{NumberReady: 1}},
	}
	if includeNVMet {
		objects = append(objects, &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: "nvmet-controller", Namespace: zfsCSINamespace},
			Status:     appsv1.DaemonSetStatus{NumberReady: 1},
		})
	}
	return objects
}

func TestAppendDriverLogsPreservesFailures(t *testing.T) {
	var b strings.Builder
	runner := func(context.Context, string, ...string) ([]byte, error) {
		return []byte("partial stderr"), fmt.Errorf("exit 1")
	}
	appendDriverLogs(context.Background(), &b, "kubeconfig", []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: zfsCSINamespace},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "driver"}}},
	}}, runner)
	if got := b.String(); !strings.Contains(got, "log collection failed: exit 1") || !strings.Contains(got, "partial stderr") {
		t.Fatalf("failed log output not preserved:\n%s", got)
	}
}

func TestInstallDriverOverridesAreSortedAndPropagated(t *testing.T) {
	args := appendSortedHelmOverrides(nil, map[string]string{"node.maxVolumesPerNode": "3", "controller.replicas": "1"})
	want := []string{"--set-string", "controller.replicas=1", "--set-string", "node.maxVolumesPerNode=3"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("helm overrides = %#v, want %#v", args, want)
	}
}

func TestEscapeHelmMapKey(t *testing.T) {
	if got, want := escapeHelmMapKey("zfs-csi.randomvariable.co.uk/consumer-group"), `zfs-csi\.randomvariable\.co\.uk/consumer-group`; got != want {
		t.Fatalf("escaped key = %q, want %q", got, want)
	}
}

func TestStorageOwnerDeploymentNameUsesRenderedOwnerIdentity(t *testing.T) {
	owner := storageOwner{Name: "storage-a", Node: storageNode{Name: "ip-10-0-92-202.ec2.internal"}}
	if got, want := storageOwnerDeploymentName(owner.Node.Name), "zfs-csi-storage-ip-10-0-92-202-ec2-internal-17f03e45"; got != want {
		t.Fatalf("storage owner Deployment name = %q, want %q", got, want)
	}
}

func TestInstallChartCRDsRejectsOCIReferenceBeforeMutation(t *testing.T) {
	err := installChartCRDs(context.Background(), "/tmp/unused-kubeconfig", "oci://registry.example/zfs-csi")
	if err == nil || !strings.Contains(err.Error(), "requires a local chart reference") {
		t.Fatalf("installChartCRDs OCI error = %v", err)
	}
}
