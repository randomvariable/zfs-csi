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
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

type fakeOwnerMachineResolver struct {
	machine ownerMachine
	err     error
}

func (r fakeOwnerMachineResolver) ResolveOwnerMachine(context.Context, client.Client, string, string, e2econfig.StorageOwner) (ownerMachine, error) {
	return r.machine, r.err
}

type fakeOwnerDiskResolver struct {
	device string
	err    error
}

func (r fakeOwnerDiskResolver) ResolveDeviceID(context.Context, ownerMachine, e2econfig.StorageOwner) (string, error) {
	return r.device, r.err
}

type fakeNodeCommandRunner struct {
	output string
	err    error
}

func (r fakeNodeCommandRunner) Run(context.Context, string, string) (string, error) {
	return r.output, r.err
}

type fakeEC2 struct {
	output *ec2.DescribeInstancesOutput
	err    error
}

func (f fakeEC2) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	return f.output, f.err
}

func TestDiscoverOwnerNodeRequiresExactlyOneSelectorMatch(t *testing.T) {
	scheme := newSchemeForTest(t)
	selector := map[string]string{e2eStorageOwnerLabelKey: "storage-a"}
	for _, tc := range []struct {
		name  string
		nodes []client.Object
	}{
		{name: "zero"},
		{name: "multiple", nodes: []client.Object{
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: selector}},
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "b", Labels: selector}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.nodes...).Build()
			if _, err := discoverOwnerNode(context.Background(), c, "a", selector); err == nil {
				t.Fatal("expected fail-closed owner discovery")
			}
		})
	}
}

func TestResolveStorageOwnersVerifiesExactDevice(t *testing.T) {
	t.Setenv(e2econfig.Env[e2econfig.StorageOwnersKey], "storage-a,storage-a,/dev/disk/by-id/virtio-tank-a,fabric-a,10.19.1.20,10.19.1.20")
	t.Setenv(e2econfig.Env[e2econfig.InfrastructureConfigKey], "")
	if err := e2econfig.Init(); err != nil {
		t.Fatal(err)
	}
	scheme := newSchemeForTest(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{e2eStorageOwnerLabelKey: "storage-a"}},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.19.1.20"}}},
	}).Build()
	owners, err := resolveStorageOwners(context.Background(), c, c, "ns", "cluster",
		fakeOwnerMachineResolver{machine: ownerMachine{Name: "machine-a", NodeName: "node-a"}},
		fakeOwnerDiskResolver{device: "/dev/disk/by-id/virtio-tank-a"},
		fakeNodeCommandRunner{output: "/dev/vdb\n"})
	if err != nil || len(owners) != 1 || owners[0].DataDeviceID != "/dev/disk/by-id/virtio-tank-a" {
		t.Fatalf("resolveStorageOwners = %#v, %v", owners, err)
	}
}

func TestResolveStorageOwnersRejectsMissingOrAmbiguousDisk(t *testing.T) {
	for _, output := range []string{"", "/dev/vdb\n/dev/vdc\n"} {
		if err := verifyExactDevice(context.Background(), fakeNodeCommandRunner{output: output}, "node-a", "/dev/disk/by-id/virtio-tank-a"); err == nil {
			t.Fatalf("verifyExactDevice(%q) unexpectedly succeeded", output)
		}
	}
}

func TestResolvedStorageOwnersRejectDuplicateNodeOrDisk(t *testing.T) {
	base := []storageOwner{
		{Name: "storage-a", Node: storageNode{Name: "node-a"}, DataDeviceID: "/dev/disk/by-id/virtio-a"},
		{Name: "storage-b", Node: storageNode{Name: "node-b"}, DataDeviceID: "/dev/disk/by-id/virtio-b"},
	}
	for _, mutate := range []func([]storageOwner){
		func(owners []storageOwner) { owners[1].Node.Name = owners[0].Node.Name },
		func(owners []storageOwner) { owners[1].DataDeviceID = owners[0].DataDeviceID },
	} {
		owners := append([]storageOwner(nil), base...)
		mutate(owners)
		if err := validateResolvedStorageOwners(owners); err == nil {
			t.Fatalf("expected duplicate resolved identity rejection: %#v", owners)
		}
	}
}

func TestAWSAttachmentResolverConvertsExactVolumeID(t *testing.T) {
	resolver := awsAttachmentResolver{ec2: fakeEC2{output: &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{{
		InstanceId:          aws.String("i-123"),
		BlockDeviceMappings: []ec2types.InstanceBlockDeviceMapping{{DeviceName: aws.String("/dev/xvdb"), Ebs: &ec2types.EbsInstanceBlockDevice{VolumeId: aws.String("vol-0123456789abcdef0")}}},
	}}}}}}}
	got, err := resolver.ResolveDeviceID(context.Background(), ownerMachine{InstanceID: "i-123"}, e2econfig.StorageOwner{Name: "storage-a", DiskDiscovery: e2econfig.DiskDiscovery{Provider: "aws-ebs-volume-id", AttachmentDeviceName: "/dev/xvdb"}})
	if err != nil || got != "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol0123456789abcdef0" {
		t.Fatalf("ResolveDeviceID = %q, %v", got, err)
	}
}

func TestApplyOwnerRolesRejectsConflictingConsumerTopology(t *testing.T) {
	scheme := newSchemeForTest(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Labels: map[string]string{
		"zfs-csi.randomvariable.co.uk/consumer-group": "workers-a",
		e2eNetworkDomainLabelKey:                      "wrong",
	}}}).Build()
	err := applyOwnerRolesAndConsumerDomains(context.Background(), c, nil, []e2econfig.ConsumerWorker{{Name: "workers-a", NodeSelector: map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": "workers-a"}, Replicas: 1, NetworkDomain: "fabric-a"}})
	if err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("expected conflicting topology label error, got %v", err)
	}
}

func TestApplyOwnerRolesSupportsTwoOwnersSharingOneConsumerDomain(t *testing.T) {
	scheme := newSchemeForTest(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-plane"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "storage-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "storage-b"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Labels: map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": "workers-a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Labels: map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": "workers-a"}}},
	).Build()
	owners := []storageOwner{
		{Name: "storage-a", Node: storageNode{Name: "storage-a"}, NodeSelector: map[string]string{e2eStorageOwnerLabelKey: "storage-a"}, NetworkDomain: "fabric-a"},
		{Name: "storage-b", Node: storageNode{Name: "storage-b"}, NodeSelector: map[string]string{e2eStorageOwnerLabelKey: "storage-b"}, NetworkDomain: "fabric-a"},
	}
	workers := []e2econfig.ConsumerWorker{{Name: "workers-a", MachineDeploymentSuffix: "md-0", NodeSelector: map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": "workers-a"}, Replicas: 2, NetworkDomain: "fabric-a"}}
	if err := applyOwnerRolesAndConsumerDomains(context.Background(), c, owners, workers); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"worker-a", "worker-b"} {
		node := &corev1.Node{}
		if err := c.Get(context.Background(), client.ObjectKey{Name: name}, node); err != nil {
			t.Fatal(err)
		}
		if got := node.Labels[e2eNetworkDomainLabelKey]; got != "fabric-a" {
			t.Fatalf("Node %q network domain = %q, want fabric-a", name, got)
		}
	}
	for _, name := range []string{"storage-a", "storage-b", "control-plane"} {
		node := &corev1.Node{}
		if err := c.Get(context.Background(), client.ObjectKey{Name: name}, node); err != nil {
			t.Fatal(err)
		}
		if got := node.Labels[e2eNetworkDomainLabelKey]; got != "fabric-a" {
			t.Fatalf("storage Node %q network domain = %q, want fabric-a", name, got)
		}
	}
}

func TestApplyOwnerRolesRestoresStaticConsumerLabelsByNodeName(t *testing.T) {
	scheme := newSchemeForTest(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-b"}},
	).Build()
	worker := e2econfig.ConsumerWorker{
		Name: "workers-a", NodeNames: []string{"worker-a", "worker-b"}, Replicas: 2,
		NodeSelector:  map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": "workers-a"},
		NetworkDomain: "fabric-a",
	}
	if err := applyOwnerRolesAndConsumerDomains(context.Background(), c, nil, []e2econfig.ConsumerWorker{worker}); err != nil {
		t.Fatal(err)
	}
	for _, name := range worker.NodeNames {
		node := &corev1.Node{}
		if err := c.Get(context.Background(), client.ObjectKey{Name: name}, node); err != nil {
			t.Fatal(err)
		}
		if node.Labels["zfs-csi.randomvariable.co.uk/consumer-group"] != "workers-a" || node.Labels[e2eNetworkDomainLabelKey] != "fabric-a" {
			t.Fatalf("Node %q labels = %#v", name, node.Labels)
		}
	}
}

func TestApplyOwnerRolesStaticConsumerConflictFailsBeforeWrites(t *testing.T) {
	scheme := newSchemeForTest(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Labels: map[string]string{e2eNetworkDomainLabelKey: "wrong"}}},
	).Build()
	worker := e2econfig.ConsumerWorker{
		Name: "workers-a", NodeNames: []string{"worker-a", "worker-b"}, Replicas: 2,
		NodeSelector:  map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": "workers-a"},
		NetworkDomain: "fabric-a",
	}
	if err := applyOwnerRolesAndConsumerDomains(context.Background(), c, nil, []e2econfig.ConsumerWorker{worker}); err == nil {
		t.Fatal("expected static consumer topology conflict")
	}
	node := &corev1.Node{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "worker-a"}, node); err != nil {
		t.Fatal(err)
	}
	if _, exists := node.Labels["zfs-csi.randomvariable.co.uk/consumer-group"]; exists {
		t.Fatalf("worker-a was patched before conflict validation: %#v", node.Labels)
	}
}

func TestHelmStorageOwnerValuesSupportsOneTwoThreeAndSharedDomain(t *testing.T) {
	for count := 1; count <= 3; count++ {
		owners := make([]storageOwner, 0, count)
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("storage-%c", 'a'+i)
			owners = append(owners, storageOwner{Name: name, Node: storageNode{Name: "node-" + name, NFSServer: fmt.Sprintf("10.0.0.%d", 10+i), PortalHost: fmt.Sprintf("10.0.0.%d", 10+i)}, NodeSelector: map[string]string{e2eStorageOwnerLabelKey: name}, DataDeviceID: "/dev/disk/by-id/virtio-" + name, PoolName: "tank", PoolGUID: fmt.Sprintf("%d", 1111+i), NetworkDomain: "shared", ReachableFrom: []string{"shared"}, NFSPort: 2049, NVMePort: 4420})
		}
		values, err := helmStorageOwnerValues(owners)
		if err != nil || len(values) != count {
			t.Fatalf("count %d values = %#v, %v", count, values, err)
		}
		for i := range values {
			if values[i].Name != owners[i].Node.Name {
				t.Fatalf("count %d owner %d identity = %q, want Kubernetes Node %q", count, i, values[i].Name, owners[i].Node.Name)
			}
		}
	}
}

func TestMultiOwnerRolloutTargetUsesRenderedNodeIdentity(t *testing.T) {
	owner := storageOwner{Name: "storage-a", Node: storageNode{Name: "ip-10-0-92-202.ec2.internal"}}
	if got, want := storageOwnerDeploymentName(owner.Node.Name), "zfs-csi-storage-ip-10-0-92-202-ec2-internal-17f03e45"; got != want {
		t.Fatalf("storage rollout target = %q, want %q", got, want)
	}
}

func TestGeneratedMultiOwnerValuesRenderOneTwoThreeOwners(t *testing.T) {
	root := repositoryRootForTestDriver(t, filepath.Join("data", "infrastructure-kubevirt", "two-owner.yaml"))
	chartPath := filepath.Join(root, "charts", "zfs-csi")
	for count := 1; count <= 3; count++ {
		owners := make([]storageOwner, 0, count)
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("storage-%c", 'a'+i)
			owners = append(owners, storageOwner{Name: name, Node: storageNode{Name: "node-" + name, NFSServer: fmt.Sprintf("10.0.0.%d", 10+i), PortalHost: fmt.Sprintf("10.0.0.%d", 10+i)}, NodeSelector: map[string]string{e2eStorageOwnerLabelKey: name}, DataDeviceID: "/dev/disk/by-id/virtio-" + name, PoolName: "tank", PoolGUID: fmt.Sprintf("%d", 1111+i), NetworkDomain: "shared", ReachableFrom: []string{"shared"}, NFSPort: 2049, NVMePort: 4420})
		}
		path := filepath.Join(t.TempDir(), "values.yaml")
		if err := writeHelmStorageOwnerValues(path, owners); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("helm", "template", "zfs-csi", chartPath, "--values", path, "--set", "image.repository=example.test/zfs-csi", "--set", "image.tag=test")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("helm template count %d: %v\n%s", count, err, out)
		}
		if got := strings.Count(string(out), "\nkind: StorageNode\n"); got != count {
			t.Fatalf("StorageNode count = %d, want %d", got, count)
		}
	}
}

func TestMultiOwnerReadinessRejectsInventoryMismatch(t *testing.T) {
	owner := storageOwner{Name: "storage-a", PoolName: "tank", PoolGUID: "1111", NetworkDomain: "fabric-a", ReachableFrom: []string{"fabric-a"}, NFSPort: 2049, NVMePort: 4420, Node: storageNode{NFSServer: "10.0.0.1", PortalHost: "10.0.0.1"}}
	objects := []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-controller", Namespace: zfsCSINamespace}, Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](2), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "controller"}, {Name: "provisioner"}}}}}, Status: appsv1.DeploymentStatus{ReadyReplicas: 2}},
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-node", Namespace: zfsCSINamespace}, Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "node"}, {Name: "registrar"}}}}}, Status: appsv1.DaemonSetStatus{NumberReady: 1}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: storageOwnerDeploymentName(owner.Name), Namespace: zfsCSINamespace}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "storage"}}}}}, Status: appsv1.DeploymentStatus{ReadyReplicas: 1}},
		&zfscsiv1.StorageNode{ObjectMeta: metav1.ObjectMeta{Name: owner.Name}, Spec: zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: []string{"9999"}, NetworkDomain: "fabric-a"}},
	}
	scheme := newSchemeForTest(t)
	if err := zfscsiv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	if err := waitForMultiOwnerDriverReady(context.Background(), c, []storageOwner{owner}); err == nil {
		t.Fatal("expected readiness inventory mismatch")
	}
}

func TestDeleteOwnedObjectRefusesUnownedResource(t *testing.T) {
	scheme := newSchemeForTest(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "setup", Namespace: "default", Labels: map[string]string{"run-id": "other"}}}).Build()
	err := deleteOwnedObject(context.Background(), c, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "setup", Namespace: "default"}}, map[string]string{"run-id": "expected"})
	if err == nil || !strings.Contains(err.Error(), "refusing to delete unowned") {
		t.Fatalf("expected ownership refusal, got %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "setup"}, &corev1.Pod{}); err != nil {
		t.Fatalf("unowned object was deleted: %v", err)
	}
}

func TestNodeCommandPodSerializesKubernetesTypeMetadata(t *testing.T) {
	pod := nodeCommandPod("default", "node-check", "node-a", "example.test/preflight:v1", "true")
	jsonData, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := yaml.JSONToYAML(jsonData)
	if err != nil {
		t.Fatal(err)
	}
	text := string(manifest)
	if !strings.Contains(text, "apiVersion: v1\n") || !strings.Contains(text, "kind: Pod\n") {
		t.Fatalf("serialized pod lacks Kubernetes type metadata:\n%s", text)
	}
}

func TestDiscoverStorageNodeRequiresExactlyOne(t *testing.T) {
	ctx := context.Background()
	scheme := newSchemeForTest(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "storage-a", Labels: map[string]string{e2eStorageRoleLabel: e2eStorageRoleValue}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "storage-b", Labels: map[string]string{e2eStorageRoleLabel: e2eStorageRoleValue}}},
	).Build()

	if _, err := discoverStorageNode(ctx, c); err == nil {
		t.Fatal("expected duplicate storage nodes to fail discovery")
	}
}

func TestApplyStorageRoleLabelsAndTaintsNode(t *testing.T) {
	ctx := context.Background()
	scheme := newSchemeForTest(t)
	const (
		nodeName = "zfs-csi-e2e-worker-0"
		nodeIP   = "10.20.30.40"
	)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: nodeIP}},
		},
	}).Build()

	if err := applyStorageRole(ctx, c, nodeName); err != nil {
		t.Fatalf("apply storage role: %v", err)
	}

	node, err := discoverStorageNode(ctx, c)
	if err != nil {
		t.Fatalf("discover storage node: %v", err)
	}
	// PortalHost/NFSServer must be the InternalIP (reachable cluster-wide), not
	// the hostname.
	if node.Name != nodeName || node.PortalHost != nodeIP || node.NFSServer != nodeIP {
		t.Fatalf("unexpected storage node: %#v", node)
	}

	// The canonical storage taint must be applied so smoke pods stay off.
	updated := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if !hasTaint(updated.Spec.Taints, e2eStorageTaintKey, corev1.TaintEffectNoSchedule) {
		t.Fatalf("storage taint not applied: %#v", updated.Spec.Taints)
	}
}

func TestDriverImageFromEnvRequiresValue(t *testing.T) {
	t.Setenv(e2econfig.Env[e2econfig.DriverImageKey], "")
	if _, err := driverImageFromEnv(); err == nil {
		t.Fatal("expected missing image to fail")
	}

	t.Setenv(e2econfig.Env[e2econfig.DriverImageKey], "registry.example.invalid/zfs-csi:e2e")
	image, err := driverImageFromEnv()
	if err != nil {
		t.Fatalf("expected configured image to pass: %v", err)
	}
	if image != "registry.example.invalid/zfs-csi:e2e" {
		t.Fatalf("unexpected image %q", image)
	}
}

func TestMutableHarnessImagesAlwaysPull(t *testing.T) {
	image := "registry.example.invalid/zfs-csi/zfs-csi:mutable"
	t.Setenv(e2econfig.Env[e2econfig.DriverImageKey], image)
	node := storageNode{Name: "storage-a"}
	pods := []*corev1.Pod{
		nodeCommandPod("e2e", "command", node.Name, image, "true"),
		createOwnerPoolPod("e2e", "pool", node, "tank", "/dev/disk/by-id/virtio-tank-a"),
		ownerPoolPreflightPod("e2e", "preflight", node, "tank"),
	}
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			if container.ImagePullPolicy != corev1.PullAlways {
				t.Errorf("Pod %q container %q pull policy = %q, want %q", pod.Name, container.Name, container.ImagePullPolicy, corev1.PullAlways)
			}
		}
	}
}

func TestPVCObjectUsesRequestedClassAndMode(t *testing.T) {
	pvc := pvcObject("default", "zfs-csi-e2e-nvme", "zfs-tank-nvme", corev1.ReadWriteMany)
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "zfs-tank-nvme" {
		t.Fatalf("unexpected storage class: %#v", pvc.Spec.StorageClassName)
	}
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteMany {
		t.Fatalf("unexpected access modes: %#v", pvc.Spec.AccessModes)
	}
}

// Consumer smoke pods must not tolerate the storage taint, so they stay on
// non-storage workers and exercise genuine guest-to-guest transport.
func TestSmokePodHasNoStorageToleration(t *testing.T) {
	pod := smokePod("default", "consumer", "pvc", "", "true")
	for _, tol := range pod.Spec.Tolerations {
		if tol.Key == e2eStorageTaintKey {
			t.Fatalf("consumer pod must not tolerate the storage taint")
		}
	}
	if pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != "pvc" {
		t.Fatalf("pod volume does not reference the pvc")
	}
}

// A grouped RWX pod requires anti-affinity so writer+reader land on distinct
// nodes (the only honest cross-node RWX proof).
func TestSmokePodGroupAddsAntiAffinity(t *testing.T) {
	pod := smokePod("default", "reader", "pvc", "nfs-rwx", "true")
	aff := pod.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil ||
		len(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("grouped smoke pod missing required pod anti-affinity: %#v", aff)
	}
	term := aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	if term.TopologyKey != corev1.LabelHostname {
		t.Fatalf("anti-affinity topology key = %q, want hostname", term.TopologyKey)
	}
}

func TestResetSmokeObjectsWaitsForPodsBeforeDeletingPVC(t *testing.T) {
	ctx := context.Background()
	pvc := pvcObject("default", "zfs-csi-e2e-nfs", "zfs-tank-nfs", corev1.ReadWriteMany)
	writer := smokePod("default", "zfs-csi-e2e-nfs-writer", pvc.Name, "nfs-rwx", "true")
	reader := smokePod("default", "zfs-csi-e2e-nfs-reader", pvc.Name, "nfs-rwx", "true")
	base := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(pvc, writer, reader).Build()
	c := &delayedPodDeletionClient{Client: base, pendingPods: map[client.ObjectKey]bool{}}

	if err := resetSmokeObjects(ctx, c, pvc, reader, writer); err != nil {
		t.Fatalf("reset smoke objects: %v", err)
	}
	for _, obj := range []client.Object{reader, writer, pvc} {
		if err := base.Get(ctx, client.ObjectKeyFromObject(obj), obj.DeepCopyObject().(client.Object)); !apierrors.IsNotFound(err) {
			t.Fatalf("expected %T %s to be deleted, got %v", obj, client.ObjectKeyFromObject(obj), err)
		}
	}
}

// delayedPodDeletionClient models the live API: pod Delete returns before the
// object disappears, and the PVC rejects deletion while consumers remain.
type delayedPodDeletionClient struct {
	client.Client
	pendingPods map[client.ObjectKey]bool
}

func (c *delayedPodDeletionClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		c.pendingPods[client.ObjectKeyFromObject(obj)] = true
		return nil
	}
	if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok && len(c.pendingPods) > 0 {
		return apierrors.NewConflict(
			schema.GroupResource{Resource: "persistentvolumeclaims"},
			pvc.Name,
			fmt.Errorf("consumer pods still exist"),
		)
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *delayedPodDeletionClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Pod); ok && c.pendingPods[key] {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
		if err := c.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		delete(c.pendingPods, key)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func newSchemeForTest(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	return scheme
}
