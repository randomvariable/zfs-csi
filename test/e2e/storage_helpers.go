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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

const (
	// e2eStorageRoleLabel/Value and e2eStorageTaintKey are the canonical
	// storage-node convention shared with the Helm chart
	// (storageNode.selector / storageNode.tolerations): the storage-owning
	// components select this label and tolerate this taint, and nothing else
	// schedules onto the tainted node.
	e2eStorageRoleLabel      = "zfs.csi.randomvariable.co.uk/storage"
	e2eStorageRoleValue      = "true"
	e2eStorageTaintKey       = "zfs.csi.randomvariable.co.uk/storage"
	e2eStorageOwnerLabelKey  = "zfs.csi.randomvariable.co.uk/storage-owner"
	e2eNetworkDomainLabelKey = "topology.zfs.csi.randomvariable.co.uk/network-domain"

	zfsCSINamespace = "zfs-csi"
)

type storageNode struct {
	Name       string
	HostName   string
	PortalHost string
	NFSServer  string
}

type storageOwner struct {
	Name                  string
	MachineDeploymentName string
	MachineName           string
	Node                  storageNode
	NodeSelector          map[string]string
	DataDeviceID          string
	PoolName              string
	PoolGUID              string
	NetworkDomain         string
	ReachableFrom         []string
	NFSPort               int
	NVMePort              int
	OwnershipLabels       map[string]string
}

type ownerMachine struct {
	Name       string
	InstanceID string
	NodeName   string
}

type ownerMachineResolver interface {
	ResolveOwnerMachine(context.Context, client.Client, string, string, e2econfig.StorageOwner) (ownerMachine, error)
}

type ownerDiskResolver interface {
	ResolveDeviceID(context.Context, ownerMachine, e2econfig.StorageOwner) (string, error)
}

type nodeCommandRunner interface {
	Run(context.Context, string, string) (string, error)
}

type kubectlNodeRunner struct {
	kubeconfig string
	namespace  string
	image      string
}

func (r kubectlNodeRunner) Run(ctx context.Context, nodeName, command string) (string, error) {
	name := "zfs-csi-node-check-" + shortResourceSuffix(nodeName)
	if out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", r.kubeconfig, "-n", r.namespace, "delete", "pod", name, "--ignore-not-found=true", "--wait=true", "--timeout=1m").CombinedOutput(); err != nil {
		return "", fmt.Errorf("reset node command pod %q: %w: %s", name, err, out)
	}
	pod := nodeCommandPod(r.namespace, name, nodeName, r.image, command, smokeOwnershipLabels())
	jsonData, err := json.Marshal(pod)
	if err != nil {
		return "", fmt.Errorf("marshal node command pod: %w", err)
	}
	data, err := yaml.JSONToYAML(jsonData)
	if err != nil {
		return "", fmt.Errorf("encode node command pod as YAML: %w", err)
	}
	apply := exec.CommandContext(ctx, "kubectl", "--kubeconfig", r.kubeconfig, "apply", "-f", "-")
	apply.Stdin = bytes.NewReader(data)
	if out, err := apply.CombinedOutput(); err != nil {
		return "", fmt.Errorf("create node command pod: %w: %s", err, out)
	}
	defer exec.CommandContext(context.Background(), "kubectl", "--kubeconfig", r.kubeconfig, "-n", r.namespace, "delete", "pod", name, "--ignore-not-found=true", "--wait=false").Run()
	if out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", r.kubeconfig, "-n", r.namespace, "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/"+name, "--timeout=5m").CombinedOutput(); err != nil {
		logs, _ := exec.CommandContext(ctx, "kubectl", "--kubeconfig", r.kubeconfig, "-n", r.namespace, "logs", name).CombinedOutput()
		return "", fmt.Errorf("node command pod %q failed: %w: %s: %s", name, err, out, logs)
	}
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", r.kubeconfig, "-n", r.namespace, "logs", name).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("read node command pod %q logs: %w: %s", name, err, out)
	}
	return string(out), nil
}

func shortResourceSuffix(value string) string {
	value = strings.ToLower(value)
	value = regexp.MustCompile(`[^a-z0-9-]+`).ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if len(value) > 35 {
		value = value[len(value)-35:]
	}
	return value
}

func nodeCommandPod(namespace, name, nodeName, image, command string, ownershipLabels ...map[string]string) *corev1.Pod {
	labels := map[string]string{
		"app.kubernetes.io/name":      "zfs-csi-e2e",
		"app.kubernetes.io/component": "node-command",
	}
	for _, extra := range ownershipLabels {
		for key, value := range extra {
			labels[key] = value
		}
	}
	return &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeName:      nodeName,
			HostPID:       true,
			Tolerations:   []corev1.Toleration{storageNodeToleration()},
			Volumes:       []corev1.Volume{{Name: "dev", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev"}}}},
			Containers: []corev1.Container{{
				Name:            "command",
				Image:           image,
				ImagePullPolicy: corev1.PullAlways,
				Command:         []string{"sh", "-ceu", command},
				SecurityContext: &corev1.SecurityContext{Privileged: boolPtr(true)},
				VolumeMounts:    []corev1.VolumeMount{{Name: "dev", MountPath: "/dev"}},
			}},
		},
	}
}

func deleteOwnedObject(ctx context.Context, c client.Client, object client.Object, ownershipLabels map[string]string) error {
	if err := c.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	for key, value := range ownershipLabels {
		if object.GetLabels()[key] != value {
			return fmt.Errorf("refusing to delete unowned %T %s: label %s=%q, want %q", object, client.ObjectKeyFromObject(object), key, object.GetLabels()[key], value)
		}
	}
	if err := c.Delete(ctx, object); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

type capiOwnerMachineResolver struct{}

func (capiOwnerMachineResolver) ResolveOwnerMachine(ctx context.Context, mgmt client.Client, namespace, clusterName string, owner e2econfig.StorageOwner) (ownerMachine, error) {
	machines := &clusterv1.MachineList{}
	deploymentName := clusterName + "-" + owner.MachineSuffix
	if err := mgmt.List(ctx, machines,
		client.InNamespace(namespace),
		client.MatchingLabels{
			clusterv1.ClusterNameLabel:           clusterName,
			clusterv1.MachineDeploymentNameLabel: deploymentName,
		},
	); err != nil {
		return ownerMachine{}, fmt.Errorf("list owner %q machines: %w", owner.Name, err)
	}
	if len(machines.Items) != 1 {
		return ownerMachine{}, fmt.Errorf("storage owner %q selector %q matched %d Machines; require exactly one", owner.Name, deploymentName, len(machines.Items))
	}
	machine := machines.Items[0]
	if !machine.Status.NodeRef.IsDefined() {
		return ownerMachine{}, fmt.Errorf("storage owner %q Machine %q has no NodeRef", owner.Name, machine.Name)
	}
	resolved := ownerMachine{Name: machine.Name, NodeName: machine.Status.NodeRef.Name}
	if machine.Spec.InfrastructureRef.IsDefined() && machine.Spec.InfrastructureRef.Kind == "AWSMachine" {
		awsMachine := &unstructured.Unstructured{}
		awsMachine.SetAPIVersion(machine.Spec.InfrastructureRef.APIGroup + "/v1beta2")
		awsMachine.SetKind(machine.Spec.InfrastructureRef.Kind)
		if err := mgmt.Get(ctx, types.NamespacedName{Namespace: namespace, Name: machine.Spec.InfrastructureRef.Name}, awsMachine); err != nil {
			return ownerMachine{}, fmt.Errorf("get AWSMachine for owner %q: %w", owner.Name, err)
		}
		instanceID, found, err := unstructured.NestedString(awsMachine.Object, "spec", "instanceID")
		if err != nil || !found || instanceID == "" {
			return ownerMachine{}, fmt.Errorf("storage owner %q AWSMachine has no spec.instanceID", owner.Name)
		}
		resolved.InstanceID = instanceID
	}
	return resolved, nil
}

// staticNodeResolver resolves a storage owner directly to a workload-cluster
// Node by the owner's nodeSelector labels from the InfrastructureConfig. It is
// the static infrastructure provider's ownerMachineResolver: no CAPI Machine
// objects exist for a pre-existing cluster, so the management-cluster client
// argument is ignored and the owner→node mapping is the explicit label
// selector. downstream discoverOwnerNode re-lists with the same selector and
// asserts the resolved name matches, keeping the exactly-one invariant.
type staticNodeResolver struct {
	workload client.Client
}

func (r staticNodeResolver) ResolveOwnerMachine(ctx context.Context, _ client.Client, _, _ string, owner e2econfig.StorageOwner) (ownerMachine, error) {
	if len(owner.NodeSelector) == 0 {
		return ownerMachine{}, fmt.Errorf("storage owner %q has no nodeSelector; the static provider requires an explicit owner-to-node label mapping", owner.Name)
	}
	nodes := &corev1.NodeList{}
	if err := r.workload.List(ctx, nodes, client.MatchingLabels(owner.NodeSelector)); err != nil {
		return ownerMachine{}, fmt.Errorf("list workload Nodes for owner %q selector %v: %w", owner.Name, owner.NodeSelector, err)
	}
	if len(nodes.Items) != 1 {
		return ownerMachine{}, fmt.Errorf("storage owner %q selector %v matched %d workload Nodes; require exactly one", owner.Name, owner.NodeSelector, len(nodes.Items))
	}
	return ownerMachine{Name: nodes.Items[0].Name, NodeName: nodes.Items[0].Name}, nil
}

type staticDeviceResolver struct{}

func (staticDeviceResolver) ResolveDeviceID(_ context.Context, _ ownerMachine, owner e2econfig.StorageOwner) (string, error) {
	if owner.PoolDeviceID == "" {
		return "", fmt.Errorf("storage owner %q has no configured exact device identity", owner.Name)
	}
	return owner.PoolDeviceID, nil
}

type ec2DescribeInstancesAPI interface {
	DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
}

type awsAttachmentResolver struct {
	ec2 ec2DescribeInstancesAPI
}

func newAWSAttachmentResolver(ctx context.Context) (ownerDiskResolver, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS SDK credentials/config for EBS attachment discovery: %w", err)
	}
	return awsAttachmentResolver{ec2: ec2.NewFromConfig(cfg)}, nil
}

func (r awsAttachmentResolver) ResolveDeviceID(ctx context.Context, machine ownerMachine, owner e2econfig.StorageOwner) (string, error) {
	if machine.InstanceID == "" {
		return "", fmt.Errorf("storage owner %q has no AWS instance ID", owner.Name)
	}
	deviceName := owner.DiskDiscovery.AttachmentDeviceName
	if owner.DiskDiscovery.Provider != "aws-ebs-volume-id" || deviceName == "" {
		return "", fmt.Errorf("storage owner %q lacks AWS EBS attachment discovery binding", owner.Name)
	}
	result, err := r.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{machine.InstanceID}})
	if err != nil {
		return "", fmt.Errorf("describe AWS instance %q for owner %q: %w", machine.InstanceID, owner.Name, err)
	}
	var volumeIDs []string
	for _, reservation := range result.Reservations {
		for _, instance := range reservation.Instances {
			if aws.ToString(instance.InstanceId) != machine.InstanceID {
				continue
			}
			for _, mapping := range instance.BlockDeviceMappings {
				if aws.ToString(mapping.DeviceName) == deviceName && mapping.Ebs != nil {
					volumeIDs = append(volumeIDs, aws.ToString(mapping.Ebs.VolumeId))
				}
			}
		}
	}
	if len(volumeIDs) != 1 {
		return "", fmt.Errorf("storage owner %q instance %q attachment %q resolved %d EBS volumes; require exactly one", owner.Name, machine.InstanceID, deviceName, len(volumeIDs))
	}
	return resolveAWSEBSDeviceID(volumeIDs[0])
}

var canonicalEBSVolumeIDPattern = regexp.MustCompile(`^vol-[0-9a-f]{17}$`)

func resolveAWSEBSDeviceID(volumeID string) (string, error) {
	if !canonicalEBSVolumeIDPattern.MatchString(volumeID) {
		return "", fmt.Errorf("EBS volume ID %q must be canonical vol- followed by 17 lowercase hexadecimal characters", volumeID)
	}
	return "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol" + strings.TrimPrefix(volumeID, "vol-"), nil
}

func resolveStorageOwners(ctx context.Context, mgmt client.Client, workload client.Client, namespace, clusterName string, machineResolver ownerMachineResolver, diskResolver ownerDiskResolver, runner nodeCommandRunner) ([]storageOwner, error) {
	configured, err := e2econfig.StorageOwners()
	if err != nil {
		return nil, err
	}
	resolved := make([]storageOwner, 0, len(configured))
	for _, input := range configured {
		machine, err := machineResolver.ResolveOwnerMachine(ctx, mgmt, namespace, clusterName, input)
		if err != nil {
			return nil, err
		}
		node, err := discoverOwnerNode(ctx, workload, machine.NodeName, input.NodeSelector)
		if err != nil {
			return nil, err
		}
		deviceID, err := diskResolver.ResolveDeviceID(ctx, machine, input)
		if err != nil {
			return nil, err
		}
		if err := verifyExactDevice(ctx, runner, node.Name, deviceID); err != nil {
			return nil, fmt.Errorf("storage owner %q: %w", input.Name, err)
		}
		resolved = append(resolved, storageOwner{
			Name:                  input.Name,
			MachineDeploymentName: clusterName + "-" + input.MachineSuffix,
			MachineName:           machine.Name,
			Node:                  node,
			NodeSelector:          input.NodeSelector,
			DataDeviceID:          deviceID,
			PoolName:              input.PoolName,
			NetworkDomain:         input.NetworkDomain,
			ReachableFrom:         input.ReachableFrom,
			NFSPort:               input.NFSPort,
			NVMePort:              input.NVMePort,
		})
	}
	if err := validateResolvedStorageOwners(resolved); err != nil {
		return nil, err
	}
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].Name < resolved[j].Name })
	return resolved, nil
}

func validateResolvedStorageOwners(owners []storageOwner) error {
	nodes := map[string]string{}
	devices := map[string]string{}
	for _, owner := range owners {
		if owner.Node.Name == "" || owner.DataDeviceID == "" {
			return fmt.Errorf("storage owner %q lacks resolved Node or exact device identity", owner.Name)
		}
		if previous := nodes[owner.Node.Name]; previous != "" {
			return fmt.Errorf("storage owners %q and %q resolve to the same Node %q", previous, owner.Name, owner.Node.Name)
		}
		nodes[owner.Node.Name] = owner.Name
		if previous := devices[owner.DataDeviceID]; previous != "" {
			return fmt.Errorf("storage owners %q and %q resolve to the same device identity %q", previous, owner.Name, owner.DataDeviceID)
		}
		devices[owner.DataDeviceID] = owner.Name
	}
	return nil
}

func discoverOwnerNode(ctx context.Context, c client.Client, nodeName string, selector map[string]string) (storageNode, error) {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes, client.MatchingLabels(selector)); err != nil {
		return storageNode{}, fmt.Errorf("list nodes for selector %v: %w", selector, err)
	}
	if len(nodes.Items) != 1 {
		return storageNode{}, fmt.Errorf("owner selector %v matched %d Nodes; require exactly one", selector, len(nodes.Items))
	}
	if nodes.Items[0].Name != nodeName {
		return storageNode{}, fmt.Errorf("owner selector %v resolved Node %q, but CAPI Machine references %q", selector, nodes.Items[0].Name, nodeName)
	}
	node := nodes.Items[0]
	host := node.Labels[corev1.LabelHostname]
	if host == "" {
		host = node.Name
	}
	ip, err := nodeInternalIP(node)
	if err != nil {
		return storageNode{}, err
	}
	return storageNode{Name: node.Name, HostName: host, PortalHost: ip, NFSServer: ip}, nil
}

func verifyExactDevice(ctx context.Context, runner nodeCommandRunner, nodeName, deviceID string) error {
	if runner == nil {
		return fmt.Errorf("no node command runner configured for exact device verification")
	}
	output, err := runner.Run(ctx, nodeName, "test -b "+shellQuote(deviceID)+" && readlink -f "+shellQuote(deviceID))
	if err != nil {
		return fmt.Errorf("verify exact device %q on Node %q: %w", deviceID, nodeName, err)
	}
	lines := strings.Fields(output)
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "/") {
		return fmt.Errorf("exact device %q on Node %q resolved ambiguously: %q", deviceID, nodeName, output)
	}
	return nil
}

func storagePoolFromEnv() string {
	return e2econfig.PoolName()
}

func preflightImageFromEnv() string {
	return e2econfig.PreflightImageRef()
}

func driverImageFromEnv() (string, error) {
	image := e2econfig.DriverImageRef()
	if image == "" {
		return "", fmt.Errorf("%s must point at the libzfs-enabled E2E image", e2econfig.Env[e2econfig.DriverImageKey])
	}

	return image, nil
}

// storageMachineDeploymentName is the storage MachineDeployment suffix in the
// zfs-csi CAPK flavor (<cluster>-storage).
func storageMachineDeploymentName(clusterName string) string {
	return clusterName + "-storage"
}

// discoverStorageNodeName finds the workload node backing the storage
// MachineDeployment. It lists Machines on the MANAGEMENT cluster in the per-run
// namespace matching the cluster and storage deployment labels, asserts exactly
// one, and returns its NodeRef name. This replaces any hardcoded node name so
// the harness works against a freshly provisioned CAPK cluster.
func discoverStorageNodeName(ctx context.Context, mgmt client.Client, namespace, clusterName string) (string, error) {
	machines := &clusterv1.MachineList{}
	if err := mgmt.List(ctx, machines,
		client.InNamespace(namespace),
		client.MatchingLabels{
			clusterv1.ClusterNameLabel:           clusterName,
			clusterv1.MachineDeploymentNameLabel: storageMachineDeploymentName(clusterName),
		},
	); err != nil {
		return "", fmt.Errorf("list storage machines: %w", err)
	}
	if len(machines.Items) != 1 {
		return "", fmt.Errorf("expected exactly one storage machine for %s, got %d", storageMachineDeploymentName(clusterName), len(machines.Items))
	}
	nodeRef := machines.Items[0].Status.NodeRef
	if !nodeRef.IsDefined() {
		return "", fmt.Errorf("storage machine %q has no NodeRef yet", machines.Items[0].Name)
	}
	return nodeRef.Name, nil
}

func applyStorageRole(ctx context.Context, c client.Client, nodeName string) error {
	node := &corev1.Node{}
	if err := c.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return fmt.Errorf("get storage node %q: %w", nodeName, err)
	}
	base := node.DeepCopy()
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels[e2eStorageRoleLabel] = e2eStorageRoleValue
	if !hasTaint(node.Spec.Taints, e2eStorageTaintKey, corev1.TaintEffectNoSchedule) {
		node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
			Key:    e2eStorageTaintKey,
			Value:  e2eStorageRoleValue,
			Effect: corev1.TaintEffectNoSchedule,
		})
	}

	if err := c.Patch(ctx, node, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("label storage node %q: %w", nodeName, err)
	}

	return nil
}

func applyOwnerRolesAndConsumerDomains(ctx context.Context, c client.Client, owners []storageOwner, workers []e2econfig.ConsumerWorker) error {
	for _, owner := range owners {
		node := &corev1.Node{}
		if err := c.Get(ctx, types.NamespacedName{Name: owner.Node.Name}, node); err != nil {
			return fmt.Errorf("get storage owner %q Node %q: %w", owner.Name, owner.Node.Name, err)
		}
		base := node.DeepCopy()
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		for key, value := range owner.NodeSelector {
			if existing := node.Labels[key]; existing != "" && existing != value {
				return fmt.Errorf("storage owner %q Node %q label %q conflicts: have %q, want %q", owner.Name, node.Name, key, existing, value)
			}
			node.Labels[key] = value
		}
		node.Labels[e2eStorageRoleLabel] = e2eStorageRoleValue
		if existing := node.Labels[e2eNetworkDomainLabelKey]; existing != "" && existing != owner.NetworkDomain {
			return fmt.Errorf("storage owner %q Node %q has conflicting %s=%q, want %q", owner.Name, node.Name, e2eNetworkDomainLabelKey, existing, owner.NetworkDomain)
		}
		node.Labels[e2eNetworkDomainLabelKey] = owner.NetworkDomain
		if !hasTaint(node.Spec.Taints, e2eStorageTaintKey, corev1.TaintEffectNoSchedule) {
			node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: e2eStorageTaintKey, Value: e2eStorageRoleValue, Effect: corev1.TaintEffectNoSchedule})
		}
		if err := c.Patch(ctx, node, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("patch storage owner %q Node %q: %w", owner.Name, node.Name, err)
		}
	}
	for _, worker := range workers {
		nodes, err := consumerWorkerNodes(ctx, c, worker)
		if err != nil {
			return err
		}
		// Validate every node before patching any. This keeps topology conflicts
		// fail-closed instead of leaving a partially restored group.
		for i := range nodes {
			node := &nodes[i]
			if existing := node.Labels[e2eNetworkDomainLabelKey]; existing != "" && existing != worker.NetworkDomain {
				return fmt.Errorf("consumer Node %q has conflicting %s=%q, want %q", node.Name, e2eNetworkDomainLabelKey, existing, worker.NetworkDomain)
			}
			for key, value := range worker.NodeSelector {
				if existing := node.Labels[key]; existing != "" && existing != value {
					return fmt.Errorf("consumer worker %q Node %q label %q conflicts: have %q, want %q", worker.Name, node.Name, key, existing, value)
				}
			}
		}
		for i := range nodes {
			node := &nodes[i]
			base := node.DeepCopy()
			if node.Labels == nil {
				node.Labels = map[string]string{}
			}
			for key, value := range worker.NodeSelector {
				node.Labels[key] = value
			}
			node.Labels[e2eNetworkDomainLabelKey] = worker.NetworkDomain
			if err := c.Patch(ctx, node, client.MergeFrom(base)); err != nil {
				return fmt.Errorf("label consumer Node %q: %w", node.Name, err)
			}
		}
	}
	// Control-plane nodes also run the node DaemonSet. Give nodes outside explicit
	// owner/consumer groups the sole configured domain when it is unambiguous.
	domains := map[string]struct{}{}
	for _, owner := range owners {
		domains[owner.NetworkDomain] = struct{}{}
	}
	for _, worker := range workers {
		domains[worker.NetworkDomain] = struct{}{}
	}
	if len(domains) == 1 {
		var domain string
		for domain = range domains {
		}
		nodes := &corev1.NodeList{}
		if err := c.List(ctx, nodes); err != nil {
			return fmt.Errorf("list Nodes for default network domain: %w", err)
		}
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if node.Labels[e2eNetworkDomainLabelKey] != "" {
				continue
			}
			base := node.DeepCopy()
			if node.Labels == nil {
				node.Labels = map[string]string{}
			}
			node.Labels[e2eNetworkDomainLabelKey] = domain
			if err := c.Patch(ctx, node, client.MergeFrom(base)); err != nil {
				return fmt.Errorf("label Node %q with sole network domain: %w", node.Name, err)
			}
		}
	}
	return nil
}

func consumerWorkerNodes(ctx context.Context, c client.Client, worker e2econfig.ConsumerWorker) ([]corev1.Node, error) {
	if len(worker.NodeNames) > 0 {
		if len(worker.NodeNames) != worker.Replicas {
			return nil, fmt.Errorf("consumer worker group %q explicitly names %d nodes, want %d", worker.Name, len(worker.NodeNames), worker.Replicas)
		}
		nodes := make([]corev1.Node, 0, len(worker.NodeNames))
		seen := map[string]struct{}{}
		for _, name := range worker.NodeNames {
			if _, exists := seen[name]; exists {
				return nil, fmt.Errorf("consumer worker group %q explicitly names Node %q more than once", worker.Name, name)
			}
			seen[name] = struct{}{}
			node := &corev1.Node{}
			if err := c.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
				return nil, fmt.Errorf("get explicitly named consumer Node %q for group %q: %w", name, worker.Name, err)
			}
			nodes = append(nodes, *node)
		}
		return nodes, nil
	}
	nodeList := &corev1.NodeList{}
	if err := c.List(ctx, nodeList, client.MatchingLabels(worker.NodeSelector)); err != nil {
		return nil, fmt.Errorf("list consumer worker group %q: %w", worker.Name, err)
	}
	if len(nodeList.Items) != worker.Replicas {
		return nil, fmt.Errorf("consumer worker group %q selector %v matched %d Nodes, want %d", worker.Name, worker.NodeSelector, len(nodeList.Items), worker.Replicas)
	}
	return nodeList.Items, nil
}

func discoverStorageNode(ctx context.Context, c client.Client) (storageNode, error) {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes, client.MatchingLabels{e2eStorageRoleLabel: e2eStorageRoleValue}); err != nil {
		return storageNode{}, fmt.Errorf("list storage nodes: %w", err)
	}
	if len(nodes.Items) != 1 {
		return storageNode{}, fmt.Errorf("expected exactly one storage node with %s=%s, got %d", e2eStorageRoleLabel, e2eStorageRoleValue, len(nodes.Items))
	}

	node := nodes.Items[0]
	host := node.Labels[corev1.LabelHostname]
	if host == "" {
		host = node.Name
	}

	// The NVMe-TCP portal and NFS server must be an IP reachable cluster-wide,
	// not the hostname (which consumer nodes may not resolve).
	ip, err := nodeInternalIP(node)
	if err != nil {
		return storageNode{}, err
	}

	return storageNode{Name: node.Name, HostName: host, PortalHost: ip, NFSServer: ip}, nil
}

// nodeInternalIP returns the node's InternalIP address.
func nodeInternalIP(node corev1.Node) (string, error) {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
			return addr.Address, nil
		}
	}

	return "", fmt.Errorf("storage node %q has no InternalIP address", node.Name)
}

// storageNodeToleration tolerates the canonical storage-node taint. Only pods
// that must run on the storage node (pool preflight/prep) use it; consumer
// smoke pods deliberately omit it so they stay on non-storage workers and
// exercise genuine guest-to-guest NVMe-TCP / NFS.
func storageNodeToleration() corev1.Toleration {
	return corev1.Toleration{
		Key:      e2eStorageTaintKey,
		Operator: corev1.TolerationOpEqual,
		Value:    e2eStorageRoleValue,
		Effect:   corev1.TaintEffectNoSchedule,
	}
}

func hasTaint(taints []corev1.Taint, key string, effect corev1.TaintEffect) bool {
	for _, taint := range taints {
		if taint.Key == key && taint.Effect == effect {
			return true
		}
	}

	return false
}

// dataDiskByID is the deterministic device path for the storage node's ZFS pool
// disk. Provider-specific: KubeVirt uses the virtio-serial data disk
// (/dev/disk/by-id/virtio-tank0); AWS uses the EBS/NVMe by-id of the dedicated
// gp3 volume. Resolved at call time via e2econfig.DataDiskByID() so the lane is
// selected by E2E_DATA_DISK_BY_ID without touching this helper.

// createPoolPod creates the ZFS pool on the storage node as an E2E setup action.
// In production the pool is externally managed and the CSI driver never creates
// pools; but in E2E the harness owns the whole substrate, so it must create the
// pool on the golden-image storage node before the driver runs. It is
// idempotent (a re-run finds the pool already imported) and pinned to the
// storage node, privileged, with host access to load zfs and open the disk.
func createPoolPod(namespace string, node storageNode, pool string) *corev1.Pod {
	return createOwnerPoolPod(namespace, "zfs-csi-tank-create", node, pool, e2econfig.DataDiskByID())
}

func createOwnerPoolPod(namespace, name string, node storageNode, pool, deviceID string) *corev1.Pod {
	// Everything runs in the HOST namespaces via nsenter -t 1 (the pod is
	// HostPID:true). Two reasons:
	//   - zpool create: on NVMe/EBS disks zpool writes a GPT then opens the new
	//     partition node immediately; the container namespace has no udev, so the
	//     partition device (/dev/nvme1n1p1) is not created in time and zpool fails
	//     "failed to detect device partitions ... 19" (ENODEV). The host has real
	//     udev and the node's own zfsutils.
	//   - modprobe: the preflight/driver image (ubuntu runtime) ships zfsutils but
	//     not kmod, so an in-container `modprobe` is "not found". The host has both
	//     kmod and the module, and nsenter'ing it loads into the shared kernel.
	// The pool lives in the shared host kernel, so the in-container driver still
	// uses it.
	script := fmt.Sprintf(
		"nsenter -t 1 -m -u -i -n modprobe zfs && "+
			"if nsenter -t 1 -m -u -i -n zpool list -H %[1]s >/dev/null 2>&1; then echo pool %[1]s already exists; "+
			"else nsenter -t 1 -m -u -i -n zpool create -f %[1]s %[2]s && echo created pool %[1]s; fi && "+
			"nsenter -t 1 -m -u -i -n zpool status -x %[1]s",
		shellQuote(pool), shellQuote(deviceID),
	)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "zfs-csi-e2e",
				"app.kubernetes.io/component": "zpool-create",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeName:      node.Name,
			HostPID:       true,
			Tolerations:   []corev1.Toleration{storageNodeToleration()},
			Volumes: []corev1.Volume{
				{Name: "dev", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev"}}},
				{Name: "modules", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/lib/modules"}}},
			},
			Containers: []corev1.Container{{
				Name:            "zpool",
				Image:           preflightImageFromEnv(),
				ImagePullPolicy: corev1.PullAlways,
				Command:         []string{"sh", "-ceu", script},
				SecurityContext: &corev1.SecurityContext{
					Privileged: boolPtr(true),
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "dev", MountPath: "/dev"},
					{Name: "modules", MountPath: "/lib/modules", ReadOnly: true},
				},
			}},
		},
	}
}

func poolPreflightPod(namespace string, node storageNode, pool string) *corev1.Pod {
	return ownerPoolPreflightPod(namespace, "zfs-csi-tank-preflight", node, pool)
}

func ownerPoolPreflightPod(namespace, name string, node storageNode, pool string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "zfs-csi-e2e",
				"app.kubernetes.io/component": "zpool-preflight",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeName:      node.Name,
			// Pinned to the storage node by NodeName; tolerates the storage
			// taint so it can land there.
			Tolerations: []corev1.Toleration{storageNodeToleration()},
			Containers: []corev1.Container{{
				Name:            "zpool",
				Image:           preflightImageFromEnv(),
				ImagePullPolicy: corev1.PullAlways,
				Command:         []string{"sh", "-ceu", fmt.Sprintf("zpool list -H %s >/dev/null && zpool status -x %s", shellQuote(pool), shellQuote(pool))},
				SecurityContext: &corev1.SecurityContext{
					Privileged: boolPtr(true),
				},
			}},
		},
	}
}

func prepareOwnerPools(ctx context.Context, c client.Client, namespace string, owners []storageOwner, runner nodeCommandRunner, ownershipLabels map[string]string) (err error) {
	for i := range owners {
		owner := &owners[i]
		for _, name := range []string{"zfs-csi-pool-create-" + shortResourceSuffix(owner.Name), "zfs-csi-pool-preflight-" + shortResourceSuffix(owner.Name)} {
			defer func(name string) {
				cleanupErr := deleteOwnedObject(context.Background(), c, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}, ownershipLabels)
				if cleanupErr != nil {
					err = errors.Join(err, cleanupErr)
				}
			}(name)
		}
	}
	for i := range owners {
		owner := &owners[i]
		nameSuffix := shortResourceSuffix(owner.Name)
		create := createOwnerPoolPod(namespace, "zfs-csi-pool-create-"+nameSuffix, owner.Node, owner.PoolName, owner.DataDeviceID)
		setObjectLabels(create, ownershipLabels)
		if err := deleteOwnedObject(ctx, c, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: create.Name, Namespace: namespace}}, ownershipLabels); err != nil {
			return err
		}
		if err := c.Create(ctx, create); err != nil {
			return fmt.Errorf("create pool pod for owner %q: %w", owner.Name, err)
		}
		if err := waitForPodSucceeded(ctx, c, types.NamespacedName{Namespace: namespace, Name: create.Name}, 5*time.Minute); err != nil {
			return err
		}
		if err := c.Delete(ctx, create); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete pool pod for owner %q: %w", owner.Name, err)
		}
		preflight := ownerPoolPreflightPod(namespace, "zfs-csi-pool-preflight-"+nameSuffix, owner.Node, owner.PoolName)
		setObjectLabels(preflight, ownershipLabels)
		if err := deleteOwnedObject(ctx, c, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: preflight.Name, Namespace: namespace}}, ownershipLabels); err != nil {
			return err
		}
		if err := c.Create(ctx, preflight); err != nil {
			return fmt.Errorf("create pool preflight pod for owner %q: %w", owner.Name, err)
		}
		if err := waitForPodSucceeded(ctx, c, types.NamespacedName{Namespace: namespace, Name: preflight.Name}, 5*time.Minute); err != nil {
			return err
		}
		if err := c.Delete(ctx, preflight); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete pool preflight pod for owner %q: %w", owner.Name, err)
		}
		if runner == nil {
			return fmt.Errorf("storage owner %q has no node command runner for pool GUID discovery", owner.Name)
		}
		output, err := runner.Run(ctx, owner.Node.Name, "zpool get -Hp -o value guid "+shellQuote(owner.PoolName))
		if err != nil {
			return fmt.Errorf("discover pool GUID for storage owner %q: %w", owner.Name, err)
		}
		guid := strings.TrimSpace(output)
		if !regexp.MustCompile(`^[1-9][0-9]{0,19}$`).MatchString(guid) {
			return fmt.Errorf("storage owner %q returned non-canonical pool GUID %q", owner.Name, guid)
		}
		owner.PoolGUID = guid
	}
	return nil
}

func setObjectLabels(object client.Object, labels map[string]string) {
	merged := object.GetLabels()
	if merged == nil {
		merged = map[string]string{}
	}
	for key, value := range labels {
		merged[key] = value
	}
	object.SetLabels(merged)
}

// smokeGroupLabel groups the RWX writer+reader pods so required pod
// anti-affinity schedules them onto two distinct nodes.
const smokeGroupLabel = "zfs.csi.randomvariable.co.uk/smoke-group"

// smokeOwnershipLabels returns the run ownership labels so every smoke object
// (PVC, consumer pod) is reachable by the label-scoped static-lane cleanup.
// The run ID is read from config — the single source of truth also used by the
// PodCertificate acceptance path — so builders need no signature change.
func smokeOwnershipLabels() map[string]string {
	return e2eOwnershipLabels(e2econfig.RunID())
}

// pvcObject builds a smoke PVC with the given access mode.
func pvcObject(namespace, name, storageClass string, mode corev1.PersistentVolumeAccessMode) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: smokeOwnershipLabels()},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{mode},
			StorageClassName: &storageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
}

// smokePod builds a consumer pod that mounts claimName at /data and runs cmd.
// It carries no storage-node toleration, so it stays on a non-storage worker
// (the storage node is tainted NoSchedule) and exercises genuine guest-to-guest
// NVMe-TCP / NFS. When group is non-empty, required pod anti-affinity forces
// pods in the same group onto distinct nodes.
func smokePod(namespace, name, claimName, group, cmd string) *corev1.Pod {
	labels := map[string]string{"app.kubernetes.io/name": "zfs-csi-e2e"}
	for key, value := range smokeOwnershipLabels() {
		labels[key] = value
	}
	if group != "" {
		labels[smokeGroupLabel] = group
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeSelector:  smokeConsumerNodeSelector(),
			Containers: []corev1.Container{{
				Name:         "consumer",
				Image:        "busybox:1.37",
				Command:      []string{"sh", "-ceu", cmd},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName}},
			}},
		},
	}
	if group != "" {
		pod.Spec.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{smokeGroupLabel: group}},
					TopologyKey:   corev1.LabelHostname,
				}},
			},
		}
	}

	return pod
}

// writeProofCmd writes message to /data/proof, verifies it, then holds the
// mount open so a concurrent reader on another node can read it (RWX).
func writeProofCmd(message string) string {
	return fmt.Sprintf("printf %%s %s > /data/proof && test \"$(cat /data/proof)\" = %s && sleep 600", shellQuote(message), shellQuote(message))
}

// readProofCmd polls /data/proof until it equals message, then exits (RWX
// reader proving it sees the writer's bytes while both mounts are live).
func readProofCmd(message string) string {
	return fmt.Sprintf(
		"for i in $(seq 1 120); do [ \"$(cat /data/proof 2>/dev/null)\" = %s ] && exit 0; sleep 2; done; echo timed out waiting for proof; exit 1",
		shellQuote(message),
	)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

// smokeConsumerNodeSelector pins smoke consumer pods to the first consumer
// worker group on the static provider. Static lanes share the cluster: the
// node-plugin DaemonSet is restricted to consumer nodes, so a smoke pod that
// lands on any other untainted node can never mount. CAPI lanes return nil
// (the DaemonSet runs on every node there). The config was already validated
// in BeforeAll, so a resolution error degrades to nil rather than failing
// pod construction.
func smokeConsumerNodeSelector() map[string]string {
	if e2econfig.InfrastructureProvider() != "static" {
		return nil
	}
	workers, err := e2econfig.ConsumerWorkers()
	if err != nil || len(workers) == 0 || len(workers[0].NodeSelector) == 0 {
		return nil
	}
	selector := make(map[string]string, len(workers[0].NodeSelector))
	for key, value := range workers[0].NodeSelector {
		selector[key] = value
	}
	return selector
}

func boolPtr(value bool) *bool { return &value }

func deleteIfExists(ctx context.Context, c client.Client, obj client.Object) error {
	err := c.Delete(ctx, obj)
	if err == nil || apierrors.IsNotFound(err) {
		return nil
	}

	return err
}

func waitForPodSucceeded(ctx context.Context, c client.Client, key types.NamespacedName, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pod := &corev1.Pod{}
		if err := c.Get(ctx, key, pod); err != nil {
			if apierrors.IsNotFound(err) {
				time.Sleep(2 * time.Second)
				continue
			}
			return err
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			// Pods with TerminationMessageFallbackToLogsOnError carry the failing
			// command output in the terminated message — surface it so failures
			// name the actual broken command rather than a bare "pod failed".
			for _, status := range pod.Status.ContainerStatuses {
				if t := status.State.Terminated; t != nil && t.Message != "" {
					return fmt.Errorf("pod %s/%s failed: %s", key.Namespace, key.Name, t.Message)
				}
			}
			return fmt.Errorf("pod %s/%s failed", key.Namespace, key.Name)
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timed out waiting for pod %s/%s to succeed", key.Namespace, key.Name)
}
