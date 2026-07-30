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

package performance

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func PoolEnvironmentFromEnv(name string) (PoolEnvironment, error) {
	get := func(key string) (string, error) {
		v := strings.TrimSpace(os.Getenv(key))
		if v == "" {
			return "", fmt.Errorf("%s is required", key)
		}
		return v, nil
	}
	health, err := get("ZFS_CSI_PERF_POOL_HEALTH")
	if err != nil {
		return PoolEnvironment{}, err
	}
	version, err := get("ZFS_CSI_PERF_ZFS_VERSION")
	if err != nil {
		return PoolEnvironment{}, err
	}
	topology, err := get("ZFS_CSI_PERF_POOL_TOPOLOGY")
	if err != nil {
		return PoolEnvironment{}, err
	}
	free, err := strconv.ParseUint(os.Getenv("ZFS_CSI_PERF_POOL_FREE_BYTES"), 10, 64)
	if err != nil {
		return PoolEnvironment{}, fmt.Errorf("invalid pool free bytes")
	}
	size, err := strconv.ParseUint(os.Getenv("ZFS_CSI_PERF_POOL_SIZE_BYTES"), 10, 64)
	if err != nil {
		return PoolEnvironment{}, fmt.Errorf("invalid pool size bytes")
	}
	frag, err := strconv.Atoi(os.Getenv("ZFS_CSI_PERF_POOL_FRAGMENTATION"))
	if err != nil {
		return PoolEnvironment{}, fmt.Errorf("invalid pool fragmentation")
	}
	p := PoolEnvironment{
		Name:          name,
		Health:        health,
		ZFSVersion:    version,
		Topology:      topology,
		FreeBytes:     free,
		SizeBytes:     size,
		Fragmentation: frag,
	}
	return p, validatePoolEnvironment(p)
}

// CollectEnvironment fingerprints stable Kubernetes facts. Host-only facts are
// supplied by the privileged diagnostics collector and validated here before a
// benchmark can start.
func CollectEnvironment(
	ctx context.Context,
	c client.Client,
	base Environment,
	diagnostic map[string]NodeEnvironment,
	pool PoolEnvironment,
) (Environment, error) {
	var nodes corev1.NodeList
	if err := c.List(ctx, &nodes); err != nil {
		return Environment{}, err
	}
	base.Nodes = nil
	for _, node := range nodes.Items {
		d, ok := diagnostic[node.Name]
		if !ok {
			return Environment{}, fmt.Errorf("missing privileged diagnostic facts for node %s", node.Name)
		}
		d.Name = node.Name
		d.Kernel = node.Status.NodeInfo.KernelVersion
		d.Architecture = node.Status.NodeInfo.Architecture
		d.ContainerRuntime = node.Status.NodeInfo.ContainerRuntimeVersion
		d.CPUs = int(node.Status.Capacity.Cpu().Value())
		d.Labels = map[string]string{"kubernetes.io/hostname": node.Labels["kubernetes.io/hostname"]}
		if err := validateNodeEnvironment(d); err != nil {
			return Environment{}, fmt.Errorf("node %s: %w", node.Name, err)
		}
		base.Nodes = append(base.Nodes, d)
	}
	if err := validatePoolEnvironment(pool); err != nil {
		return Environment{}, err
	}
	base.Pool = pool
	sort.Slice(base.Nodes, func(i, j int) bool { return base.Nodes[i].Name < base.Nodes[j].Name })
	fingerprint, err := Fingerprint(base)
	if err != nil {
		return Environment{}, err
	}
	base.Fingerprint = fingerprint
	return base, nil
}

func validateNodeEnvironment(n NodeEnvironment) error {
	if n.Kernel == "" || n.Architecture == "" || n.ContainerRuntime == "" || n.CPUModel == "" || n.CPUs <= 0 {
		return fmt.Errorf("incomplete kernel/runtime/CPU facts")
	}
	if len(n.NICs) == 0 {
		return fmt.Errorf("no NIC facts")
	}
	for _, nic := range n.NICs {
		if nic.Name == "" || nic.MTU <= 0 || nic.SpeedMbps <= 0 {
			return fmt.Errorf("incomplete NIC facts")
		}
	}
	return nil
}

func validatePoolEnvironment(p PoolEnvironment) error {
	if p.Name == "" || p.Health != "ONLINE" || p.ZFSVersion == "" || p.Topology == "" || p.SizeBytes == 0 ||
		p.FreeBytes == 0 ||
		p.FreeBytes >= p.SizeBytes ||
		p.Fragmentation < 0 ||
		p.Fragmentation > 100 {
		return fmt.Errorf("incomplete or unhealthy ZFS pool facts")
	}
	return nil
}

// DiagnosticPod is intentionally privileged and node-pinned through required
// affinity. It emits a compact fact record; callers parse it with
// ParseDiagnosticFacts and delete the pod before running workloads.
func DiagnosticPod(namespace, name, node, image string) *corev1.Pod {
	privileged := true
	command := `set -eu; nic=$(ls /sys/class/net | grep -v '^lo$' | head -1); mtu=$(cat /sys/class/net/$nic/mtu); speed=$(cat /sys/class/net/$nic/speed); cpu=$(sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -1); printf 'CPU_MODEL=%s\nNIC=%s\nMTU=%s\nSPEED=%s\n' "$cpu" "$nic" "$mtu" "$speed"`
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			Affinity:      requiredNodeAffinity(node),
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "diagnostic",
					Image:           image,
					Command:         []string{"/bin/sh", "-c", command},
					SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10m"),
							corev1.ResourceMemory: resource.MustParse("16Mi"),
						},
					},
				},
			},
		},
	}
}

func ParseDiagnosticFacts(raw string) (NodeEnvironment, error) {
	values := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			values[key] = value
		}
	}
	mtu, err := strconv.Atoi(values["MTU"])
	if err != nil {
		return NodeEnvironment{}, fmt.Errorf("invalid MTU")
	}
	speed, err := strconv.Atoi(values["SPEED"])
	if err != nil {
		return NodeEnvironment{}, fmt.Errorf("invalid NIC speed")
	}
	return NodeEnvironment{
		CPUModel: values["CPU_MODEL"],
		NICs:     []NICEnvironment{{Name: values["NIC"], MTU: mtu, SpeedMbps: speed}},
	}, nil
}
