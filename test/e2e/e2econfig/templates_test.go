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

package e2econfig

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestTwoOwnerProviderTemplates(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		machineKind string
		deviceIDs   []string
	}{
		{name: "kubevirt", path: filepath.Join("..", "kubevirt", "cluster-template-zfs-csi.yaml"), machineKind: "KubevirtMachineTemplate", deviceIDs: []string{"tank-a", "tank-b"}},
		{name: "aws", path: filepath.Join("..", "aws", "cluster-template-zfs-csi-aws.yaml"), machineKind: "AWSMachineTemplate", deviceIDs: []string{"/dev/xvdb", "/dev/xvdb"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			docs, body := readTemplateDocuments(t, tc.path)
			fixtureDocs, _ := readTemplateDocuments(t, filepath.Join("..", "data", "infrastructure-"+tc.name, "two-owner.yaml"))
			fixture := fixtureDocs[0]
			for i, suffix := range []string{"storage-a", "storage-b"} {
				name := "${CLUSTER_NAME}-" + suffix
				md := requireDocument(t, docs, "MachineDeployment", name)
				if got := nestedString(md, "spec", "template", "spec", "bootstrap", "configRef", "name"); got != name {
					t.Fatalf("%s bootstrap ref = %q", name, got)
				}
				if got := nestedString(md, "spec", "template", "spec", "infrastructureRef", "name"); got != name {
					t.Fatalf("%s infrastructure ref = %q", name, got)
				}
				machine := requireDocument(t, docs, tc.machineKind, name)
				machineYAML, err := yaml.Marshal(machine)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(machineYAML, []byte(tc.deviceIDs[i])) {
					t.Fatalf("%s lacks stable pool device identity %q", name, tc.deviceIDs[i])
				}
				bootstrap := requireDocument(t, docs, "KubeadmConfigTemplate", name)
				bootstrapYAML, err := yaml.Marshal(bootstrap)
				if err != nil {
					t.Fatal(err)
				}
				for _, want := range []string{"storage-owner=" + suffix, "storage=true:NoSchedule"} {
					if !bytes.Contains(bootstrapYAML, []byte(want)) {
						t.Fatalf("%s bootstrap lacks %q", name, want)
					}
				}
			}
			if strings.Contains(string(body), "poolGUID:") || strings.Contains(string(body), "authoritativePoolGUID") {
				t.Fatal("provider template must not hardcode pool GUID identity")
			}
			if !strings.Contains(string(body), "zfs-csi.randomvariable.co.uk/run-id") {
				t.Fatal("provider template lacks run ownership labels")
			}
			owners := nestedSlice(fixture, "spec", "storageOwners")
			if len(owners) != 2 {
				t.Fatalf("configured storage owners = %d, want 2", len(owners))
			}
			for _, item := range owners {
				owner := item.(map[string]any)
				suffix := nestedString(owner, "machineDeploymentSuffix")
				md := requireDocument(t, docs, "MachineDeployment", "${CLUSTER_NAME}-"+suffix)
				for key, value := range nestedStringMap(owner, "nodeSelector") {
					if got := nestedString(md, "spec", "template", "metadata", "labels", key); got != value {
						t.Fatalf("owner %q selector %s=%q, rendered MachineDeployment has %q", suffix, key, value, got)
					}
				}
			}
			workers := nestedSlice(fixture, "spec", "consumerWorkers")
			if len(workers) != 1 {
				t.Fatalf("configured consumer groups = %d, want 1", len(workers))
			}
			worker := workers[0].(map[string]any)
			workerSuffix := nestedString(worker, "machineDeploymentSuffix")
			workerName := nestedString(worker, "name")
			workerMDName := "${CLUSTER_NAME}-" + workerSuffix
			workerMD := requireDocument(t, docs, "MachineDeployment", workerMDName)
			if got := nestedString(workerMD, "spec", "template", "spec", "bootstrap", "configRef", "name"); got != workerMDName {
				t.Fatalf("consumer group %q bootstrap ref = %q, want %q", workerName, got, workerMDName)
			}
			if got := nestedString(workerMD, "spec", "template", "spec", "infrastructureRef", "name"); got != workerMDName {
				t.Fatalf("consumer group %q infrastructure ref = %q, want %q", workerName, got, workerMDName)
			}
			workerBootstrap := requireDocument(t, docs, "KubeadmConfigTemplate", workerMDName)
			workerYAML, err := yaml.Marshal(workerBootstrap)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range nestedStringMap(worker, "nodeSelector") {
				if !bytes.Contains(workerYAML, []byte(key+"="+value)) {
					t.Fatalf("consumer group %q bootstrap lacks selector %s=%s", workerName, key, value)
				}
			}
			requireDocument(t, docs, tc.machineKind, workerMDName)
		})
	}
}

func TestKubeVirtTemplatePreservesPasstAndDeterministicSerials(t *testing.T) {
	_, body := readTemplateDocuments(t, filepath.Join("..", "kubevirt", "cluster-template-zfs-csi.yaml"))
	text := string(body)
	if strings.Contains(text, "masquerade:") {
		t.Fatal("KubeVirt template regressed to masquerade binding")
	}
	if got := strings.Count(text, "bridge: {}"); got < 4 {
		t.Fatalf("KubeVirt passt bridge bindings = %d, want at least 4", got)
	}
	for _, serial := range []string{"serial: tank-a", "serial: tank-b"} {
		if strings.Count(text, serial) != 1 {
			t.Fatalf("KubeVirt deterministic disk serial %q missing or duplicated", serial)
		}
	}
	for _, command := range []string{
		"${REGISTRY_MIRROR_IP} ${REGISTRY_MIRROR_HOST}",
	} {
		if !strings.Contains(text, command) {
			t.Fatalf("KubeVirt bootstrap lacks required management fabric command %q", command)
		}
	}
	if !strings.Contains(text, "mirror_registry=${REGISTRY_MIRROR_HOST}/${REGISTRY_MIRROR_PATH}") {
		t.Fatal("KubeVirt bootstrap does not pre-pull Kubernetes images from the configured mirror")
	}
}

func TestAWSTemplateUsesSupportedDualStackAndRunOwnedGP3Volumes(t *testing.T) {
	_, body := readTemplateDocuments(t, filepath.Join("..", "aws", "cluster-template-zfs-csi-aws.yaml"))
	text := string(body)
	for _, want := range []string{
		"targetGroupIPType: ipv6",
		"ipv6: {}",
		"assignPrimaryIPv6: enabled",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("AWS template lacks supported CAPA dual-stack input %q", want)
		}
	}
	if strings.Contains(text, "loadBalancerIPAddressType:") {
		t.Fatal("AWS template contains unsupported CAPA v2.11.1 loadBalancerIPAddressType field")
	}
	if got := strings.Count(text, "type: gp3"); got < 6 {
		t.Fatalf("gp3 volume declarations = %d, want roots plus two owner pool volumes", got)
	}
	for _, owner := range []string{"zfs-csi-e2e-owner: storage-a", "zfs-csi-e2e-owner: storage-b"} {
		if !strings.Contains(text, owner) {
			t.Fatalf("AWS owner machine lacks propagated ownership tag %q", owner)
		}
	}
	if strings.Contains(text, "type: gp3\n          tags:") {
		t.Fatal("CAPA Volume has no tags field; ownership tags must be AWSMachine additionalTags")
	}
}

func TestAWSTemplateEnablesPodCertificateRequestEverywhere(t *testing.T) {
	docs, _ := readTemplateDocuments(t, filepath.Join("..", "aws", "cluster-template-zfs-csi-aws.yaml"))
	controlPlane := requireDocument(t, docs, "KubeadmControlPlane", "${CLUSTER_NAME}-control-plane")
	controlPlaneYAML, err := yaml.Marshal(controlPlane)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"PodCertificateRequest=true",
		"certificates.k8s.io/v1beta1/podcertificaterequests=true",
		"controllerManager:",
	} {
		if !bytes.Contains(controlPlaneYAML, []byte(want)) {
			t.Fatalf("AWS control plane lacks PodCertificateRequest setting %q", want)
		}
	}

	for _, suffix := range []string{"md-0", "storage-a", "storage-b"} {
		bootstrap := requireDocument(t, docs, "KubeadmConfigTemplate", "${CLUSTER_NAME}-"+suffix)
		bootstrapYAML, err := yaml.Marshal(bootstrap)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(bootstrapYAML, []byte("PodCertificateRequest=true")) {
			t.Fatalf("AWS %s kubelet lacks PodCertificateRequest gate", suffix)
		}
	}
}

func TestAWSStorageBootstrapsFailClosedWithoutNFSTLSRuntime(t *testing.T) {
	docs, _ := readTemplateDocuments(t, filepath.Join("..", "aws", "cluster-template-zfs-csi-aws.yaml"))
	for _, suffix := range []string{"storage-a", "storage-b"} {
		bootstrap := requireDocument(t, docs, "KubeadmConfigTemplate", "${CLUSTER_NAME}-"+suffix)
		bootstrapYAML, err := yaml.Marshal(bootstrap)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{
			"modprobe tls",
			"/sys/module/tls/coresize",
			"genl ctrl list",
			"svc_tcp_handshake",
		} {
			if !bytes.Contains(bootstrapYAML, []byte(want)) {
				t.Fatalf("AWS %s bootstrap lacks NFS TLS prerequisite %q", suffix, want)
			}
		}
		if bytes.Contains(bootstrapYAML, []byte("modprobe tls || true")) {
			t.Fatalf("AWS %s bootstrap ignores TLS module failure", suffix)
		}
	}
}

func TestTopologyContractDescribesTwoOwnersAndFourConsumers(t *testing.T) {
	docs, body := readTemplateDocuments(t, filepath.Join("..", "topology.yaml"))
	if len(docs) != 1 {
		t.Fatalf("topology documents = %d, want 1", len(docs))
	}
	text := string(body)
	for _, want := range []string{
		"name: storage-a",
		"name: storage-b",
		"deviceID: /dev/disk/by-id/virtio-tank-a",
		"deviceID: /dev/disk/by-id/virtio-tank-b",
		"poolGUID: dynamic-zpool-get",
		"name: worker-a0",
		"name: worker-a1",
		"name: worker-b0",
		"name: worker-b1",
		"network-domain: fabric-a",
		"network-domain: fabric-b",
		"ipv6: endpoint-inputs-defined",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("topology contract lacks %q", want)
		}
	}
	if strings.Contains(text, "authoritativePoolGUID") {
		t.Fatal("topology contract must not hardcode authoritative pool GUIDs")
	}
}

func TestInfrastructureConfigs(t *testing.T) {
	for _, path := range []string{
		filepath.Join("..", "data", "infrastructure-kubevirt", "legacy.yaml"),
		filepath.Join("..", "data", "infrastructure-kubevirt", "two-owner.yaml"),
		filepath.Join("..", "data", "infrastructure-aws", "legacy.yaml"),
		filepath.Join("..", "data", "infrastructure-aws", "two-owner.yaml"),
	} {
		t.Run(path, func(t *testing.T) {
			docs, _ := readTemplateDocuments(t, path)
			if len(docs) != 1 {
				t.Fatalf("documents = %d, want 1", len(docs))
			}
			if nestedString(docs[0], "kind") != "InfrastructureConfig" {
				t.Fatalf("kind = %q", nestedString(docs[0], "kind"))
			}
		})
	}
}

func TestLegacyInfrastructureConfigsMatchStaticTwoOwnerFlavor(t *testing.T) {
	for _, tc := range []struct {
		provider    string
		machineKind string
	}{
		{provider: "kubevirt", machineKind: "KubevirtMachineTemplate"},
		{provider: "aws", machineKind: "AWSMachineTemplate"},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			fixtureDocs, _ := readTemplateDocuments(t, filepath.Join("..", "data", "infrastructure-"+tc.provider, "legacy.yaml"))
			templatePath := filepath.Join("..", tc.provider, "cluster-template-zfs-csi.yaml")
			if tc.provider == "aws" {
				templatePath = filepath.Join("..", "aws", "cluster-template-zfs-csi-aws.yaml")
			}
			docs, _ := readTemplateDocuments(t, templatePath)
			fixture := fixtureDocs[0]
			owner := nestedSlice(fixture, "spec", "storageOwners")[0].(map[string]any)
			worker := nestedSlice(fixture, "spec", "consumerWorkers")[0].(map[string]any)
			ownerName := "${CLUSTER_NAME}-" + nestedString(owner, "machineDeploymentSuffix")
			workerName := "${CLUSTER_NAME}-" + nestedString(worker, "machineDeploymentSuffix")
			for _, kind := range []string{"MachineDeployment", "KubeadmConfigTemplate", tc.machineKind} {
				requireDocument(t, docs, kind, ownerName)
				requireDocument(t, docs, kind, workerName)
			}
		})
	}
}

func TestAWSInfrastructureConfigsSeparateDiscoveryFromResolvedDeviceID(t *testing.T) {
	for _, name := range []string{"legacy.yaml", "two-owner.yaml"} {
		path := filepath.Join("..", "data", "infrastructure-aws", name)
		_, body := readTemplateDocuments(t, path)
		text := string(body)
		for _, want := range []string{"provider: aws-ebs-volume-id", "attachmentDeviceName: /dev/xvdb"} {
			if !strings.Contains(text, want) {
				t.Fatalf("%s lacks AWS EBS discovery binding %q", path, want)
			}
		}
		if strings.Contains(text, "deviceID:") || strings.Contains(text, "Amazon_Elastic_Block_Store_*") {
			t.Fatalf("%s must not pretend unresolved AWS EBS identity is a final deviceID", path)
		}
	}
}

func readTemplateDocuments(t *testing.T, path string) ([]map[string]any, []byte) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	var docs []map[string]any
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("parse %s: %v", path, err)
		}
		if len(doc) != 0 {
			docs = append(docs, doc)
		}
	}
	return docs, body
}

func requireDocument(t *testing.T, docs []map[string]any, kind, name string) map[string]any {
	t.Helper()
	for _, doc := range docs {
		if nestedString(doc, "kind") == kind && nestedString(doc, "metadata", "name") == name {
			return doc
		}
	}
	t.Fatalf("missing %s %s", kind, name)
	return nil
}

func nestedString(value map[string]any, path ...string) string {
	var current any = value
	for _, part := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = object[part]
	}
	result, _ := current.(string)
	return result
}

func nestedSlice(value map[string]any, path ...string) []any {
	var current any = value
	for _, key := range path {
		mapping, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = mapping[key]
	}
	items, _ := current.([]any)
	return items
}

func nestedStringMap(value map[string]any, path ...string) map[string]string {
	var current any = value
	for _, key := range path {
		mapping, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = mapping[key]
	}
	mapping, _ := current.(map[string]any)
	result := make(map[string]string, len(mapping))
	for key, item := range mapping {
		result[key], _ = item.(string)
	}
	return result
}
