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

//go:build mage

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestValidateAWSStorageDocumentsRequiresNFSServerBootstrap(t *testing.T) {
	baseDocs := func(commands ...string) []map[string]any {
		bootstrapCommands := make([]any, len(commands))
		for i, command := range commands {
			bootstrapCommands[i] = command
		}
		return []map[string]any{
			{
				"kind":     "MachineDeployment",
				"metadata": map[string]any{"name": "storage"},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
					"bootstrap":         map[string]any{"configRef": map[string]any{"name": "storage"}},
					"infrastructureRef": map[string]any{"name": "storage"},
				}}},
			},
			{
				"kind":     "KubeadmConfigTemplate",
				"metadata": map[string]any{"name": "storage"},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
					"preKubeadmCommands": bootstrapCommands,
				}}},
			},
			{
				"kind":     "AWSMachineTemplate",
				"metadata": map[string]any{"name": "storage"},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
					"assignPrimaryIPv6": "enabled",
					"nonRootVolumes":    []any{map[string]any{"deviceName": "/dev/xvdb"}},
				}}},
			},
		}
	}

	for _, tt := range []struct {
		name     string
		commands []string
		missing  string
	}{
		{
			name:     "unmask",
			commands: []string{"apt-get install -y nfs-kernel-server", "systemctl enable --now nfs-server"},
			missing:  "systemctl unmask nfs-server",
		},
		{
			name:     "enable and start",
			commands: []string{"apt-get install -y nfs-kernel-server", "systemctl unmask nfs-server"},
			missing:  "systemctl enable --now nfs-server",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAWSStorageDocuments("test-template.yaml", baseDocs(tt.commands...), "storage")
			if err == nil || !strings.Contains(err.Error(), `"`+tt.missing+`"`) {
				t.Fatalf("expected missing %q error, got %v", tt.missing, err)
			}
		})
	}

	if err := validateAWSStorageDocuments(
		"test-template.yaml",
		baseDocs(
			"apt-get install -y nfs-kernel-server",
			"systemctl unmask nfs-server",
			"systemctl enable --now nfs-server",
		),
		"storage",
	); err != nil {
		t.Fatalf("expected complete nfs-server bootstrap to validate: %v", err)
	}

	err := validateAWSStorageDocuments(
		"test-template.yaml",
		baseDocs(
			"systemctl unmask nfs-server",
			"apt-get install -y nfs-kernel-server",
			"systemctl enable --now nfs-server",
		),
		"storage",
	)
	if err == nil || !strings.Contains(err.Error(), "must install, unmask, then enable and start nfs-server") {
		t.Fatalf("expected out-of-order nfs-server bootstrap error, got %v", err)
	}
}

func TestValidateAWSStorageDocumentsRequiresEveryOwner(t *testing.T) {
	ownerDocs := func(name string) []map[string]any {
		return []map[string]any{
			{
				"kind":     "MachineDeployment",
				"metadata": map[string]any{"name": name},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
					"bootstrap":         map[string]any{"configRef": map[string]any{"name": name}},
					"infrastructureRef": map[string]any{"name": name},
				}}},
			},
			{
				"kind":     "KubeadmConfigTemplate",
				"metadata": map[string]any{"name": name},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
					"preKubeadmCommands": []any{
						"apt-get install -y nfs-kernel-server",
						"systemctl unmask nfs-server",
						"systemctl enable --now nfs-server",
					},
				}}},
			},
			{
				"kind":     "AWSMachineTemplate",
				"metadata": map[string]any{"name": name},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
					"assignPrimaryIPv6": "enabled",
					"nonRootVolumes":    []any{map[string]any{"deviceName": "/dev/xvdb"}},
				}}},
			},
		}
	}
	docs := append(ownerDocs("storage-a"), ownerDocs("storage-b")...)
	if err := validateAWSStorageDocuments("test-template.yaml", docs, "storage-a", "storage-b"); err != nil {
		t.Fatalf("expected two complete owners to validate: %v", err)
	}
	if err := validateAWSStorageDocuments("test-template.yaml", ownerDocs("storage-a"), "storage-a", "storage-b"); err == nil || !strings.Contains(err.Error(), "storage-b") {
		t.Fatalf("expected missing storage-b to fail, got %v", err)
	}
}

func TestValidateInfrastructureConfigs(t *testing.T) {
	if err := validateInfrastructureConfigs(filepath.Join("..", "test", "e2e", "data")); err != nil {
		t.Fatal(err)
	}
}

func TestValidateProvisionableInfrastructureConfigs(t *testing.T) {
	for _, path := range []string{
		filepath.Join("..", "test", "e2e", "data", "infrastructure-kubevirt", "legacy.yaml"),
		filepath.Join("..", "test", "e2e", "data", "infrastructure-kubevirt", "two-owner.yaml"),
		filepath.Join("..", "test", "e2e", "data", "infrastructure-aws", "legacy.yaml"),
		filepath.Join("..", "test", "e2e", "data", "infrastructure-aws", "two-owner.yaml"),
	} {
		provider := "kubevirt"
		if strings.Contains(path, "infrastructure-aws") {
			provider = "aws"
		}
		if err := validateInfrastructureConfig(path, provider, strings.HasSuffix(path, "legacy.yaml")); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

func TestValidateInfrastructureConfigRejectsUnsafeAWSDeviceIdentity(t *testing.T) {
	path := filepath.Join("..", "test", "e2e", "data", "infrastructure-aws", "legacy.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		addition string
	}{
		{name: "wildcard", addition: "        deviceID: /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_*\n"},
		{name: "ambiguous prefix", addition: "        deviceID: /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_\n"},
		{name: "non persistent", addition: "        deviceID: /dev/nvme1n1\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := strings.Replace(string(body), "        discovery:\n", tc.addition+"        discovery:\n", 1)
			badPath := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(badPath, []byte(bad), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := validateInfrastructureConfig(badPath, "aws", true); err == nil || !strings.Contains(err.Error(), "exact /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol") {
				t.Fatalf("expected unsafe AWS device identity to fail, got %v", err)
			}
		})
	}
}

func TestValidateInfrastructureConfigAcceptsResolvedAWSDeviceIdentity(t *testing.T) {
	path := filepath.Join("..", "test", "e2e", "data", "infrastructure-aws", "legacy.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	resolved := strings.Replace(string(body), "        discovery:\n", "        deviceID: /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol0123456789abcdef0\n        discovery:\n", 1)
	resolvedPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(resolvedPath, []byte(resolved), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateInfrastructureConfig(resolvedPath, "aws", true); err != nil {
		t.Fatalf("resolved exact AWS EBS by-id should validate: %v", err)
	}
}

func TestValidateInfrastructureConfigRejectsDuplicateResolvedAWSDeviceIdentity(t *testing.T) {
	path := filepath.Join("..", "test", "e2e", "data", "infrastructure-aws", "two-owner.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const device = "        deviceID: /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol0123456789abcdef0\n"
	resolved := strings.ReplaceAll(string(body), "        discovery:\n", device+"        discovery:\n")
	resolvedPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(resolvedPath, []byte(resolved), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateInfrastructureConfig(resolvedPath, "aws", false); err == nil || !strings.Contains(err.Error(), "duplicate storage owner pool device") {
		t.Fatalf("expected duplicate resolved AWS device identity to fail, got %v", err)
	}
}

func TestResolveAWSEBSDeviceID(t *testing.T) {
	path := filepath.Join("..", "test", "e2e", "data", "infrastructure-aws", "legacy.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config infrastructureConfig
	if err := yaml.Unmarshal(body, &config); err != nil {
		t.Fatal(err)
	}
	owner := config.Spec.StorageOwners[0]
	got, err := resolveAWSEBSDeviceID(owner, "vol-0123456789abcdef0")
	if err != nil {
		t.Fatal(err)
	}
	if want := "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol0123456789abcdef0"; got != want {
		t.Fatalf("resolved deviceID = %q, want %q", got, want)
	}
	for _, volumeID := range []string{
		"",
		"0123456789abcdef0",
		"volume-0123456789abcdef0",
		"vol-0123456789abcdef",
		"vol-0123456789abcdef00",
		"vol-0123456789abcdeF0",
		"vol-*",
		"vol-root/data",
		" vol-0123456789abcdef0",
		"vol-0123456789abcdef0 ",
		"vol-01234567 89abcdef0",
	} {
		if _, err := resolveAWSEBSDeviceID(owner, volumeID); err == nil {
			t.Fatalf("expected volume ID %q to fail", volumeID)
		}
	}
}

func TestValidateInfrastructureConfigRejectsMissingOrDuplicateTopology(t *testing.T) {
	path := filepath.Join("..", "test", "e2e", "data", "infrastructure-kubevirt", "two-owner.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		oldValue  string
		newValue  string
		errorText string
	}{
		{
			name:      "missing consumer domain",
			oldValue:  "      networkDomain: fabric-a\n      endpoints:\n        nfs: {ipv4: 10.19.1.21",
			newValue:  "      networkDomain: fabric-c\n      endpoints:\n        nfs: {ipv4: 10.19.1.21",
			errorText: "has no consumer workers",
		},
		{
			name:      "duplicate owner selector",
			oldValue:  "        zfs.csi.randomvariable.co.uk/storage-owner: storage-b\n",
			newValue:  "        zfs.csi.randomvariable.co.uk/storage-owner: storage-a\n",
			errorText: "duplicate storage owner node selector",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := strings.Replace(string(body), tc.oldValue, tc.newValue, 1)
			badPath := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(badPath, []byte(bad), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := validateInfrastructureConfig(badPath, "kubevirt", false); err == nil || !strings.Contains(err.Error(), tc.errorText) {
				t.Fatalf("expected %q error, got %v", tc.errorText, err)
			}
		})
	}
}

func TestValidateInfrastructureConfigAllowsSharedNetworkDomain(t *testing.T) {
	path := filepath.Join("..", "test", "e2e", "data", "infrastructure-kubevirt", "two-owner.yaml")
	if err := validateInfrastructureConfig(path, "kubevirt", false); err != nil {
		t.Fatalf("shared owner network domain should validate: %v", err)
	}
}

func TestValidateInfrastructureConfigAllowsConsumerGroupsToShareNetworkDomain(t *testing.T) {
	path := filepath.Join("..", "test", "e2e", "data", "infrastructure-kubevirt", "two-owner.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	extraWorker := "\n    - name: workers-shared\n      machineDeploymentSuffix: workers-shared\n      replicas: 1\n      networkDomain: fabric-a\n"
	sharedPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(sharedPath, append(body, []byte(extraWorker)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateInfrastructureConfig(sharedPath, "kubevirt", false); err != nil {
		t.Fatalf("consumer groups sharing a reachability domain should validate: %v", err)
	}
}

func TestValidateAWSClusterDocumentRequiresRestrictedBastion(t *testing.T) {
	clusterDocument := map[string]any{
		"apiVersion": "cluster.x-k8s.io/v1beta2",
		"kind":       "Cluster",
		"spec": map[string]any{
			"infrastructureRef": map[string]any{
				"apiGroup": "infrastructure.cluster.x-k8s.io",
				"kind":     "AWSCluster",
				"name":     "${CLUSTER_NAME}",
			},
			"controlPlaneRef": map[string]any{
				"apiGroup": "controlplane.cluster.x-k8s.io",
				"kind":     "KubeadmControlPlane",
				"name":     "${CLUSTER_NAME}-control-plane",
			},
		},
	}
	awsClusterDocument := func(enabled bool, allowed ...any) map[string]any {
		return map[string]any{
			"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2",
			"kind":       "AWSCluster",
			"spec": map[string]any{
				"bastion": map[string]any{
					"enabled":           enabled,
					"allowedCIDRBlocks": allowed,
				},
				"network": map[string]any{"vpc": map[string]any{"ipv6": map[string]any{}}},
				"controlPlaneLoadBalancer": map[string]any{
					"loadBalancerType":  "nlb",
					"targetGroupIPType": "ipv6",
				},
			},
		}
	}

	for _, tt := range []struct {
		name string
		doc  map[string]any
	}{
		{name: "disabled", doc: awsClusterDocument(false, "${E2E_AWS_BASTION_ALLOWED_CIDR}")},
		{name: "open CIDR", doc: awsClusterDocument(true, "0.0.0.0/0")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateAWSClusterDocument("test-template.yaml", []map[string]any{tt.doc, clusterDocument}); err == nil {
				t.Fatal("expected invalid bastion configuration to fail")
			}
		})
	}

	if err := validateAWSClusterDocument("test-template.yaml", []map[string]any{
		awsClusterDocument(true, "${E2E_AWS_BASTION_ALLOWED_CIDR}"), clusterDocument,
	}); err != nil {
		t.Fatalf("expected restricted CAPA bastion to validate: %v", err)
	}
	unsupported := awsClusterDocument(true, "${E2E_AWS_BASTION_ALLOWED_CIDR}")
	unsupported["spec"].(map[string]any)["controlPlaneLoadBalancer"].(map[string]any)["loadBalancerIPAddressType"] = "dualstack"
	if err := validateAWSClusterDocument("test-template.yaml", []map[string]any{unsupported, clusterDocument}); err == nil || !strings.Contains(err.Error(), "unsupported CAPA v2.11.1 field") {
		t.Fatalf("expected unsupported loadBalancerIPAddressType to fail, got %v", err)
	}
}
