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

package zfscsi

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const csiDriverName = "zfs.csi.randomvariable.co.uk"

func TestStorageClassDefaultsDoNotEnableNFS(t *testing.T) {
	values, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatalf("read chart values: %v", err)
	}
	if strings.Contains(string(values), "192.168.192.") {
		t.Fatal("chart defaults must not contain a homelab address")
	}

	manifest, err := os.ReadFile("templates/storageclasses.yaml")
	if err != nil {
		t.Fatalf("read StorageClass template: %v", err)
	}
	if !strings.Contains(string(manifest), "nfsExportCIDRs is required when tankNFS is enabled") {
		t.Fatal("enabled tank NFS StorageClass must require an export CIDR")
	}
}

func TestDefaultRenderHasNoWorkloadsOrStorageClasses(t *testing.T) {
	output := renderChart(t)
	for _, kind := range []string{"kind: Deployment", "kind: DaemonSet", "kind: StorageClass"} {
		if strings.Contains(output, kind) {
			t.Fatalf("default chart render unexpectedly contains %s", kind)
		}
	}
}

func TestStorageClassesAreNeverRenderedAsDefault(t *testing.T) {
	output := renderChart(t,
		"--set", "controller.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "storageNode.name=storage-0",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set-json", `storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/16"]`,
	)
	if strings.Contains(output, "storageclass.kubernetes.io/is-default-class") {
		t.Fatal("zfs-csi StorageClasses must never be marked as the Kubernetes default")
	}
}

func TestSelectedStorageClassBecomesClusterDefault(t *testing.T) {
	base := []string{
		"--set", "controller.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "storageNode.name=storage-0",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "network.tls.enabled=true",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set", "storageClasses.tankNFS.enabled=true",
		"--set", "storageClasses.tankNFSTLS.enabled=true",
		"--set", "storageClasses.tankNVMeTLS.enabled=true",
		"--set-json", `storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/16"]`,
		"--set-json", `storageClasses.tankNFSTLS.nfsExportCIDRs=["10.0.0.0/16"]`,
	}

	for defaultClass, wantName := range map[string]string{
		"tankNVMeTLS": "zfs-tank-nvme-tls",
		"tankNFSTLS":  "zfs-tank-nfs-tls",
		"tankNVMe":    "zfs-tank-nvme",
		"tankNFS":     "zfs-tank-nfs",
	} {
		args := append(append([]string{}, base...), "--set", "storageClasses.defaultClass="+defaultClass)
		objects := objectsByKind(renderedObjects(t, renderChart(t, args...)), "StorageClass")
		if len(objects) != 4 {
			t.Fatalf("defaultClass=%s rendered %d StorageClasses, want 4", defaultClass, len(objects))
		}
		defaults := defaultAnnotatedStorageClassNames(t, objects)
		if len(defaults) != 1 || defaults[0] != wantName {
			t.Fatalf("defaultClass=%s annotated %v as cluster default, want exactly [%s]", defaultClass, defaults, wantName)
		}
	}
}

// The encrypted variant of a selected class carries the default annotation only
// when defaultClassVariant selects it, so encryption never silently moves the
// cluster default onto a different StorageClass.
func TestDefaultStorageClassVariantSelectsEncryptedClass(t *testing.T) {
	base := []string{
		"--set", "controller.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "storageNode.name=storage-0",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "network.tls.enabled=true",
		"--set", "encryption.enabled=true",
		"--set", "storageClasses.defaultClass=tankNVMeTLS",
	}

	for variant, wantName := range map[string]string{
		"plain":     "zfs-tank-nvme-tls",
		"encrypted": "zfs-tank-nvme-tls-encrypted",
	} {
		args := append(append([]string{}, base...), "--set", "storageClasses.defaultClassVariant="+variant)
		objects := objectsByKind(renderedObjects(t, renderChart(t, args...)), "StorageClass")
		defaults := defaultAnnotatedStorageClassNames(t, objects)
		if len(defaults) != 1 || defaults[0] != wantName {
			t.Fatalf("defaultClassVariant=%s annotated %v as cluster default, want exactly [%s]", variant, defaults, wantName)
		}
	}
}

// A selected default must actually exist in the release, otherwise the operator
// silently gets no default StorageClass at all.
func TestDefaultStorageClassRejectsUnrenderedSelection(t *testing.T) {
	base := []string{
		"--set", "controller.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "storageNode.name=storage-0",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "network.tls.enabled=true",
	}

	assertRenderFails(t, append(append([]string{}, base...), "--set", "storageClasses.defaultClass=flashNVMe"),
		`storageClasses.defaultClass "flashNVMe" requires storageClasses.flashNVMe.enabled=true`)
	assertRenderFails(t, append(append([]string{}, base...),
		"--set", "storageClasses.defaultClass=tankNVMeTLS",
		"--set", "storageClasses.defaultClassVariant=encrypted"),
		`storageClasses.defaultClass "tankNVMeTLS" with defaultClassVariant=encrypted requires encryption.enabled=true`)
	assertRenderFails(t, []string{
		"--set", "controller.enabled=false",
		"--set", "storageClasses.defaultClass=tankNVMeTLS",
	}, `is not rendered by this release`)
}

func defaultAnnotatedStorageClassNames(t *testing.T, objects []map[string]any) []string {
	t.Helper()
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		metadata, _ := object["metadata"].(map[string]any)
		annotations, _ := metadata["annotations"].(map[string]any)
		if annotations["storageclass.kubernetes.io/is-default-class"] == "true" {
			names = append(names, objectName(object))
		}
	}
	sort.Strings(names)
	return names
}

func TestStorageClassSchemaDefaultMatchesValues(t *testing.T) {
	schema, err := os.ReadFile("values.schema.json")
	if err != nil {
		t.Fatalf("read values schema: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(schema, &document); err != nil {
		t.Fatalf("decode values schema: %v", err)
	}
	properties := document["properties"].(map[string]any)
	storageClasses := properties["storageClasses"].(map[string]any)
	defaultClass := storageClasses["properties"].(map[string]any)["defaultClass"].(map[string]any)
	if got := defaultClass["default"]; got != "" {
		t.Fatalf("schema storageClasses.defaultClass default = %v, want empty string", got)
	}
	values, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatalf("read chart values: %v", err)
	}
	var valuesDocument map[string]any
	if err := yaml.Unmarshal(values, &valuesDocument); err != nil {
		t.Fatalf("decode chart values: %v", err)
	}
	valuesStorageClasses := valuesDocument["storageClasses"].(map[string]any)
	if got := valuesStorageClasses["defaultClass"]; got != "" {
		t.Fatalf("values storageClasses.defaultClass = %v, want empty string", got)
	}
}

func TestPartialWorkloadRendersDoNotRequireDefaultStorageClass(t *testing.T) {
	for _, args := range [][]string{
		{"--set", "controller.enabled=true", "--set", "storageNode.name=storage-0", "--set", "network.tls.enabled=false", "--set", "storageClasses.tankNFSTLS.enabled=false", "--set", "storageClasses.tankNVMeTLS.enabled=false", "--set", "storageClasses.defaultClass="},
		{"--set", "node.enabled=true", "--set", "network.tls.enabled=false", "--set", "storageClasses.tankNFSTLS.enabled=false", "--set", "storageClasses.tankNVMeTLS.enabled=false"},
		{"--set", "storage.enabled=true", "--set", "network.tls.enabled=false", "--set", "storageNode.name=storage-0", "--set", "storageNode.networkDomain=storage", "--set-string", "storageNode.authoritativePoolGUIDs[0]=1", "--set", "network.portalHost=10.0.0.7", "--set", "network.nfsServer=10.0.0.7"},
	} {
		renderChart(t, args...)
	}
}

func TestServiceAccountTokenDefaultsOff(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--set", "serviceAccountToken.requiresRepublish=true"},
	} {
		spec := renderedCSIDriverSpec(t, args...)
		if _, ok := spec["tokenRequests"]; ok {
			t.Fatal("disabled CSIDriver must not request service account tokens")
		}
		if _, ok := spec["serviceAccountTokenInSecrets"]; ok {
			t.Fatal("disabled CSIDriver must not enable service account token secrets")
		}
		if got := spec["requiresRepublish"]; got != false {
			t.Fatalf("disabled requiresRepublish = %v, want false", got)
		}
	}
}

func TestServiceAccountTokenEnabledShape(t *testing.T) {
	spec := renderedCSIDriverSpec(t,
		"--set", "serviceAccountToken.enabled=true",
		"--set-string", "serviceAccountToken.audience=  zfs-csi.example.com  ",
	)
	if got := spec["serviceAccountTokenInSecrets"]; got != true {
		t.Fatalf("serviceAccountTokenInSecrets = %v, want true", got)
	}
	if got := spec["requiresRepublish"]; got != false {
		t.Fatalf("requiresRepublish = %v, want false", got)
	}

	requests, ok := spec["tokenRequests"].([]any)
	if !ok || len(requests) != 1 {
		t.Fatalf("tokenRequests = %#v, want one request", spec["tokenRequests"])
	}
	request, ok := requests[0].(map[string]any)
	if !ok {
		t.Fatalf("tokenRequests[0] = %#v, want object", requests[0])
	}
	if got := request["audience"]; got != "zfs-csi.example.com" {
		t.Fatalf("tokenRequests[0].audience = %v", got)
	}
	if got := request["expirationSeconds"]; got != 3600 {
		t.Fatalf("tokenRequests[0].expirationSeconds = %v, want default 3600", got)
	}
}

func TestServiceAccountTokenRequiresRepublish(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "false", enabled: false},
		{name: "true", enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := renderedCSIDriverSpec(t,
				"--set", "serviceAccountToken.enabled=true",
				"--set", "serviceAccountToken.audience=zfs-csi.example.com",
				"--set", "serviceAccountToken.requiresRepublish="+tc.name,
			)
			if got := spec["requiresRepublish"]; got != tc.enabled {
				t.Fatalf("requiresRepublish = %v, want %v", got, tc.enabled)
			}
		})
	}
}

func TestServiceAccountTokenValuesAreValidated(t *testing.T) {
	assertRenderFails(t, []string{
		"--set", "serviceAccountToken.enabled=true",
	}, "serviceAccountToken.audience must be non-empty")
	assertRenderFails(t, []string{
		"--set", "serviceAccountToken.enabled=true",
		"--set-string", "serviceAccountToken.audience=   ",
	}, "serviceAccountToken.audience must be non-empty")
	assertRenderFails(t, []string{
		"--set", "serviceAccountToken.enabled=true",
		"--set", "serviceAccountToken.audience=zfs-csi.example.com",
		"--set", "serviceAccountToken.expirationSeconds=599",
	}, "serviceAccountToken.expirationSeconds must be at least 600")
}

func TestEnabledTransportValuesAreValidatedIndependently(t *testing.T) {
	assertRenderFails(
		t,
		[]string{"--set", "node.enabled=true", "--set", "storageClasses.tankNVMe.enabled=true"},
		"network.portalHost is required",
	)
	assertRenderFails(
		t,
		[]string{
			"--set",
			"node.enabled=true",
			"--set",
			"storageClasses.tankNVMeTLS.enabled=false",
			"--set",
			"storageClasses.tankNFS.enabled=true",
			"--set-json",
			`storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/16"]`,
		},
		"network.nfsServer is required",
	)

	output := renderChart(t,
		"--set", "controller.enabled=true",
		"--set", "node.enabled=true",
		"--set", "network.tls.enabled=false",
		"--set", "storageClasses.tankNVMeTLS.enabled=false",
		"--set", "storageClasses.tankNFSTLS.enabled=false",
		"--set", "storageNode.name=storage-0",
		"--set", "storageClasses.tankNFS.enabled=true",
		"--set", "storageClasses.defaultClass=tankNFS",
		"--set-json", `storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/16","2001:db8::/64"]`,
		"--set", "network.nfsServer=10.0.0.7",
	)
	if strings.Contains(output, "--portal-host=") {
		t.Fatal("pure NFS configuration must not require or render an NVMe portal")
	}
	if !strings.Contains(output, `nfsExportCIDRs: "10.0.0.0/16,2001:db8::/64"`) {
		t.Fatal("NFS StorageClass did not join the configured CIDR list")
	}
}

func TestTLSStorageClassesEnabledAndCanBeExplicitlyDisabled(t *testing.T) {
	assertRenderFails(t, []string{"--set", "controller.enabled=true", "--set", "node.enabled=true", "--set", "storage.enabled=true", "--set", "storageNode.name=storage-0", "--set", "storageNode.networkDomain=workers", "--set-string", "storageNode.authoritativePoolGUIDs[0]=1", "--set", "network.portalHost=10.0.0.7", "--set", "network.nfsServer=10.0.0.7", "--set", "network.tls.enabled=false", "--set", "storageClasses.tankNFSTLS.enabled=true", "--set", "storageClasses.tankNVMeTLS.enabled=false", "--set", "storageClasses.defaultClass=tankNFSTLS"}, "storageClasses.tankNFSTLS.enabled requires network.tls.enabled=true")
	assertRenderFails(t, []string{"--set", "controller.enabled=true", "--set", "node.enabled=true", "--set", "storage.enabled=true", "--set", "storageNode.name=storage-0", "--set", "storageNode.networkDomain=workers", "--set-string", "storageNode.authoritativePoolGUIDs[0]=1", "--set", "network.portalHost=10.0.0.7", "--set", "network.nfsServer=10.0.0.7", "--set", "network.tls.enabled=false", "--set", "storageClasses.tankNFSTLS.enabled=false", "--set", "storageClasses.tankNVMeTLS.enabled=true", "--set", "storageClasses.defaultClass=tankNVMeTLS"}, "storageClasses.tankNVMeTLS.enabled requires network.tls.enabled=true")

	output := renderChart(t,
		"--set", "controller.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storageNode.name=storage-0",
		"--set", "node.networkDomain=workers",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "network.tls.enabled=true",
		"--set", "storageClasses.tankNFSTLS.enabled=true",
		"--set-json", `storageClasses.tankNFSTLS.nfsExportCIDRs=["10.0.0.0/16"]`,
		"--set", "storageClasses.tankNVMeTLS.enabled=true",
	)
	for _, want := range []string{"name: zfs-tank-nfs-tls", "nfsTLS: \"true\"", "name: zfs-tank-nvme-tls", "nvmeTLS: \"true\""} {
		if !strings.Contains(output, want) {
			t.Fatalf("TLS StorageClass render missing %q", want)
		}
	}
	if strings.Contains(output, "storageclass.kubernetes.io/is-default-class") {
		t.Fatal("TLS StorageClasses must not be marked as the Kubernetes default")
	}
	plaintext := renderChart(t,
		"--set", "controller.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storageNode.name=storage-0",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "network.tls.enabled=false",
		"--set", "storageClasses.tankNFSTLS.enabled=false",
		"--set", "storageClasses.tankNVMeTLS.enabled=false",
		"--set", "storageClasses.tankNVMe.enabled=true",
	)
	if strings.Contains(plaintext, "nfsTLS: \"true\"") || strings.Contains(plaintext, "nvmeTLS: \"true\"") {
		t.Fatal("TLS-off StorageClasses must not enable TLS parameters")
	}
}

func TestEncryptedNVMeRequiresAndRendersNodePortal(t *testing.T) {
	assertRenderFails(t, []string{
		"--set", "node.enabled=true",
		"--set", "encryption.enabled=true",
	}, "network.portalHost is required")

	output := renderChart(t,
		"--set", "controller.enabled=true",
		"--set", "node.enabled=true",
		"--set", "network.tls.enabled=false",
		"--set", "storageClasses.tankNFSTLS.enabled=false",
		"--set", "storageClasses.tankNVMeTLS.enabled=false",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set", "storageNode.name=storage-0",
		"--set", "encryption.enabled=true",
		"--set", "network.portalHost=10.0.0.7",
	)
	if !strings.Contains(output, "--portal-host=10.0.0.7") {
		t.Fatal("encrypted NVMe configuration must render the node portal host")
	}
	if !strings.Contains(output, "name: zfs-tank-nvme-encrypted") {
		t.Fatal("encryption must render the encrypted NVMe StorageClass")
	}
}

func TestEncryptedStorageClassMatrix(t *testing.T) {
	output := renderChart(t,
		"--set", "controller.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "storageNode.name=storage-0",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1",
		"--set", "network.portalHost=10.0.0.7",
		"--set", "network.nfsServer=10.0.0.7",
		"--set", "network.tls.enabled=true",
		"--set", "encryption.enabled=true",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set", "storageClasses.tankNFS.enabled=true",
		"--set", "storageClasses.tankNFSTLS.enabled=true",
		"--set", "storageClasses.tankNVMeTLS.enabled=true",
		"--set", "storageClasses.flashNVMe.enabled=true",
		"--set", "storageClasses.flashNFS.enabled=true",
		"--set-json", `storageClasses.tankNFS.nfsExportCIDRs=["10.0.0.0/16"]`,
		"--set-json", `storageClasses.flashNFS.nfsExportCIDRs=["10.0.0.0/16"]`,
	)
	want := map[string][]string{
		"zfs-tank-nvme-encrypted":     {"encrypted: \"true\""},
		"zfs-tank-nfs-encrypted":      {"encrypted: \"true\""},
		"zfs-tank-nfs-tls-encrypted":  {"encrypted: \"true\"", "nfsTLS: \"true\""},
		"zfs-tank-nvme-tls-encrypted": {"encrypted: \"true\"", "nvmeTLS: \"true\""},
		"zfs-flash-nvme-encrypted":    {"encrypted: \"true\""},
		"zfs-flash-nfs-encrypted":     {"encrypted: \"true\""},
	}
	for name, parameters := range want {
		var text string
		for _, object := range objectsByKind(renderedObjects(t, output), "StorageClass") {
			if objectName(object) == name {
				text = marshalObject(t, object)
				break
			}
		}
		if text == "" {
			t.Fatalf("encrypted StorageClass %q missing", name)
		}
		for _, parameter := range parameters {
			if !strings.Contains(text, parameter) {
				t.Fatalf("StorageClass %q missing %q", name, parameter)
			}
		}
	}

	assertRenderFails(t, []string{
		"--set", "controller.enabled=true",
		"--set", "node.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "network.tls.enabled=false",
		"--set", "encryption.enabled=true",
		"--set", "storageClasses.tankNFSTLS.enabled=true",
		"--set", "storageClasses.tankNVMeTLS.enabled=false",
	}, "storageClasses.tankNFSTLS.enabled requires network.tls.enabled=true")
}

func TestVolumeImportControllerIsExplicitlyGated(t *testing.T) {
	base := legacyStorageArgs()
	disabled := renderChart(t, base...)
	if strings.Contains(disabled, "--enable-volume-imports=true") {
		t.Fatal("volume imports enabled by default")
	}
	enabled := renderChart(t, append(base, "--set", "storage.enableVolumeImports=true")...)
	if !strings.Contains(enabled, "--enable-volume-imports=true") {
		t.Fatal("volume import gate not rendered")
	}
}

func TestStorageNodeObjectRendersOnlyWithExplicitIntent(t *testing.T) {
	if output := renderChart(t); strings.Contains(output, "\nkind: StorageNode\nmetadata:\n  name:") {
		t.Fatal("StorageNode rendered without explicit authoritativePoolGUIDs and networkDomain")
	}
	output := renderChart(t,
		"--set", "storageNode.name=storage-a",
		"--set-string", "storageNode.authoritativePoolGUIDs[0]=1234",
		"--set", "storageNode.networkDomain=workers",
	)
	for _, want := range []string{"kind: StorageNode", "name: storage-a", "authoritativePoolGUIDs:", "- \"1234\"", `networkDomain: "workers"`} {
		if !strings.Contains(output, want) {
			t.Fatalf("render missing %q", want)
		}
	}
}

func TestMultiOwnerControllerIsOwnerIndependentAndActiveActive(t *testing.T) {
	args := append(multiOwnerArgs(false, false),
		"--set", "controller.enabled=true",
		"--set", "storageClasses.defaultClass=",
		"--set", "controller.replicas=2",
		"--set-json", `controller.nodeSelector={"node-role.kubernetes.io/control-plane":""}`,
	)
	output := renderChart(t, args...)
	objects := renderedObjects(t, output)
	controllers := objectsByKind(objects, "Deployment")
	if len(controllers) != 1 || objectName(controllers[0]) != "zfs-csi-controller" {
		t.Fatalf("controller deployments = %#v, want one zfs-csi-controller", controllers)
	}
	text := marshalObject(t, controllers[0])
	for _, want := range []string{"kubernetes.io/arch: amd64", "node-role.kubernetes.io/control-plane: \"\""} {
		if !strings.Contains(text, want) {
			t.Fatalf("active-active controller missing merged node selector %q", want)
		}
	}
	for _, want := range []string{
		"replicas: 2",
		"maxSurge: 0",
		"maxUnavailable: 1",
		"requiredDuringSchedulingIgnoredDuringExecution",
		"topologySpreadConstraints",
		"--leader-elect=false",
		"--leader-election=true",
		"--enable-capacity",
		"--capacity-ownerref-level=2",
		"--capacity-for-immediate-binding=true",
		"--feature-gates=Topology=true",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("active-active controller missing %q", want)
		}
	}
	for _, unwanted := range []string{"storage-a", "storage-b", "10.0.0.11", "10.0.0.22", "--portal-host="} {
		if strings.Contains(text, unwanted) {
			t.Fatalf("owner-independent controller contains %q", unwanted)
		}
	}
	if strings.Count(text, "--leader-election=true") != 5 {
		t.Fatalf("leader-election=true count = %d, want all five controller sidecars", strings.Count(text, "--leader-election=true"))
	}

	pdbs := objectsByKind(objects, "PodDisruptionBudget")
	if len(pdbs) != 1 || objectName(pdbs[0]) != "zfs-csi-controller" {
		t.Fatalf("PDBs = %#v, want one controller PDB", pdbs)
	}
}

func TestMultiOwnerControllerDefaultsToAMD64(t *testing.T) {
	args := append(multiOwnerArgs(false, false), "--set", "controller.enabled=true")
	controllers := objectsByKind(renderedObjects(t, renderChart(t, args...)), "Deployment")
	if len(controllers) != 1 || objectName(controllers[0]) != "zfs-csi-controller" {
		t.Fatalf("controller deployments = %#v, want one zfs-csi-controller", controllers)
	}
	assertNodeSelectorValues(t, controllers[0], map[string]string{"kubernetes.io/arch": "amd64"})
}

func TestLegacyControllerDefaultsToOneReplicaAndNoPDB(t *testing.T) {
	output := renderChart(t,
		"--set", "controller.enabled=true",
		"--set", "storageNode.name=storage-a",
	)
	objects := renderedObjects(t, output)
	controllers := objectsByKind(objects, "Deployment")
	if len(controllers) != 1 {
		t.Fatalf("controller Deployment count = %d, want 1", len(controllers))
	}
	text := marshalObject(t, controllers[0])
	for _, want := range []string{"replicas: 1", "--leader-elect=true", "zfs.csi.randomvariable.co.uk/storage"} {
		if !strings.Contains(text, want) {
			t.Fatalf("legacy controller missing %q", want)
		}
	}
	assertNodeSelectorValues(t, controllers[0], map[string]string{
		"kubernetes.io/arch":                   "amd64",
		"zfs.csi.randomvariable.co.uk/storage": "true",
	})
	if strings.Contains(text, "topologySpreadConstraints") || strings.Contains(text, "requiredDuringSchedulingIgnoredDuringExecution") {
		t.Fatal("single-replica legacy controller rendered active-active scheduling constraints")
	}
	if len(objectsByKind(objects, "PodDisruptionBudget")) != 0 {
		t.Fatal("single-replica controller rendered an unsatisfiable PDB")
	}
}

func assertNodeSelectorValues(t *testing.T, workload map[string]any, want map[string]string) {
	t.Helper()
	podSpec := workload["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	selector, ok := podSpec["nodeSelector"].(map[string]any)
	if !ok {
		t.Fatalf("workload %s nodeSelector = %#v, want map", objectName(workload), podSpec["nodeSelector"])
	}
	for key, value := range want {
		if selector[key] != value {
			t.Fatalf("workload %s nodeSelector[%q] = %#v, want %q", objectName(workload), key, selector[key], value)
		}
	}
}

func TestLegacyControllerRejectsActiveActiveReplicaCount(t *testing.T) {
	assertRenderFails(t, []string{
		"--set", "controller.enabled=true",
		"--set", "controller.replicas=2",
		"--set", "storageNode.name=storage-a",
	}, "controller.replicas greater than 1 requires storageOwners")
}

func TestImageDigestTakesPrecedenceOverTag(t *testing.T) {
	args := append(multiOwnerArgs(true, false),
		"--set", "image.repository=registry.example/zfs-csi",
		"--set", "image.tag=unsafe-tag",
		"--set", "image.digest=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	output := renderChart(t, args...)
	if strings.Contains(output, "registry.example/zfs-csi:unsafe-tag") {
		t.Fatal("tag rendered despite image digest")
	}
	if !strings.Contains(output, "registry.example/zfs-csi@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef") {
		t.Fatal("digest image reference missing")
	}
}

func renderChart(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("helm", append([]string{"template", "zfs-csi", "."}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, output)
	}
	return string(output)
}

func renderedCSIDriverSpec(t *testing.T, args ...string) map[string]any {
	t.Helper()
	for document := range bytes.SplitSeq([]byte(renderChart(t, args...)), []byte("\n---\n")) {
		var object map[string]any
		if err := yaml.Unmarshal(document, &object); err != nil {
			t.Fatalf("decode rendered manifest: %v", err)
		}
		if object["kind"] != "CSIDriver" {
			continue
		}
		metadata, _ := object["metadata"].(map[string]any)
		if metadata["name"] != csiDriverName {
			continue
		}
		spec, ok := object["spec"].(map[string]any)
		if !ok {
			t.Fatalf("CSIDriver spec = %#v, want object", object["spec"])
		}
		return spec
	}
	t.Fatalf("CSIDriver %q missing from rendered chart", csiDriverName)
	return nil
}

func assertRenderFails(t *testing.T, args []string, want string) {
	t.Helper()
	cmd := exec.Command("helm", append([]string{"template", "zfs-csi", "."}, args...)...)
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), want) {
		t.Fatalf("helm template error = %v, output = %s, want %q", err, output, want)
	}
}

func TestControllerEnablesVolumeAttributesClassResizer(t *testing.T) {
	manifest, err := os.ReadFile("templates/controller.yaml")
	if err != nil {
		t.Fatalf("read controller template: %v", err)
	}
	text := string(manifest)
	if !strings.Contains(text, "csi-resizer:v2.2.1") {
		t.Fatal("controller must pin external-resizer v2.2.1")
	}
	if !strings.Contains(text, "--feature-gates=VolumeAttributesClass=true") {
		t.Fatal("external-resizer must enable VolumeAttributesClass")
	}
}

func TestControllerIncludesExternalHealthMonitor(t *testing.T) {
	args := append(multiOwnerArgs(true, false), "--set", "controller.enabled=true")
	deployments := objectsByKind(renderedObjects(t, renderChart(t, args...)), "Deployment")
	for _, deployment := range deployments {
		if objectName(deployment) != "zfs-csi-controller" {
			continue
		}
		for _, container := range podContainers(t, deployment) {
			if container["name"] != "csi-external-health-monitor-controller" {
				continue
			}
			image, _ := container["image"].(string)
			// KEP-1432 support ships only in a staging build, so the sidecar
			// must stay digest-pinned rather than following a floating tag.
			if !strings.Contains(image, "@sha256:") {
				t.Fatalf("health monitor image = %q, want a digest-pinned reference", image)
			}
			return
		}
		t.Fatalf("controller Deployment has no health monitor container: %s", marshalObject(t, deployment))
	}
	t.Fatal("controller Deployment missing from rendered chart")
}

func podContainers(t *testing.T, workload map[string]any) []map[string]any {
	t.Helper()
	spec, _ := workload["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	raw, ok := podSpec["containers"].([]any)
	if !ok {
		t.Fatalf("workload %q has no containers: %s", objectName(workload), marshalObject(t, workload))
	}
	containers := make([]map[string]any, 0, len(raw))
	for _, entry := range raw {
		container, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("container entry = %#v, want object", entry)
		}
		containers = append(containers, container)
	}

	return containers
}
