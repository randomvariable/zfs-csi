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

package v1alpha1_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

func TestStorageCRDsAreClusterScopedWithPluralCIDRs(t *testing.T) {
	t.Parallel()

	files := []string{
		"zfs.csi.randomvariable.co.uk_volumes.yaml",
		"zfs.csi.randomvariable.co.uk_volumeimports.yaml",
		"zfs.csi.randomvariable.co.uk_snapshots.yaml",
		"nvmet.randomvariable.co.uk_nvmeexports.yaml",
		"zfs.csi.randomvariable.co.uk_storagenodes.yaml",
	}
	for _, name := range files {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			crd := readCRD(t, filepath.Join("..", "..", "deploy", "crd", name))
			if crd.Spec.Scope != extensionsv1.ClusterScoped {
				t.Fatalf("scope = %q, want %q", crd.Spec.Scope, extensionsv1.ClusterScoped)
			}
		})
	}

	for _, tc := range []struct {
		file string
		path string
	}{
		{"zfs.csi.randomvariable.co.uk_volumes.yaml", "nfsExportCIDRs"},
		{"zfs.csi.randomvariable.co.uk_volumeimports.yaml", "nfsExportCIDRs"},
	} {
		crd := readCRD(t, filepath.Join("..", "..", "deploy", "crd", tc.file))
		field := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties[tc.path]
		if field.Type != "array" || field.Items == nil || field.Items.Schema == nil || field.Items.Schema.Type != "string" || field.MinItems == nil || *field.MinItems != 1 || field.XListType == nil || *field.XListType != "set" {
			t.Fatalf("%s spec.%s schema = %#v, want non-empty string set", tc.file, tc.path, field)
		}
	}

	volumeCRD := readCRD(t, filepath.Join("..", "..", "deploy", "crd", "zfs.csi.randomvariable.co.uk_volumes.yaml"))
	volumeSpec := volumeCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
	ownerNode := volumeSpec.Properties["ownerNode"]
	if ownerNode.MinLength == nil || *ownerNode.MinLength != 1 {
		t.Fatalf("Volume spec.ownerNode minLength = %v, want 1", ownerNode.MinLength)
	}
	if !contains(volumeSpec.Required, "ownerNode") {
		t.Fatalf("Volume spec required fields = %v, want ownerNode", volumeSpec.Required)
	}
	if len(ownerNode.XValidations) != 1 || ownerNode.XValidations[0].Rule != "self == oldSelf" {
		t.Fatalf("Volume spec.ownerNode validations = %#v, want immutable CEL rule", ownerNode.XValidations)
	}
	for _, name := range []string{"nfsTLSEnabled", "nvmeTLSEnabled", "nvmeTLSPSKSecretName"} {
		field := volumeSpec.Properties[name]
		if len(field.XValidations) != 1 || field.XValidations[0].Rule != "self == oldSelf" {
			t.Fatalf("Volume spec.%s validations = %#v, want immutable CEL rule", name, field.XValidations)
		}
	}
	nfsRootSquash := volumeSpec.Properties["nfsRootSquash"]
	if len(nfsRootSquash.XValidations) != 1 || nfsRootSquash.XValidations[0].Rule != "self == oldSelf || (oldSelf == false && self == true)" {
		t.Fatalf("Volume spec.nfsRootSquash validation = %#v, want one-way tightening rule", nfsRootSquash.XValidations)
	}
	// ZFS fixes a zvol's volblocksize at creation and a clone inherits it from its
	// origin. Every capacity the driver aligns (create, expand, clone, restore) is
	// computed against this value, so a mutated volBlockSize would make already
	// persisted capacities illegal volsizes.
	volBlockSize := volumeSpec.Properties["volBlockSize"]
	if contains(volumeSpec.Required, "volBlockSize") {
		t.Fatalf("Volume spec.volBlockSize must stay optional for filesystem volumes and legacy CRs")
	}
	if len(volBlockSize.XValidations) != 1 || volBlockSize.XValidations[0].Rule != "self == oldSelf" {
		t.Fatalf("Volume spec.volBlockSize validations = %#v, want immutable CEL rule", volBlockSize.XValidations)
	}

	if got := volumeSpec.Properties["nvmeTLSPSKSecretName"].Pattern; got != "^zfs-csi-nvme-psk-[a-z0-9]([-a-z0-9]*[a-z0-9])?$" {
		t.Fatalf("Volume spec.nvmeTLSPSKSecretName pattern = %q", got)
	}
	var hasNVMeModeContract, hasNVMeDerivedRefContract bool
	for _, validation := range volumeCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.XValidations {
		if strings.Contains(validation.Rule, "nvmeTLSEnabled") && strings.Contains(validation.Rule, "nvmeTLSPSKSecretName") {
			hasNVMeModeContract = true
		}
		if strings.Contains(validation.Rule, "nvmeTLSPSKSecretName") && strings.Contains(validation.Rule, "self.metadata.name") {
			hasNVMeDerivedRefContract = true
		}
	}
	if !hasNVMeModeContract || !hasNVMeDerivedRefContract {
		t.Fatalf("Volume CRD NVMe TLS contracts = mode/ref %t, derived ref %t", hasNVMeModeContract, hasNVMeDerivedRefContract)
	}
	for _, validation := range volumeCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.XValidations {
		if strings.Contains(validation.Rule, "nvmeTLSEnabled") && (strings.Contains(validation.Rule, "sourceSnapshotID") || strings.Contains(validation.Rule, "sourceVolumeID")) {
			t.Fatalf("Volume CRD NVMe TLS validation still rejects sourced volumes: %q", validation.Rule)
		}
	}
	snapshotCRD := readCRD(t, filepath.Join("..", "..", "deploy", "crd", "zfs.csi.randomvariable.co.uk_snapshots.yaml"))
	snapshotSpec := snapshotCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
	snapshotOwnerNode := snapshotSpec.Properties["ownerNode"]
	if !contains(snapshotSpec.Required, "ownerNode") || len(snapshotOwnerNode.XValidations) != 1 || snapshotOwnerNode.XValidations[0].Rule != "self == oldSelf" {
		t.Fatalf("Snapshot spec.ownerNode required=%v validations=%#v, want required immutable CEL rule", snapshotSpec.Required, snapshotOwnerNode.XValidations)
	}
	// A restore clones the snapshot and inherits its source's volblocksize, so the
	// Snapshot records that block size immutably and stays authoritative for
	// capacity alignment even when the parent Volume CR is gone.
	sourceVolBlockSize := snapshotSpec.Properties["sourceVolBlockSize"]
	if contains(snapshotSpec.Required, "sourceVolBlockSize") {
		t.Fatalf("Snapshot spec.sourceVolBlockSize must stay optional for snapshots taken by older drivers")
	}
	if sourceVolBlockSize.Type != "string" || sourceVolBlockSize.Pattern != volumeSpec.Properties["volBlockSize"].Pattern {
		t.Fatalf("Snapshot spec.sourceVolBlockSize schema = %#v, want string matching Volume spec.volBlockSize pattern %q",
			sourceVolBlockSize, volumeSpec.Properties["volBlockSize"].Pattern)
	}
	if len(sourceVolBlockSize.XValidations) != 1 || sourceVolBlockSize.XValidations[0].Rule != "self == oldSelf" {
		t.Fatalf("Snapshot spec.sourceVolBlockSize validations = %#v, want immutable CEL rule", sourceVolBlockSize.XValidations)
	}
	for kind, field := range map[string]extensionsv1.JSONSchemaProps{
		"Volume":   volumeSpec.Properties["poolGUID"],
		"Snapshot": snapshotSpec.Properties["poolGUID"],
	} {
		if field.Pattern != "^[1-9][0-9]{0,19}$" || len(field.XValidations) != 1 || field.XValidations[0].Rule != "self == oldSelf" {
			t.Fatalf("%s spec.poolGUID schema = %#v, want canonical immutable identity", kind, field)
		}
	}

	storageNodeCRD := readCRD(t, filepath.Join("..", "..", "deploy", "crd", "zfs.csi.randomvariable.co.uk_storagenodes.yaml"))
	storageNodeSpec := storageNodeCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
	if !contains(storageNodeSpec.Required, "authoritativePoolGUIDs") || !contains(storageNodeSpec.Required, "networkDomain") {
		t.Fatalf("StorageNode required fields = %v", storageNodeSpec.Required)
	}
	if storageNodeSpec.Properties["enabled"].Default == nil {
		t.Fatal("StorageNode spec.enabled lacks default")
	}
	identity := storageNodeSpec.Properties["authoritativePoolGUIDs"]
	if identity.MinItems == nil || *identity.MinItems != 1 || identity.XListType == nil || *identity.XListType != "set" {
		t.Fatalf("StorageNode authoritativePoolGUIDs schema = %#v", identity)
	}
	if len(identity.XValidations) != 2 || identity.XValidations[1].Rule != "self == oldSelf" {
		t.Fatalf("StorageNode authoritativePoolGUIDs validations = %#v", identity.XValidations)
	}
	pools := storageNodeCRD.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"].Properties["pools"]
	if pools.XListType == nil || *pools.XListType != "map" || len(pools.XListMapKeys) != 1 || pools.XListMapKeys[0] != "guid" {
		t.Fatalf("StorageNode status.pools topology = %#v", pools)
	}
}

// TestChartCRDsMatchDeployedCRDs guards the regeneration workflow: controller-gen
// writes only deploy/crd, but the Helm chart ships its own copy under
// templates/crd and the E2E lane installs discovery from THAT copy. A schema
// regenerated in one location and not the other silently gives chart-installed
// clusters a different API contract from the one this repo's tests validate.
func TestChartCRDsMatchDeployedCRDs(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"zfs.csi.randomvariable.co.uk_volumes.yaml",
		"zfs.csi.randomvariable.co.uk_volumeimports.yaml",
		"zfs.csi.randomvariable.co.uk_snapshots.yaml",
		"nvmet.randomvariable.co.uk_nvmeexports.yaml",
		"zfs.csi.randomvariable.co.uk_storagenodes.yaml",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			deployed := readCRD(t, filepath.Join("..", "..", "deploy", "crd", name))
			charted := readCRD(t, filepath.Join("..", "..", "charts", "zfs-csi", "templates", "crd", name))
			if !reflect.DeepEqual(deployed.Spec, charted.Spec) {
				t.Fatalf("chart CRD %s spec differs from deploy/crd; re-run the controller-gen workflow and copy the result into charts/zfs-csi/templates/crd", name)
			}
		})
	}
}

func TestValidateAuthoritativePoolGUIDs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		guids []string
		valid bool
	}{
		{name: "single", guids: []string{"1"}, valid: true},
		{name: "uint64 max", guids: []string{"18446744073709551615"}, valid: true},
		{name: "required", valid: false},
		{name: "zero", guids: []string{"0"}, valid: false},
		{name: "leading zero", guids: []string{"01"}, valid: false},
		{name: "overflow", guids: []string{"18446744073709551616"}, valid: false},
		{name: "duplicate", guids: []string{"1", "1"}, valid: false},
		{name: "non decimal", guids: []string{"0x1"}, valid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := zfscsiv1.ValidateAuthoritativePoolGUIDs(tc.guids)
			if (err == nil) != tc.valid {
				t.Fatalf("ValidateAuthoritativePoolGUIDs(%v) error=%v, valid=%v", tc.guids, err, tc.valid)
			}
		})
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func readCRD(t *testing.T, path string) *extensionsv1.CustomResourceDefinition {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	crd := &extensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(data, crd); err != nil {
		t.Fatal(err)
	}
	return crd
}
