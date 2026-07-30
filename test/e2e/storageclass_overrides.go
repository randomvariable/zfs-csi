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
	"slices"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

const (
	helmReleaseNameAnnotation      = "meta.helm.sh/release-name"
	helmReleaseNamespaceAnnotation = "meta.helm.sh/release-namespace"
	helmManagedByLabel             = "app.kubernetes.io/managed-by"
	helmManagedByValue             = "Helm"
	zfsCSIHelmReleaseName          = "zfs-csi"
)

// chartStorageClassDefaults maps the chart's storageClasses value keys to the
// default StorageClass names the chart renders. E2E_STORAGE_CLASS_OVERRIDES
// entries are validated against these keys, and the default names are what the
// baseline testdrivers/smokes reference by literal. The chart derives the
// encrypted class name as "<tankNVMe.name>-encrypted", so it has no key here —
// renameStorageClassMap derives its rename from the tankNVMe override.
var chartStorageClassDefaults = map[string]string{
	"tankNVMe":    "zfs-tank-nvme",
	"tankNFS":     "zfs-tank-nfs",
	"tankNFSTLS":  "zfs-tank-nfs-tls",
	"tankNVMeTLS": "zfs-tank-nvme-tls",
	"flashNVMe":   "zfs-flash-nvme",
	"flashNFS":    "zfs-flash-nfs",
}

// constrainStaticStorageClasses keeps upstream-created test pods on static
// consumer nodes when ClientNodeSelection is nil. External-storage tests copy
// these constraints from the chart class into each generated class.
func constrainStaticStorageClasses(ctx context.Context, c client.Client, domains []string) error {
	domains = slices.Clone(domains)
	slices.Sort(domains)
	domains = slices.Compact(domains)
	if len(domains) == 0 {
		return fmt.Errorf("static StorageClass topology requires at least one consumer network domain")
	}
	want := []corev1.TopologySelectorTerm{{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{
		Key: e2eNetworkDomainLabelKey, Values: domains,
	}}}}

	classes := &storagev1.StorageClassList{}
	if err := c.List(ctx, classes); err != nil {
		return fmt.Errorf("list StorageClasses for static topology: %w", err)
	}
	var owned []*storagev1.StorageClass
	for i := range classes.Items {
		sc := &classes.Items[i]
		if sc.Provisioner != zfsCSIProvisioner ||
			sc.Annotations[helmReleaseNameAnnotation] != zfsCSIHelmReleaseName ||
			sc.Annotations[helmReleaseNamespaceAnnotation] != zfsCSINamespace ||
			sc.Labels[helmManagedByLabel] != helmManagedByValue {
			continue
		}
		if len(sc.AllowedTopologies) > 0 && !staticStorageClassTopologiesCompatible(sc.AllowedTopologies, domains) {
			return fmt.Errorf("Helm-owned zfs-csi StorageClass %q has conflicting AllowedTopologies", sc.Name)
		}
		owned = append(owned, sc)
	}
	if len(owned) == 0 {
		return fmt.Errorf("no Helm-owned zfs-csi StorageClasses found for static topology")
	}
	for _, sc := range owned {
		if len(sc.AllowedTopologies) > 0 {
			continue
		}
		base := sc.DeepCopy()
		sc.AllowedTopologies = want
		if err := c.Patch(ctx, sc, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("patch Helm-owned zfs-csi StorageClass %q topology: %w", sc.Name, err)
		}
	}
	return nil
}

func staticStorageClassTopologiesCompatible(topologies []corev1.TopologySelectorTerm, domains []string) bool {
	if len(topologies) != 1 || len(topologies[0].MatchLabelExpressions) != 1 {
		return false
	}
	requirement := topologies[0].MatchLabelExpressions[0]
	values := slices.Clone(requirement.Values)
	slices.Sort(values)
	values = slices.Compact(values)
	return requirement.Key == e2eNetworkDomainLabelKey && slices.Equal(values, domains)
}

// storageClassHelmOverrides converts the configured SC-name overrides into
// `--set-string storageClasses.<key>.name=<name>` style helm override pairs,
// suitable for installDriverFromChart / installMultiOwnerDriverFromChart
// extraOverrides. Empty when no overrides are configured (no behaviour change).
func storageClassHelmOverrides() (map[string]string, error) {
	configured, err := e2econfig.StorageClassOverrides()
	if err != nil {
		return nil, err
	}
	overrides := make(map[string]string, len(configured))
	for key, name := range configured {
		if _, known := chartStorageClassDefaults[key]; !known {
			return nil, fmt.Errorf("%s references unknown chart StorageClass key %q", e2econfig.Env[e2econfig.StorageClassOverridesKey], key)
		}
		overrides["storageClasses."+key+".name"] = name
	}
	return overrides, nil
}

// renameStorageClassMap returns default-name -> overridden-name pairs for the
// configured overrides, including the derived "<tankNVMe>-encrypted" class.
// Empty when no overrides are configured.
func renameStorageClassMap() (map[string]string, error) {
	configured, err := e2econfig.StorageClassOverrides()
	if err != nil {
		return nil, err
	}
	renames := make(map[string]string, len(configured)+1)
	for key, name := range configured {
		defaultName, known := chartStorageClassDefaults[key]
		if !known {
			return nil, fmt.Errorf("%s references unknown chart StorageClass key %q", e2econfig.Env[e2econfig.StorageClassOverridesKey], key)
		}
		renames[defaultName] = name
		if key == "tankNVMe" {
			renames[defaultName+"-encrypted"] = name + "-encrypted"
		}
	}
	return renames, nil
}

// smokeStorageClassName maps a baseline (default) StorageClass name through
// the configured overrides so the smoke/scenario paths target the classes the
// chart actually installed. Identity when no override applies.
func smokeStorageClassName(defaultName string) (string, error) {
	renames, err := renameStorageClassMap()
	if err != nil {
		return "", err
	}
	if renamed, ok := renames[defaultName]; ok {
		return renamed, nil
	}
	return defaultName, nil
}

// materializeTestDrivers returns the testdriver manifests the conformance run
// should mount. When no transformation applies it returns the baseline
// manifests unchanged. Otherwise each manifest is copied into
// <artifactDir>/testdriver/ with:
//   - StorageClass.FromExistingClassName rewritten through the rename map, so
//     the external suite copies the RENAMED chart-installed class instead of a
//     same-named class owned by someone else.
//   - top-level NodeSelectors injected from the consumer-group selector on the
//     static provider. The external suite merges those selectors into EVERY
//     test pod's node selection (external.go PrepareTest), so pods only land on
//     nodes that run the node-plugin DaemonSet — the upstream multi-node tests
//     then add a NotIn[firstNode] affinity that still lands on the OTHER
//     consumer-group node. Without the pin, cross-node tests schedule onto any
//     untainted node (e.g. one without the plugin) and time out mounting.
//
// Snapshot/VolumeAttributesClass references are not StorageClass names and are
// left untouched. Generated copies are plain YAML round-trips (comments drop).
func materializeTestDrivers(artifactDir string, manifests []string) ([]string, error) {
	renames, err := renameStorageClassMap()
	if err != nil {
		return nil, err
	}
	nodeSelectors := smokeConsumerNodeSelector()
	if len(renames) == 0 && len(nodeSelectors) == 0 {
		return manifests, nil
	}
	outDir := filepath.Join(artifactDir, "testdriver")
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return nil, fmt.Errorf("create generated testdriver dir: %w", err)
	}
	generated := make([]string, 0, len(manifests))
	for _, manifest := range manifests {
		body, err := os.ReadFile(manifest)
		if err != nil {
			return nil, fmt.Errorf("read testdriver %q: %w", manifest, err)
		}
		rewritten, err := rewriteTestDriverStorageClass(body, renames, nodeSelectors)
		if err != nil {
			return nil, fmt.Errorf("rewrite testdriver %q: %w", manifest, err)
		}
		outPath := filepath.Join(outDir, filepath.Base(manifest))
		if err := os.WriteFile(outPath, rewritten, 0o600); err != nil {
			return nil, fmt.Errorf("write generated testdriver %q: %w", outPath, err)
		}
		generated = append(generated, outPath)
	}
	return generated, nil
}

// rewriteTestDriverStorageClass rewrites StorageClass.FromExistingClassName in
// one testdriver document through the rename map and injects top-level
// NodeSelectors when nodeSelectors is non-empty. A testdriver referencing a
// class with no configured rename keeps its reference unchanged.
func rewriteTestDriverStorageClass(body []byte, renames map[string]string, nodeSelectors map[string]string) ([]byte, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	storageClass, ok := doc["StorageClass"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("testdriver lacks a StorageClass section")
	}
	name, ok := storageClass["FromExistingClassName"].(string)
	if !ok || name == "" {
		return nil, fmt.Errorf("testdriver StorageClass lacks FromExistingClassName")
	}
	if renamed, found := renames[name]; found {
		storageClass["FromExistingClassName"] = renamed
	}
	if len(nodeSelectors) > 0 {
		selectors := make(map[string]any, len(nodeSelectors))
		for key, value := range nodeSelectors {
			selectors[key] = value
		}
		doc["NodeSelectors"] = selectors
	}
	return yaml.Marshal(doc)
}
