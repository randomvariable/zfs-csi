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
	"sort"

	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultStorageClassAnnotation = "storageclass.kubernetes.io/is-default-class"
	zfsCSIProvisioner             = "zfs.csi.randomvariable.co.uk"
)

func snapshotDefaultStorageClasses(ctx context.Context, reader client.Reader) ([]string, error) {
	var classes storagev1.StorageClassList
	if err := reader.List(ctx, &classes); err != nil {
		return nil, fmt.Errorf("list StorageClasses before zfs-csi install: %w", err)
	}
	return defaultStorageClassNames(classes.Items), nil
}

func assertStaticStorageClassDefaults(ctx context.Context, reader client.Reader, baseline, baselineZFS []string) error {
	var classes storagev1.StorageClassList
	if err := reader.List(ctx, &classes); err != nil {
		return fmt.Errorf("list StorageClasses after zfs-csi install: %w", err)
	}

	current := defaultStorageClassNames(classes.Items)
	if err := validateStaticStorageClassDefaults(baseline, classes.Items); err != nil {
		return fmt.Errorf("StorageClass default invariant failed (before=%v after=%v before zfs defaults=%v after zfs defaults=%v): %w",
			baseline, current, baselineZFS, zfsDefaultStorageClassNames(classes.Items), err)
	}
	return nil
}

func defaultStorageClassNames(classes []storagev1.StorageClass) []string {
	defaults := make([]string, 0, len(classes))
	for _, class := range classes {
		if class.Annotations[defaultStorageClassAnnotation] == "true" {
			defaults = append(defaults, class.Name)
		}
	}
	sort.Strings(defaults)
	return defaults
}

func zfsDefaultStorageClassNames(classes []storagev1.StorageClass) []string {
	defaults := make([]string, 0, len(classes))
	for _, class := range classes {
		if class.Provisioner == zfsCSIProvisioner && class.Annotations[defaultStorageClassAnnotation] == "true" {
			defaults = append(defaults, class.Name)
		}
	}
	sort.Strings(defaults)
	return defaults
}

func validateStaticStorageClassDefaults(baseline []string, classes []storagev1.StorageClass) error {
	current := defaultStorageClassNames(classes)
	if zfsDefaults := zfsDefaultStorageClassNames(classes); len(zfsDefaults) != 0 {
		return fmt.Errorf("zfs-csi StorageClasses annotated default: %v", zfsDefaults)
	}
	if !defaultStorageClassSetsEqual(baseline, current) {
		return fmt.Errorf("pre-existing default StorageClasses changed: baseline=%v current=%v", baseline, current)
	}
	return nil
}

func defaultStorageClassSetsEqual(before, after []string) bool {
	return sameStringSet(before, after)
}
