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
	"strings"
	"testing"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateStaticStorageClassDefaults(t *testing.T) {
	classes := []storagev1.StorageClass{
		{ObjectMeta: metav1.ObjectMeta{Name: "ceph-rbd", Annotations: map[string]string{defaultStorageClassAnnotation: "true"}}, Provisioner: "rook-ceph.rbd.csi.ceph.com"},
		{ObjectMeta: metav1.ObjectMeta{Name: "zfs-tank-nvme", Annotations: map[string]string{defaultStorageClassAnnotation: "true"}}, Provisioner: zfsCSIProvisioner},
	}
	if err := validateStaticStorageClassDefaults([]string{"ceph-rbd"}, classes); err == nil || !strings.Contains(err.Error(), "zfs-csi StorageClasses annotated default: [zfs-tank-nvme]") {
		t.Fatalf("zfs default error = %v", err)
	}
}

func TestValidateStaticStorageClassDefaultsDetectsChangedSet(t *testing.T) {
	classes := []storagev1.StorageClass{{ObjectMeta: metav1.ObjectMeta{Name: "new-default", Annotations: map[string]string{defaultStorageClassAnnotation: "true"}}}}
	err := validateStaticStorageClassDefaults([]string{"ceph-rbd"}, classes)
	if err == nil || !strings.Contains(err.Error(), "baseline=[ceph-rbd] current=[new-default]") {
		t.Fatalf("changed default set error = %v", err)
	}
}

func TestDefaultStorageClassNamesSortsNamesAndIgnoresNonDefaults(t *testing.T) {
	classes := []storagev1.StorageClass{
		{ObjectMeta: metav1.ObjectMeta{Name: "zeta", Annotations: map[string]string{defaultStorageClassAnnotation: "true"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "ignored", Annotations: map[string]string{defaultStorageClassAnnotation: "false"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "alpha", Annotations: map[string]string{defaultStorageClassAnnotation: "true"}}},
	}
	if got, want := defaultStorageClassNames(classes), []string{"alpha", "zeta"}; !sameStringSet(got, want) || got[0] != "alpha" || got[1] != "zeta" {
		t.Fatalf("default StorageClasses = %v, want %v", got, want)
	}
}

func TestZFSDefaultStorageClassNamesSortsAndFilters(t *testing.T) {
	classes := []storagev1.StorageClass{
		{ObjectMeta: metav1.ObjectMeta{Name: "zeta", Annotations: map[string]string{defaultStorageClassAnnotation: "true"}}, Provisioner: zfsCSIProvisioner},
		{ObjectMeta: metav1.ObjectMeta{Name: "other", Annotations: map[string]string{defaultStorageClassAnnotation: "true"}}, Provisioner: "other.csi.example"},
		{ObjectMeta: metav1.ObjectMeta{Name: "alpha", Annotations: map[string]string{defaultStorageClassAnnotation: "true"}}, Provisioner: zfsCSIProvisioner},
	}
	if got, want := zfsDefaultStorageClassNames(classes), []string{"alpha", "zeta"}; !sameStringSet(got, want) || got[0] != "alpha" || got[1] != "zeta" {
		t.Fatalf("zfs default StorageClasses = %v, want %v", got, want)
	}
}

func TestDefaultStorageClassSetsEqualIsOrderIndependent(t *testing.T) {
	if !defaultStorageClassSetsEqual([]string{"zeta", "alpha"}, []string{"alpha", "zeta"}) {
		t.Fatal("equal default StorageClass sets reported unequal")
	}
	if defaultStorageClassSetsEqual([]string{"alpha"}, []string{"alpha", "zeta"}) {
		t.Fatal("different default StorageClass sets reported equal")
	}
}
