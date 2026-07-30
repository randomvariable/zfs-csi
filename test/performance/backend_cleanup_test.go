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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvmetv1alpha1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfsv1alpha1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

func cleanupTestRunner(t *testing.T, objects ...runtime.Object) *Runner {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := storagev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := zfsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := nvmetv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return &Runner{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build(),
		DriverNamespace: "zfs-csi",
		PollInterval:    time.Millisecond,
	}
}

func TestStorageDeletionPassesWithoutMatchingExport(t *testing.T) {
	r := cleanupTestRunner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.waitStorageDeleted(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"}}, "pv", "csi:pool:block:id"); err != nil {
		t.Fatal(err)
	}
}

func TestStorageDeletionDetectsMatchingClusterNVMeExport(t *testing.T) {
	export := &nvmetv1alpha1.NVMeExport{
		ObjectMeta: metav1.ObjectMeta{Name: "export"},
		Spec: nvmetv1alpha1.NVMeExportSpec{
			TargetNQN:  "nqn.2026-01.csi.randomvariable:zfs:pool:block:id",
			DevicePath: "/dev/zvol/pool/csi/block/id",
			Portal:     "127.0.0.1:4420",
		},
	}
	r := cleanupTestRunner(t, export)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := r.waitStorageDeleted(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"}}, "pv", "csi:pool:block:id"); err == nil {
		t.Fatal("accepted leaked NVMeExport")
	}
}

func TestStorageDeletionIgnoresOtherTarget(t *testing.T) {
	export := &nvmetv1alpha1.NVMeExport{
		ObjectMeta: metav1.ObjectMeta{Name: "export"},
		Spec: nvmetv1alpha1.NVMeExportSpec{
			TargetNQN:  "nqn.2026-01.csi.randomvariable:zfs:other:block:id",
			DevicePath: "/dev/zvol/other/csi/block/id",
			Portal:     "127.0.0.1:4420",
		},
	}
	r := cleanupTestRunner(t, export)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.waitStorageDeleted(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"}}, "pv", "csi:pool:block:id"); err != nil {
		t.Fatal(err)
	}
}
