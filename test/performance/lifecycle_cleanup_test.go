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
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type lifecycleClient struct {
	client.Client
	created, deleted []string
}

func (c *lifecycleClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.created = append(c.created, obj.GetName())
	return c.Client.Create(ctx, obj, opts...)
}

func (c *lifecycleClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.deleted = append(c.deleted, obj.GetName())
	if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok {
		current := &corev1.PersistentVolumeClaim{}
		if c.Get(ctx, client.ObjectKeyFromObject(pvc), current) == nil && current.Spec.VolumeName != "" {
			_ = c.Client.Delete(
				ctx,
				&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: current.Spec.VolumeName}},
			)
		}
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func lifecycleRunner(t *testing.T) (*Runner, *lifecycleClient) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	tracked := &lifecycleClient{Client: base}
	r := &Runner{
		Client:          tracked,
		Namespace:       "default",
		DriverNamespace: "zfs-csi",
		ConsumerNode:    "worker",
		PollInterval:    time.Millisecond,
		runID:           "run-one",
		Environment:     Environment{Fingerprint: "fp"},
	}
	return r, tracked
}

func bindLifecyclePVC(t *testing.T, c client.Client, pvcName string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: pvcName}, pvc); err != nil {
		t.Fatal(err)
	}
	pvName := "pv-" + pvcName
	handle := "csi:pool:block:" + safeName(pvcName)
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: Provisioner, VolumeHandle: handle},
			},
		},
	}
	if err := c.Create(context.Background(), pv); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(pvc.DeepCopy())
	pvc.Spec.VolumeName = pvName
	if err := c.Patch(context.Background(), pvc, patch); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleCleanupRunsAfterPVCAndPodFailures(t *testing.T) {
	for _, stage := range []string{"after-pvc-create", "after-pod-create"} {
		t.Run(stage, func(t *testing.T) {
			r, tracked := lifecycleRunner(t)
			r.lifecycleHook = func(got string) error {
				if got == "after-pvc-create" {
					bindLifecyclePVC(t, tracked, safeName("perf-run-one-s-v-00"))
				}
				if got == stage {
					return errors.New("injected")
				}
				return nil
			}
			err := r.runLifecycleIteration(
				context.Background(),
				Scenario{Name: "s", Transport: "nvme"},
				Variant{Name: "v"},
				0,
				false,
			)
			if err == nil {
				t.Fatal("expected injected failure")
			}
			for _, name := range tracked.created {
				probe := &corev1.Pod{}
				podErr := tracked.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, probe)
				pvcErr := tracked.Get(
					context.Background(),
					client.ObjectKey{Namespace: "default", Name: name},
					&corev1.PersistentVolumeClaim{},
				)
				if podErr == nil || pvcErr == nil {
					t.Fatalf("leaked object %s", name)
				}
				if !apierrors.IsNotFound(podErr) && !apierrors.IsNotFound(pvcErr) {
					t.Fatalf("unexpected errors %v %v", podErr, pvcErr)
				}
			}
		})
	}
}

func TestLifecycleNamesDifferAcrossRuns(t *testing.T) {
	r1, _ := lifecycleRunner(t)
	r2, _ := lifecycleRunner(t)
	r2.runID = "run-two"
	n1 := safeName("perf-" + r1.runID + "-scenario-variant-00")
	n2 := safeName("perf-" + r2.runID + "-scenario-variant-00")
	if n1 == n2 {
		t.Fatal("lifecycle names collide across runs")
	}
}
