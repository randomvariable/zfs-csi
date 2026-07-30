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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// zfsCSIDriverName is the CSI driver / provisioner name; used to assert the
// smoke PV was actually provisioned by zfs-csi and not some fallback.
const zfsCSIDriverName = "zfs.csi.randomvariable.co.uk"

// nfsRwxSmoke proves the NFS (RWX) path: a writer and a reader pod on two
// distinct non-storage nodes mount the same RWX PVC concurrently, and the
// reader reads the bytes the writer wrote while both mounts are live. This is
// the only honest RWX proof — a single pod, or sequential writer-then-reader on
// one node, would prove nothing beyond RWO.
func nfsRwxSmoke(ctx context.Context, c client.Client, namespace, storageClass string) error {
	const group = "nfs-rwx"
	message := "zfs-csi-nfs-rwx-proof"
	suffix := smokeStorageClassSuffix(storageClass)
	pvc := pvcObject(namespace, "zfs-csi-e2e-nfs-"+suffix, storageClass, corev1.ReadWriteMany)
	writer := smokePod(namespace, "zfs-csi-e2e-nfs-writer-"+suffix, pvc.Name, group, writeProofCmd(message))
	reader := smokePod(namespace, "zfs-csi-e2e-nfs-reader-"+suffix, pvc.Name, group, readProofCmd(message))

	if err := resetSmokeObjects(ctx, c, pvc, reader, writer); err != nil {
		return err
	}
	if err := c.Create(ctx, pvc); err != nil {
		return fmt.Errorf("create nfs rwx pvc: %w", err)
	}
	if err := c.Create(ctx, writer); err != nil {
		return fmt.Errorf("create nfs writer: %w", err)
	}
	if err := c.Create(ctx, reader); err != nil {
		return fmt.Errorf("create nfs reader: %w", err)
	}

	// The writer holds its mount open (sleep) while the reader polls for the
	// bytes; the reader exits 0 once it sees them, proving concurrent RWX.
	if err := waitForPodRunning(ctx, c, keyOf(writer), 5*time.Minute); err != nil {
		return fmt.Errorf("nfs writer not running: %w", err)
	}
	if err := waitForPodSucceeded(ctx, c, keyOf(reader), 10*time.Minute); err != nil {
		return fmt.Errorf("nfs reader did not read writer bytes cross-node: %w", err)
	}
	if err := assertProvisionedByDriver(ctx, c, keyOf(pvc)); err != nil {
		return err
	}

	if err := resetSmokeObjects(ctx, c, pvc, reader, writer); err != nil {
		return err
	}

	return nil
}

// nvmeSmoke proves the NVMe-TCP (RWO) path: one pod on a non-storage worker
// binds an RWO PVC, writes and reads back its own bytes, and the volume is
// attached via a VolumeAttachment. The pod carries no storage toleration, so it
// runs on a non-storage node and exercises genuine guest-to-guest NVMe-TCP.
func nvmeSmoke(ctx context.Context, c client.Client, namespace, storageClass string) error {
	message := "zfs-csi-nvme-rwo-proof"
	suffix := smokeStorageClassSuffix(storageClass)
	pvc := pvcObject(namespace, "zfs-csi-e2e-nvme-"+suffix, storageClass, corev1.ReadWriteOnce)
	// writeProofCmd writes, verifies its own read, then holds the mount so the
	// VolumeAttachment can be asserted while the volume is live.
	pod := smokePod(namespace, "zfs-csi-e2e-nvme-consumer-"+suffix, pvc.Name, "", writeProofCmd(message))

	if err := resetSmokeObjects(ctx, c, pvc, pod); err != nil {
		return err
	}
	if err := c.Create(ctx, pvc); err != nil {
		return fmt.Errorf("create nvme rwo pvc: %w", err)
	}
	if err := c.Create(ctx, pod); err != nil {
		return fmt.Errorf("create nvme consumer: %w", err)
	}

	// Reaching Running means the write+read verify passed (the command tests
	// its own read before sleeping); a failed verify would make the pod Failed.
	if err := waitForPodRunning(ctx, c, keyOf(pod), 10*time.Minute); err != nil {
		return fmt.Errorf("nvme consumer not running (write/read/attach failed): %w", err)
	}
	if err := assertProvisionedByDriver(ctx, c, keyOf(pvc)); err != nil {
		return err
	}

	if err := resetSmokeObjects(ctx, c, pvc, pod); err != nil {
		return err
	}

	return nil
}

func smokeStorageClassSuffix(storageClass string) string {
	return strings.Trim(strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return '-'
	}, strings.ToLower(storageClass)), "-")
}

func resetSmokeObjects(
	ctx context.Context,
	c client.Client,
	pvc *corev1.PersistentVolumeClaim,
	pods ...client.Object,
) error {
	const deletionTimeout = 3 * time.Minute

	// Sweep EVERY pod and PVC in the namespace carrying this run's ownership
	// labels, not only the fixed-name objects about to be recreated. A failed
	// earlier smoke of a different StorageClass variant (e.g. zfs-csi-nfs-tls
	// vs zfs-csi-nfs) leaves different-named pods whose shared anti-affinity
	// group would otherwise occupy both consumer nodes and deadlock
	// scheduling. The ownership labels scope the sweep to this run only.
	stale, err := listOwnedSmokeObjects(ctx, c, pvc.Namespace)
	if err != nil {
		return fmt.Errorf("list stale smoke objects: %w", err)
	}
	// Preserve the pods-before-PVC deletion contract: a PVC must not be
	// deleted (and recreated) while any consumer pod still references it.
	stalePods, stalePVCs := []client.Object{}, []client.Object{}
	for _, obj := range stale {
		switch obj.(type) {
		case *corev1.Pod:
			stalePods = append(stalePods, obj)
		case *corev1.PersistentVolumeClaim:
			stalePVCs = append(stalePVCs, obj)
		}
	}

	// Pods must be fully gone before deleting or recreating their fixed-name PVC.
	allPods := append(stalePods, pods...)
	if err := waitForSmokeObjectsDeleted(ctx, c, allPods, deletionTimeout); err != nil {
		return fmt.Errorf("delete smoke pods: %w", err)
	}
	allPVCs := append(stalePVCs, pvc)
	if err := waitForSmokeObjectsDeleted(ctx, c, allPVCs, deletionTimeout); err != nil {
		return fmt.Errorf("delete smoke pvc: %w", err)
	}
	return nil
}

// listOwnedSmokeObjects returns every pod and PVC in namespace carrying the
// run's ownership labels, for the cross-variant stale sweep in
// resetSmokeObjects.
func listOwnedSmokeObjects(ctx context.Context, c client.Client, namespace string) ([]client.Object, error) {
	selector := client.MatchingLabels(smokeOwnershipLabels())
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList, client.InNamespace(namespace), selector); err != nil {
		return nil, err
	}
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcList, client.InNamespace(namespace), selector); err != nil {
		return nil, err
	}
	objects := make([]client.Object, 0, len(podList.Items)+len(pvcList.Items))
	for i := range podList.Items {
		objects = append(objects, &podList.Items[i])
	}
	for i := range pvcList.Items {
		objects = append(objects, &pvcList.Items[i])
	}
	return objects, nil
}

func waitForSmokeObjectsDeleted(
	ctx context.Context,
	c client.Client,
	objects []client.Object,
	timeout time.Duration,
) error {
	for _, obj := range objects {
		if err := deleteIfExists(ctx, c, obj); err != nil {
			return err
		}
	}
	return waitForScenarioObjectsGone(ctx, c, objects, timeout)
}

// assertProvisionedByDriver verifies the PVC's PV was provisioned by zfs-csi and
// is genuinely attached (a VolumeAttachment referencing the PV reports
// Status.Attached==true). Without this a mis-provisioned local/hostPath-like
// fallback could pass the write/read smoke.
func assertProvisionedByDriver(ctx context.Context, c client.Client, pvcKey types.NamespacedName) error {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, pvcKey, pvc); err != nil {
		return fmt.Errorf("get pvc %s: %w", pvcKey, err)
	}
	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		return fmt.Errorf("pvc %s has no bound PV", pvcKey)
	}

	pv := &corev1.PersistentVolume{}
	if err := c.Get(ctx, types.NamespacedName{Name: pvName}, pv); err != nil {
		return fmt.Errorf("get pv %s: %w", pvName, err)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != zfsCSIDriverName {
		return fmt.Errorf("pv %s was not provisioned by %s (CSI=%v)", pvName, zfsCSIDriverName, pv.Spec.CSI)
	}

	return waitForVolumeAttached(ctx, c, pvName, 2*time.Minute)
}

// waitForVolumeAttached waits until a VolumeAttachment referencing pvName
// reports Status.Attached==true.
func waitForVolumeAttached(ctx context.Context, c client.Client, pvName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		list := &storagev1.VolumeAttachmentList{}
		if err := c.List(ctx, list); err != nil {
			return fmt.Errorf("list volumeattachments: %w", err)
		}
		for i := range list.Items {
			va := &list.Items[i]
			if va.Spec.Attacher != zfsCSIDriverName {
				continue
			}
			if va.Spec.Source.PersistentVolumeName != nil && *va.Spec.Source.PersistentVolumeName == pvName && va.Status.Attached {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timed out waiting for an attached VolumeAttachment for pv %s", pvName)
}

// waitForPodRunning waits until the pod is Running (or already Succeeded);
// returns an error if it Fails.
func waitForPodRunning(ctx context.Context, c client.Client, key types.NamespacedName, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pod := &corev1.Pod{}
		if err := c.Get(ctx, key, pod); err != nil {
			if apierrors.IsNotFound(err) {
				time.Sleep(2 * time.Second)
				continue
			}
			return err
		}
		switch pod.Status.Phase {
		case corev1.PodRunning, corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			return fmt.Errorf("pod %s/%s failed", key.Namespace, key.Name)
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timed out waiting for pod %s/%s to run", key.Namespace, key.Name)
}

func keyOf(obj client.Object) types.NamespacedName {
	return types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
}
