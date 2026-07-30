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
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	vacName            = "zfs-csi-e2e-compression-zstd-3"
	vacUpdateName      = "zfs-csi-e2e-compression-lz4"
	vacImmutableName   = "zfs-csi-e2e-immutable-blocksize"
	vacScenarioPVC     = "zfs-csi-e2e-vac"
	vacScenarioPod     = "zfs-csi-e2e-vac-consumer"
	vacScenarioTimeout = 10 * time.Minute
)

var volumeGVR = schema.GroupVersionResource{
	Group:    "zfs.csi.randomvariable.co.uk",
	Version:  "v1alpha1",
	Resource: "volumes",
}

// runVolumeAttributesClassScenario proves both supported VAC paths. The initial
// class becomes CreateVolume mutable parameters, then a second class triggers
// ControllerModifyVolume after the PVC is bound.
func runVolumeAttributesClassScenario(
	ctx context.Context,
	c client.Client,
	kubeconfig, namespace, storageClass string,
) error {
	pvc := pvcObject(namespace, vacScenarioPVC, storageClass, corev1.ReadWriteOnce)
	pvc.Spec.VolumeAttributesClassName = stringPtr(vacName)
	pod := smokePod(namespace, vacScenarioPod, pvc.Name, "", "exec sleep 3600")
	objects := []client.Object{pod, pvc}
	if err := resetScenarioObjects(ctx, c, objects); err != nil {
		return err
	}
	defer deleteVolumeAttributesClass(context.Background(), kubeconfig, vacName)
	defer deleteVolumeAttributesClass(context.Background(), kubeconfig, vacUpdateName)
	defer deleteVolumeAttributesClass(context.Background(), kubeconfig, vacImmutableName)
	defer func() { _ = cleanupScenarioObjects(c, objects) }()

	if err := applyVolumeAttributesClass(ctx, kubeconfig, "compression-zstd-3.yaml", vacName); err != nil {
		return err
	}
	if err := c.Create(ctx, pvc); err != nil {
		return fmt.Errorf("create VAC PVC: %w", err)
	}
	if err := c.Create(ctx, pod); err != nil {
		return fmt.Errorf("create VAC consumer: %w", err)
	}
	if err := waitForPodRunning(ctx, c, keyOf(pod), vacScenarioTimeout); err != nil {
		return fmt.Errorf("VAC consumer did not mount: %w", err)
	}
	identity, err := boundVolumeIdentity(ctx, c, pvc)
	if err != nil {
		return fmt.Errorf("get VAC PV identity: %w", err)
	}
	if err := waitForVACConvergence(ctx, c, kubeconfig, namespace, pvc.Name, identity, vacName, "zstd-3"); err != nil {
		return fmt.Errorf("initial VAC CreateVolume convergence: %w", err)
	}
	if err := applyVolumeAttributesClass(ctx, kubeconfig, "compression-lz4.yaml", vacUpdateName); err != nil {
		return err
	}
	if err := setPVCVolumeAttributesClass(ctx, c, pvc, vacUpdateName); err != nil {
		return err
	}
	if err := waitForVACConvergence(ctx, c, kubeconfig, namespace, pvc.Name, identity, vacUpdateName, "lz4"); err != nil {
		return err
	}
	if err := waitForVACDeletionProtection(ctx, c, kubeconfig, namespace, pvc.Name, vacUpdateName); err != nil {
		return err
	}
	if err := proveImmutableVACRejected(ctx, c, kubeconfig, namespace, pvc.Name, identity); err != nil {
		return err
	}

	return nil
}

// proveImmutableVACRejected is conditional because Kubernetes versions may reject
// a new VAC association before the resizer calls the driver. If association is
// accepted, the driver must report InvalidArgument without changing compression.
func proveImmutableVACRejected(
	ctx context.Context,
	c client.Client,
	kubeconfig, namespace, pvcName string,
	identity volumeIdentity,
) error {
	if err := applyVolumeAttributesClass(ctx, kubeconfig, "immutable-blocksize.yaml", vacImmutableName); err != nil {
		return err
	}
	if err := setPVCVolumeAttributesClass(
		ctx, c,
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: pvcName}},
		vacImmutableName,
	); apierrors.IsInvalid(
		err,
	) {
		// Some Kubernetes releases reject this association at API validation, so
		// the driver never receives ControllerModifyVolume to reject it.
		return nil
	} else if err != nil {
		return fmt.Errorf("associate immutable VAC: %w", err)
	}
	return pollScenario(ctx, 2*time.Minute, func() (bool, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName}, pvc); err != nil {
			return false, err
		}
		if pvc.Status.ModifyVolumeStatus == nil {
			return false, nil
		}
		if pvc.Status.ModifyVolumeStatus.Status != corev1.PersistentVolumeClaimModifyVolumeInfeasible {
			return false, nil
		}
		property, err := storageAgentZFSProperty(ctx, c, kubeconfig, identity.volumeHandle, "compression")
		if err != nil {
			return false, err
		}
		if property != "lz4" {
			return false, fmt.Errorf("immutable VAC changed compression to %q", property)
		}
		return true, nil
	})
}

func applyVolumeAttributesClass(ctx context.Context, kubeconfig, filename, name string) error {
	path, err := filepath.Abs(filepath.Join("test", "e2e", "data", "vac", filename))
	if err != nil {
		return fmt.Errorf("resolve VolumeAttributesClass manifest: %w", err)
	}
	manifest, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read VolumeAttributesClass manifest: %w", err)
	}
	if !strings.Contains(string(manifest), "name: "+name) {
		return fmt.Errorf("VolumeAttributesClass manifest does not define %q", name)
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(string(manifest))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("apply VolumeAttributesClass %s: %w\n%s", name, err, string(out))
	}

	return nil
}

func deleteVolumeAttributesClass(ctx context.Context, kubeconfig, name string) {
	_ = exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"delete", "volumeattributesclass", name, "--ignore-not-found", "--wait=false",
	).Run()
}

func setPVCVolumeAttributesClass(
	ctx context.Context,
	c client.Client,
	key *corev1.PersistentVolumeClaim,
	name string,
) error {
	current := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: key.Name}, current); err != nil {
		return fmt.Errorf("get bound PVC for VAC: %w", err)
	}
	base := current.DeepCopy()
	current.Spec.VolumeAttributesClassName = &name
	if err := c.Patch(ctx, current, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("set PVC VolumeAttributesClassName: %w", err)
	}

	return nil
}

func waitForVACConvergence(
	ctx context.Context,
	c client.Client,
	kubeconfig, namespace, pvcName string,
	identity volumeIdentity,
	className, compressionWant string,
) error {
	return pollScenario(ctx, vacScenarioTimeout, func() (bool, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName}, pvc); err != nil {
			return false, err
		}
		if pvc.Spec.VolumeAttributesClassName == nil || *pvc.Spec.VolumeAttributesClassName != className {
			return false, nil
		}
		if pvc.Status.CurrentVolumeAttributesClassName == nil ||
			*pvc.Status.CurrentVolumeAttributesClassName != className {
			return false, nil
		}

		volumeName, err := volumeCRName(identity.volumeHandle)
		if err != nil {
			return false, err
		}
		volume := &unstructured.Unstructured{}
		volume.SetGroupVersionKind(volumeGVR.GroupVersion().WithKind("Volume"))
		if err := c.Get(ctx, types.NamespacedName{Name: volumeName}, volume); err != nil {
			return false, fmt.Errorf("get Volume CR: %w", err)
		}
		compression, found, err := unstructured.NestedString(volume.Object, "spec", "compression")
		if err != nil || !found || compression != compressionWant {
			return false, err
		}
		blockSize, _, err := unstructured.NestedString(volume.Object, "spec", "volBlockSize")
		if err != nil {
			return false, err
		}
		if blockSize != "16k" {
			return false, fmt.Errorf("VAC changed immutable volBlockSize to %q", blockSize)
		}

		property, err := storageAgentZFSProperty(ctx, c, kubeconfig, identity.volumeHandle, "compression")
		if err != nil {
			return false, err
		}
		return property == compressionWant, nil
	})
}

func volumeCRName(volumeHandle string) (string, error) {
	parts := strings.Split(volumeHandle, ":")
	if len(parts) != 4 || parts[0] != "csi" || parts[3] == "" {
		return "", fmt.Errorf("unexpected CSI volume handle %q", volumeHandle)
	}
	return parts[3], nil
}

func storageAgentZFSProperty(
	ctx context.Context,
	c client.Client,
	kubeconfig, volumeHandle, property string,
) (string, error) {
	parts := strings.Split(volumeHandle, ":")
	if len(parts) != 4 {
		return "", fmt.Errorf("unexpected CSI volume handle %q", volumeHandle)
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods,
		client.InNamespace(zfsCSINamespace),
		client.MatchingLabels{"app.kubernetes.io/name": "zfs-csi", "app.kubernetes.io/component": "storage"},
	); err != nil {
		return "", fmt.Errorf("list storage-agent pods: %w", err)
	}
	if len(pods.Items) != 1 {
		return "", fmt.Errorf("expected one storage-agent pod, got %d", len(pods.Items))
	}
	dataset := fmt.Sprintf("%s/csi/%s/%s", parts[1], parts[2], parts[3])
	command := fmt.Sprintf("zfs get -H -o value %s %s", shellQuote(property), shellQuote(dataset))
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"-n", zfsCSINamespace, "exec", "pod/"+pods.Items[0].Name, "-c", "storage", "--", "sh", "-ec", command,
	).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("read storage-agent ZFS property %s: %w\n%s", property, err, string(out))
	}

	return strings.TrimSpace(string(out)), nil
}

func waitForVACDeletionProtection(
	ctx context.Context,
	c client.Client,
	kubeconfig, namespace, pvcName, className string,
) error {
	if err := deleteVolumeAttributesClassWithWait(ctx, kubeconfig, className); err != nil {
		return err
	}
	return pollScenario(ctx, 2*time.Minute, func() (bool, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName}, pvc); err != nil {
			return false, err
		}
		if pvc.Spec.VolumeAttributesClassName == nil || *pvc.Spec.VolumeAttributesClassName != className {
			return false, nil
		}
		result := exec.CommandContext(
			ctx,
			"kubectl",
			"--kubeconfig",
			kubeconfig,
			"get",
			"volumeattributesclass",
			className,
			"-o",
			"name",
		)
		return result.Run() == nil, nil
	})
}

func stringPtr(value string) *string { return &value }

func deleteVolumeAttributesClassWithWait(ctx context.Context, kubeconfig, name string) error {
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"delete", "volumeattributesclass", name, "--wait=false",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("delete protected VolumeAttributesClass %s: %w\n%s", name, err, string(out))
	}
	return nil
}
