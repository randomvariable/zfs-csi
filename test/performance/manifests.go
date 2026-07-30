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
	"fmt"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const Provisioner = "zfs.csi.randomvariable.co.uk"

func StorageClassFor(v Variant, transport, pool string, nfsExportCIDRs []string) (*storagev1.StorageClass, error) {
	reclaim := corev1.PersistentVolumeReclaimDelete
	binding := storagev1.VolumeBindingImmediate
	params := map[string]string{"pool": pool, "type": "filesystem"}
	if transport == "nvme" {
		params["type"], params["transport"] = "block", "nvme-tcp"
	} else {
		if len(nfsExportCIDRs) == 0 {
			return nil, fmt.Errorf("nfs export CIDRs are required for filesystem performance StorageClasses")
		}
		params["nfsExportCIDRs"] = strings.Join(nfsExportCIDRs, ",")
	}
	for k, value := range v.Parameters {
		params[k] = value
	}
	return &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: v.StorageClass},
		Provisioner:       Provisioner,
		Parameters:        params,
		MountOptions:      v.MountOptions,
		ReclaimPolicy:     &reclaim,
		VolumeBindingMode: &binding,
	}, nil
}

func PVC(namespace, name, storageClass, transport string) *corev1.PersistentVolumeClaim {
	mode := corev1.ReadWriteOnce
	volumeMode := corev1.PersistentVolumeFilesystem
	if transport == "nfs" {
		mode = corev1.ReadWriteMany
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{mode},
			StorageClassName: &storageClass,
			VolumeMode:       &volumeMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("8Gi")},
			},
		},
	}
}

func FioJobManifest(namespace, name, pvc, image, node string, workload FioWorkload, phase string) *batchv1.Job {
	backoff, deadline := int32(0), int64(workload.RuntimeSeconds+300)
	args := []string{
		"--name=" + workload.Name,
		"--filename=/data/fio.dat",
		"--rw=" + workload.RW,
		"--bs=" + workload.BlockSize,
		"--iodepth=" + strconv.Itoa(workload.QueueDepth),
		"--numjobs=" + strconv.Itoa(workload.Jobs),
		"--direct=1",
		"--time_based=1",
		"--runtime=" + strconv.Itoa(workload.RuntimeSeconds),
		"--group_reporting=1",
		"--output-format=json+",
	}
	if phase == "precondition" {
		args[2], args[7], args[8] = "--rw=write", "--time_based=0", "--runtime=0"
		args = append(args, "--size=4Gi")
	}
	if phase == "warmup" {
		args[8] = "--runtime=30"
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoff,
			ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Affinity:      requiredNodeAffinity(node),
				Containers: []corev1.Container{{
					Name: "fio", Image: image, Args: args,
					VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				}},
				Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc},
				}}},
			}},
		},
	}
}

func requiredNodeAffinity(node string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{node}},
						},
					},
				},
			},
		},
	}
}
