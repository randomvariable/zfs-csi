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

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type VolumeImportState string

const (
	VolumeImportStatePending VolumeImportState = "Pending"
	VolumeImportStateReady   VolumeImportState = "Ready"
	VolumeImportStateFailed  VolumeImportState = "Failed"
)

type VolumeProvenance string

const (
	VolumeProvenanceDynamic  VolumeProvenance = "Dynamic"
	VolumeProvenanceImported VolumeProvenance = "Imported"
)

type VolumeDeletionPolicy string

const (
	VolumeDeletionPolicyDelete VolumeDeletionPolicy = "Delete"
	VolumeDeletionPolicyRetain VolumeDeletionPolicy = "Retain"
)

type VolumeImportSpec struct {
	// +required
	// +kubebuilder:validation:MinLength=1
	Pool string `json:"pool"`
	// backendPath is the canonical existing ZFS dataset or zvol name.
	// +required
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9][a-zA-Z0-9_.:-]*(/[a-zA-Z0-9][a-zA-Z0-9_.:-]*)+$`
	BackendPath string `json:"backendPath"`
	// +required
	// +kubebuilder:validation:Enum=block;filesystem
	Type VolumeType `json:"type"`
	// +required
	// +kubebuilder:validation:Minimum=1
	Capacity int64 `json:"capacity"`
	// +required
	// +kubebuilder:validation:MinLength=1
	OwnerNode string `json:"ownerNode"`
	// +optional
	// +kubebuilder:validation:Enum=nvme-tcp
	// +default="nvme-tcp"
	Transport TransportKind `json:"transport,omitempty"`
	// fsType is empty for raw block, or ext4/xfs for a formatted zvol.
	// +optional
	// +kubebuilder:validation:Enum="";ext4;xfs
	FsType string `json:"fsType,omitempty"`
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	NFSExportCIDRs []string `json:"nfsExportCIDRs,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=rw;ro
	// +default="rw"
	NFSExportAccessMode string `json:"nfsExportAccessMode,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=Retain
	// +default="Retain"
	DeletionPolicy VolumeDeletionPolicy `json:"deletionPolicy,omitempty"`
}

type VolumeImportStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	State VolumeImportState `json:"state,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	VolumeHandle string `json:"volumeHandle,omitempty"`
	// +optional
	VolumeRef string `json:"volumeRef,omitempty"`
	// +optional
	ActualCapacity int64 `json:"actualCapacity,omitempty"`
	// +optional
	ExportPath string `json:"exportPath,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=zvi
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Path",type=string,JSONPath=`.spec.backendPath`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Handle",type=string,JSONPath=`.status.volumeHandle`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.spec == oldSelf.spec",message="VolumeImport spec is immutable"
type VolumeImport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              VolumeImportSpec   `json:"spec"`
	Status            VolumeImportStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type VolumeImportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VolumeImport `json:"items"`
}

func (v *VolumeImport) GetConditions() []metav1.Condition           { return v.Status.Conditions }
func (v *VolumeImport) SetConditions(conditions []metav1.Condition) { v.Status.Conditions = conditions }
