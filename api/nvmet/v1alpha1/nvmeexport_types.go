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

// NVMeExportState is the reconcile lifecycle state of an NVMeExport.
type NVMeExportState string

const (
	NVMeExportStatePending  NVMeExportState = "Pending"
	NVMeExportStateReady    NVMeExportState = "Ready"
	NVMeExportStateDeleting NVMeExportState = "Deleting"
	NVMeExportStateError    NVMeExportState = "Error"

	// NVMeExportFinalizer blocks CR deletion until the nvmet controller
	// tears down the configfs subsystem (gated on no live connection).
	NVMeExportFinalizer = "nvmet.randomvariable.co.uk/export-protect"
)

// NVMeExportConditionType names a specific condition on an NVMeExport.
type NVMeExportConditionType string

const (
	// NVMeExportConditionReady tracks whether the export is reconciled and usable.
	NVMeExportConditionReady NVMeExportConditionType = "Ready"
	// NVMeExportConditionDeleting indicates deletion is blocked by active clients.
	NVMeExportConditionDeleting NVMeExportConditionType = "Deleting"
)

// NVMeExportSpec is DESIRED state. Written solely by the export's creator
// (e.g. the zfs-csi storage agent). The nvmet controller treats it read-only.
type NVMeExportSpec struct {
	// devicePath is the absolute host path to the backing block device
	// (/dev/zvol/... for zfs, /dev/rbd0 for ceph, /dev/mapper/... for lvm).
	// Written verbatim to configfs namespaces/<id>/device_path; the controller
	// does NOT interpret it.
	// +required
	// +kubebuilder:validation:Pattern=`^/.+`
	DevicePath string `json:"devicePath"`

	// targetNQN is the NVMe subsystem NQN to own (deterministic per volume).
	// +required
	// +kubebuilder:validation:MinLength=1
	TargetNQN string `json:"targetNQN"`

	// portal is host:port the initiator connects to (nvmet port address).
	// +required
	Portal string `json:"portal"`

	// deviceGUID is embedded in the namespace NGUID/EUI for stable identity.
	// +optional
	DeviceGUID string `json:"deviceGUID,omitempty"`

	// namespaceID is the 1-based NVMe namespace id.
	// +optional
	// +default=1
	// +kubebuilder:validation:Minimum=1
	NamespaceID int32 `json:"namespaceID,omitempty"`

	// allowedInitiators is the DESIRED allow-host set (initiator NQNs). The
	// controller reconciles configfs allowed_hosts to EXACTLY this set.
	// +optional
	// +listType=set
	AllowedInitiators []string `json:"allowedInitiators,omitempty"`
}

// NVMeExportStatus is OBSERVED state. Written solely by the nvmet controller.
type NVMeExportStatus struct {
	// conditions carries reconcile health.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// state is the reconcile lifecycle state.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Ready;Deleting;Error
	State NVMeExportState `json:"state,omitempty"`

	// admittedInitiators is the CONFIRMED allow-host set present in configfs.
	// The consumer polls this to confirm a publish.
	// +optional
	// +listType=set
	AdmittedInitiators []string `json:"admittedInitiators,omitempty"`

	// activeConnection reports whether any initiator holds a live transport
	// connection. Gates safe teardown on delete.
	// +optional
	ActiveConnection bool `json:"activeConnection,omitempty"`

	// observedGeneration is the spec generation the controller last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=nvex
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="NQN",type=string,JSONPath=`.spec.targetNQN`
// +kubebuilder:printcolumn:name="Device",type=string,JSONPath=`.spec.devicePath`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Active",type=boolean,JSONPath=`.status.activeConnection`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// NVMeExport declares a desired NVMe-oF subsystem+namespace+ACL set that the
// nvmet controller materialises in configfs.
type NVMeExport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NVMeExportSpec   `json:"spec"`
	Status NVMeExportStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NVMeExportList is the list type for NVMeExport.
type NVMeExportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []NVMeExport `json:"items"`
}
