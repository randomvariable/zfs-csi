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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotState is the reconcile lifecycle state of a Snapshot.
type SnapshotState string

const (
	// SnapshotFinalizer blocks CR deletion until the storage-agent destroys the ZFS snapshot.
	SnapshotFinalizer = "zfs.csi.randomvariable.co.uk/snapshot-protect"

	// SnapshotStatePending means the CR was created but not yet acted on.
	SnapshotStatePending SnapshotState = "Pending"
	// SnapshotStateReady means the ZFS snapshot was successfully taken.
	SnapshotStateReady SnapshotState = "Ready"
	// SnapshotStateError means the reconciler encountered a terminal error.
	SnapshotStateError SnapshotState = "Error"
	// SnapshotStateDeleting means DeleteSnapshot was called; agent is cleaning up.
	SnapshotStateDeleting SnapshotState = "Deleting"
)

// SnapshotConditionType names a specific condition on a Snapshot.
type SnapshotConditionType string

const (
	// SnapshotConditionReady tracks overall snapshot readiness.
	SnapshotConditionReady SnapshotConditionType = "Ready"
)

// SnapshotSpec is the CSI CreateSnapshot intent, materialised by the storage-agent.
type SnapshotSpec struct {
	// poolGUID is the immutable ZFS pool identity copied from the source Volume.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="poolGUID is immutable"
	PoolGUID string `json:"poolGUID"`

	// volumeRef references the cluster-scoped parent Volume CR.
	// +required
	VolumeRef string `json:"volumeRef"`

	// sourceVolumeID is the CSI source volume handle.
	// +required
	// +kubebuilder:validation:MinLength=1
	SourceVolumeID string `json:"sourceVolumeID"`

	// sourceVolBlockSize is the source Volume's ZFS volblocksize/recordsize at
	// snapshot time (for example "16k"). A restore clones the snapshot, so the
	// restored volume inherits this block size and its capacity must align to it.
	// The field is authoritative when the parent Volume CR no longer exists.
	// Empty on snapshots taken by older drivers and on filesystem sources.
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]+[kKmMgG]?$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="sourceVolBlockSize is immutable"
	SourceVolBlockSize string `json:"sourceVolBlockSize,omitempty"`

	// snapName is the human-readable CSI snapshot name; derives the ZFS snapshot leaf.
	// +required
	// +kubebuilder:validation:MinLength=1
	SnapName string `json:"snapName"`

	// snapshotID is the CSI snapshot handle returned to the snapshotter sidecar.
	// +required
	// +kubebuilder:validation:MinLength=1
	SnapshotID string `json:"snapshotID"`

	// ownerNode is the storage node that materialises this snapshot.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ownerNode is immutable"
	OwnerNode string `json:"ownerNode"`
}

// SnapshotStatus is materialised by the storage-agent reconciler.
type SnapshotStatus struct {
	// conditions carries reconcile health.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// state is the reconcile lifecycle state.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Ready;Error;Deleting
	State SnapshotState `json:"state,omitempty"`

	// observedGeneration is the spec generation the controller last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// readyToUse mirrors the CSI snapshot readyToUse field.
	// +optional
	ReadyToUse bool `json:"readyToUse,omitempty"`
	// size is the snapshot size in bytes.
	// +optional
	Size int64 `json:"size,omitempty"`
	// createdAt is the ZFS snapshot creation time (unix seconds).
	// +optional
	CreatedAt int64 `json:"createdAt,omitempty"`
	// datasetPath is the full ZFS snapshot name (e.g. tank/csi/block/<id>@<snap>).
	// +optional
	DatasetPath string `json:"datasetPath,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=zsnap
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceVolumeID`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.readyToUse`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.spec.ownerNode == oldSelf.spec.ownerNode",message="ownerNode is immutable"

// Snapshot is the desired + observed state of a single CSI-provisioned ZFS snapshot.
type Snapshot struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard Kubernetes object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of the Snapshot.
	// +required
	Spec SnapshotSpec `json:"spec"`

	// status reports the observed state of the Snapshot.
	// +optional
	Status SnapshotStatus `json:"status,omitempty"`
}

// GetConditions returns a copy-safe view of the conditions owned by reconcilers.
// It implements the condition contract used by the agent patch helper.
func (s *Snapshot) GetConditions() []metav1.Condition {
	return s.Status.Conditions
}

// SetConditions replaces the conditions owned by reconcilers.
func (s *Snapshot) SetConditions(conditions []metav1.Condition) {
	s.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// SnapshotList is the list type for Snapshot.
type SnapshotList struct {
	metav1.TypeMeta `           json:",inline"`
	metav1.ListMeta `           json:"metadata,omitempty"`
	Items           []Snapshot `json:"items"`
}
