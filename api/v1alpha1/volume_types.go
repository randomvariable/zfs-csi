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

// VolumeType selects block (zvol) vs filesystem (dataset) provisioning.
type VolumeType string

const (
	// VolumeFinalizer blocks CR deletion until the storage-agent destroys backing resources.
	VolumeFinalizer = "zfs.csi.randomvariable.co.uk/volume-protect"

	// ForceDeleteAnnotation, when set to "true" on a Volume CR, overrides the
	// in-use deletion guard. Deletion is normally refused while the volume is
	// still published to a node (non-empty status.mappedInitiators) so a stale
	// VolumeAttachment or operator error cannot destroy a volume out from under
	// live I/O. Setting this annotation is the explicit, auditable operator
	// escape when the mapping is known-stale (the consumer is truly gone but
	// ControllerUnpublish never ran).
	ForceDeleteAnnotation = "zfs.csi.randomvariable.co.uk/force-delete"

	// VolumeTypeBlock provisions a zvol (block device).
	VolumeTypeBlock VolumeType = "block"
	// VolumeTypeFilesystem provisions a ZFS dataset (filesystem).
	VolumeTypeFilesystem VolumeType = "filesystem"
)

// TransportKind selects the block transport protocol.
type TransportKind string

const (
	// TransportNVMeTCP uses NVMe-over-TCP (nvmet configfs).
	TransportNVMeTCP TransportKind = "nvme-tcp"
)

// VolumeState is the reconcile lifecycle state of a Volume.
type VolumeState string

const (
	// VolumeStatePending means the CR was created but not yet acted on.
	VolumeStatePending VolumeState = "Pending"
	// VolumeStateReady means the dataset and transport export are active.
	VolumeStateReady VolumeState = "Ready"
	// VolumeStateReadyToPublish is an alias used by the node stage flow.
	VolumeStateReadyToPublish VolumeState = "ReadyToPublish"
	// VolumeStateDeleting means DeleteVolume was called; agent is cleaning up.
	VolumeStateDeleting VolumeState = "Deleting"
	// VolumeStateDestroyed means the dataset and export have been removed.
	VolumeStateDestroyed VolumeState = "Destroyed"
	// VolumeStateError means the reconciler encountered a terminal error.
	VolumeStateError VolumeState = "Error"
)

// KeyStatus is the ZFS native encryption key availability state.
type KeyStatus string

const (
	// KeyStatusAvailable means the key is loaded and the dataset is accessible.
	KeyStatusAvailable KeyStatus = "Available"
	// KeyStatusUnavailable means the key is NOT loaded (encrypted dataset locked).
	KeyStatusUnavailable KeyStatus = "Unavailable"
)

// VolumeConditionType names a specific condition on a Volume.
type VolumeConditionType string

const (
	// VolumeConditionReady tracks overall provisioning readiness.
	VolumeConditionReady VolumeConditionType = "Ready"
	// VolumeConditionEncrypted tracks encryption key availability.
	VolumeConditionEncrypted VolumeConditionType = "Encrypted"
	// VolumeConditionBackendHealthy tracks the durable health of the ZFS backing
	// dataset and its serving endpoint.
	VolumeConditionBackendHealthy VolumeConditionType = "BackendHealthy"
)

// VolumeSpec captures the full provisioning intent for a volume. It is the on-wire
// contract between the CSI controller (writer) and the server7 storage-agent (reconciler).
type VolumeSpec struct {
	// provenance distinguishes controller-provisioned storage from validated imports.
	// +optional
	// +kubebuilder:validation:Enum=Dynamic;Imported
	// +default="Dynamic"
	Provenance VolumeProvenance `json:"provenance,omitempty"`

	// backendPath is the immutable canonical ZFS object name for an imported volume.
	// It is empty for dynamically provisioned volumes.
	// +optional
	BackendPath string `json:"backendPath,omitempty"`

	// deletionPolicy is Retain for imports and Delete for dynamic volumes.
	// +optional
	// +kubebuilder:validation:Enum=Delete;Retain
	// +default="Delete"
	DeletionPolicy VolumeDeletionPolicy `json:"deletionPolicy,omitempty"`

	// pool is the ZFS pool to provision into (e.g. "tank", "flash").
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Pool string `json:"pool"`

	// poolGUID is the immutable ZFS pool identity in canonical non-zero decimal form.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]{0,19}$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="poolGUID is immutable"
	PoolGUID string `json:"poolGUID"`

	// capacity is the requested size in bytes.
	// +required
	// +kubebuilder:validation:Minimum=1
	Capacity int64 `json:"capacity"`

	// type selects zvol (block) vs dataset (filesystem).
	// +optional
	// +default="block"
	Type VolumeType `json:"type,omitempty"`

	// fsType is the filesystem to mkfs on a block volume before mount (xfs|ext4). Ignored for filesystem type.
	// +optional
	// +kubebuilder:validation:Enum=xfs;ext4
	// +default="xfs"
	FsType string `json:"fsType,omitempty"`

	// volBlockSize is the ZFS volblocksize / recordsize property (e.g. "16k"). Empty = ZFS default.
	// It is immutable: ZFS fixes a zvol's volblocksize at creation and a clone
	// inherits it from its origin, so every capacity aligned against this value
	// (create, expand, clone, restore) would become invalid if it could change.
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]+[kKmMgG]?$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="volBlockSize is immutable"
	VolBlockSize string `json:"volBlockSize,omitempty"`

	// compression is the ZFS compression property (on|off|lz4|zstd-<level>). Empty = inherit.
	// +optional
	// +kubebuilder:validation:Pattern=`^(on|off|lz4|gzip|zstd|zstd-[1-9]|zstd-[1-9]-fast)$`
	Compression string `json:"compression,omitempty"`

	// encryptionKeyRef is the OpenBao Transit key reference for the per-volume DEK.
	// Empty means no encryption. Format: "transit/<keyName>" or "kv/<path>".
	// +optional
	// +kubebuilder:validation:Pattern=`^(transit|kv)/[a-zA-Z0-9._/-]{1,255}$`
	EncryptionKeyRef string `json:"encryptionKeyRef,omitempty"`

	// transport is the block transport for a block volume. Ignored for filesystem type.
	// +optional
	// +default="nvme-tcp"
	Transport TransportKind `json:"transport,omitempty"`

	// ownerNode is the storage node that owns (materialises) this volume.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ownerNode is immutable"
	OwnerNode string `json:"ownerNode"`

	// networkDomain is the immutable consumer reachability domain selected at
	// creation. It is a topology segment value, never a storage owner identity.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self.matches('^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$')",message="networkDomain must be a valid Kubernetes label value"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="networkDomain is immutable"
	NetworkDomain string `json:"networkDomain"`

	// volName is the human-readable CSI volume name; used to derive the ZFS dataset leaf name.
	// +required
	// +kubebuilder:validation:MinLength=1
	VolName string `json:"volName"`

	// volumeID is the CSI volume handle (the opaque string returned to the provisioner).
	// +required
	// +kubebuilder:validation:MinLength=1
	VolumeID string `json:"volumeID"`

	// sourceSnapshotID is the CSI snapshot handle to restore into this volume.
	// Empty means this volume is not sourced from a snapshot.
	// +optional
	// +kubebuilder:validation:MinLength=1
	SourceSnapshotID string `json:"sourceSnapshotID,omitempty"`

	// sourceVolumeID is the CSI volume handle to clone into this volume.
	// Empty means this volume is not sourced from another volume.
	// +optional
	// +kubebuilder:validation:MinLength=1
	SourceVolumeID string `json:"sourceVolumeID,omitempty"`

	// nfsExportCIDRs are the IPv4 and IPv6 networks allowed to mount filesystem volumes over NFS.
	// At least one is required for filesystem volumes and the field is ignored for block volumes.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	NFSExportCIDRs []string `json:"nfsExportCIDRs,omitempty"`

	// nfsExportAccessMode controls whether the allowed NFS CIDRs are exported read-write or read-only.
	// The reconciler always adds root_squash and never accepts raw sharenfs strings.
	// +optional
	// +kubebuilder:validation:Enum=rw;ro
	// +default="rw"
	NFSExportAccessMode string `json:"nfsExportAccessMode,omitempty"`

	// nfsTLSEnabled requests RPC-with-TLS for filesystem NFS consumers. It is
	// immutable because changing transport security on an existing export can
	// leave mounted consumers with incompatible session semantics.
	// +optional
	// +default=false
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nfsTLSEnabled is immutable"
	NFSTLSEnabled bool `json:"nfsTLSEnabled,omitempty"`

	// nvmeTLSEnabled requests TLS 1.3 PSK authentication for NVMe/TCP block
	// consumers. It is valid only for dynamically provisioned NVMe/TCP block volumes.
	// +optional
	// +default=false
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nvmeTLSEnabled is immutable"
	NVMeTLSEnabled bool `json:"nvmeTLSEnabled,omitempty"`

	// nvmeTLSPSKSecretName is the controller-derived name of the immutable Secret
	// containing this volume's configured NVMe/TCP TLS PSK.
	// +optional
	// +kubebuilder:validation:Pattern=`^zfs-csi-nvme-psk-[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nvmeTLSPSKSecretName is immutable"
	NVMeTLSPSKSecretName string `json:"nvmeTLSPSKSecretName,omitempty"`

	// importFsTypeDeclaration preserves the import request's explicit block
	// format declaration. It avoids conflating raw block (empty) with the Volume
	// CRD's historical xfs default when a VolumeImport is deleted and recreated.
	// Set only for imported volumes.
	// +optional
	// +kubebuilder:validation:Enum="";ext4;xfs
	ImportFsTypeDeclaration string `json:"importFsTypeDeclaration,omitempty"`
}

// MappedInitiator records a consumer node that is currently allowed to attach a block volume.
type MappedInitiator struct {
	// nodeName is the kubernetes Node name of the consumer.
	// +required
	NodeName string `json:"nodeName"`
	// initiatorID is the NQN the consumer presents.
	// +required
	InitiatorID string `json:"initiatorID"`
	// attachedAt is when the mapping was established.
	// +optional
	AttachedAt metav1.Time `json:"attachedAt,omitempty"`
}

// VolumeStatus is materialised by the storage-agent reconciler and read by the CSI
// controller/node to learn the transport handle.
type VolumeStatus struct {
	// conditions carries reconcile health.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// state is the reconcile lifecycle state.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Ready;ReadyToPublish;Deleting;Destroyed;Error
	State VolumeState `json:"state,omitempty"`

	// observedGeneration is the spec generation the controller last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// targetNQN is the NVMe-TCP subsystem NQN (block, nvme-tcp).
	// +optional
	TargetNQN string `json:"targetNQN,omitempty"`
	// portal is the "host:port" the consumer connects to (block transport).
	// +optional
	Portal string `json:"portal,omitempty"`
	// deviceGUID is a stable per-volume GUID embedded in the NVMe namespace NGUID/EUI.
	// +optional
	DeviceGUID string `json:"deviceGUID,omitempty"`
	// portalHost is the concrete persisted NVMe-TCP endpoint host.
	// +optional
	PortalHost string `json:"portalHost,omitempty"`
	// portalPort is the concrete persisted NVMe-TCP endpoint port.
	// +optional
	PortalPort int32 `json:"portalPort,omitempty"`

	// exportPath is the NFS export path (filesystem type): "server7:<dataset>".
	// +optional
	ExportPath string `json:"exportPath,omitempty"`
	// nfsRootPath is the authoritative host mountpoint serving as this host's
	// NFSv4 pseudo-root. Consumers translate exportPath relative to this exact
	// path; it may differ from "/<pool>" when ZFS has a custom mountpoint.
	// +optional
	NFSRootPath string `json:"nfsRootPath,omitempty"`
	// nfsServer is the concrete persisted NFS endpoint host.
	// +optional
	NFSServer string `json:"nfsServer,omitempty"`

	// mappedInitiators is the authoritative list of consumers allowed to attach (block).
	// Drift between this and live configfs is reconciled.
	// +optional
	// +listType=map
	// +listMapKey=initiatorID
	MappedInitiators []MappedInitiator `json:"mappedInitiators,omitempty"`

	// publishedInitiators is the set of initiatorIDs the storage-agent has
	// confirmed are live in the transport (configfs allow-host applied).
	// Owned solely by the agent; the controller polls this to confirm a publish.
	// +optional
	PublishedInitiators []string `json:"publishedInitiators,omitempty"`

	// keyStatus reports ZFS native-encryption key availability (encrypted volumes).
	// +optional
	KeyStatus KeyStatus `json:"keyStatus,omitempty"`

	// zvolPath is the /dev/zvol/<pool>/csi/<type>/<id> path on server7 (block).
	// +optional
	ZvolPath string `json:"zvolPath,omitempty"`
	// datasetPath is the full ZFS dataset name (e.g. tank/csi/block/<id>).
	// +optional
	DatasetPath string `json:"datasetPath,omitempty"`

	// actualCapacity is the size ZFS actually provisioned (bytes).
	// +optional
	ActualCapacity int64 `json:"actualCapacity,omitempty"`

	// capacityAccountedAt is the pool capacity sample known to include this dataset.
	// The storage agent owns this marker; placement treats missing, stale, or future
	// markers as reservations.
	// +optional
	CapacityAccountedAt *metav1.Time `json:"capacityAccountedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=zv
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.status.actualCapacity`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.spec.provenance == oldSelf.spec.provenance",message="provenance is immutable"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.backendPath) == !has(oldSelf.spec.backendPath) && (!has(self.spec.backendPath) || self.spec.backendPath == oldSelf.spec.backendPath)",message="backendPath is immutable"
// +kubebuilder:validation:XValidation:rule="self.spec.deletionPolicy == oldSelf.spec.deletionPolicy",message="deletionPolicy is immutable"
// +kubebuilder:validation:XValidation:rule="!self.spec.nvmeTLSEnabled || (self.spec.type == 'block' && self.spec.transport == 'nvme-tcp' && self.spec.provenance == 'Dynamic')",message="NVMe TLS is supported only for dynamic NVMe/TCP block volumes"
// +kubebuilder:validation:XValidation:rule="self.spec.nvmeTLSEnabled == has(self.spec.nvmeTLSPSKSecretName)",message="NVMe TLS requires exactly one PSK Secret reference"
// +kubebuilder:validation:XValidation:rule="!has(self.spec.nvmeTLSPSKSecretName) || self.spec.nvmeTLSPSKSecretName == 'zfs-csi-nvme-psk-' + self.metadata.name",message="NVMe TLS PSK Secret reference must be derived from Volume metadata.name"

// Volume is the desired + observed state of a single CSI-provisioned ZFS volume.
type Volume struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard Kubernetes object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of the Volume.
	// +required
	Spec VolumeSpec `json:"spec"`

	// status reports the observed state of the Volume.
	// +optional
	Status VolumeStatus `json:"status,omitempty"`
}

// GetConditions returns a copy-safe view of the conditions owned by reconcilers.
// It implements the condition contract used by the agent patch helper.
func (v *Volume) GetConditions() []metav1.Condition {
	return v.Status.Conditions
}

// SetConditions replaces the conditions owned by reconcilers.
func (v *Volume) SetConditions(conditions []metav1.Condition) {
	v.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// VolumeList is the list type for Volume.
type VolumeList struct {
	metav1.TypeMeta `         json:",inline"`
	metav1.ListMeta `         json:"metadata,omitempty"`
	Items           []Volume `json:"items"`
}
