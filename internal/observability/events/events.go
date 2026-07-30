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

// Package events defines the stable Kubernetes Event vocabulary used by reconcilers.
package events

const (
	// TypeNormal reports a successful lifecycle transition.
	TypeNormal = "Normal"
	// TypeWarning reports an actionable lifecycle failure.
	TypeWarning = "Warning"
)

const (
	// ActionProvisioning covers dataset and snapshot creation work.
	ActionProvisioning = "Provisioning"
	// ActionExporting covers transport export work.
	ActionExporting = "Exporting"
	// ActionExpanding covers capacity expansion work.
	ActionExpanding = "Expanding"
	// ActionDeleting covers resource teardown work.
	ActionDeleting = "Deleting"
	// ActionHealthChecking covers backend and target health transitions.
	ActionHealthChecking = "HealthChecking"
)

const (
	ReasonVolumeCreateFailed        = "VolumeCreateFailed"
	ReasonVolumeCloneFailed         = "VolumeCloneFailed"
	ReasonVolumeReady               = "VolumeReady"
	ReasonExportFailed              = "ExportFailed"
	ReasonExportRecovered           = "ExportRecovered"
	ReasonInitiatorFenced           = "InitiatorFenced"
	ReasonExpansionFailed           = "ExpansionFailed"
	ReasonVolumeExpanded            = "VolumeExpanded"
	ReasonDeleteBlockedInUse        = "DeleteBlockedInUse"
	ReasonVolumeDeleteFailed        = "VolumeDeleteFailed"
	ReasonVolumeDestroyed           = "VolumeDestroyed"
	ReasonSnapshotInvalidVolumeID   = "SnapshotInvalidVolumeID"
	ReasonSnapshotInvalidSnapshotID = "SnapshotInvalidSnapshotID"
	ReasonSnapshotCreateFailed      = "SnapshotCreateFailed"
	ReasonSnapshotReady             = "SnapshotReady"
	ReasonSnapshotDestroyFailed     = "SnapshotDestroyFailed"
	ReasonSnapshotDeleting          = "SnapshotDeleting"
	ReasonMappedInitiatorsFailed    = "MappedInitiatorsFailed"
	ReasonExportReconciled          = "ExportReconciled"
	ReasonUnexportFailed            = "UnexportFailed"
	ReasonBackendUnhealthy          = "BackendUnhealthy"
	ReasonBackendRecovered          = "BackendRecovered"
)
