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

package logging

// Canonical structured-log key constants. Centralising them prevents the same
// concept being logged under subtly different strings (e.g. "volume" vs
// "volumeID" vs "vol"), which breaks log queries and dashboard filters.
//
// Use these as the key arguments to LogWith / WithValues / logr.Info instead of
// bare string literals. Prefixed Key to avoid clashing with types.
const (
	// Identity / resource keys.
	KeyVolumeID       = "volumeID"
	KeyVolume         = "volume"
	KeySnapshotID     = "snapshotID"
	KeySnapshot       = "snapshot"
	KeySourceVolumeID = "sourceVolumeID"
	KeyCRName         = "crName"
	KeyName           = "name"
	KeyNamespace      = "namespace"
	KeyNodeID         = "nodeID"
	KeyOwnerNode      = "ownerNode"
	KeyDataset        = "dataset"
	KeyDatasetPath    = "datasetPath"
	KeyPool           = "pool"
	KeyZvolPath       = "zvolPath"
	KeyZvol           = "zvol"

	// ZFS / storage keys.
	KeyCapacity            = "capacity"
	KeyActualCapacity      = "actualCapacity"
	KeyKind                = "kind"
	KeyCompression         = "compression"
	KeyFsType              = "fsType"
	KeyBlockSize           = "blockSize"
	KeyNFSExportCIDRs      = "nfsExportCIDRs"
	KeyNFSExportAccessMode = "nfsExportAccessMode"
	KeyState               = "state"
	KeyKeyStatus           = "keyStatus"
	KeyEncryption          = "encryption"
	KeyKeyRef              = "encryptionKeyRef"
	KeyKeyLocation         = "keyLocation"

	// Transport keys.
	KeyTargetNQN   = "targetNQN"
	KeyPortal      = "portal"
	KeyTransport   = "transport"
	KeyInitiatorID = "initiatorID"
	KeyInitiator   = "initiator"
	KeyDeviceGUID  = "deviceGUID"
	KeyNamespaceID = "namespaceID"

	// Node / mount keys.
	KeyDevice   = "device"
	KeySource   = "source"
	KeyTarget   = "target"
	KeyPath     = "path"
	KeyReadonly = "readonly"

	// Operation / outcome keys (used internally by OpLog; exposed for tests).
	KeyOperation = "operation"
	KeyDuration  = "duration_seconds"
	KeyStatus    = "status"
	KeyMethod    = "method"
)

// Operation message constants (OpIDs) — the first argument to LogWith. Using
// stable, registered strings keeps operation-log queries reliable and lets
// dashboards group by operation without collating free-text messages.
const (
	// ZFS backend operations.
	OpZFSCreate          = "zfs create"
	OpZFSClone           = "zfs clone"
	OpZFSDestroy         = "zfs destroy"
	OpZFSExpand          = "zfs expand"
	OpZFSShare           = "zfs share"
	OpZFSSetProperty     = "zfs set property"
	OpZFSExists          = "zfs exists"
	OpZFSSnapshot        = "zfs snapshot"
	OpZFSDestroySnapshot = "zfs destroy snapshot"
	OpZFSLoadKey         = "zfs load key"
	OpZFSUnloadKey       = "zfs unload key"
	OpZFSKeyStatus       = "zfs key status"

	// Transport operations.
	OpTransportExport          = "transport export"
	OpTransportUnexport        = "transport unexport"
	OpTransportMapInitiator    = "transport map initiator"
	OpTransportUnmapInitiator  = "transport unmap initiator"
	OpTransportQuery           = "transport mapped initiators query"
	OpTransportForceDisconnect = "transport force disconnect"

	// Crypto operations.
	OpCryptoFetch  = "fetch DEK"
	OpCryptoStage  = "stage DEK"
	OpCryptoShred  = "shred staged DEK"
	OpCryptoDelete = "crypto-shred DEK"

	// CR / API operations.
	OpCreateVolumeCR      = "create Volume CR"
	OpPatchVolumeCR       = "patch Volume CR"
	OpDeleteVolumeCR      = "delete Volume CR"
	OpPatchVolumeStatus   = "patch Volume CR status"
	OpPatchSnapshotStatus = "patch Snapshot CR status"
	OpCreateSnapshotCR    = "create Snapshot CR"
	OpDeleteSnapshotCR    = "delete Snapshot CR"

	// Node / mount operations.
	OpNFSMount       = "nfs mount"
	OpBlockAttach    = "attach block transport"
	OpBlockDetach    = "detach block transport"
	OpBlockFormat    = "format block device"
	OpBlockMount     = "mount block device"
	OpBindMount      = "bind mount"
	OpUnmountStaging = "unmount staging"
	OpUnmountTarget  = "unmount target"
	OpResize         = "resize filesystem"
)
