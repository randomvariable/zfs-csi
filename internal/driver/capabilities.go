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

// Package driver implements the CSI Identity, Controller, and Node gRPC
// services (CSI spec v1.12.0) on top of the controller-runtime client (the
// controller) and the transport + mount surfaces (the node plugin).
//
// The controller is a pure CR writer — it translates CSI intent into
// Volume/Snapshot CRs and lets the storage-agent reconciler materialise them
// (PLAN §1). The node plugin attaches the block transport (or mounts NFS) and
// bind-mounts into the pod path.
package driver

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
)

// Plugin identity (SPEC §3 Identity).
const (
	PluginName    = "zfs.csi.randomvariable.co.uk"
	PluginVersion = "0.1.0"
)

// capabilities returns the Identity plugin capabilities.
func identityCapabilities() []*csi.PluginCapability {
	return []*csi.PluginCapability{
		{Type: &csi.PluginCapability_Service_{
			Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE},
		}},
		{Type: &csi.PluginCapability_Service_{
			Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS},
		}},
	}
}

// controllerCapabilities returns the Controller RPC capabilities (SPEC §3).
func controllerCapabilities() []*csi.ControllerServiceCapability {
	mk := func(c csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: c}},
		}
	}

	return []*csi.ControllerServiceCapability{
		mk(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
		mk(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME),
		mk(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
		mk(csi.ControllerServiceCapability_RPC_EXPAND_VOLUME),
		mk(csi.ControllerServiceCapability_RPC_LIST_VOLUMES),
		// GET_CAPACITY reports per-StorageClass pool free bytes. The controller
		// itself has no ZFS access, so the storage-agent Publisher writes each
		// pool's free bytes to a ConfigMap that GetCapacity reads. Required so
		// external-provisioner --enable-capacity and csi-sanity see a driver that
		// advertises the RPC it actually serves.
		mk(csi.ControllerServiceCapability_RPC_GET_CAPACITY),
		mk(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
		mk(csi.ControllerServiceCapability_RPC_CLONE_VOLUME),
		// Advertises support for the SINGLE_NODE_SINGLE_WRITER / SINGLE_NODE_MULTI_
		// WRITER access modes (ReadWriteOncePod maps to SINGLE_NODE_SINGLE_WRITER).
		// validateCapabilities already accepts these modes; block publish enforces
		// single-node via the nvmet allowlist (single active initiator), and the CO
		// (scheduler/kubelet) enforces the single-pod guarantee for RWOP.
		mk(csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER),
		// VolumeAttributesClass / ControllerModifyVolume: live-mutate ZFS
		// properties (compression) on an existing volume without recreation —
		// external-resizer drives the RPC. The agent applies the change via
		// `zfs set` in its level-triggered reconcile.
		mk(csi.ControllerServiceCapability_RPC_MODIFY_VOLUME),
		// Volume health monitoring: GET_VOLUME lets the external-health-monitor
		// query per-volume condition; VOLUME_CONDITION signals that
		// ControllerGetVolume populates VolumeStatus.VolumeCondition (derived from
		// the Volume CR reconcile state).
		mk(csi.ControllerServiceCapability_RPC_GET_VOLUME),
		mk(csi.ControllerServiceCapability_RPC_VOLUME_CONDITION),
	}
}

// nodeCapabilities returns the Node RPC capabilities (SPEC §3).
func nodeCapabilities() []*csi.NodeServiceCapability {
	mk := func(c csi.NodeServiceCapability_RPC_Type) *csi.NodeServiceCapability {
		return &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: c}},
		}
	}

	return []*csi.NodeServiceCapability{
		mk(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME),
		mk(csi.NodeServiceCapability_RPC_EXPAND_VOLUME),
		mk(csi.NodeServiceCapability_RPC_GET_VOLUME_STATS),
		// NodeGetVolumeStats populates VolumeCondition (mount liveness), so the
		// node advertises VOLUME_CONDITION for the external-health-monitor.
		mk(csi.NodeServiceCapability_RPC_VOLUME_CONDITION),
	}
}

// scParams extracts zfs-csi parameters from a CSI map (StorageClass parameters).
type scParams struct {
	Pool                string
	Type                string // block|filesystem
	FsType              string
	BlockSize           string
	Compression         string
	Transport           string
	Encrypted           bool
	NFSExportCIDRs      []string
	NFSExportAccessMode string
	NFSTLSEnabled       bool
	NFSTLSSpecified     bool
	NVMeTLSEnabled      bool
	NVMeTLSSpecified    bool
}
