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

// Package transport defines the block-export abstraction the storage-agent uses
// to expose a zvol to consumer nodes, and the node plugin uses to attach it.
//
// The implementation writes nvmet configfs directly; no nvmetcli.
// A fake is provided for tests (internal/transport/fake).
package transport

import "context"

// Kind enumerates block transport backends.
type Kind string

const (
	KindNVMeTCP Kind = "nvme-tcp"
)

// ExportOptions carries the data needed to export a zvol.
type ExportOptions struct {
	// ZvolPath is the /dev/zvol/<pool>/csi/block/<id> backing device on server7.
	ZvolPath string
	// DeviceGUID is embedded in the namespace NGUID/EUI so the consumer sees a
	// stable, per-volume device identity (used by udev + fs labeling).
	DeviceGUID string
	// TargetNQN is the NVMe subsystem NQN to create/use.
	TargetNQN string
	// PortalHost:PortalPort is the address the consumer connects to.
	Portal string
	// Kind selects the backend.
	Kind Kind
	// TLS requests a TLS NVMe/TCP export. TLS targets use a dedicated configfs
	// port because addr_tsas is immutable once a port has a subsystem. Credential
	// material is deliberately not part of this transport contract.
	TLS bool
}

// TargetRef is the handle returned by Export; passed to Map/Attach.
type TargetRef struct {
	Kind        Kind
	TargetNQN   string // NVMe-TCP subsystem NQN
	Portal      string // host:port
	NamespaceID int    // NVMe-TCP namespace id (1-based)
	DeviceGUID  string
	// TLS identifies an export on the dedicated TLS port. It carries no key
	// material.
	TLS bool
}

// Server is the server7-side surface (storage-agent): create/remove the export
// and allow/disallow individual consumer initiators.
type Server interface {
	// Export creates (idempotently) the subsystem+namespace for a zvol and returns
	// the TargetRef. Returns ErrAlreadyExported if already present (caller treats as success).
	Export(ctx context.Context, opts ExportOptions) (TargetRef, error)
	// Unexport removes the subsystem+namespace (idempotent; missing → success).
	Unexport(ctx context.Context, ref TargetRef) error
	// MapInitiator allows a consumer's initiator NQN to attach. Idempotent.
	MapInitiator(ctx context.Context, ref TargetRef, initiatorID string) error
	// UnmapInitiator removes an initiator's mapping. Idempotent; missing → success.
	UnmapInitiator(ctx context.Context, ref TargetRef, initiatorID string) error
	// MappedInitiators returns the currently-allowed initiator IDs for a target.
	MappedInitiators(ctx context.Context, ref TargetRef) ([]string, error)
	// ForceDisconnect terminates ALL established controllers of the target's
	// subsystem, not just the allow-list entry. Removing an initiator from the
	// allow-list (UnmapInitiator) does not drop its live NVMe controller, so a
	// zombie node whose kubelet died but OS survived keeps writing to a namespace
	// another node has taken over — split-brain corruption. ForceDisconnect is
	// the fence primitive: it bounces the port→subsystem link so the kernel drops
	// every controller; legitimate initiators (still in the allow-list) reconnect
	// automatically, the fenced host is rejected. Idempotent; missing target →
	// success.
	ForceDisconnect(ctx context.Context, ref TargetRef) error
}

// Client is the consumer-node surface (node plugin): attach/detach the transport.
type Client interface {
	// Attach connects with nvme-cli and returns the /dev node path.
	Attach(ctx context.Context, ref TargetRef, initiatorID string) (devicePath string, err error)
	// Detach disconnects with nvme-cli. Idempotent.
	Detach(ctx context.Context, ref TargetRef) error
}

// Sentinel errors.
var (
	ErrAlreadyExported = errSentinel("transport: target already exported")
	ErrNotExported     = errSentinel("transport: target not exported")
	ErrInitiatorMapped = errSentinel("transport: initiator already mapped")
	// ErrDeviceNotReady means the ZFS zvol exists but its /dev node has not
	// surfaced yet. Callers should retry without treating the Volume as failed.
	ErrDeviceNotReady = errSentinel("transport: zvol device not ready")

	errNVMETTargetNQNRequired   = errSentinel("transport: nvmet target nqn required")
	errNVMETZvolPathRequired    = errSentinel("transport: nvmet zvol path required")
	errInitiatorIDRequired      = errSentinel("transport: initiator id required")
	errNVMEAttachDeviceNotFound = errSentinel("transport: nvme attach produced no device")
)

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
