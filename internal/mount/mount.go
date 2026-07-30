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

// Package mount defines the filesystem-mount surface the node plugin needs
// (mkfs + mount + resize + bind-mount), wrapping k8s.io/mount-utils. A fake
// backs tests.
package mount

import (
	"context"

	mountutils "k8s.io/mount-utils"
)

// Interface aliases the upstream mount abstraction so callers can inject fakes
// without depending on the concrete Kubernetes mounter.
type Interface = mountutils.Interface

// MountOps is the node-plugin mount surface.
type MountOps interface {
	// Format creates a filesystem on a block device if none exists (ext4|xfs).
	Format(ctx context.Context, device, fsType string) error
	// IsFormatted reports whether device already has a filesystem of fsType.
	IsFormatted(ctx context.Context, device, fsType string) (bool, error)
	// Mount mounts source at target with the given fsType + options.
	Mount(ctx context.Context, source, target, fsType string, opts []string) error
	// Unmount unmounts target. Idempotent (already-unmounted → success).
	Unmount(ctx context.Context, target string) error
	// IsMounted reports whether source is mounted at target.
	IsMounted(ctx context.Context, source, target string) (bool, error)
	// Resize grows the filesystem on device to fill the block device (resize2fs/xfs_growfs).
	Resize(ctx context.Context, device, mountpoint, fsType string) error
	// BindMount bind-mounts staging → target (NodePublishVolume path).
	BindMount(ctx context.Context, staging, target string, readOnly bool) error
	// BindMountDevice bind-mounts a raw block device node onto target, which is
	// created as a regular file (not a directory). Used for volumeMode: Block:
	// the /dev node is bound onto the staging file and then onto the pod target.
	// No filesystem, no format. Idempotent.
	BindMountDevice(ctx context.Context, device, target string, readOnly bool) error
	// DeviceFromMount returns the backing block device for a mounted path (from
	// mountinfo). Used by NodeExpandVolume to resolve the device to resize —
	// passing an empty device to resize2fs is a silent no-op.
	DeviceFromMount(ctx context.Context, mountpoint string) (string, error)
}
