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

// Package zfs defines the cgo-unaware ZFS surface the driver needs.
//
// The real implementation is the cgo libzfs binding in internal/libzfs (built only
// with the `libzfs` build tag + libzfs dev headers/libs). All other code —
// reconcilers, transport, tests — depends on this Go interface and is fully
// testable with the in-process fake in internal/zfs/fake. The CLI is never used
// (NO CLI wrapping golden rule; see PLAN §5a, DECISIONS.md).
package zfs

import "context"

// VolumeKind selects zvol (block) vs dataset (filesystem).
type VolumeKind string

const (
	KindBlock      VolumeKind = "block"      // zvol
	KindFilesystem VolumeKind = "filesystem" // dataset
)

// KeyFormat is the ZFS encryption key format.
type KeyFormat string

const (
	KeyFormatRaw    KeyFormat = "raw"
	KeyFormatHex    KeyFormat = "hex"
	KeyFormatPass   KeyFormat = "passphrase"
	KeyFormatAbsent KeyFormat = "" // unencrypted
)

// KeyLocality describes where a key currently lives (ZFS keystatus).
type KeyLocality string

const (
	KeyUnavailable KeyLocality = "unavailable"
	KeyAvailable   KeyLocality = "available"
	KeyNone        KeyLocality = "none" // not encrypted
)

// CreateOptions carries all parameters needed to create a zvol or dataset.
type CreateOptions struct {
	Name        string     // full dataset name, e.g. tank/csi/block/<id>
	Kind        VolumeKind // block (zvol) or filesystem (dataset)
	Capacity    int64      // bytes (volsize for zvol; refquota for dataset)
	VolBlockSz  string     // volblocksize (zvol) / recordsize (dataset); "" = default
	Compression string     // "" = inherit
	// Encryption:
	Encrypted   bool
	KeyFormat   KeyFormat
	RawKey      []byte // tmpfs-staged key material; never logged
	KeyLocation string // file://<tmpfs> for create; "prompt" not used
	// Filesystem-only:
	ShareNFS string // sharenfs= value, e.g. "rw=@10.42.0.0/16"
	Atime    string // atime property; filesystem datasets use "off"
	XAttr    string // xattr property; filesystem datasets use "sa"
}

// DatasetInfo is the observed state of a dataset.
type DatasetInfo struct {
	Name        string
	Kind        VolumeKind
	Capacity    int64
	Compression string
	Encrypted   bool
	KeyStatus   KeyLocality
	// Block device path for a zvol (/dev/zvol/<name>); empty for filesystem.
	DevPath string
	// NFS export path is the configured dataset mountpoint, not proof of live
	// mount state. Use Mounted for that state.
	ExportPath string
	// Mounted reports live kernel mount state for filesystem datasets.
	Mounted bool
	// Format is the detected filesystem signature for a zvol (ext4/xfs), or
	// empty for raw block and filesystem datasets.
	Format string
}

// Backend is the ZFS surface the driver requires. Implementations:
// internal/libzfs (cgo) + internal/zfs/fake (in-process).
//
// All methods are idempotent where the underlying ZFS op is idempotent (create
// of an existing dataset with matching props is treated as success; destroy of a
// missing dataset is success). Per-dataset locking is the caller's responsibility.
type Backend interface {
	// Create creates a zvol or dataset. Returns ErrAlreadyExists if the dataset
	// exists; callers treat that as success after verifying props match.
	Create(ctx context.Context, opts CreateOptions) error
	// Destroy destroys a dataset recursively (zfs destroy -r).
	Destroy(ctx context.Context, name string) error
	// Get fetches observed state. Returns ErrNotFound if absent.
	Get(ctx context.Context, name string) (DatasetInfo, error)
	// Exists reports whether the dataset exists.
	Exists(ctx context.Context, name string) (bool, error)
	// ListSnapshots lists snapshot names under the dataset.
	ListSnapshots(ctx context.Context, name string) ([]string, error)

	// SetProperty sets a single ZFS property (e.g. volsize, refquota, sharenfs, mountpoint).
	SetProperty(ctx context.Context, name, prop, value string) error
	// GetProperty fetches a single ZFS property value.
	GetProperty(ctx context.Context, name, prop string) (string, error)

	// Snapshot creates <name>@<snap>. Idempotent (ErrAlreadyExists → success).
	Snapshot(ctx context.Context, name, snap string) error
	// DestroySnapshot destroys <name>@<snap>.
	DestroySnapshot(ctx context.Context, name, snap string) error
	// Clone creates a writable clone <clonename> from <src>@<snap>.
	Clone(ctx context.Context, src, snap, clonename string) error
	// Share mounts a filesystem dataset and exports it over NFS with the given
	// sharenfs value (mount + zfs_prop_set sharenfs + commit + chmod 0777). It is
	// the clone-path equivalent of what Create does inline: Create sets sharenfs
	// on a fresh dataset, but Clone only reparents COW data, so the clone must be
	// mounted+shared separately or nfsd has no export for it (mount fails "No such
	// file or directory"). Idempotent; no-op for block volumes (shareNFS empty).
	Share(ctx context.Context, name, shareNFS string) error
	// ShareImported mounts and exports an adopted filesystem without changing its
	// root ownership or mode.
	ShareImported(ctx context.Context, name, shareNFS string) error
	// Unshare removes NFS exposure from an adopted filesystem without unmounting
	// or destroying retained storage.
	Unshare(ctx context.Context, name string) error

	// Expand grows a volume (zvol: set volsize; dataset: set refquota).
	Expand(ctx context.Context, name string, capacity int64) error

	// --- native encryption (the per-PVC DEK surface) ---

	// LoadKey loads an encryption key for a dataset (zfs load-key equivalent via
	// libzfs_crypto_load_key). Key material comes from KeyMaterial, written to a
	// caller-provided tmpfs path referenced by KeyLocation. Idempotent: if the
	// key is already loaded this is a no-op.
	LoadKey(ctx context.Context, name string, keyLocation string) error
	// UnloadKey unloads the encryption key (libzfs_crypto_unload_key).
	UnloadKey(ctx context.Context, name string) error
	// ChangeKey rotates the DEK (libzfs_crypto_change_key) — rekey path.
	ChangeKey(ctx context.Context, name string, keyLocation string) error
	// KeyStatus reports current key locality.
	KeyStatus(ctx context.Context, name string) (KeyLocality, error)

	// PoolNames lists imported pools on this host.
	PoolNames(ctx context.Context) ([]string, error)
	// PoolFreeBytes reports free space in a pool.
	PoolFreeBytes(ctx context.Context, pool string) (int64, error)
	// PoolGUID reports stable pool identity as canonical nonzero decimal uint64.
	PoolGUID(ctx context.Context, pool string) (string, error)
	// PoolHealth reports the raw ZFS health property (for example ONLINE).
	PoolHealth(ctx context.Context, pool string) (string, error)
}

// Sentinel errors.
var (
	ErrNotFound      = errSentinel("zfs: dataset not found")
	ErrAlreadyExists = errSentinel("zfs: dataset already exists")
	ErrPoolNotFound  = errSentinel("zfs: pool not found")
)

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
