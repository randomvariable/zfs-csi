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

// Package fake provides an in-memory implementation of zfs.Backend for tests.
// It has no dependency on cgo, libzfs, or the zfs CLI. It models enough of the
// ZFS dataset lifecycle (create/destroy/snapshot/clone/props/crypto) to drive
// reconciler + transport + crypto unit and property tests deterministically.
package fake

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"strconv"
	"strings"
	"sync"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

var (
	errBlockCapacityRequired  = errors.New("zfs: block capacity must be positive")
	errExpandCapacityRequired = errors.New("zfs: expand capacity must be positive")
	errSnapshotNotFound       = errors.New("zfs: snapshot does not exist")
	errDatasetNotEncrypted    = errors.New("zfs: dataset is not encrypted")
	errVolsizeNotAligned      = errors.New("zfs: volsize must be a multiple of volblocksize")
)

// Backend is a concurrency-safe, in-memory fake ZFS backend.
type Backend struct {
	mu       sync.Mutex
	instance uint64
	pools    map[string]pool
	dsets    map[string]*dataset
}

type pool struct {
	free   int64
	guid   string
	health string
}

type dataset struct {
	name          string
	kind          zfs.VolumeKind
	capacity      int64
	volBlockSz    string
	compression   string
	props         map[string]string
	encrypted     bool
	keyFormat     zfs.KeyFormat
	keyStatus     zfs.KeyLocality
	keyLocation   string
	snapshots     map[string]bool // snap name → exists
	origin        string          // "src@snap" if this dataset is a clone, else ""
	rootUID       int
	rootGID       int
	rootMode      uint32
	format        string
	exportPath    string
	exportPathSet bool
	mounted       bool
}

// New returns a zero-state fake Backend.
var fakeInstance uint64

func New() *Backend {
	fakeInstance++
	return &Backend{instance: fakeInstance, pools: map[string]pool{}, dsets: map[string]*dataset{}}
}

// WithPool registers a pool with a free-bytes capacity (for tests).
func (b *Backend) WithPool(name string, free int64) *Backend {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pools[name] = pool{free: free, guid: defaultPoolGUID(b.instance, name), health: "ONLINE"}

	return b
}

// WithPoolIdentity registers explicit pool identity and health.
func (b *Backend) WithPoolIdentity(name string, free int64, guid, health string) *Backend {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pools[name] = pool{free: free, guid: guid, health: health}
	return b
}

// ReplacePool simulates reguid/replacement while retaining raw pool name.
func (b *Backend) ReplacePool(name string, free int64, guid, health string) *Backend {
	return b.WithPoolIdentity(name, free, guid, health)
}

func defaultPoolGUID(instance uint64, name string) string {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%d:%s", instance, name)
	guid := h.Sum64()
	if guid == 0 {
		guid = 1
	}
	return strconv.FormatUint(guid, 10)
}

func (b *Backend) Create(ctx context.Context, opts zfs.CreateOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.dsets[opts.Name]; ok {
		return zfs.ErrAlreadyExists
	}

	if opts.Kind == zfs.KindBlock && opts.Capacity <= 0 {
		return errBlockCapacityRequired
	}

	// Mirror the real libzfs contract: volsize must be a multiple of
	// volblocksize, otherwise zvol creation fails. refquota on a dataset has no
	// such constraint, so only block volumes are checked.
	if opts.Kind == zfs.KindBlock {
		if err := checkVolsizeAlignment(opts.Capacity, opts.VolBlockSz); err != nil {
			return err
		}
	}

	d := &dataset{
		name:        opts.Name,
		kind:        opts.Kind,
		capacity:    opts.Capacity,
		volBlockSz:  opts.VolBlockSz,
		compression: opts.Compression,
		props:       map[string]string{},
		snapshots:   map[string]bool{},
		encrypted:   opts.Encrypted,
		keyFormat:   opts.KeyFormat,
		mounted:     opts.Kind == zfs.KindFilesystem && opts.ShareNFS != "",
	}
	if opts.Encrypted {
		d.keyStatus = zfs.KeyAvailable // create with key present loads it
		d.keyLocation = opts.KeyLocation
	} else {
		d.keyStatus = zfs.KeyNone
	}

	if opts.VolBlockSz != "" {
		prop := "volblocksize"
		if opts.Kind == zfs.KindFilesystem {
			prop = "recordsize"
		}

		d.props[prop] = opts.VolBlockSz
	}

	if opts.Compression != "" {
		d.props["compression"] = opts.Compression
	}
	if opts.Atime != "" {
		d.props["atime"] = opts.Atime
	}
	if opts.XAttr != "" {
		d.props["xattr"] = opts.XAttr
	}

	if opts.Kind == zfs.KindFilesystem && opts.ShareNFS != "" {
		d.props["sharenfs"] = opts.ShareNFS
	}

	b.dsets[opts.Name] = d

	return nil
}

// checkVolsizeAlignment enforces the ZFS invariant that a zvol's volsize is a
// whole number of volblocksize units. Datasets are exempt: refquota is byte-exact.
func checkVolsizeAlignment(capacity int64, volBlockSz string) error {
	blockSize, err := zfs.EffectiveBlockSize(volBlockSz)
	if err != nil {
		return err
	}
	if capacity%blockSize != 0 {
		return fmt.Errorf("%w: volsize=%d volblocksize=%d", errVolsizeNotAligned, capacity, blockSize)
	}

	return nil
}

func (b *Backend) Destroy(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Promote-aware, mirroring the libzfs Destroy contract: a clone shares blocks
	// with a snapshot of its origin, so destroying the origin must NOT delete the
	// clone (that is the `zfs destroy -R` data-loss trap). Instead reparent every
	// clone that depends on one of this dataset's snapshots — the clone becomes
	// independent (origin cleared) and SURVIVES. Any snapshot the clone owned
	// moves with it.
	for _, d := range b.dsets {
		if d.origin == "" {
			continue
		}
		if src, snap, ok := strings.Cut(d.origin, "@"); ok && src == name {
			d.origin = ""
			d.snapshots[snap] = true // the shared snapshot moves to the clone
		}
	}

	// recursive: destroy children first (their snapshots go with them)
	for n := range b.dsets {
		if strings.HasPrefix(n, name+"/") {
			delete(b.dsets, n)
		}
	}

	if _, ok := b.dsets[name]; !ok {
		return zfs.ErrNotFound
	}

	delete(b.dsets, name)

	return nil
}

func (b *Backend) Get(ctx context.Context, name string) (zfs.DatasetInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.DatasetInfo{}, zfs.ErrNotFound
	}

	info := zfs.DatasetInfo{
		Name:        d.name,
		Kind:        d.kind,
		Capacity:    d.capacity,
		Compression: d.compression,
		Encrypted:   d.encrypted,
		KeyStatus:   d.keyStatus,
		Format:      d.format,
	}
	if d.kind == zfs.KindBlock {
		info.DevPath = "/dev/zvol/" + d.name
	} else {
		info.ExportPath = d.exportPath
		if !d.exportPathSet {
			info.ExportPath = "/" + d.name // dataset mountpoint convention
		}
		info.Mounted = d.mounted
	}

	return info, nil
}

func (b *Backend) Exists(ctx context.Context, name string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, ok := b.dsets[name]

	return ok, nil
}

func (b *Backend) ListSnapshots(ctx context.Context, name string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return nil, zfs.ErrNotFound
	}

	out := make([]string, 0, len(d.snapshots))
	for s := range d.snapshots {
		out = append(out, s)
	}

	return out, nil
}

func (b *Backend) SetProperty(ctx context.Context, name, prop, value string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}

	if d.props == nil {
		d.props = map[string]string{}
	}

	d.props[prop] = value
	switch prop {
	case "volsize", "refquota":
		// best-effort numeric parse; ignore non-numeric
		var n int64
		if _, err := fmt.Sscanf(value, "%d", &n); err == nil {
			d.capacity = n
		}
	}

	return nil
}

func (b *Backend) GetProperty(ctx context.Context, name, prop string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return "", zfs.ErrNotFound
	}

	if v, ok := d.props[prop]; ok {
		return v, nil
	}

	switch prop {
	case "volsize", "refquota":
		return strconv.FormatInt(d.capacity, 10), nil
	case "compression":
		return d.compression, nil
	case "keystatus":
		return string(d.keyStatus), nil
	}

	return "", nil
}

func (b *Backend) Snapshot(ctx context.Context, name, snap string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}

	if _, exists := d.snapshots[snap]; exists {
		return zfs.ErrAlreadyExists
	}

	d.snapshots[snap] = true

	return nil
}

func (b *Backend) DestroySnapshot(ctx context.Context, name, snap string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}

	delete(d.snapshots, snap)

	return nil
}

func (b *Backend) Clone(ctx context.Context, src, snap, clonename string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[src]
	if !ok {
		return zfs.ErrNotFound
	}

	if _, exists := d.snapshots[snap]; !exists {
		return fmt.Errorf("%w: %s@%s", errSnapshotNotFound, src, snap)
	}

	if _, exists := b.dsets[clonename]; exists {
		return zfs.ErrAlreadyExists
	}

	clone := *d
	clone.name = clonename

	clone.props = map[string]string{}
	maps.Copy(clone.props, d.props)
	// zfs clone does not mount the new filesystem; Share mounts it later.
	clone.mounted = false

	clone.snapshots = map[string]bool{}
	clone.origin = src + "@" + snap
	b.dsets[clonename] = &clone

	return nil
}

// Share records the sharenfs value on the dataset (the fake has no real nfsd).
// Mirrors the libzfs contract: no-op when shareNFS is empty; records the
// property otherwise so tests can assert the clone path shared the volume.
func (b *Backend) Share(_ context.Context, name, shareNFS string) error {
	return b.share(name, shareNFS)
}

func (b *Backend) ShareImported(_ context.Context, name, shareNFS string) error {
	return b.share(name, shareNFS)
}

func (b *Backend) share(name, shareNFS string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if shareNFS == "" {
		return nil
	}
	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}
	if d.props == nil {
		d.props = map[string]string{}
	}
	d.props["sharenfs"] = shareNFS
	if d.kind == zfs.KindFilesystem {
		d.mounted = true
	}

	return nil
}

func (b *Backend) Unshare(_ context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}
	if d.kind != zfs.KindFilesystem {
		return nil
	}
	d.props["sharenfs"] = "off"

	return nil
}

func (b *Backend) Expand(ctx context.Context, name string, capacity int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}

	if capacity <= 0 {
		return errExpandCapacityRequired
	}

	if d.kind == zfs.KindBlock {
		if err := checkVolsizeAlignment(capacity, d.volBlockSz); err != nil {
			return err
		}
	}

	d.capacity = capacity
	if d.kind == zfs.KindBlock {
		d.props["volsize"] = strconv.FormatInt(capacity, 10)
	} else {
		d.props["refquota"] = strconv.FormatInt(capacity, 10)
	}

	return nil
}

func (b *Backend) LoadKey(ctx context.Context, name, keyLocation string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}

	if !d.encrypted {
		return fmt.Errorf("%w: %s", errDatasetNotEncrypted, name)
	}

	d.keyStatus = zfs.KeyAvailable
	d.keyLocation = keyLocation

	return nil
}

func (b *Backend) UnloadKey(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}

	if !d.encrypted {
		return fmt.Errorf("%w: %s", errDatasetNotEncrypted, name)
	}

	d.keyStatus = zfs.KeyUnavailable

	return nil
}

func (b *Backend) ChangeKey(ctx context.Context, name, keyLocation string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.ErrNotFound
	}

	if !d.encrypted {
		return fmt.Errorf("%w: %s", errDatasetNotEncrypted, name)
	}

	d.keyLocation = keyLocation
	d.keyStatus = zfs.KeyAvailable

	return nil
}

func (b *Backend) KeyStatus(ctx context.Context, name string) (zfs.KeyLocality, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return zfs.KeyNone, zfs.ErrNotFound
	}

	return d.keyStatus, nil
}

func (b *Backend) PoolNames(ctx context.Context) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]string, 0, len(b.pools))
	for p := range b.pools {
		out = append(out, p)
	}

	return out, nil
}

func (b *Backend) PoolFreeBytes(ctx context.Context, pool string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.pools[pool]
	if !ok {
		return 0, zfs.ErrPoolNotFound
	}

	return p.free, nil
}

func (b *Backend) PoolGUID(ctx context.Context, name string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.pools[name]
	if !ok {
		return "", zfs.ErrPoolNotFound
	}
	return p.guid, nil
}

func (b *Backend) PoolHealth(ctx context.Context, name string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.pools[name]
	if !ok {
		return "", zfs.ErrPoolNotFound
	}
	return p.health, nil
}

// SnapshotCount is a test helper.
func (b *Backend) SnapshotCount(name string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.dsets[name]
	if !ok {
		return 0
	}

	return len(d.snapshots)
}

// WithDataset seeds a dataset directly (test helper). For a filesystem dataset,
// shared=true records an active sharenfs export (so Get().ExportPath is
// non-empty); shared=false models a post-reboot dataset that exists but is no
// longer exported. keyStatus seeds the encryption key locality.
func (b *Backend) WithDataset(name string, kind zfs.VolumeKind, shared bool, keyStatus zfs.KeyLocality) *Backend {
	b.mu.Lock()
	defer b.mu.Unlock()

	d := &dataset{
		name:      name,
		kind:      kind,
		props:     map[string]string{},
		snapshots: map[string]bool{},
		keyStatus: keyStatus,
		encrypted: keyStatus == zfs.KeyAvailable || keyStatus == zfs.KeyUnavailable,
		mounted:   kind == zfs.KindFilesystem,
	}
	if kind == zfs.KindFilesystem && shared {
		d.props["sharenfs"] = "rw=@0.0.0.0/0"
	}
	b.dsets[name] = d

	return b
}

// WithDatasetCapacity seeds an existing object with observable capacity.
func (b *Backend) WithDatasetCapacity(name string, kind zfs.VolumeKind, capacity int64, shared bool, keyStatus zfs.KeyLocality) *Backend {
	b.WithDataset(name, kind, shared, keyStatus)
	b.mu.Lock()
	b.dsets[name].capacity = capacity
	b.mu.Unlock()

	return b
}

// WithRootMetadata records filesystem root metadata for adoption-safety tests.
func (b *Backend) WithRootMetadata(name string, uid, gid int, mode uint32) *Backend {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d := b.dsets[name]; d != nil {
		d.rootUID, d.rootGID, d.rootMode = uid, gid, mode
	}
	return b
}

func (b *Backend) RootMetadata(name string) (int, int, uint32, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d := b.dsets[name]
	if d == nil {
		return 0, 0, 0, false
	}
	return d.rootUID, d.rootGID, d.rootMode, true
}

func (b *Backend) WithFormat(name, format string) *Backend {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d := b.dsets[name]; d != nil {
		d.format = format
	}
	return b
}

func (b *Backend) WithExportPath(name, exportPath string) *Backend {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d := b.dsets[name]; d != nil {
		d.exportPath = exportPath
		d.exportPathSet = true
	}
	return b
}

// WithMounted changes live mount state without changing configured mountpoint.
func (b *Backend) WithMounted(name string, mounted bool) *Backend {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d := b.dsets[name]; d != nil && d.kind == zfs.KindFilesystem {
		d.mounted = mounted
	}
	return b
}

// RemovePool simulates a pool that is not imported (test helper): the dataset
// map is untouched but PoolNames no longer reports the pool.
func (b *Backend) RemovePool(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.pools, name)
}

// Compiles only if Backend satisfies zfs.Backend at build time.
var _ zfs.Backend = (*Backend)(nil)
