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

//go:build cgo && libzfs

// Package libzfs is the cgo binding to libzfs (OpenZFS userspace). It is the
// ONLY place that links libzfs and is the real implementation of zfs.Backend
// (PLAN §5a: NO CLI wrapping).
//
// BUILD: compiles only with the `libzfs` build tag + cgo + libzfs dev headers
// and -lzfs/-lzfs_core/-lnvpair. EXCLUDED from the default
// `CGO_ENABLED=0 go build/test ./...` so the rest of the driver is fully
// buildable + testable without libzfs present (internal/zfs/fake backs tests).
//
// !!! VERIFICATION GAP (DECISIONS.md): the libzfs_crypto_* signatures are
// written from documented prototypes and MUST be compiled against OpenZFS dev
// headers before the storage binary is trusted in CI.
package libzfs

/*
#cgo pkg-config: libzfs libzfs_core
#cgo CFLAGS: -D_GNU_SOURCE

// Debian/Ubuntu ship the OpenZFS headers under /usr/include/libzfs and
// /usr/include/libspl (surfaced via pkg-config above), and libzfs.h already
// pulls in the nvpair declarations transitively, so nvpair.h is not included
// directly.
#include <libzfs.h>
#include <libzfs_core.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>

// ---- nvlist helpers (C preamble; keeps Go side free of nvpair gymnastics) ----

// rv_zfs_unmount_force force-unmounts a dataset before destroy. libzfs
// zfs_destroy (unlike the `zfs destroy` CLI) does not auto-unmount a mounted
// filesystem — it returns EZFS_MOUNTED. MS_FORCE (from <sys/mount.h>, pulled in
// by libspl) drops busy references. mountpoint=NULL = "unmount this dataset".
// libzfs treats an already-unmounted dataset as a harmless no-op, preserving
// destroy idempotency while allowing real unmount failures to be returned.
static int rv_zfs_unmount_force(zfs_handle_t *zhp) {
	return zfs_unmount(zhp, NULL, MS_FORCE);
}

// rv_zfs_unshare_all removes any NFS/SMB share for a dataset before destroy. A
// filesystem that is still NFS-exported (sharenfs set + active exportfs entry)
// is BUSY: zfs_destroy returns EBUSY even after unmount, because the kernel NFS
// server still holds an export reference on the mountpoint. This is the
// delete-side mirror of the create path's share step — create does mount+share
// (via zfs_prop_set "sharenfs"), so destroy must unshare.
//
// We deliberately set sharenfs=off via zfs_prop_set rather than call
// zfs_unshare directly: zfs_unshare's signature DIVERGES across libzfs versions
// (1-arg zfs_handle_t* in OpenZFS 2.1 shipped by the Debian bookworm BUILD
// stage; 3-arg zhp,mountpoint,proto in 2.2/2.4), so no single literal call
// compiles on every toolchain. zfs_prop_set(zfs_handle_t*, const char*, const
// char*) is signature-stable across 2.1/2.2/2.4, and setting sharenfs=off runs
// libzfs's changelist callback that unshares — the exact reverse of the create
// path. The return is intentionally ignored: a not-shared dataset (zvol, NFS
// never enabled, or already unshared) is a harmless no-op, and destroy surfaces
// any real error.
static void rv_zfs_unshare_all(zfs_handle_t *zhp) {
	(void) zfs_prop_set(zhp, "sharenfs", "off");
}

static int rv_nvlist_alloc(nvlist_t **out) {
	return nvlist_alloc(out, NV_UNIQUE_NAME, 0);
}
static int rv_nvlist_add_string(nvlist_t *nvl, const char *k, const char *v) {
	return nvlist_add_string(nvl, k, v);
}
static void rv_nvlist_free(nvlist_t *nvl) { nvlist_free(nvl); }

// ---- property fetch (known enum) ----
static int rv_prop_get_str(zfs_handle_t *zhp, zfs_prop_t prop, char *buf, size_t len) {
	return zfs_prop_get(zhp, prop, buf, len, NULL, NULL, 0, B_TRUE);
}

// ---- property-name helpers ----
static zfs_prop_t rv_zfs_name_to_prop(const char *prop) {
	return zfs_name_to_prop(prop);
}
static int rv_zfs_prop_user(const char *prop) {
	return zfs_prop_user(prop) == B_TRUE;
}

// ---- user/arbitrary property fetch ----
static int rv_user_prop_get_str(zfs_handle_t *zhp, const char *prop, char *buf, size_t len) {
	nvlist_t *props = zfs_get_user_props(zhp);
	nvlist_t *entry = NULL;
	char *value = NULL;

	if (props == NULL) return -1;
	if (nvlist_lookup_nvlist(props, prop, &entry) != 0) return -1;
	// The 3rd arg is `char **` on OpenZFS 2.2 (Ubuntu 24.04 runtime) but
	// `const char **` on 2.4 (nixpkgs). A (void *) cast converts implicitly to
	// either without -Wincompatible-pointer-types, so this compiles clean on both.
	if (nvlist_lookup_string(entry, ZPROP_VALUE, (void *)&value) != 0) return -1;

	strncpy(buf, value, len-1); buf[len-1] = '\0';
	return 0;
}

// ---- pool property helpers ----
static uint64_t rv_zpool_get_prop_int(zpool_handle_t *php, zpool_prop_t prop) {
	return zpool_get_prop_int(php, prop, NULL);
}
static int rv_zpool_get_prop_str(zpool_handle_t *php, zpool_prop_t prop, char *buf, size_t len) {
	return zpool_get_prop(php, prop, buf, len, NULL, B_TRUE);
}

// ---- iteration callback glue ----
// The callback "data" pointer carries a runtime/cgo.Handle (a uintptr) that
// resolves, on the Go side, to the *[]string the exported callback appends
// names to. These thunks match the zfs_iter_f / zpool_iter_f prototypes and
// forward into the exported Go callbacks. The extern forward declarations are
// required so the C compiler sees rvSnapIterGo/rvPoolIterGo before the thunks
// reference them; cgo emits the real definitions into _cgo_export.c.
// The callback "data" is a runtime/cgo.Handle, which is a uintptr — an integer
// index into a Go-side registry, NOT a pointer to be dereferenced. It is passed
// as uintptr_t end-to-end so the Go side never converts uintptr->unsafe.Pointer
// (which go vet's unsafeptr flags as a possible dangling-pointer misuse). The
// zfs_iter/zpool_iter callback ABI requires void*, so the thunks cast the
// integer handle to/from void* purely for transport — it is never dereferenced
// as a pointer.
extern int rvSnapIterGo(zfs_handle_t *zhp, uintptr_t data);
extern int rvPoolIterGo(zpool_handle_t *php, uintptr_t data);

static int rv_snap_iter_cb(zfs_handle_t *zhp, void *data) {
	return rvSnapIterGo(zhp, (uintptr_t)data);
}
static int rv_pool_iter_cb(zpool_handle_t *php, void *data) {
	return rvPoolIterGo(php, (uintptr_t)data);
}

// The iteration entry points are wrapped whole in C. The static thunks above
// have internal linkage, so taking their address from Go (e.g. via a
// C.zfs_iter_f cast) would emit an unresolved data-relocation to a file-local
// symbol and fail at final binary link. Keeping the function-pointer purely
// inside C — Go only ever calls rv_iter_snapshots / rv_zpool_iter — sidesteps
// that. rv_iter_snapshots passes min_txg=max_txg=0 (no txg bound); B_FALSE is
// the "not simple" iterator mode matching the original binding.
static int rv_iter_snapshots(zfs_handle_t *zhp, uintptr_t data) {
	return zfs_iter_snapshots(zhp, B_FALSE, rv_snap_iter_cb, (void *)data, 0, 0);
}
static int rv_zpool_iter(libzfs_handle_t *hdl, uintptr_t data) {
	return zpool_iter(hdl, rv_pool_iter_cb, (void *)data);
}
*/
import "C"

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime/cgo"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

// Backend is the cgo libzfs implementation of zfs.Backend.
type Backend struct {
	hdl *C.libzfs_handle_t
}

// New initialises a libzfs handle. One per storage-agent process.
func New() (*Backend, error) {
	hdl := C.libzfs_init()
	if hdl == nil {
		return nil, fmt.Errorf("libzfs: libzfs_init failed (check /dev/zfs access + perms)")
	}
	return &Backend{hdl: hdl}, nil
}

// Close releases the handle.
func (b *Backend) Close() {
	if b.hdl != nil {
		C.libzfs_fini(b.hdl)
		b.hdl = nil
	}
}

func (b *Backend) lastErr(prefix string) error {
	if b.hdl == nil {
		return fmt.Errorf("%s: handle closed", prefix)
	}
	return errFromHandle(b.hdl, prefix)
}

// errFromHandle renders the current libzfs error for an arbitrary handle.
// libzfs_error_description(hdl) returns the actual error text for the handle's
// current error state (e.g. "parent does not exist"). Do NOT use
// libzfs_error_init(libzfs_errno(hdl)) here: libzfs_error_init only maps the
// libzfs_init-time errnos and returns the generic "Failed to initialize the
// libzfs library." string for every operational errno, masking the real failure.
func errFromHandle(hdl *C.struct_libzfs_handle, prefix string) error {
	if hdl == nil {
		return fmt.Errorf("%s: handle closed", prefix)
	}

	return fmt.Errorf("%s: %s", prefix, C.GoString(C.libzfs_error_description(hdl)))
}

func toKind(t C.zfs_type_t) (zfs.VolumeKind, error) {
	switch t {
	case C.ZFS_TYPE_VOLUME:
		return zfs.KindBlock, nil
	case C.ZFS_TYPE_FILESYSTEM:
		return zfs.KindFilesystem, nil
	}
	return "", fmt.Errorf("libzfs: unknown dataset type %d", int(t))
}

func cstr(s string) (*C.char, func()) {
	c := C.CString(s)
	return c, func() { C.free(unsafe.Pointer(c)) }
}

// parseInt64 parses a ZFS numeric property string (e.g. volsize/refquota in
// bytes) into an int64, returning 0 for an empty or unparseable value. ZFS
// always renders these as plain base-10 integers.
func parseInt64(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}

	return n
}

// --- Create ---

func (b *Backend) Create(ctx context.Context, opts zfs.CreateOptions) error {
	var ztype C.zfs_type_t
	switch opts.Kind {
	case zfs.KindBlock:
		ztype = C.ZFS_TYPE_VOLUME
	case zfs.KindFilesystem:
		ztype = C.ZFS_TYPE_FILESYSTEM
	default:
		return fmt.Errorf("libzfs: invalid kind %q", opts.Kind)
	}

	var nvl *C.nvlist_t
	if C.rv_nvlist_alloc(&nvl) != 0 {
		return fmt.Errorf("libzfs: nvlist_alloc failed")
	}
	defer C.rv_nvlist_free(nvl)

	addStr := func(k, v string) error {
		ck, done := cstr(k)
		defer done()
		cv, done2 := cstr(v)
		defer done2()
		if C.rv_nvlist_add_string(nvl, ck, cv) != 0 {
			return fmt.Errorf("libzfs: nvlist_add_string(%s) failed", k)
		}
		return nil
	}

	sizeProp := "volsize"
	if opts.Kind == zfs.KindFilesystem {
		sizeProp = "refquota"
	}
	if err := addStr(sizeProp, fmt.Sprintf("%d", opts.Capacity)); err != nil {
		return err
	}
	if opts.VolBlockSz != "" {
		p := "volblocksize"
		if opts.Kind == zfs.KindFilesystem {
			p = "recordsize"
		}
		if err := addStr(p, opts.VolBlockSz); err != nil {
			return err
		}
	}
	if opts.Compression != "" {
		if err := addStr("compression", opts.Compression); err != nil {
			return err
		}
	}
	if opts.Atime != "" {
		if err := addStr("atime", opts.Atime); err != nil {
			return err
		}
	}
	if opts.XAttr != "" {
		if err := addStr("xattr", opts.XAttr); err != nil {
			return err
		}
	}
	// NOTE: sharenfs is deliberately NOT set in the create nvlist. Setting it at
	// create time records the property but does not trigger libshare's
	// share/export machinery. Instead we set it via zfs_prop_set AFTER mounting
	// (in mountAndShare), which runs the changelist callback that actually shares
	// and exports the dataset — the same path the `zfs set sharenfs=` CLI takes.
	if opts.Encrypted {
		if err := addStr("encryption", "on"); err != nil {
			return err
		}
		if opts.KeyFormat != "" {
			if err := addStr("keyformat", string(opts.KeyFormat)); err != nil {
				return err
			}
		}
		if opts.KeyLocation != "" {
			if err := addStr("keylocation", opts.KeyLocation); err != nil {
				return err
			}
		}
	}

	cname, done := cstr(opts.Name)
	defer done()
	// The volume name is a nested path (e.g. tank/csi/block/<id>), but zfs_create
	// does NOT create intermediate parents — it fails "parent does not exist" if
	// tank/csi or tank/csi/block are absent. zfs_create_ancestors creates every
	// missing parent filesystem (idempotent; the CLI `zfs create -p` equivalent).
	if C.zfs_create_ancestors(b.hdl, cname) != 0 {
		return b.lastErr("libzfs create ancestors")
	}
	if C.zfs_create(b.hdl, cname, ztype, nvl) != 0 {
		if C.libzfs_errno(b.hdl) == C.EZFS_EXISTS {
			return zfs.ErrAlreadyExists
		}
		return b.lastErr("libzfs create")
	}

	// For NFS filesystem volumes: mount, then set sharenfs via zfs_prop_set. NFS
	// exports the mountpoint, so the dataset must be mounted first; then setting
	// the property through zfs_prop_set runs libzfs's changelist callback that
	// actually shares+exports it (unlike setting it in the create nvlist, which
	// silently records the property without exporting). Without this,
	// `showmount`/`exportfs -v` are empty and consumers get "access denied".
	if opts.Kind == zfs.KindFilesystem && opts.ShareNFS != "" {
		if err := b.mountAndShare(cname, opts.ShareNFS, true); err != nil {
			return err
		}
	}

	return nil
}

// mountAndShare mounts the (just-created) filesystem and applies its NFS share
// by SETTING the sharenfs property via zfs_prop_set — which is what actually
// triggers libzfs's share machinery.
//
// Two things make the difference between "exports" and "silent no-op":
//  1. Setting sharenfs through zfs_prop_set (not in the create nvlist) runs the
//     changelist callback that shares+exports the dataset — the exact path the
//     `zfs set sharenfs=` CLI takes. Standalone zfs_share() on a just-created
//     dataset returns success but registers nothing.
//  2. A FRESH libzfs handle is used, not the long-lived b.hdl: libshare keeps
//     per-handle state that goes stale on the long-lived handle, so the share
//     silently no-ops there. The zfs CLI inits a new handle per invocation.
//
// The handle is finalised at the end so nothing leaks. Requires /etc/exports.d
// to exist (baked into the image) — libshare's NFS backend writes the export to
// /etc/exports.d/zfs.exports and disables itself if the directory is missing.
func (b *Backend) mountAndShare(cname *C.char, shareNFS string, chmodRoot bool) error {
	hdl := C.libzfs_init()
	if hdl == nil {
		return fmt.Errorf("libzfs: init handle for share failed")
	}
	defer C.libzfs_fini(hdl)

	zhp := C.zfs_open(hdl, cname, C.ZFS_TYPE_FILESYSTEM)
	if zhp == nil {
		return errFromHandle(hdl, "libzfs open for share")
	}
	defer C.zfs_close(zhp)

	// Mount the filesystem first (NFS exports the mountpoint). This must be
	// idempotent: the agent calls Share on EVERY level-triggered ensure pass
	// (reboot recovery re-exports lost kernel state), so an already-mounted
	// dataset is the common case. C.zfs_mount on an already-mounted dataset
	// returns EZFS_MOUNTED (nonzero), which would otherwise flap a healthy Ready
	// NFS volume to Error every pass — so we SKIP the mount when already mounted.
	// zfs_is_mounted reflects live in-kernel mount state (reset on reboot), so
	// post-reboot this correctly falls through to mount.
	if C.zfs_is_mounted(zhp, nil) == C.B_FALSE {
		if C.zfs_mount(zhp, nil, 0) != 0 {
			return errFromHandle(hdl, "libzfs mount")
		}
	}

	// Open the volume root world-writable. The export uses root_squash (maps a
	// consumer's root to nobody), and a fresh dataset mountpoint is 0755
	// root:root, so a root-running consumer pod (squashed to nobody) cannot
	// write. 0777 lets any UID write into the shared volume — the standard
	// CSI-NFS behaviour for a dynamically provisioned RWX volume. The mountpoint
	// is buf-read via the same property path Get() uses.
	if chmodRoot {
		mp := make([]byte, 1024)
		if C.rv_prop_get_str(zhp, C.ZFS_PROP_MOUNTPOINT, (*C.char)(unsafe.Pointer(&mp[0])), C.size_t(len(mp))) == 0 {
			if dir := C.GoString((*C.char)(unsafe.Pointer(&mp[0]))); dir != "" && dir != "none" {
				if err := os.Chmod(dir, 0o777); err != nil {
					return fmt.Errorf("chmod share root %s: %w", dir, err)
				}
			}
		}
	}

	// Set sharenfs via zfs_prop_set: this runs the changelist callback that
	// shares and exports the dataset (the working `zfs set sharenfs=` path).
	cprop, dp := cstr("sharenfs")
	defer dp()
	cval, dv := cstr(shareNFS)
	defer dv()
	if C.zfs_prop_set(zhp, cprop, cval) != 0 {
		return errFromHandle(hdl, "libzfs set sharenfs")
	}

	// Flush the share table to the kernel exports (drives exportfs). NULL proto
	// = commit all protocols, matching the CLI's behaviour.
	C.zfs_commit_shares(nil)

	return nil
}

// --- Destroy (recursive, promote-aware) ---

// maxPromoteIters bounds the promote loop. Each promotion moves at least one
// snapshot off the dataset, so the iteration count is bounded by the dataset's
// dependent-clone count; the cap is a safety net against a non-converging graph.
const maxPromoteIters = 1024

// Destroy removes a dataset and everything that keeps ZFS from removing it.
//
// A filesystem/volume cannot be destroyed while it has children (snapshots), and
// a snapshot cannot be destroyed while it has dependent CLONES — `zfs clone` is
// copy-on-write, so the clone shares on-disk blocks with the origin snapshot.
// CSI volume-from-source (pvcDataSource / snapshot restore) creates exactly this
// lineage: source A -> snapshot A@clone-B -> clone B. A naive single
// `zfs_destroy(A)` then fails EZFS_EXISTS ("volume has children") forever, and
// the delete retries endlessly while the dataset leaks.
//
// The fix mirrors `zfs promote`: reparent each dependent clone so IT owns the
// shared snapshot lineage (freeing the origin's snapshots), then recursively
// destroy the origin's own — now clone-free — snapshots and the origin itself.
// This deliberately never uses `zfs destroy -R`, which would also destroy the
// live clone B (a separate CSI volume with its own PVC) — silent data loss.
//
// Uses a FRESH libzfs handle (not b.hdl): the long-lived handle's errno is
// stale, so error strings would misreport (the classic "dataset already exists"
// masking the real errno). Mirrors Create/mountAndShare.
func (b *Backend) Destroy(ctx context.Context, name string) error {
	hdl := C.libzfs_init()
	if hdl == nil {
		return fmt.Errorf("libzfs destroy: libzfs_init failed")
	}
	defer C.libzfs_fini(hdl)

	if err := promoteDependents(hdl, name); err != nil {
		return err
	}
	if err := destroyOwnSnapshots(hdl, name); err != nil {
		return err
	}

	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME)
	if zhp == nil {
		return zfs.ErrNotFound
	}
	defer C.zfs_close(zhp)

	// Unshare the NFS export FIRST. A filesystem still exported via NFS
	// (sharenfs set + live exportfs entry) is BUSY — zfs_destroy returns EBUSY
	// even after unmount because the kernel NFS server holds an export
	// reference on the mountpoint. This is the delete-side mirror of the create
	// path (mount+share); destroy must unshare+unmount. No-op for zvols /
	// never-shared datasets.
	C.rv_zfs_unshare_all(zhp)

	// libzfs zfs_destroy does NOT auto-unmount (unlike the CLI): a mounted
	// filesystem returns EZFS_MOUNTED. Force-unmount next; libzfs treats an
	// already-unmounted dataset as a harmless no-op.
	if C.rv_zfs_unmount_force(zhp) != 0 {
		return errFromHandle(hdl, "libzfs unmount")
	}

	// defer MUST be B_FALSE here. zfs_destroy's first line is
	//   if (zhp->zfs_type != ZFS_TYPE_SNAPSHOT && defer) return (EINVAL);
	// so defer=B_TRUE on a filesystem/volume returns EINVAL *without attempting
	// the destroy* AND without setting the handle errno — which this code then
	// misreported as a bogus "device release pending" and retried forever. The
	// CLI (`zfs destroy`, no -d) passes defer=B_FALSE for exactly this reason;
	// defer=B_TRUE is only for `zfs destroy -d` on snapshots (see
	// destroyOwnSnapshots, where it is correct).
	if C.zfs_destroy(zhp, C.boolean_t(0)) != 0 {
		errno := C.libzfs_errno(hdl)
		if errno == C.EZFS_NOENT {
			return zfs.ErrNotFound
		}
		// A genuinely-busy zvol (e.g. /dev/zd* still held during async release
		// after nvmet teardown) comes back as a real EBUSY errno here; the
		// reconciler's rate-limited backoff retries and it self-heals once the
		// device closes. errFromHandle reports the true errno.
		return errFromHandle(hdl, "libzfs destroy")
	}
	return nil
}

// promoteDependents reparents every clone that depends on one of the dataset's
// snapshots so those snapshots stop blocking the dataset's destroy. It re-reads
// live state each pass (a single promote can move a RANGE of snapshots) and
// stops once no snapshot of the dataset carries a dependent clone.
func promoteDependents(hdl *C.struct_libzfs_handle, name string) error {
	for i := 0; i < maxPromoteIters; i++ {
		clone := firstDependentClone(hdl, name)
		if clone == "" {
			return nil
		}
		if err := promoteClone(hdl, clone); err != nil {
			return fmt.Errorf("libzfs promote %s (freeing %s): %w", clone, name, err)
		}
	}

	return fmt.Errorf("libzfs destroy %s: promote did not converge after %d iterations", name, maxPromoteIters)
}

// firstDependentClone returns the name of the first clone found on any snapshot
// of the dataset, or "" if none. Reads live state on each call.
func firstDependentClone(hdl *C.struct_libzfs_handle, name string) string {
	for _, snap := range snapshotsOf(hdl, name) {
		for _, clone := range clonesOfSnapshot(hdl, snap) {
			if clone != "" {
				return clone
			}
		}
	}

	return ""
}

// snapshotsOf lists the full snapshot names (dataset@snap) of a dataset opened
// on the given handle. Returns nil if the dataset is absent.
func snapshotsOf(hdl *C.struct_libzfs_handle, name string) []string {
	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME)
	if zhp == nil {
		return nil
	}
	defer C.zfs_close(zhp)

	var snaps []string
	h := registerIter(&snaps)
	defer releaseIter(h)
	C.rv_iter_snapshots(zhp, C.uintptr_t(h))

	return snaps
}

// clonesOfSnapshot reads a snapshot's CLONES property (comma-separated clone
// dataset names). Returns nil when the snapshot has no clones.
func clonesOfSnapshot(hdl *C.struct_libzfs_handle, snap string) []string {
	csnap, done := cstr(snap)
	defer done()
	zhp := C.zfs_open(hdl, csnap, C.ZFS_TYPE_SNAPSHOT)
	if zhp == nil {
		return nil
	}
	defer C.zfs_close(zhp)

	buf := make([]byte, 8192)
	if C.rv_prop_get_str(zhp, C.ZFS_PROP_CLONES, (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))) != 0 {
		return nil
	}

	return splitClones(C.GoString((*C.char)(unsafe.Pointer(&buf[0]))))
}

// splitClones parses the comma-separated CLONES property value; "-" / empty mean
// no clones.
func splitClones(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "-" {
		return nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}

	return out
}

// promoteClone runs zfs_promote on a clone, reparenting the shared snapshot
// lineage onto it so the clone's origin can be destroyed. A vanished clone
// (already destroyed/promoted away) is a no-op.
func promoteClone(hdl *C.struct_libzfs_handle, clone string) error {
	cclone, done := cstr(clone)
	defer done()
	zhp := C.zfs_open(hdl, cclone, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME)
	if zhp == nil {
		return nil
	}
	defer C.zfs_close(zhp)

	if C.zfs_promote(zhp) != 0 {
		return errFromHandle(hdl, "zfs_promote")
	}

	return nil
}

// destroyOwnSnapshots destroys the (now clone-free) snapshots of the dataset so
// the dataset itself can be destroyed. Snapshots already moved away by a promote
// simply aren't listed. defer=B_TRUE tolerates a snapshot carrying a hold.
func destroyOwnSnapshots(hdl *C.struct_libzfs_handle, name string) error {
	for _, snap := range snapshotsOf(hdl, name) {
		csnap, done := cstr(snap)
		zhp := C.zfs_open(hdl, csnap, C.ZFS_TYPE_SNAPSHOT)
		if zhp == nil {
			done()

			continue
		}
		rc := C.zfs_destroy(zhp, C.boolean_t(1))
		errno := C.libzfs_errno(hdl)
		C.zfs_close(zhp)
		done()
		if rc != 0 && errno != C.EZFS_NOENT {
			return errFromHandle(hdl, "libzfs destroy snapshot "+snap)
		}
	}

	return nil
}

// --- Get ---

func (b *Backend) Get(ctx context.Context, name string) (zfs.DatasetInfo, error) {
	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(b.hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME|C.ZFS_TYPE_SNAPSHOT)
	if zhp == nil {
		return zfs.DatasetInfo{}, zfs.ErrNotFound
	}
	defer C.zfs_close(zhp)

	kind, err := toKind(C.zfs_get_type(zhp))
	if err != nil {
		return zfs.DatasetInfo{}, err
	}
	info := zfs.DatasetInfo{Name: name, Kind: kind}

	buf := make([]byte, 1024)
	getProp := func(p C.zfs_prop_t) string {
		if C.rv_prop_get_str(zhp, p, (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))) != 0 {
			return ""
		}
		return cBufToString(buf)
	}
	if kind == zfs.KindBlock {
		info.Capacity = parseInt64(getProp(C.ZFS_PROP_VOLSIZE))
		info.DevPath = "/dev/zvol/" + name
	} else {
		info.Capacity = parseInt64(getProp(C.ZFS_PROP_REFQUOTA))
		// Keep configured mountpoint separate from live mount state.
		info.ExportPath = getProp(C.ZFS_PROP_MOUNTPOINT)
		info.Mounted = C.zfs_is_mounted(zhp, nil) != C.B_FALSE
	}
	info.Compression = getProp(C.ZFS_PROP_COMPRESSION)
	if enc := getProp(C.ZFS_PROP_ENCRYPTION); enc != "" && enc != "off" {
		info.Encrypted = true
		info.KeyStatus = zfs.KeyLocality(getProp(C.ZFS_PROP_KEYSTATUS))
	}
	return info, nil
}

// cBufToString reads a NUL-terminated C string from a Go byte buffer.
func cBufToString(buf []byte) string {
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i])
		}
	}
	return string(buf)
}

// --- Exists ---

func (b *Backend) Exists(ctx context.Context, name string) (bool, error) {
	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(b.hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME|C.ZFS_TYPE_SNAPSHOT)
	if zhp == nil {
		return false, nil
	}
	C.zfs_close(zhp)
	return true, nil
}

// --- iteration bridge (cgo.Handle) ---

// registerIter wraps the destination slice pointer in a runtime/cgo.Handle so
// it can cross into C as an opaque token and back without violating the cgo
// pointer-passing rules (no Go pointer is ever stored in C memory; only the
// handle's uintptr index travels through the void* data argument).
func registerIter(out *[]string) cgo.Handle {
	return cgo.NewHandle(out)
}

// releaseIter frees the handle registered by registerIter.
func releaseIter(h cgo.Handle) {
	h.Delete()
}

// rvSnapIterGo is the zfs_iter_snapshots callback. libzfs hands us an OPEN
// zfs_handle_t for each snapshot and (per lib/libzfs/libzfs_iter.c in OpenZFS
// 2.1.11) never closes it — the callback owns it — so we zfs_close it here
// after reading the name. Returning 0 continues iteration.
//
//export rvSnapIterGo
func rvSnapIterGo(zhp *C.zfs_handle_t, data C.uintptr_t) C.int {
	out := cgo.Handle(uintptr(data)).Value().(*[]string)
	*out = append(*out, C.GoString(C.zfs_get_name(zhp)))
	C.zfs_close(zhp)
	return 0
}

// rvPoolIterGo is the zpool_iter callback. As with the snapshot iterator,
// zpool_iter (lib/libzfs/libzfs_config.c) hands the callback an OPEN
// zpool_handle_t that the callback owns; the canonical zpool(8) add_pool
// callback zpool_close()es any handle it does not retain, so we do the same.
//
//export rvPoolIterGo
func rvPoolIterGo(php *C.zpool_handle_t, data C.uintptr_t) C.int {
	out := cgo.Handle(uintptr(data)).Value().(*[]string)
	*out = append(*out, C.GoString(C.zpool_get_name(php)))
	C.zpool_close(php)
	return 0
}

// --- ListSnapshots (iteration via cgo.Handle) ---

var snapHandleCtr uint64

func (b *Backend) ListSnapshots(ctx context.Context, name string) ([]string, error) {
	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(b.hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME)
	if zhp == nil {
		return nil, zfs.ErrNotFound
	}
	defer C.zfs_close(zhp)

	var snaps []string
	h := registerIter(&snaps)
	defer releaseIter(h)
	// rv_iter_snapshots wraps the 6-arg zfs_iter_snapshots (OpenZFS 2.1.11)
	// entirely in C; see the preamble note on why the callback is not passed
	// from Go.
	C.rv_iter_snapshots(zhp, C.uintptr_t(h))
	return snaps, nil
}

// --- SetProperty / GetProperty ---

func (b *Backend) SetProperty(ctx context.Context, name, prop, value string) error {
	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(b.hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME)
	if zhp == nil {
		return zfs.ErrNotFound
	}
	defer C.zfs_close(zhp)
	cprop, d1 := cstr(prop)
	defer d1()
	cval, d2 := cstr(value)
	defer d2()
	if C.zfs_prop_set(zhp, cprop, cval) != 0 {
		return b.lastErr("libzfs setprop " + prop)
	}
	return nil
}

func (b *Backend) GetProperty(ctx context.Context, name, prop string) (string, error) {
	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(b.hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME)
	if zhp == nil {
		return "", zfs.ErrNotFound
	}
	defer C.zfs_close(zhp)
	buf := make([]byte, 1024)
	cprop, d1 := cstr(prop)
	defer d1()
	cbuf := (*C.char)(unsafe.Pointer(&buf[0]))
	if zprop := C.rv_zfs_name_to_prop(cprop); zprop != C.ZPROP_INVAL {
		if C.rv_prop_get_str(zhp, zprop, cbuf, C.size_t(len(buf))) != 0 {
			return "", b.lastErr("libzfs getprop " + prop)
		}
		return cBufToString(buf), nil
	}
	if C.rv_zfs_prop_user(cprop) == 0 {
		return "", fmt.Errorf("libzfs getprop %s: unknown property", prop)
	}
	if C.rv_user_prop_get_str(zhp, cprop, cbuf, C.size_t(len(buf))) != 0 {
		return "", b.lastErr("libzfs getprop " + prop)
	}
	return cBufToString(buf), nil
}

// --- Snapshot / DestroySnapshot / Clone ---

func (b *Backend) Snapshot(ctx context.Context, name, snap string) error {
	full := name + "@" + snap
	cname, done := cstr(full)
	defer done()
	var nvl *C.nvlist_t
	if C.rv_nvlist_alloc(&nvl) != 0 {
		return fmt.Errorf("libzfs: nvlist_alloc failed")
	}
	defer C.rv_nvlist_free(nvl)
	if C.zfs_snapshot(b.hdl, cname, C.boolean_t(0), nvl) != 0 {
		if C.libzfs_errno(b.hdl) == C.EZFS_EXISTS {
			return zfs.ErrAlreadyExists
		}
		return b.lastErr("libzfs snapshot")
	}
	return nil
}

func (b *Backend) DestroySnapshot(ctx context.Context, name, snap string) error {
	// Fresh handle, NOT the long-lived b.hdl: the process-lifetime handle's
	// errno is stale (leftover from a prior create), so a failed zfs_destroy is
	// misreported as the previous op's error — most visibly "dataset already
	// exists" (EZFS_EXISTS) on a DESTROY, which is nonsensical and masked the
	// true errno. Same stale-handle class as Destroy / the create+share paths.
	hdl := C.libzfs_init()
	if hdl == nil {
		return fmt.Errorf("libzfs destroy snapshot: init failed")
	}
	defer C.libzfs_fini(hdl)

	full := name + "@" + snap
	cname, done := cstr(full)
	defer done()
	zhp := C.zfs_open(hdl, cname, C.ZFS_TYPE_SNAPSHOT)
	if zhp == nil {
		return zfs.ErrNotFound
	}
	defer C.zfs_close(zhp)
	if C.zfs_destroy(zhp, C.boolean_t(0)) != 0 {
		if C.libzfs_errno(hdl) == C.EZFS_NOENT {
			return zfs.ErrNotFound
		}

		return errFromHandle(hdl, "libzfs destroy snapshot")
	}
	return nil
}

func (b *Backend) Clone(ctx context.Context, src, snap, clonename string) error {
	full := src + "@" + snap
	cfull, d0 := cstr(full)
	defer d0()
	zhp := C.zfs_open(b.hdl, cfull, C.ZFS_TYPE_SNAPSHOT)
	if zhp == nil {
		return zfs.ErrNotFound
	}
	defer C.zfs_close(zhp)
	cclone, d1 := cstr(clonename)
	defer d1()
	var nvl *C.nvlist_t
	if C.rv_nvlist_alloc(&nvl) != 0 {
		return fmt.Errorf("libzfs: nvlist_alloc failed")
	}
	defer C.rv_nvlist_free(nvl)
	if C.zfs_clone(zhp, cclone, nvl) != 0 {
		// Map the "clone already exists" errno to the sentinel (mirroring Snapshot
		// and Create) so cloneAndGrow's idempotent retry treats a re-clone as
		// success. Without this the raw error string never matches
		// errors.Is(err, ErrAlreadyExists) and the clone volume loops forever in
		// Error state, never reaching Ready.
		if C.libzfs_errno(b.hdl) == C.EZFS_EXISTS {
			return zfs.ErrAlreadyExists
		}
		return b.lastErr("libzfs clone")
	}
	return nil
}

// Share mounts the filesystem dataset and exports it over NFS. It reuses the
// same mountAndShare path Create uses for a fresh dataset: zfs_mount, chmod 0777
// (root_squash), zfs_prop_set sharenfs (runs the changelist callback that
// exports via exportfs), zfs_commit_shares. Required on the clone path because
// zfs_clone only reparents COW data and never mounts/shares the clone, so
// without this an NFS clone is created but never exported and the consumer mount
// fails "No such file or directory". No-op when shareNFS is empty (block volume).
func (b *Backend) Share(_ context.Context, name, shareNFS string) error {
	if shareNFS == "" {
		return nil
	}
	cname, d := cstr(name)
	defer d()
	return b.mountAndShare(cname, shareNFS, true)
}

func (b *Backend) ShareImported(_ context.Context, name, shareNFS string) error {
	if shareNFS == "" {
		return nil
	}
	cname, done := cstr(name)
	defer done()
	return b.mountAndShare(cname, shareNFS, false)
}

func (b *Backend) Unshare(_ context.Context, name string) error {
	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(b.hdl, cname, C.ZFS_TYPE_FILESYSTEM)
	if zhp == nil {
		return zfs.ErrNotFound
	}
	defer C.zfs_close(zhp)
	cprop, freeProp := cstr("sharenfs")
	defer freeProp()
	cvalue, freeValue := cstr("off")
	defer freeValue()
	if C.zfs_prop_set(zhp, cprop, cvalue) != 0 {
		return b.lastErr("libzfs unshare")
	}
	C.zfs_commit_shares(nil)
	// Imported datasets are retained storage. De-adoption withdraws the NFS
	// export but keeps the observed mountpoint available to preserve its data.
	return nil
}

// --- Expand ---

func (b *Backend) Expand(ctx context.Context, name string, capacity int64) error {
	cname, done := cstr(name)
	defer done()
	zhp := C.zfs_open(b.hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME)
	if zhp == nil {
		return zfs.ErrNotFound
	}
	prop := "volsize"
	if C.zfs_get_type(zhp) == C.ZFS_TYPE_FILESYSTEM {
		prop = "refquota"
	}
	C.zfs_close(zhp)
	return b.SetProperty(ctx, name, prop, fmt.Sprintf("%d", capacity))
}

// --- native encryption (libzfs_crypto_*) — VERIFY signatures (DECISIONS.md) ---

func openDS(b *Backend, name string) (*C.zfs_handle_t, func(), error) {
	cname, done := cstr(name)
	zhp := C.zfs_open(b.hdl, cname, C.ZFS_TYPE_FILESYSTEM|C.ZFS_TYPE_VOLUME)
	done()
	if zhp == nil {
		return nil, nil, zfs.ErrNotFound
	}
	return zhp, func() { C.zfs_close(zhp) }, nil
}

func (b *Backend) LoadKey(ctx context.Context, name, keyLocation string) error {
	zhp, closeFn, err := openDS(b, name)
	if err != nil {
		return err
	}
	defer closeFn()
	// libzfs_crypto_load_key(zhp, boolean_t noop, const char *alt_keylocation)
	if C.zfs_crypto_load_key(zhp, C.boolean_t(0), nil) != 0 {
		return b.lastErr("libzfs crypto_load_key")
	}
	return nil
}

func (b *Backend) UnloadKey(ctx context.Context, name string) error {
	zhp, closeFn, err := openDS(b, name)
	if err != nil {
		return err
	}
	defer closeFn()
	if C.zfs_crypto_unload_key(zhp) != 0 {
		return b.lastErr("libzfs crypto_unload_key")
	}
	return nil
}

func (b *Backend) ChangeKey(ctx context.Context, name, keyLocation string) error {
	zhp, closeFn, err := openDS(b, name)
	if err != nil {
		return err
	}
	defer closeFn()
	// OpenZFS 2.1.11 has no zfs_crypto_change_key. The rekey primitive is
	// zfs_crypto_rewrap(zhp, raw_props, inheritkey). A NULL raw_props with
	// inheritkey==B_FALSE selects DCP_CMD_NEW_KEY and, per
	// lib/libzfs/libzfs_crypto.c, reuses the dataset's existing keyformat and
	// keylocation (keylocation arg is not yet plumbed through the Go signature),
	// generating a fresh wrapping key — i.e. a genuine DEK rotation, not a no-op.
	if C.zfs_crypto_rewrap(zhp, (*C.nvlist_t)(nil), C.boolean_t(0)) != 0 {
		return b.lastErr("libzfs crypto_rewrap")
	}
	return nil
}

func (b *Backend) KeyStatus(ctx context.Context, name string) (zfs.KeyLocality, error) {
	zhp, closeFn, err := openDS(b, name)
	if err != nil {
		return zfs.KeyNone, err
	}
	defer closeFn()
	buf := make([]byte, 64)
	if C.rv_prop_get_str(zhp, C.ZFS_PROP_KEYSTATUS, (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))) != 0 {
		return zfs.KeyNone, nil
	}
	return zfs.KeyLocality(cBufToString(buf)), nil
}

// --- Pool ops (iteration via cgo.Handle) ---

func (b *Backend) PoolNames(ctx context.Context) ([]string, error) {
	var names []string
	h := registerIter(&names)
	defer releaseIter(h)
	C.rv_zpool_iter(b.hdl, C.uintptr_t(h))
	return names, nil
}

func (b *Backend) PoolFreeBytes(ctx context.Context, pool string) (int64, error) {
	cpool, done := cstr(pool)
	defer done()
	php := C.zpool_open(b.hdl, cpool)
	if php == nil {
		return 0, zfs.ErrPoolNotFound
	}
	defer C.zpool_close(php)
	buf := make([]byte, 64)
	if C.rv_zpool_get_prop_str(php, C.ZPOOL_PROP_FREE, (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))) != 0 {
		return 0, b.lastErr("libzfs pool free")
	}
	free := uint64(C.rv_zpool_get_prop_int(php, C.ZPOOL_PROP_FREE))
	if free > math.MaxInt64 {
		return 0, fmt.Errorf("libzfs pool free exceeds int64: %d", free)
	}
	return int64(free), nil
}

func (b *Backend) PoolGUID(ctx context.Context, pool string) (string, error) {
	php, closeFn, err := openPool(b, pool)
	if err != nil {
		return "", err
	}
	defer closeFn()
	guid := uint64(C.rv_zpool_get_prop_int(php, C.ZPOOL_PROP_GUID))
	if guid == 0 {
		return "", fmt.Errorf("libzfs pool %q has zero GUID", pool)
	}
	return strconv.FormatUint(guid, 10), nil
}

func (b *Backend) PoolHealth(ctx context.Context, pool string) (string, error) {
	php, closeFn, err := openPool(b, pool)
	if err != nil {
		return "", err
	}
	defer closeFn()
	buf := make([]byte, 64)
	if C.rv_zpool_get_prop_str(php, C.ZPOOL_PROP_HEALTH, (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))) != 0 {
		return "", b.lastErr("libzfs pool health")
	}
	return cBufToString(buf), nil
}

func openPool(b *Backend, pool string) (*C.zpool_handle_t, func(), error) {
	cpool, done := cstr(pool)
	defer done()
	php := C.zpool_open(b.hdl, cpool)
	if php == nil {
		return nil, nil, zfs.ErrPoolNotFound
	}
	return php, func() { C.zpool_close(php) }, nil
}

// _ = unused import guard
var _ = atomic.AddUint64

// Compile-time assertion.
var _ zfs.Backend = (*Backend)(nil)
