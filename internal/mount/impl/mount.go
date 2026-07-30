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

package impl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	mountutils "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	api "github.com/randomvariable/zfs-csi/internal/mount"
)

var ErrUnsupportedFS = errors.New("unsupported filesystem type")

type Ops struct {
	rootPrefix string
	mounter    api.Interface
	formatter  *mountutils.SafeFormatAndMount
	resizer    *mountutils.ResizeFs
}

func New(rootPrefix string, mounter api.Interface, exec utilexec.Interface) api.MountOps {
	return &Ops{
		rootPrefix: filepath.Clean(rootPrefix),
		mounter:    mounter,
		formatter:  mountutils.NewSafeFormatAndMount(mounter, exec),
		resizer:    mountutils.NewResizeFs(exec),
	}
}

func (o *Ops) Format(ctx context.Context, device, fsType string) (retErr error) {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("format context: %w", err)
	}

	fsType = normalizeFSType(fsType)
	if !supportedFSType(fsType) {
		return fmt.Errorf("%w: %q", ErrUnsupportedFS, fsType)
	}

	// SafeFormatAndMount.FormatAndMount formats the device (if unformatted) and
	// then mounts it — there is no format-only entry point. Each call needs an
	// isolated mount target because NodeStage can format volumes concurrently.
	parent := o.formatProbeParent()
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return fmt.Errorf("create format probe parent %s: %w", parent, err)
	}
	probe, err := os.MkdirTemp(parent, "format-probe-")
	if err != nil {
		return fmt.Errorf("create format probe dir: %w", err)
	}
	defer func() {
		if err := o.mounter.Unmount(probe); err != nil && retErr == nil {
			retErr = fmt.Errorf("unmount format probe %s: %w", probe, err)
		}
		if err := os.RemoveAll(probe); err != nil && retErr == nil {
			retErr = fmt.Errorf("remove format probe %s: %w", probe, err)
		}
	}()

	if err := o.formatter.FormatAndMount(device, probe, fsType, []string{"defaults"}); err != nil {
		return fmt.Errorf("format device %s as %s: %w", device, fsType, err)
	}

	return nil
}

func (o *Ops) IsFormatted(ctx context.Context, device, fsType string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("is formatted context: %w", err)
	}

	format, err := o.formatter.GetDiskFormat(device)
	if err != nil {
		return false, fmt.Errorf("get disk format for %s: %w", device, err)
	}

	if format == "" {
		return false, nil
	}

	fsType = normalizeFSType(fsType)
	if fsType == "" {
		return true, nil
	}

	return format == fsType, nil
}

func (o *Ops) Mount(ctx context.Context, source, target, fsType string, opts []string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mount context: %w", err)
	}

	rooted := o.rooted(target)

	// Idempotent: NodeStage/NodePublish are frequently retried by the kubelet,
	// and re-issuing mount(2) on an already-mounted target fails with exit
	// status 32 ("already mounted"). If the target is already a mount point,
	// treat the mount as done. IsLikelyNotMountPoint returns ErrNotExist when the
	// dir is missing (first mount) — fall through to create+mount below.
	if notMnt, err := o.mounter.IsLikelyNotMountPoint(rooted); err == nil && !notMnt {
		return nil
	}

	if err := o.mounter.Mount(source, rooted, fsType, cloneStrings(opts)); err != nil {
		return fmt.Errorf("mount %s on %s: %w", source, target, err)
	}

	return nil
}

func (o *Ops) Unmount(ctx context.Context, target string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("unmount context: %w", err)
	}

	// Authoritative mountpoint test via a mountinfo scan. IsLikelyNotMountPoint's
	// st_dev heuristic is unreliable for a bind-mounted FILE — notably a raw
	// block device node bound onto a staging file (volumeMode: Block) — where it
	// can falsely report "not a mount point" and skip the unmount. That leaks the
	// bind, holds the NVMe device open (delete_controller cannot remove a busy
	// controller), and pins the volume in the node's volumesInUse forever, which
	// orphans the VolumeAttachment (blocks PV deletion and later reattach). A
	// mountinfo scan reports the file bind correctly. Non-existent / never-mounted
	// targets simply aren't listed → treated as already unmounted (idempotent).
	mounted, err := o.IsMounted(ctx, "", target)
	if err != nil {
		return fmt.Errorf("check mount point %s: %w", target, err)
	}

	if !mounted {
		return nil
	}

	rooted := o.rooted(target)

	// A plain umount(2) on a hard NFS mount whose server is gone blocks
	// indefinitely, so NodeUnstage times out, retries forever, pins the volume in
	// volumesInUse, and wedges the node drain. Bound the primary unmount with a
	// deadline; on timeout OR a hard error, fall back to a lazy MNT_DETACH
	// unmount. Lazy detach is safe here: the staging path is being torn down, the
	// kubelet has already killed the consumer pod, and no new opens can arrive —
	// the detach removes the mount from the namespace and lets the reference drain
	// asynchronously. Applies mainly to NFS; harmless for block/local umounts,
	// which return promptly on the primary path.
	errCh := make(chan error, 1)
	go func() { errCh <- o.mounter.Unmount(rooted) }()

	timeout := unmountTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-errCh:
		if err == nil {
			return nil
		}
		// Primary unmount errored (e.g. EBUSY). Try lazy detach before giving up.
		if lerr := lazyUnmount(rooted); lerr != nil {
			return fmt.Errorf("unmount %s: %w (lazy fallback: %v)", target, err, lerr)
		}

		return nil
	case <-timer.C:
		// Primary unmount is hung (unresponsive NFS server). Force a lazy detach.
		if lerr := lazyUnmount(rooted); lerr != nil {
			return fmt.Errorf("lazy unmount %s after timeout: %w", target, lerr)
		}

		return nil
	case <-ctx.Done():
		return fmt.Errorf("unmount %s: %w", target, ctx.Err())
	}
}

// unmountTimeout bounds a primary umount(2) before falling back to lazy detach.
// Overridable in tests.
var unmountTimeout = 30 * time.Second

// lazyUnmount performs a lazy (MNT_DETACH) unmount. Overridable in tests.
var lazyUnmount = func(target string) error {
	return unix.Unmount(target, unix.MNT_DETACH)
}

func (o *Ops) IsMounted(ctx context.Context, source, target string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("is mounted context: %w", err)
	}

	target = o.rooted(target)

	mounts, err := o.mounter.List()
	if err != nil {
		return false, fmt.Errorf("list mounts: %w", err)
	}

	for _, mp := range mounts {
		if mp.Path == target && (source == "" || mp.Device == source) {
			return true, nil
		}
	}

	return false, nil
}

func (o *Ops) Resize(ctx context.Context, device, mountpoint, fsType string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("resize context: %w", err)
	}

	if fsType = normalizeFSType(fsType); !supportedFSType(fsType) {
		return fmt.Errorf("%w: %q", ErrUnsupportedFS, fsType)
	}

	_, err := o.resizer.Resize(device, o.rooted(mountpoint))
	if err != nil {
		return fmt.Errorf("resize filesystem %s at %s: %w", device, mountpoint, err)
	}

	return nil
}

func (o *Ops) BindMount(ctx context.Context, staging, target string, readOnly bool) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bind mount context: %w", err)
	}

	opts := []string{"bind"}
	if readOnly {
		opts = append(opts, "ro")
	}

	rootedTarget := o.rooted(target)

	// Idempotent: NodePublish is retried by the kubelet; re-issuing the bind
	// mount on an already-mounted target fails exit status 32.
	if notMnt, err := o.mounter.IsLikelyNotMountPoint(rootedTarget); err == nil && !notMnt {
		return nil
	}

	if err := o.mounter.Mount(o.rooted(staging), rootedTarget, "", opts); err != nil {
		return fmt.Errorf("bind mount %s on %s: %w", staging, target, err)
	}

	return nil
}

// BindMountDevice bind-mounts a raw block device node onto target, which must be
// a regular file. Unlike a directory bind mount, the target file must pre-exist
// (mount(2) will not create it). fsType is always empty (no filesystem). ro is
// applied by mounter.Mount's bind+ro two-step remount. Idempotent via a
// mountinfo scan (IsLikelyNotMountPoint is unreliable on a bind-mounted file).
func (o *Ops) BindMountDevice(ctx context.Context, device, target string, readOnly bool) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bind device context: %w", err)
	}

	rootedTarget := o.rooted(target)

	// The target file must exist before mount --bind. Create parent dir + an
	// empty file. Both idempotent on kubelet retry.
	if err := os.MkdirAll(filepath.Dir(rootedTarget), 0o750); err != nil {
		return fmt.Errorf("mkdir device target parent: %w", err)
	}
	f, err := os.OpenFile(rootedTarget, os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("create device target file: %w", err)
	}
	_ = f.Close()

	// Idempotent: re-binding an already-bound target fails exit status 32. Use a
	// mountinfo scan keyed on the target (device path in mountinfo is the source
	// block dev, matched loosely).
	if mounted, err := o.IsMounted(ctx, "", target); err == nil && mounted {
		return nil
	}

	// Device-level read-only enforcement for ROX. A bind mount's "ro" flag governs
	// the mount namespace, NOT writes issued through a block device node: an
	// opener can still write(2) to the underlying device. BLKROSET (what
	// `blockdev --setro` does) marks the block device read-only in the kernel so
	// every write fails EROFS for ALL openers on this node — the correct
	// enforcement for MULTI_NODE_READER_ONLY. Applied to the real /dev node, not
	// the bind target.
	if readOnly {
		if err := setBlockDeviceReadOnly(device); err != nil {
			return fmt.Errorf("set block device read-only %s: %w", device, err)
		}
	}

	opts := []string{"bind"}
	if readOnly {
		opts = append(opts, "ro")
	}
	if err := o.mounter.Mount(device, rootedTarget, "", opts); err != nil {
		return fmt.Errorf("bind device %s on %s: %w", device, target, err)
	}

	return nil
}

// setBlockDeviceReadOnly marks a block device read-only at the kernel layer via
// the BLKROSET ioctl (equivalent to `blockdev --setro`). Unlike a read-only bind
// mount, this causes every write to the device to fail with EROFS. Idempotent.
func setBlockDeviceReadOnly(device string) error {
	f, err := os.OpenFile(device, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", device, err)
	}
	defer func() { _ = f.Close() }()

	one := 1
	if err := unix.IoctlSetPointerInt(int(f.Fd()), unix.BLKROSET, one); err != nil {
		return fmt.Errorf("BLKROSET %s: %w", device, err)
	}

	return nil
}

// DeviceFromMount resolves the backing block device of a mounted path via a
// mountinfo scan. Returns an error if the path is not a mount point.
func (o *Ops) DeviceFromMount(ctx context.Context, mountpoint string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("device from mount context: %w", err)
	}

	rooted := o.rooted(mountpoint)
	mounts, err := o.mounter.List()
	if err != nil {
		return "", fmt.Errorf("list mounts: %w", err)
	}

	for _, mp := range mounts {
		if mp.Path == rooted {
			return mp.Device, nil
		}
	}

	return "", fmt.Errorf("%s is not a mount point", mountpoint)
}

func (o *Ops) rooted(path string) string {
	if o.rootPrefix == "." || o.rootPrefix == string(filepath.Separator) || filepath.IsAbs(path) {
		return filepath.Clean(path)
	}

	return filepath.Join(o.rootPrefix, path)
}

func (o *Ops) formatProbeParent() string {
	if o.rootPrefix == "." || o.rootPrefix == string(filepath.Separator) {
		return os.TempDir()
	}

	return o.rootPrefix
}

func normalizeFSType(fsType string) string {
	return strings.ToLower(strings.TrimSpace(fsType))
}

func supportedFSType(fsType string) bool {
	switch fsType {
	case "ext3", "ext4", "xfs":
		return true
	default:
		return false
	}
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}

	out := make([]string, len(in))
	copy(out, in)

	return out
}
