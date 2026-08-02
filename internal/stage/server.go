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

// Package stage implements the node-local StagePlugin gRPC contract: a
// synchronous attach+format+mount (NVMe), format+mount (Device), or mount-only
// (NFS) surface that the CSI node driver routes to by transport kind.
//
// It wraps the existing transport.Client and mount.MountOps interfaces as its
// internals. It has NO CSI/ZFS awareness — the node driver (the legitimate
// provider-aware translator) flattens volID + publishContext into the proto
// fields. Import hygiene: this package imports only its own proto package,
// transport, mount, observability, grpc, and protobuf.
package stage

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/randomvariable/zfs-csi/internal/mount"
	"github.com/randomvariable/zfs-csi/internal/observability/logging"
	"github.com/randomvariable/zfs-csi/internal/observability/metrics"
	"github.com/randomvariable/zfs-csi/internal/psk"
	"github.com/randomvariable/zfs-csi/internal/reachability"
	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

const (
	nfsDefaultFsType = "nfs4"
	nfsVersion       = "vers=4.2"
	nfsNConnect      = "nconnect=8"

	// KindNVMe / KindNFS are the plugin kind identifiers used by the binary to
	// select which server to construct. They mirror transport kinds.
	KindNVMe = "nvmet-stage"
	KindNFS  = "nfs-stage"

	// BlockDeviceFile is the file name, created inside the kubelet-provided
	// (directory) staging path, onto which the raw /dev node is bind-mounted for
	// volumeMode: Block. The kubelet pre-creates staging_target_path as a
	// directory, so the device must be bound to a file *inside* it, not onto the
	// staging path itself. NodePublish then binds this file onto the pod target.
	BlockDeviceFile = "block-device"
)

// NewNVMeServer constructs a StagePlugin server for nvme-tcp (attach + format +
// mount). version is the binary version reported via PluginInfo.
func NewNVMeServer(version string, log logr.Logger, block transport.Client, mnt mount.MountOps) *NVMeStagePlugin {
	return &NVMeStagePlugin{Block: block, Mount: mnt, Log: log, Name: KindNVMe, Version: version}
}

// NewNFSServer constructs a StagePlugin server for nfs (mount only).
func NewNFSServer(version string, log logr.Logger, mnt mount.MountOps) *NFSStagePlugin {
	return &NFSStagePlugin{Mount: mnt, Log: log, Name: KindNFS, Version: version}
}

// NVMeStagePlugin implements StagePlugin for nvme-tcp: attach + format + mount.
type NVMeStagePlugin struct {
	stagepb.UnimplementedStagePluginServer
	Block            transport.Client
	Mount            mount.MountOps
	Log              logr.Logger
	Name             string
	Version          string
	NVMeTLSNamespace string
	NVMeTLSSecrets   NVMeTLSSecretReader
	NVMeTLSPSK       NVMeTLSPSKProvisioner
	BeforeAttach     func()
}

// NFSStagePlugin implements StagePlugin for nfs: mount only.
type NFSStagePlugin struct {
	stagepb.UnimplementedStagePluginServer
	Mount   mount.MountOps
	Log     logr.Logger
	Name    string
	Version string
}

// PluginInfo identifies the plugin.
func (p *NVMeStagePlugin) PluginInfo(_ context.Context, _ *stagepb.PluginInfoRequest) (*stagepb.PluginInfoResponse, error) {
	return &stagepb.PluginInfoResponse{Name: p.Name, VendorVersion: p.Version}, nil
}

// PluginInfo identifies the plugin.
func (p *NFSStagePlugin) PluginInfo(_ context.Context, _ *stagepb.PluginInfoRequest) (*stagepb.PluginInfoResponse, error) {
	return &stagepb.PluginInfoResponse{Name: p.Name, VendorVersion: p.Version}, nil
}

// NodeStage attaches (NVMe) → formats → mounts. Idempotent.
func (p *NVMeStagePlugin) NodeStage(ctx context.Context, req *stagepb.NodeStageRequest) (*stagepb.NodeStageResponse, error) {
	staging := req.GetStagingPath()
	if staging == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_path required")
	}

	nvme, err := nvmeSource(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ref := transport.TargetRef{
		Kind:        transport.KindNVMeTCP,
		TargetNQN:   nvme.GetTargetNqn(),
		Portal:      nvme.GetPortal(),
		NamespaceID: int(nvme.GetNamespaceId()),
		DeviceGUID:  nvme.GetDeviceGuid(),
		TLS:         nvme.GetTls(),
	}
	var tlsPSK psk.Interchange
	if nvme.GetTls() {
		var err error
		tlsPSK, err = p.ensureTLSPSK(ctx, nvme.GetPskSecret(), nvme.GetInitiatorId(), nvme.GetTargetNqn())
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
	}
	if p.BeforeAttach != nil {
		p.BeforeAttach()
	}

	attachLog := logging.LogWith(p.Log, logging.OpBlockAttach,
		logging.KeyTargetNQN, ref.TargetNQN,
		logging.KeyPortal, ref.Portal,
	).Metric(metrics.TransportOperationsTotal, string(ref.Kind), "attach")
	device, attachErr := p.Block.Attach(ctx, ref, nvme.GetInitiatorId())
	if attachErr != nil {
		attachLog.Failed(attachErr)
		if nvme.GetTls() {
			// The key was installed for this attach attempt. Do not retain it when
			// the controller was never connected; keep the attach error authoritative.
			p.revokeInstalledTLSPSK(tlsPSK, nvme.GetInitiatorId(), nvme.GetTargetNqn())
		}

		return nil, status.Errorf(codes.Internal, "attach: %v", attachErr)
	}
	attachLog.With(logging.KeyDevice, device).OK()

	// Raw block: bind the /dev node onto a file INSIDE the staging directory
	// (the kubelet pre-creates staging_path as a directory), skipping format +
	// filesystem mount. NodePublish then binds that file onto the pod target.
	if req.GetBlock() {
		blockFile := filepath.Join(staging, BlockDeviceFile)
		bindLog := logging.LogWith(p.Log, logging.OpBlockMount,
			logging.KeyDevice, device,
			logging.KeyTarget, blockFile,
		)
		if err := p.Mount.BindMountDevice(ctx, device, blockFile, req.GetReadOnly()); err != nil {
			bindLog.Failed(err)

			return nil, status.Errorf(codes.Internal, "bind block device: %v", err)
		}
		bindLog.OK()

		return &stagepb.NodeStageResponse{DevicePath: device}, nil
	}

	fsType := req.GetFsType()
	formatted, fmtErr := p.Mount.IsFormatted(ctx, device, fsType)
	if fmtErr != nil {
		return nil, status.Errorf(codes.Internal, "check formatted: %v", fmtErr)
	}

	if !formatted {
		formatLog := logging.LogWith(p.Log, logging.OpBlockFormat,
			logging.KeyDevice, device,
			logging.KeyFsType, fsType,
		)
		if err := p.Mount.Format(ctx, device, fsType); err != nil {
			formatLog.Failed(err)

			return nil, status.Errorf(codes.Internal, "format: %v", err)
		}
		formatLog.OK()
	}

	mountLog := logging.LogWith(p.Log, logging.OpBlockMount,
		logging.KeyDevice, device,
		logging.KeyTarget, staging,
		logging.KeyFsType, fsType,
	)
	mountFlags := filesystemMountFlags(fsType, formatted, req.GetMountFlags())
	if err := p.Mount.Mount(ctx, device, staging, fsType, mountFlags); err != nil {
		mountLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "mount: %v", err)
	}
	mountLog.OK()

	// Grow the filesystem to fill the device when it was ALREADY formatted (a
	// clone/snapshot-restore, not freshly formatted by us above). A filesystem
	// restored from a snapshot carries the SOURCE volume's (smaller) filesystem
	// size, while the CSI provisioner may have created the backing zvol at a
	// LARGER requested capacity. Without an online resize here the pod sees the
	// old smaller size (the "restore snapshot to larger size" conformance case:
	// "Restored fs size is not larger than origin fs size. HINT: check the volume
	// in NodeStageVolume and resize if needed"). resize2fs/xfs_growfs is
	// idempotent — a no-op when the filesystem already fills the device — so this
	// is safe on every stage of a pre-formatted volume.
	if formatted {
		resizeLog := logging.LogWith(p.Log, logging.OpResize,
			logging.KeyDevice, device,
			logging.KeyTarget, staging,
			logging.KeyFsType, fsType,
		)
		if err := p.Mount.Resize(ctx, device, staging, fsType); err != nil {
			resizeLog.Failed(err)

			return nil, status.Errorf(codes.Internal, "resize restored filesystem: %v", err)
		}
		resizeLog.OK()
	}

	return &stagepb.NodeStageResponse{DevicePath: device}, nil
}

// filesystemMountFlags permits mounting pre-existing XFS filesystems that may
// share a UUID with their clone or snapshot source on this node.
func filesystemMountFlags(fsType string, formatted bool, userFlags []string) []string {
	flags := append([]string(nil), userFlags...)
	if !formatted || !strings.EqualFold(fsType, "xfs") {
		return flags
	}

	for _, flag := range userFlags {
		key, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(flag)), "=")
		if key == "nouuid" {
			return flags
		}
	}

	return append(flags, "nouuid")
}

// NodeStage mounts the nfs export. No attach, no format. Idempotent.
func (p *NFSStagePlugin) NodeStage(ctx context.Context, req *stagepb.NodeStageRequest) (*stagepb.NodeStageResponse, error) {
	staging := req.GetStagingPath()
	if staging == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_path required")
	}

	nfs, err := nfsSource(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	src, err := reachability.NFSMountSource(nfs.GetServer(), nfs.GetExportPath())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid NFS endpoint: %v", err)
	}
	// This is the NFS staging plugin, so the mount fsType MUST be an NFS family
	// type. The CSI provisioner defaults a filesystem volume's fsType to "ext4"
	// when the StorageClass sets none, and honouring that here produces
	// `mount -t ext4 <server>:/export` which fails with "wrong fs type / bad
	// superblock" (exit 32). Only accept an explicit NFS-family override
	// (nfs/nfs4); anything else (incl. the ext4 default) is forced to nfs4.
	fsType := req.GetFsType()
	if fsType != "nfs" && fsType != "nfs4" {
		fsType = nfsDefaultFsType
	}

	flags, err := nfsMountFlags(req.GetMountFlags(), nfs.GetTls())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	mountLog := logging.LogWith(p.Log, logging.OpNFSMount,
		logging.KeyTarget, staging,
		logging.KeyFsType, fsType,
	)
	if err := p.Mount.Mount(ctx, src, staging, fsType, flags); err != nil {
		mountLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "mount nfs: %v", err)
	}
	mountLog.OK()

	return &stagepb.NodeStageResponse{}, nil
}

// nfsMountFlags adds performance defaults without overriding StorageClass
// options. The staging plugin always mounts the NFSv4 export, so v2/v3 options
// are rejected instead of passing a contradictory fsType/options pair to mount.
func nfsMountFlags(userFlags []string, tls bool) ([]string, error) {
	flags := append([]string(nil), userFlags...)
	hasVersion := false
	hasNConnect := false
	hasXprtsec := false

	for _, flag := range userFlags {
		key, value, hasValue := strings.Cut(strings.ToLower(strings.TrimSpace(flag)), "=")
		switch key {
		case "vers", "nfsvers":
			if hasVersion {
				return nil, fmt.Errorf("multiple NFS version mount options")
			}
			if !hasValue || !isNFSv4Version(value) {
				return nil, fmt.Errorf("NFS staging requires vers=4, vers=4.0, vers=4.1, or vers=4.2")
			}
			hasVersion = true
		case "nconnect":
			if hasNConnect {
				return nil, fmt.Errorf("multiple nconnect mount options")
			}
			connections, err := strconv.Atoi(value)
			if !hasValue || err != nil || connections < 1 || connections > 16 {
				return nil, fmt.Errorf("nconnect must be an integer between 1 and 16")
			}
			hasNConnect = true
		case "xprtsec":
			if hasXprtsec {
				return nil, fmt.Errorf("multiple xprtsec mount options")
			}
			if tls && (!hasValue || value != "mtls") {
				return nil, fmt.Errorf("NFS TLS staging requires xprtsec=mtls")
			}
			hasXprtsec = true
		}
	}

	if !hasVersion {
		flags = append(flags, nfsVersion)
	}
	// Linux RPC-with-TLS currently rejects nconnect mounts after the first
	// connection because each transport needs a separate TLS handshake. Keep
	// TLS staging single-connection unless the caller explicitly opts in.
	if !hasNConnect && !tls {
		flags = append(flags, nfsNConnect)
	}
	if tls && !hasXprtsec {
		flags = append(flags, "xprtsec=mtls")
	}

	return flags, nil
}

func isNFSv4Version(version string) bool {
	switch version {
	case "4", "4.0", "4.1", "4.2":
		return true
	default:
		return false
	}
}

// NodeUnstage unmounts then detaches (NVMe). Idempotent.
func (p *NVMeStagePlugin) NodeUnstage(ctx context.Context, req *stagepb.NodeUnstageRequest) (*stagepb.NodeUnstageResponse, error) {
	staging := req.GetStagingPath()
	if staging == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_path required")
	}

	// Raw block (volumeMode: Block) binds the /dev node onto a FILE inside the
	// staging directory (<staging>/block-device), not onto <staging> itself.
	// Unmount that file bind FIRST — otherwise the bind leaks, the NVMe device
	// stays open (delete_controller cannot remove a busy controller), and the
	// volume is pinned in the node's volumesInUse forever (orphaning the
	// VolumeAttachment: blocks PV deletion and later reattach). Filesystem
	// volumes never create this file, so the mountinfo-based Unmount no-ops.
	blockFile := filepath.Join(staging, BlockDeviceFile)
	blockUnmountLog := logging.LogWith(p.Log, logging.OpUnmountStaging, logging.KeyTarget, blockFile)
	if err := p.Mount.Unmount(ctx, blockFile); err != nil {
		blockUnmountLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "unmount block device: %v", err)
	}
	blockUnmountLog.OK()

	unmountLog := logging.LogWith(p.Log, logging.OpUnmountStaging, logging.KeyTarget, staging)
	if err := p.Mount.Unmount(ctx, staging); err != nil {
		unmountLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "unmount: %v", err)
	}
	unmountLog.OK()

	nvme, err := nvmeUnstageSource(req)
	if err != nil {
		// No nvme source → nothing to detach. Treat as success (could be a
		// device-source staged volume that happened to hit this plugin).
		return &stagepb.NodeUnstageResponse{}, nil
	}

	ref := transport.TargetRef{
		Kind:        transport.KindNVMeTCP,
		TargetNQN:   nvme.GetTargetNqn(),
		Portal:      nvme.GetPortal(),
		NamespaceID: int(nvme.GetNamespaceId()),
		DeviceGUID:  nvme.GetDeviceGuid(),
		TLS:         nvme.GetTls(),
	}
	detachLog := logging.LogWith(p.Log, logging.OpBlockDetach,
		logging.KeyTargetNQN, ref.TargetNQN,
		logging.KeyPortal, ref.Portal,
	).Metric(metrics.TransportOperationsTotal, string(ref.Kind), "detach")
	if err := p.Block.Detach(ctx, ref); err != nil {
		detachLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "detach: %v", err)
	}
	detachLog.OK()
	if nvme.GetTls() {
		p.revokeTLSPSK(ctx, nvme.GetPskSecret(), nvme.GetInitiatorId(), nvme.GetTargetNqn())
	}

	return &stagepb.NodeUnstageResponse{}, nil
}

// NodeUnstage unmounts only. No detach. Idempotent.
func (p *NFSStagePlugin) NodeUnstage(ctx context.Context, req *stagepb.NodeUnstageRequest) (*stagepb.NodeUnstageResponse, error) {
	staging := req.GetStagingPath()
	if staging == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_path required")
	}

	unmountLog := logging.LogWith(p.Log, logging.OpUnmountStaging, logging.KeyTarget, staging)
	if err := p.Mount.Unmount(ctx, staging); err != nil {
		unmountLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "unmount: %v", err)
	}
	unmountLog.OK()

	return &stagepb.NodeUnstageResponse{}, nil
}

// nvmeSource extracts the NVMeSource variant from a NodeStageRequest.
func nvmeSource(req *stagepb.NodeStageRequest) (*stagepb.NVMeSource, error) {
	switch s := req.GetSource().(type) {
	case *stagepb.NodeStageRequest_Nvme:
		return s.Nvme, nil
	default:
		return nil, fmt.Errorf("this plugin handles nvme sources only, got %T", req.GetSource())
	}
}

// nfsSource extracts the NFSSource variant from a NodeStageRequest.
func nfsSource(req *stagepb.NodeStageRequest) (*stagepb.NFSSource, error) {
	switch s := req.GetSource().(type) {
	case *stagepb.NodeStageRequest_Nfs:
		return s.Nfs, nil
	default:
		return nil, fmt.Errorf("this plugin handles nfs sources only, got %T", req.GetSource())
	}
}

// nvmeUnstageSource extracts the NVMeSource variant from a NodeUnstageRequest
// (may be absent for device/nfs-sourced volumes → returns nil error).
func nvmeUnstageSource(req *stagepb.NodeUnstageRequest) (*stagepb.NVMeSource, error) {
	switch s := req.GetSource().(type) {
	case *stagepb.NodeUnstageRequest_Nvme:
		return s.Nvme, nil
	default:
		return nil, fmt.Errorf("no nvme source")
	}
}
