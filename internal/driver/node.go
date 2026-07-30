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

package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/mount"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/observability/logging"
	"github.com/randomvariable/zfs-csi/internal/reachability"
	"github.com/randomvariable/zfs-csi/internal/stage"
	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	"github.com/randomvariable/zfs-csi/internal/transport"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const (
	stagingDirPerm      = 0o755
	targetParentDirPerm = 0o755
	defaultBlockFsType  = "xfs"
	defaultNVMePort     = "4420"
	nvmeTLSPort         = int32(4421)
	defaultNamespaceID  = 1
	nvmeIdentitySuffix  = ".zfs-csi-nvme.json"
	maxInt64            = uint64(1<<63 - 1)
)

// NodeServer implements the CSI Node service. It attaches the block transport
// (nvme connect) or mounts NFS, formats + mounts block
// devices, and bind-mounts staging → pod paths.
type NodeServer struct {
	csi.UnimplementedNodeServer

	log          logr.Logger
	nodeID       string
	nodeTopoKey  string // "zfs.csi.randomvariable.co.uk/node"
	portalHost   string
	mounter      mount.MountOps
	nfsServer    string                           // hostname for NFS mounts (server7)
	stagingRoot  string                           // /var/lib/kubelet/plugins/zfs.csi.randomvariable.co.uk/staging
	publishRoot  string                           // /var/lib/kubelet/pods
	stagePlugins map[zfs.VolumeKind]*stage.Client // route NodeStage/NodeUnstage via gRPC sidecars

	// maxVolumesPerNode caps how many volumes this driver reports as attachable
	// per node (CSINode allocatable). NVMe-oF has a practical per-host
	// controller/namespace ceiling; 0 means "no limit reported".
	maxVolumesPerNode int64
	networkDomain     string

	// inflightProbes dedups outstanding statfs health probes per volume path so
	// a hung hard-NFS mount does not leak a goroutine on every kubelet stats
	// poll (F9). Guarded by probeMu.
	probeMu        sync.Mutex
	inflightProbes map[string]struct{}
}

// NodeConfig configures the NodeServer.
type NodeConfig struct {
	Log         logr.Logger
	NodeID      string
	PortalHost  string
	Mounter     mount.MountOps
	NFSServer   string
	StagingRoot string
	PublishRoot string
	// StagePlugins routes NodeStage/NodeUnstage via node-local gRPC sidecars
	// keyed by VolumeKind. Every kind in the stage path MUST have an entry or
	// the RPC fails InvalidArgument.
	StagePlugins map[zfs.VolumeKind]*stage.Client
	// MaxVolumesPerNode caps attachable volumes per node (0 = no limit reported).
	MaxVolumesPerNode int64
	// NetworkDomain is the stable worker reachability topology segment.
	NetworkDomain string
}

// NewNodeServer constructs a NodeServer.
func NewNodeServer(cfg NodeConfig) *NodeServer {
	if cfg.StagingRoot == "" {
		cfg.StagingRoot = "/var/lib/kubelet/plugins/zfs.csi.randomvariable.co.uk/staging"
	}

	if cfg.PublishRoot == "" {
		cfg.PublishRoot = "/var/lib/kubelet/pods"
	}

	return &NodeServer{
		log:               cfg.Log,
		nodeID:            cfg.NodeID,
		nodeTopoKey:       "zfs.csi.randomvariable.co.uk/node",
		portalHost:        cfg.PortalHost,
		mounter:           cfg.Mounter,
		nfsServer:         cfg.NFSServer,
		stagingRoot:       cfg.StagingRoot,
		publishRoot:       cfg.PublishRoot,
		stagePlugins:      cfg.StagePlugins,
		maxVolumesPerNode: cfg.MaxVolumesPerNode,
		networkDomain:     cfg.NetworkDomain,
	}
}

// NodeGetInfo reports the node identity + topology for scheduling.
func (n *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	resp := &csi.NodeGetInfoResponse{
		NodeId:             n.nodeID,
		AccessibleTopology: &csi.Topology{Segments: map[string]string{reachability.TopologyKeyNetworkDomain: n.networkDomain}},
	}
	// Report the per-node attach ceiling only when configured (>0). Per the CSI
	// spec, max_volumes_per_node <= 0 means "no limit reported", so leaving it
	// zero is the correct way to opt out (e.g. for the NFS-only path).
	if n.maxVolumesPerNode > 0 {
		resp.MaxVolumesPerNode = n.maxVolumesPerNode
	}

	return resp, nil
}

// NodeGetCapabilities advertises STAGE_UNSTAGE + EXPAND + GET_VOLUME_STATS.
func (n *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{Capabilities: nodeCapabilities()}, nil
}

// NodeStageVolume attaches the transport + formats + mounts to the staging path.
func (n *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id + staging_target_path required")
	}

	p, err := naming.ParseVolID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse volume id: %v", err)
	}

	staging := req.GetStagingTargetPath()
	if err := os.MkdirAll(staging, stagingDirPerm); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir staging: %v", err)
	}

	return n.stageViaPlugin(ctx, req, p, staging)
}

// stageViaPlugin routes NodeStageVolume to the transport-appropriate StagePlugin
// gRPC sidecar. Translates CSI volID + publishContext into the proto source
// union (the provider-aware translation that belongs in the driver, not the
// provider-agnostic plugin).
func (n *NodeServer) stageViaPlugin(ctx context.Context, req *csi.NodeStageVolumeRequest, p naming.ParsedVolID, staging string) (*csi.NodeStageVolumeResponse, error) {
	cli := n.stagePlugins[p.Kind]
	if cli == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no stage plugin configured for volume kind %q", p.Kind)
	}

	// Raw block: the consumer wants the device itself, not a filesystem. Only
	// meaningful for the NVMe/block transport; NFS has no raw-block mode.
	isBlock := req.GetVolumeCapability().GetBlock() != nil

	stageReq := &stagepb.NodeStageRequest{
		StagingPath: staging,
		FsType:      blockFsType(req.GetVolumeCapability()),
		MountFlags:  blockMountFlags(req.GetVolumeCapability()),
		Block:       isBlock && p.Kind == zfs.KindBlock,
		ReadOnly: req.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
			req.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
	}

	switch p.Kind {
	case zfs.KindBlock:
		ref, err := n.blockTargetRef(req.GetPublishContext())
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "NVMe target identity is invalid: %v", err)
		}
		pskSecret, err := nvmeTLSPublishContextSecret(req.GetPublishContext(), ref.TLS)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "NVMe TLS credential reference is invalid: %v", err)
		}
		stageReq.Source = &stagepb.NodeStageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{
				TargetNqn:   ref.TargetNQN,
				Portal:      ref.Portal,
				NamespaceId: int32(ref.NamespaceID),
				DeviceGuid:  ref.DeviceGUID,
				InitiatorId: n.nodeID,
				Tls:         ref.TLS,
				PskSecret:   pskSecret,
			},
		}
	case zfs.KindFilesystem:
		hostExportPath, err := fsExportPath(req.GetVolumeContext(), req.GetPublishContext(), p)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "NFS export path is not configured: %v", err)
		}
		nfsRootPath := req.GetPublishContext()[publishContextNFSRootPath]
		if nfsRootPath == "" {
			nfsRootPath = req.GetVolumeContext()[publishContextNFSRootPath]
		}
		exportPath, err := reachability.NFSClientPath(hostExportPath, nfsRootPath)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "NFS export path translation failed: %v", err)
		}
		server := req.GetPublishContext()[publishContextNFSServer]
		if server == "" {
			server = req.GetVolumeContext()[publishContextNFSServer]
		}
		if server == "" {
			server = n.nfsServer
		}
		if server == "" {
			return nil, status.Error(codes.FailedPrecondition, "NFS server is missing from authoritative volume context")
		}
		mountHost, err := reachability.NFSMountHost(server)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "NFS endpoint is invalid: %v", err)
		}
		tls, err := publishContextTLSValue(req.GetPublishContext())
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "NFS TLS configuration is invalid: %v", err)
		}
		stageReq.Source = &stagepb.NodeStageRequest_Nfs{
			Nfs: &stagepb.NFSSource{Server: mountHost, ExportPath: exportPath, Tls: tls},
		}
	}

	stageLog := logging.LogWith(n.log, logging.OpBlockMount,
		logging.KeyVolumeID, req.GetVolumeId(),
		logging.KeyTarget, staging,
		logging.KeyTransport, string(p.Kind),
	)
	if p.Kind == zfs.KindBlock {
		if err := persistNVMeIdentity(staging, stageReq.GetNvme()); err != nil {
			return nil, status.Errorf(codes.Internal, "persist NVMe target identity: %v", err)
		}
	}
	resp, err := cli.Stage(ctx, stageReq)
	if err != nil {
		stageLog.Failed(err)

		return nil, err // already a gRPC status from the plugin
	}
	stageLog.With(logging.KeyDevice, resp.GetDevicePath()).OK()

	return &csi.NodeStageVolumeResponse{}, nil
}

// unstageViaPlugin routes NodeUnstageVolume to the StagePlugin sidecar.
func (n *NodeServer) unstageViaPlugin(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	var kind zfs.VolumeKind
	if p, err := naming.ParseVolID(req.GetVolumeId()); err == nil {
		kind = p.Kind
	} else {
		// Fallback: filesystem volumes have no transport ref, but the plugin
		// needs to know whether to detach. Use filesystem as the conservative
		// default (unmount only, no detach).
		kind = zfs.KindFilesystem
	}

	cli := n.stagePlugins[kind]
	if cli == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no stage plugin configured for volume kind %q", kind)
	}

	unstageReq := &stagepb.NodeUnstageRequest{StagingPath: req.GetStagingTargetPath()}
	if kind == zfs.KindBlock {
		nvme, err := loadNVMeIdentity(req.GetStagingTargetPath())
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "load authoritative NVMe target identity: %v", err)
		}
		nvme.InitiatorId = n.nodeID
		unstageReq.Source = &stagepb.NodeUnstageRequest_Nvme{Nvme: nvme}
	}

	unstageLog := logging.LogWith(n.log, logging.OpUnmountStaging,
		logging.KeyVolumeID, req.GetVolumeId(),
		logging.KeyTarget, req.GetStagingTargetPath(),
	)
	if _, err := cli.Unstage(ctx, unstageReq); err != nil {
		unstageLog.Failed(err)

		return nil, err
	}
	unstageLog.OK()
	if kind == zfs.KindBlock {
		if err := os.Remove(nvmeIdentityPath(req.GetStagingTargetPath())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.Internal, "remove NVMe target identity: %v", err)
		}
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodeUnstageVolume unmounts + detaches.
func (n *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging_target_path required")
	}

	return n.unstageViaPlugin(ctx, req)
}

// NodePublishVolume bind-mounts staging → pod target path.
func (n *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" || req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id + staging + target required")
	}

	target := req.GetTargetPath()

	bindLog := logging.LogWith(n.log, logging.OpBindMount,
		logging.KeyVolumeID, req.GetVolumeId(),
		logging.KeySource, req.GetStagingTargetPath(),
		logging.KeyTarget, target,
		logging.KeyReadonly, req.GetReadonly(),
	)

	// Raw block: the sidecar bound the /dev node onto a file inside the staging
	// directory (BlockDeviceFile). The pod target must likewise be a regular file
	// bound to that same device file, NOT a directory (the CSI block contract).
	// BindMountDevice creates the target file and binds it.
	if req.GetVolumeCapability().GetBlock() != nil {
		blockSrc := filepath.Join(req.GetStagingTargetPath(), stage.BlockDeviceFile)
		if err := n.mounter.BindMountDevice(ctx, blockSrc, target, req.GetReadonly()); err != nil {
			bindLog.Failed(err)

			return nil, status.Errorf(codes.Internal, "bind block target: %v", err)
		}
		bindLog.OK()

		return &csi.NodePublishVolumeResponse{}, nil
	}

	// A directory bind mount requires the target directory itself to exist (not
	// just its parent), else mount(2) fails with exit status 32 ("mount point
	// does not exist"). MkdirAll the full target path; it is idempotent on retry.
	if err := os.MkdirAll(target, targetParentDirPerm); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir target: %v", err)
	}

	if err := n.mounter.BindMount(ctx, req.GetStagingTargetPath(), target, req.GetReadonly()); err != nil {
		bindLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "bind mount: %v", err)
	}
	bindLog.OK()

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the bind target.
func (n *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path required")
	}

	unmountLog := logging.LogWith(n.log, logging.OpUnmountTarget,
		logging.KeyVolumeID, req.GetVolumeId(),
		logging.KeyTarget, req.GetTargetPath(),
	)
	if err := n.mounter.Unmount(ctx, req.GetTargetPath()); err != nil {
		unmountLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "unmount: %v", err)
	}
	if err := os.Remove(req.GetTargetPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		unmountLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "remove target: %v", err)
	}
	unmountLog.OK()

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeExpandVolume grows the filesystem (resize2fs/xfs_growfs).
func (n *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id + volume_path required")
	}

	volPath := req.GetVolumePath()
	requested := req.GetCapacityRange().GetRequiredBytes()

	// The controller already grew the zvol AND revalidated the target-side nvmet
	// namespace (agent Export writes namespaces/<id>/revalidate_size). But the
	// initiator kernel does not always observe the new namespace size promptly;
	// force a controller rescan so the /dev node reflects the grown capacity
	// before we (for a filesystem) resize2fs onto it. Resolve the backing device
	// from the staged mount — passing an empty device to resize2fs is a silent
	// no-op.
	// Candidate mount points that carry the backing device in mountinfo: the fs
	// staging path, the raw-block device file inside staging, and the volume_path
	// itself (block volume_path is the bound device file).
	device := ""
	for _, cand := range []string{
		req.GetStagingTargetPath(),
		filepath.Join(req.GetStagingTargetPath(), stage.BlockDeviceFile),
		volPath,
	} {
		if cand == "" {
			continue
		}
		if d, derr := n.mounter.DeviceFromMount(ctx, cand); derr == nil && d != "" {
			device = d

			break
		}
	}
	if device == "" {
		return nil, status.Error(codes.Internal, "resolve device for expand: not a mount point")
	}

	rescanLog := logging.LogWith(n.log, logging.OpResize,
		logging.KeyVolumeID, req.GetVolumeId(),
		logging.KeyDevice, device,
	)
	if err := rescanNVMeNamespace(device); err != nil {
		// Non-fatal: the kernel may still auto-revalidate via AEN. Log and press
		// on to resize2fs, which will report the true size.
		rescanLog.With(logging.KeyTarget, volPath).Failed(err)
	} else {
		rescanLog.OK()
	}

	// Raw block: no filesystem to grow. The rescan above makes the pod's device
	// reflect the new size; report the requested capacity and return.
	if req.GetVolumeCapability().GetBlock() != nil {
		return &csi.NodeExpandVolumeResponse{CapacityBytes: requested}, nil
	}

	fsType := defaultBlockFsType
	resizeLog := logging.LogWith(n.log, logging.OpResize,
		logging.KeyVolumeID, req.GetVolumeId(),
		logging.KeyDevice, device,
		logging.KeyTarget, volPath,
		logging.KeyFsType, fsType,
	)
	if err := n.mounter.Resize(ctx, device, volPath, fsType); err != nil {
		resizeLog.Failed(err)

		return nil, status.Errorf(codes.Internal, "resize: %v", err)
	}
	resizeLog.OK()

	return &csi.NodeExpandVolumeResponse{CapacityBytes: requested}, nil
}

// NodeGetVolumeStats reports filesystem byte and inode usage via statfs.
func (n *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id required")
	}
	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_path required")
	}

	// Raw block: the volume_path is a device file (or a file bound to the /dev
	// node), not a filesystem — statfs would report the host filesystem's stats,
	// which is meaningless. Report total capacity bytes only (no used/available,
	// no inodes), per the CSI block contract. Detect via S_ISBLK on the path's
	// backing, falling back to a stat of the path itself.
	if isBlockVolumePath(req.GetVolumePath()) {
		total, err := blockDeviceSize(req.GetVolumePath())
		if err != nil {
			// F9: a vanished device node (e.g. the NVMe controller was deleted
			// after ctrl_loss_tmo, or the target went away) is precisely the
			// abnormal condition the health channel exists to surface. Report it
			// as Abnormal on a SUCCESSFUL RPC rather than a NotFound/Internal
			// error, so the external health monitor sees it.
			if os.IsNotExist(err) {
				return abnormalStats(0, "block device missing: "+err.Error()), nil
			}

			return nil, status.Errorf(codes.Internal, "block size: %v", err)
		}

		// F9: even when the device node exists and BLKGETSIZE64 succeeds, the NVMe
		// controller behind it may be in a non-live state (connecting, resetting,
		// deleting) — report that as abnormal.
		if abnormal, msg := blockPathAbnormal(sysBlockRoot, req.GetVolumePath()); abnormal {
			return abnormalStats(total, msg), nil
		}

		return &csi.NodeGetVolumeStatsResponse{
			Usage:           []*csi.VolumeUsage{{Total: total, Unit: csi.VolumeUsage_BYTES}},
			VolumeCondition: &csi.VolumeCondition{Abnormal: false, Message: "ok"},
		}, nil
	}

	// Filesystem: statfs a hard NFS mount whose server is gone blocks forever.
	// Run it under a bounded deadline with a per-path dedup guard (only one
	// outstanding probe per path) so a hung mount reports Abnormal instead of
	// wedging the RPC and leaking a goroutine on every kubelet stats poll.
	usage, err := n.volumeUsageBounded(ctx, req.GetVolumePath())
	if err != nil {
		switch {
		case os.IsNotExist(err):
			return nil, status.Errorf(codes.NotFound, "volume path not found: %v", err)
		case errors.Is(err, errStatfsTimeout):
			return abnormalStats(0, "stat timeout (mount unresponsive)"), nil
		case errors.Is(err, syscall.EIO), errors.Is(err, syscall.ESTALE):
			return abnormalStats(0, "stat failed: "+err.Error()), nil
		default:
			return nil, status.Errorf(codes.Internal, "statfs volume path: %v", err)
		}
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage:           usage,
		VolumeCondition: &csi.VolumeCondition{Abnormal: false, Message: "ok"},
	}, nil
}

// abnormalStats builds a successful NodeGetVolumeStats response that flags the
// volume as abnormal (the CSI health-monitor channel). total may be 0 when the
// size is unknown.
func abnormalStats(total int64, msg string) *csi.NodeGetVolumeStatsResponse {
	usage := []*csi.VolumeUsage{}
	if total > 0 {
		usage = append(usage, &csi.VolumeUsage{Total: total, Unit: csi.VolumeUsage_BYTES})
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage:           usage,
		VolumeCondition: &csi.VolumeCondition{Abnormal: true, Message: msg},
	}
}

// --- helpers ---

// blockTargetRef reconstructs the transport.TargetRef from the parsed volume id.
// The controller publishes the target NQN/portal in the Volume CR status; on
// the node we derive a deterministic ref from the vol id + node config.
func (n *NodeServer) blockTargetRef(publishContext map[string]string) (transport.TargetRef, error) {
	nqn := publishContext[publishContextTargetNQN]
	deviceGUID := publishContext[publishContextDeviceGUID]
	if nqn == "" || deviceGUID == "" {
		return transport.TargetRef{}, errors.New("authoritative NVMe target identity is missing from publish context")
	}
	if err := naming.ValidateTargetNQN(nqn); err != nil {
		return transport.TargetRef{}, err
	}
	if err := naming.ValidateDeviceGUID(deviceGUID); err != nil {
		return transport.TargetRef{}, err
	}

	portal := ""

	namespaceID := defaultNamespaceID
	if v := publishContext[publishContextNamespaceID]; v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed <= 0 {
			return transport.TargetRef{}, fmt.Errorf("invalid publish context namespace_id %q", v)
		}
		namespaceID = parsed
	}
	if v := publishContext[publishContextPortal]; v != "" {
		portal = v
	}
	if portal == "" {
		return transport.TargetRef{}, errors.New("NVMe portal is missing from publish context")
	}
	if _, _, err := reachability.ParsePortal(portal); err != nil {
		return transport.TargetRef{}, err
	}
	tls, err := publishContextTLSValue(publishContext)
	if err != nil {
		return transport.TargetRef{}, err
	}
	if tls {
		_, port, err := reachability.ParsePortal(portal)
		if err != nil || port != nvmeTLSPort {
			return transport.TargetRef{}, errors.New("NVMe TLS portal must use dedicated port 4421")
		}
	}

	return transport.TargetRef{
		Kind:        transport.KindNVMeTCP,
		TargetNQN:   nqn,
		Portal:      portal,
		NamespaceID: namespaceID,
		DeviceGUID:  deviceGUID,
		TLS:         tls,
	}, nil
}

func nvmeTLSPublishContextSecret(publishContext map[string]string, tls bool) (string, error) {
	name := publishContext[publishContextPSKSecret]
	if !tls {
		if name != "" {
			return "", errors.New("psk_secret is set for non-TLS NVMe volume")
		}
		return "", nil
	}
	if name == "" {
		return "", errors.New("psk_secret is missing for TLS NVMe volume")
	}
	if !strings.HasPrefix(name, nvmeTLSPSKSecretPrefix) || len(name) > 253 {
		return "", errors.New("psk_secret does not use configured NVMe TLS Secret name format")
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return "", errors.New("psk_secret does not use configured NVMe TLS Secret name format")
	}
	return name, nil
}

// publishContextTLSValue accepts only the canonical publish-context spelling.
// Any other value would make a retry's transport security ambiguous, so fail
// before asking the node-local sidecar to stage the volume.
func publishContextTLSValue(publishContext map[string]string) (bool, error) {
	v, ok := publishContext[publishContextTLS]
	if !ok || v == "" || v == "false" {
		return false, nil
	}
	if v == "true" {
		return true, nil
	}
	return false, fmt.Errorf("%s must be true or false, got %q", publishContextTLS, v)
}

func nvmeIdentityPath(staging string) string { return staging + nvmeIdentitySuffix }

func persistNVMeIdentity(staging string, source *stagepb.NVMeSource) error {
	if source == nil || source.GetPortal() == "" || source.GetNamespaceId() <= 0 {
		return errors.New("authoritative NVMe target identity is incomplete")
	}
	if err := naming.ValidateTargetNQN(source.GetTargetNqn()); err != nil {
		return err
	}
	if err := naming.ValidateDeviceGUID(source.GetDeviceGuid()); err != nil {
		return err
	}
	data, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return os.WriteFile(nvmeIdentityPath(staging), data, 0o600)
}

func loadNVMeIdentity(staging string) (*stagepb.NVMeSource, error) {
	data, err := os.ReadFile(nvmeIdentityPath(staging))
	if err != nil {
		return nil, err
	}
	source := &stagepb.NVMeSource{}
	if err := json.Unmarshal(data, source); err != nil {
		return nil, err
	}
	if source.GetPortal() == "" || source.GetNamespaceId() <= 0 {
		return nil, errors.New("persisted NVMe target identity is incomplete")
	}
	if err := naming.ValidateTargetNQN(source.GetTargetNqn()); err != nil {
		return nil, err
	}
	if err := naming.ValidateDeviceGUID(source.GetDeviceGuid()); err != nil {
		return nil, err
	}
	return source, nil
}

// blockFsType extracts the filesystem type from a volume capability.
func blockFsType(cap *csi.VolumeCapability) string {
	if cap == nil {
		return defaultBlockFsType
	}

	if m := cap.GetMount(); m != nil && m.GetFsType() != "" {
		return m.GetFsType()
	}

	return defaultBlockFsType
}

func blockMountFlags(cap *csi.VolumeCapability) []string {
	if cap == nil || cap.GetMount() == nil {
		return nil
	}

	return cap.GetMount().GetMountFlags()
}

func volumeUsage(path string) ([]*csi.VolumeUsage, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return nil, err
	}

	blockSize := statfsBlockSize(stat)
	totalBytes := statfsProduct(stat.Blocks, blockSize)
	availableBytes := statfsProduct(stat.Bavail, blockSize)
	usedBytes := statfsProduct(statfsUsed(stat.Blocks, stat.Bfree), blockSize)
	usedInodes := statfsInt64(statfsUsed(stat.Files, stat.Ffree))

	return []*csi.VolumeUsage{
		{
			Available: availableBytes,
			Total:     totalBytes,
			Used:      usedBytes,
			Unit:      csi.VolumeUsage_BYTES,
		},
		{
			Available: statfsInt64(stat.Ffree),
			Total:     statfsInt64(stat.Files),
			Used:      usedInodes,
			Unit:      csi.VolumeUsage_INODES,
		},
	}, nil
}

func statfsBlockSize(stat syscall.Statfs_t) uint64 {
	if stat.Frsize > 0 {
		return uint64(stat.Frsize)
	}
	if stat.Bsize > 0 {
		return uint64(stat.Bsize)
	}

	return 0
}

func statfsUsed(total, free uint64) uint64 {
	if free > total {
		return 0
	}

	return total - free
}

func statfsProduct(count, size uint64) int64 {
	if size != 0 && count > maxInt64/size {
		return int64(maxInt64)
	}

	return statfsInt64(count * size)
}

func statfsInt64(v uint64) int64 {
	if v > maxInt64 {
		return int64(maxInt64)
	}

	return int64(v)
}

// fsExportPath prefers the authoritative controller-resolved export path. The
// publish-context lookup preserves compatibility with already-provisioned PVs
// that predate exportPath in VolumeContext.
func fsExportPath(volumeContext, publishContext map[string]string, p naming.ParsedVolID) (string, error) {
	if v := publishContext[publishContextExportPath]; v != "" {
		return v, nil
	}
	if v := volumeContext[publishContextExportPath]; v != "" {
		return v, nil
	}
	if publishContext[publishContextProvenance] == string(zfscsiv1.VolumeProvenanceImported) || volumeContext[publishContextProvenance] == string(zfscsiv1.VolumeProvenanceImported) {
		return "", errors.New("imported filesystem requires a resolved exportPath")
	}
	return "/" + p.DatasetPath(), nil
}

// rescanNVMeNamespace forces the initiator kernel to re-read the size of the
// NVMe namespace backing device so it observes a controller-side grow before
// resize2fs runs. Resolves the controller from /sys/block/<dev>/device and
// writes 1 to its rescan_controller attribute. Best-effort: returns an error the
// caller logs but does not treat as fatal (the kernel also auto-revalidates via
// an AEN, so this closes the race rather than being the sole mechanism).
func rescanNVMeNamespace(device string) error {
	base := filepath.Base(device) // e.g. nvme1n1
	// The controller rescan attribute lives at /sys/block/<dev>/device/rescan_controller.
	rescan := filepath.Join("/sys/block", base, "device", "rescan_controller")
	if _, err := os.Stat(rescan); err != nil {
		return fmt.Errorf("no rescan_controller for %s: %w", device, err)
	}
	if err := os.WriteFile(rescan, []byte("1"), 0o200); err != nil {
		return fmt.Errorf("write rescan_controller for %s: %w", device, err)
	}

	return nil
}

// isBlockVolumePath reports whether volumePath refers to a raw block device
// (S_ISBLK), i.e. a volumeMode: Block volume. For a filesystem volume the path
// is a directory.
func isBlockVolumePath(volumePath string) bool {
	var st unix.Stat_t
	if err := unix.Stat(volumePath, &st); err != nil {
		return false
	}

	return st.Mode&unix.S_IFMT == unix.S_IFBLK
}

// errStatfsTimeout is returned by volumeUsageBounded when statfs does not
// complete within the deadline (a hung hard NFS mount).
var errStatfsTimeout = errors.New("statfs timed out")

// statfsProbeTimeout bounds a single statfs call. Overridable in tests.
var statfsProbeTimeout = 5 * time.Second

// volumeUsageBounded runs volumeUsage under a deadline with a per-path dedup
// guard. If a probe for the same path is already outstanding (a prior hung
// statfs goroutine hasn't returned), it does NOT spawn another — it reports the
// stuck state as a timeout immediately. This prevents an unbounded goroutine
// leak: statfs(2) on a hung hard mount cannot be cancelled, so the goroutine
// blocks until the mount recovers; without the guard, every ~1/min kubelet
// stats poll would spawn a new permanently-blocked goroutine.
func (n *NodeServer) volumeUsageBounded(ctx context.Context, path string) ([]*csi.VolumeUsage, error) {
	if !n.beginProbe(path) {
		// A probe is already stuck on this path.
		return nil, errStatfsTimeout
	}

	type result struct {
		usage []*csi.VolumeUsage
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		u, err := volumeUsage(path)
		ch <- result{usage: u, err: err}
		n.endProbe(path)
	}()

	timeout := statfsProbeTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case r := <-ch:
		return r.usage, r.err
	case <-timer.C:
		// Leave the goroutine running (it cannot be cancelled) and let the dedup
		// guard suppress future spawns until it returns.
		return nil, errStatfsTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// beginProbe marks a path as having an outstanding statfs probe. Returns false
// if one is already outstanding.
func (n *NodeServer) beginProbe(path string) bool {
	n.probeMu.Lock()
	defer n.probeMu.Unlock()
	if n.inflightProbes == nil {
		n.inflightProbes = map[string]struct{}{}
	}
	if _, busy := n.inflightProbes[path]; busy {
		return false
	}
	n.inflightProbes[path] = struct{}{}

	return true
}

func (n *NodeServer) endProbe(path string) {
	n.probeMu.Lock()
	defer n.probeMu.Unlock()
	delete(n.inflightProbes, path)
}

// sysBlockRoot is the sysfs block root; overridable in tests.
var sysBlockRoot = "/sys/block"

// blockPathAbnormal reports whether the NVMe controller backing a raw block
// volume path is in a non-live state. Resolves the controller from
// <sysBlock>/<dev>/device/state and treats anything other than "live" as
// abnormal. Best-effort: an unreadable/absent state file is treated as NOT
// abnormal (the device node existing + BLKGETSIZE64 succeeding is the primary
// signal; a missing sysfs attr must not produce false alarms on non-NVMe
// backings). Returns (abnormal, message).
func blockPathAbnormal(sysBlock, volumePath string) (bool, string) {
	dev, err := filepath.EvalSymlinks(volumePath)
	if err != nil {
		dev = volumePath
	}
	base := filepath.Base(dev) // e.g. nvme1n1
	stateFile := filepath.Join(sysBlock, base, "device", "state")
	data, err := os.ReadFile(stateFile)
	if err != nil {
		return false, ""
	}
	state := strings.TrimSpace(string(data))
	if state != "" && state != "live" {
		return true, "nvme controller state: " + state
	}

	return false, ""
}

// blockDeviceSize returns the capacity in bytes of a block device via the
// BLKGETSIZE64 ioctl. Used for NodeGetVolumeStats on raw block volumes, where
// statfs is meaningless.
func blockDeviceSize(volumePath string) (int64, error) {
	f, err := os.OpenFile(volumePath, os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	size, err := unix.IoctlGetInt(int(f.Fd()), unix.BLKGETSIZE64)
	if err != nil {
		return 0, fmt.Errorf("BLKGETSIZE64 %s: %w", volumePath, err)
	}

	return int64(size), nil
}

// Ensure NodeServer satisfies csi.NodeServer at build time.
var _ csi.NodeServer = (*NodeServer)(nil)
