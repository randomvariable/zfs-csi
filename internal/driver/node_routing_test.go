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
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/stage"
	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

// fakeNFSStagePlugin is a minimal StagePlugin server for routing tests: its
// NodeStage records the call so the test can assert the route reached the
// plugin (and the inline path did NOT run).
type fakeNFSStagePlugin struct {
	stagepb.UnimplementedStagePluginServer
	stageCallCount int
	exportPath     string
	tls            bool
}

type fakeNVMeStagePlugin struct {
	stagepb.UnimplementedStagePluginServer
	stageCalls    int
	stageErr      error
	source        *stagepb.NVMeSource
	unstageCalls  int
	unstageSource *stagepb.NVMeSource
}

func startNVMeStagePlugin(t *testing.T) (*fakeNVMeStagePlugin, *stage.Client) {
	t.Helper()
	plugin := &fakeNVMeStagePlugin{}
	sock := filepath.Join(t.TempDir(), "nvme.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	stagepb.RegisterStagePluginServer(server, plugin)
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() {
		server.Stop()
		_ = ln.Close()
	})
	client, err := stage.Dial(t.Context(), sock, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return plugin, client
}

func nvmeStageRequest(staging, nqn, guid string) *csi.NodeStageVolumeRequest {
	return &csi.NodeStageVolumeRequest{
		VolumeId: "csi:tank:block:same-volume", StagingTargetPath: staging,
		VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}},
		PublishContext:   map[string]string{publishContextTargetNQN: nqn, publishContextDeviceGUID: guid, publishContextPortal: "storage-a:4420", publishContextNamespaceID: "1"},
	}
}

func (f *fakeNVMeStagePlugin) PluginInfo(_ context.Context, _ *stagepb.PluginInfoRequest) (*stagepb.PluginInfoResponse, error) {
	return &stagepb.PluginInfoResponse{}, nil
}

func (f *fakeNVMeStagePlugin) NodeStage(_ context.Context, req *stagepb.NodeStageRequest) (*stagepb.NodeStageResponse, error) {
	f.stageCalls++
	f.source = req.GetNvme()
	if f.stageErr != nil {
		return nil, f.stageErr
	}
	return &stagepb.NodeStageResponse{DevicePath: "/dev/nvme0n1"}, nil
}

func (f *fakeNVMeStagePlugin) NodeUnstage(_ context.Context, req *stagepb.NodeUnstageRequest) (*stagepb.NodeUnstageResponse, error) {
	f.unstageCalls++
	f.unstageSource = req.GetNvme()
	return &stagepb.NodeUnstageResponse{}, nil
}

func (f *fakeNFSStagePlugin) PluginInfo(_ context.Context, _ *stagepb.PluginInfoRequest) (*stagepb.PluginInfoResponse, error) {
	return &stagepb.PluginInfoResponse{}, nil
}

func (f *fakeNFSStagePlugin) NodeStage(_ context.Context, req *stagepb.NodeStageRequest) (*stagepb.NodeStageResponse, error) {
	f.stageCallCount++
	f.exportPath = req.GetNfs().GetExportPath()
	f.tls = req.GetNfs().GetTls()

	return &stagepb.NodeStageResponse{}, nil
}

// TestNodeStageVolume_RoutesToNFSPlugin proves Phase 1 routing: with
// StagePlugins configured, NodeStageVolume dials the gRPC plugin and the inline
// mount path is skipped.
func TestNodeStageVolume_RoutesToNFSPlugin(t *testing.T) {
	t.Parallel()

	plugin := &fakeNFSStagePlugin{}
	sock := filepath.Join(t.TempDir(), "nfs.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	defer func() { _ = ln.Close() }()
	gs := grpc.NewServer()
	stagepb.RegisterStagePluginServer(gs, plugin)
	go func() { _ = gs.Serve(ln) }()
	defer gs.Stop()
	time.Sleep(50 * time.Millisecond)

	cli, err := stage.Dial(t.Context(), sock, logr.Discard())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = cli.Close() }()
	// NodeServer with StagePlugins set → routing path. The inline mounter
	// is a sentinel that fails if called, proving the inline path did not run.
	ns := NewNodeServer(NodeConfig{
		Log:       logr.Discard(),
		NodeID:    "node-a",
		Mounter:   &recordingMountOps{}, // unused by routing; publish/expand only
		NFSServer: "server7",
		StagePlugins: map[zfs.VolumeKind]*stage.Client{
			zfs.KindBlock:      cli,
			zfs.KindFilesystem: cli,
		},
	})

	staging := filepath.Join(t.TempDir(), "staging")

	_, err = ns.NodeStageVolume(t.Context(), &csi.NodeStageVolumeRequest{
		VolumeId:          testVolumeID(t, zfs.KindFilesystem),
		StagingTargetPath: staging,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY},
		},
		VolumeContext: map[string]string{publishContextNFSRootPath: "/tank"},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume routed: %v", err)
	}

	if plugin.stageCallCount != 1 {
		t.Fatalf("plugin NodeStage calls = %d, want 1 (route must reach plugin)", plugin.stageCallCount)
	}
}

func TestNodeStageVolumeRejectsFilesystemWithoutNFSServer(t *testing.T) {
	plugin := &fakeNFSStagePlugin{}
	sock := filepath.Join(t.TempDir(), "nfs-missing.sock")
	ln, listenErr := net.Listen("unix", sock)
	if listenErr != nil {
		t.Fatal(listenErr)
	}
	server := grpc.NewServer()
	stagepb.RegisterStagePluginServer(server, plugin)
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(server.Stop)
	stageClient, dialErr := stage.Dial(t.Context(), sock, logr.Discard())
	if dialErr != nil {
		t.Fatal(dialErr)
	}
	ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "node-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindFilesystem: stageClient}})
	_, err := ns.NodeStageVolume(t.Context(), &csi.NodeStageVolumeRequest{
		VolumeId:          testVolumeID(t, zfs.KindFilesystem),
		StagingTargetPath: t.TempDir(),
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY},
		},
		VolumeContext: map[string]string{publishContextNFSRootPath: "/tank"},
	})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "NFS server") {
		t.Fatalf("NodeStageVolume error = %v, want FailedPrecondition NFS server error", err)
	}
}

func TestNodeStageVolumeRequiresAndPropagatesAuthoritativeTransportIdentity(t *testing.T) {
	plugin := &fakeNVMeStagePlugin{}
	sock := filepath.Join(t.TempDir(), "nvme.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	server := grpc.NewServer()
	stagepb.RegisterStagePluginServer(server, plugin)
	go func() { _ = server.Serve(ln) }()
	defer server.Stop()
	client, err := stage.Dial(t.Context(), sock, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "worker-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindBlock: client}})
	request := &csi.NodeStageVolumeRequest{
		VolumeId: "csi:tank:block:same-volume", StagingTargetPath: filepath.Join(t.TempDir(), "stage"),
		VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}},
	}
	if _, err := ns.NodeStageVolume(t.Context(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing identity error=%v, want FailedPrecondition", err)
	}
	nqn, err := naming.TargetNQN("storage-a", "1", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	request.PublishContext = map[string]string{publishContextTargetNQN: nqn, publishContextDeviceGUID: "0123456789abcdef0123456789abcdef", publishContextPortal: "storage-a:4420", publishContextNamespaceID: "1"}
	if _, err := ns.NodeStageVolume(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if plugin.source.GetTargetNqn() != nqn || plugin.source.GetDeviceGuid() != request.PublishContext[publishContextDeviceGUID] || plugin.source.GetPortal() != "storage-a:4420" {
		t.Fatalf("stage source=%#v, want authoritative publish context", plugin.source)
	}
	if plugin.source.GetTls() {
		t.Fatal("stage source TLS = true, want false without TLS publish context")
	}
	if plugin.source.GetPskSecret() != "" {
		t.Fatalf("non-TLS stage source psk_secret = %q, want empty", plugin.source.GetPskSecret())
	}
}

func TestNodeStageVolume_PropagatesTLSIdentityAcrossStageAndUnstage(t *testing.T) {
	plugin, client := startNVMeStagePlugin(t)
	staging := filepath.Join(t.TempDir(), "stage")
	nqn, err := naming.TargetNQN("storage-a", "1", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "worker-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindBlock: client}})
	req := nvmeStageRequest(staging, nqn, "0123456789abcdef0123456789abcdef")
	req.PublishContext[publishContextTLS] = "true"
	req.PublishContext[publishContextPortal] = "storage-a:4421"
	req.PublishContext[publishContextPSKSecret] = "zfs-csi-nvme-psk-same-volume"
	if _, err := ns.NodeStageVolume(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if !plugin.source.GetTls() {
		t.Fatal("stage source TLS = false, want true")
	}
	if got := plugin.source.GetPskSecret(); got != req.PublishContext[publishContextPSKSecret] {
		t.Fatalf("stage source psk_secret = %q, want name-only publish context ref", got)
	}
	if _, err := ns.NodeUnstageVolume(t.Context(), &csi.NodeUnstageVolumeRequest{VolumeId: req.VolumeId, StagingTargetPath: staging}); err != nil {
		t.Fatal(err)
	}
	if !plugin.unstageSource.GetTls() {
		t.Fatal("unstage source TLS = false, want persisted true")
	}
	if got := plugin.unstageSource.GetPskSecret(); got != req.PublishContext[publishContextPSKSecret] {
		t.Fatalf("unstage source psk_secret = %q, want persisted name-only ref", got)
	}
}

func TestNodeStageVolumeRejectsTLSWithoutDedicatedPortal(t *testing.T) {
	plugin, client := startNVMeStagePlugin(t)
	nqn, err := naming.TargetNQN("storage-a", "1", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "worker-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindBlock: client}})
	req := nvmeStageRequest(filepath.Join(t.TempDir(), "stage"), nqn, "0123456789abcdef0123456789abcdef")
	req.PublishContext[publishContextTLS] = "true"
	req.PublishContext[publishContextPSKSecret] = "zfs-csi-nvme-psk-same-volume"
	if _, err := ns.NodeStageVolume(t.Context(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("NodeStageVolume error = %v, want TLS portal FailedPrecondition", err)
	}
	if plugin.stageCalls != 0 {
		t.Fatalf("stage plugin calls = %d, want no sidecar request", plugin.stageCalls)
	}
}

func TestNodeStageVolume_PropagatesNFSTLS(t *testing.T) {
	plugin := &fakeNFSStagePlugin{}
	sock := filepath.Join(t.TempDir(), "nfs-tls.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	stagepb.RegisterStagePluginServer(server, plugin)
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = ln.Close() })
	client, err := stage.Dial(t.Context(), sock, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "node-a", NFSServer: "storage-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindFilesystem: client}})
	_, err = ns.NodeStageVolume(t.Context(), &csi.NodeStageVolumeRequest{
		VolumeId:          testVolumeID(t, zfs.KindFilesystem),
		StagingTargetPath: t.TempDir(),
		PublishContext:    map[string]string{publishContextTLS: "true", publishContextNFSRootPath: "/tank"},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plugin.tls {
		t.Fatal("NFS stage source TLS = false, want true")
	}
}

func TestNVMeIdentityPersistsForConsistentUnstage(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "stage")
	nqn, err := naming.TargetNQN("storage-a", "1", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	source := &stagepb.NVMeSource{TargetNqn: nqn, Portal: "storage-a:4421", NamespaceId: 1, DeviceGuid: "0123456789abcdef0123456789abcdef", Tls: true, PskSecret: "zfs-csi-nvme-psk-same-volume"}
	if err := persistNVMeIdentity(staging, source); err != nil {
		t.Fatal(err)
	}
	got, err := loadNVMeIdentity(staging)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetTargetNqn() != source.GetTargetNqn() || got.GetDeviceGuid() != source.GetDeviceGuid() || got.GetPortal() != source.GetPortal() || got.GetTls() != source.GetTls() || got.GetPskSecret() != source.GetPskSecret() {
		t.Fatalf("loaded identity=%#v, want %#v", got, source)
	}
}

func TestNVMeIdentityOverwriteKeepsCleanupRecord(t *testing.T) {
	staging := filepath.Join(t.TempDir(), "stage")
	nqn, err := naming.TargetNQN("storage-a", "1", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	first := &stagepb.NVMeSource{TargetNqn: nqn, Portal: "storage-a:4421", NamespaceId: 1, DeviceGuid: "0123456789abcdef0123456789abcdef"}
	second := &stagepb.NVMeSource{TargetNqn: nqn, Portal: "storage-b:4421", NamespaceId: 1, DeviceGuid: "fedcba9876543210fedcba9876543210"}
	if err := persistNVMeIdentity(staging, first); err != nil {
		t.Fatal(err)
	}
	if err := persistNVMeIdentity(staging, second); err != nil {
		t.Fatal(err)
	}
	got, err := loadNVMeIdentity(staging)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetPortal() != second.GetPortal() || got.GetDeviceGuid() != second.GetDeviceGuid() {
		t.Fatalf("loaded identity=%#v, want %#v", got, second)
	}
}

func TestPublishContextTLSValueRejectsAmbiguousValue(t *testing.T) {
	t.Parallel()

	if _, err := publishContextTLSValue(map[string]string{publishContextTLS: "enabled"}); err == nil {
		t.Fatal("publishContextTLSValue accepted ambiguous value")
	}
}

func TestNVMeTLSPublishContextSecretRejectsInvalidReferences(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  map[string]string
		tls  bool
	}{
		{name: "missing TLS secret", tls: true},
		{name: "raw material", tls: true, ctx: map[string]string{publishContextPSKSecret: "NVMeTLSkey-1:01:secret:"}},
		{name: "secret on non TLS", ctx: map[string]string{publishContextPSKSecret: "zfs-csi-nvme-psk-valid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := nvmeTLSPublishContextSecret(tc.ctx, tc.tls); err == nil {
				t.Fatal("nvmeTLSPublishContextSecret accepted invalid credential reference")
			}
		})
	}
}

func TestNodeStageVolumeMalformedTransportIdentityDoesNotMutatePluginOrMetadata(t *testing.T) {
	validNQN, err := naming.TargetNQN("storage-a", "1", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	validGUID := "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct{ name, nqn, guid string }{
		{name: "wrong prefix", nqn: "nqn.2026-01.example:zfs:0123456789abcdef0123456789abcdef:block:same-volume", guid: validGUID},
		{name: "wrong owner hash", nqn: "nqn.2026-01.csi.randomvariable:zfs:xyz:block:same-volume", guid: validGUID},
		{name: "overlong", nqn: validNQN + strings.Repeat("x", 224), guid: validGUID},
		{name: "short guid", nqn: validNQN, guid: "0123"},
		{name: "uppercase guid", nqn: validNQN, guid: "0123456789ABCDEF0123456789ABCDEF"},
		{name: "nonhex guid", nqn: validNQN, guid: "g123456789abcdef0123456789abcdef"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin, client := startNVMeStagePlugin(t)
			staging := filepath.Join(t.TempDir(), "stage")
			ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "worker-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindBlock: client}})
			_, err := ns.NodeStageVolume(t.Context(), nvmeStageRequest(staging, tc.nqn, tc.guid))
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("error=%v, want FailedPrecondition", err)
			}
			if plugin.stageCalls != 0 {
				t.Fatalf("plugin Stage calls=%d, want zero", plugin.stageCalls)
			}
			if _, err := os.Stat(nvmeIdentityPath(staging)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("metadata stat error=%v, want not exist", err)
			}
		})
	}
}

func TestNodeStageFailureRetainsIdentityForLaterUnstage(t *testing.T) {
	plugin, client := startNVMeStagePlugin(t)
	plugin.stageErr = status.Error(codes.Internal, "possible partial stage")
	staging := filepath.Join(t.TempDir(), "stage")
	nqn, _ := naming.TargetNQN("storage-a", "1", zfs.KindBlock, "same-volume")
	ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "worker-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindBlock: client}})
	if _, err := ns.NodeStageVolume(t.Context(), nvmeStageRequest(staging, nqn, "0123456789abcdef0123456789abcdef")); err == nil {
		t.Fatal("Stage error=nil, want failure")
	}
	if _, err := os.Stat(nvmeIdentityPath(staging)); err != nil {
		t.Fatalf("Stage failure lost cleanup identity: %v", err)
	}
	if _, err := ns.NodeUnstageVolume(t.Context(), &csi.NodeUnstageVolumeRequest{VolumeId: "csi:tank:block:same-volume", StagingTargetPath: staging}); err != nil {
		t.Fatal(err)
	}
	if plugin.unstageCalls != 1 || plugin.unstageSource.GetTargetNqn() != nqn {
		t.Fatalf("unstage calls/source=%d/%#v", plugin.unstageCalls, plugin.unstageSource)
	}
	if _, err := os.Stat(nvmeIdentityPath(staging)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successful Unstage retained metadata: %v", err)
	}
}

func TestNodeStageVolumeDynamicImportPrefixedFilesystemUsesDerivedExportPath(t *testing.T) {
	plugin := &fakeNFSStagePlugin{}
	sock := filepath.Join(t.TempDir(), "nfs.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	server := grpc.NewServer()
	stagepb.RegisterStagePluginServer(server, plugin)
	go func() { _ = server.Serve(ln) }()
	defer server.Stop()
	time.Sleep(20 * time.Millisecond)
	client, err := stage.Dial(t.Context(), sock, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "node-a", NFSServer: "storage-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindFilesystem: client}})
	_, err = ns.NodeStageVolume(t.Context(), &csi.NodeStageVolumeRequest{
		VolumeId: "csi:tank:filesystem:import-dynamic", StagingTargetPath: t.TempDir(),
		VolumeContext:    map[string]string{"nfs_root_path": "/tank"},
		VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	if plugin.stageCallCount != 1 {
		t.Fatalf("plugin calls = %d, want 1", plugin.stageCallCount)
	}
	if plugin.exportPath != "/csi/fs/import-dynamic" {
		t.Fatalf("export path = %q", plugin.exportPath)
	}
}

func TestNodeStageVolumePrefersResolvedPublishExportPath(t *testing.T) {
	plugin := &fakeNFSStagePlugin{}
	sock := filepath.Join(t.TempDir(), "nfs.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	server := grpc.NewServer()
	stagepb.RegisterStagePluginServer(server, plugin)
	go func() { _ = server.Serve(ln) }()
	defer server.Stop()
	time.Sleep(20 * time.Millisecond)
	client, err := stage.Dial(t.Context(), sock, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ns := NewNodeServer(NodeConfig{Log: logr.Discard(), NodeID: "node-a", NFSServer: "storage-a", StagePlugins: map[zfs.VolumeKind]*stage.Client{zfs.KindFilesystem: client}})
	_, err = ns.NodeStageVolume(t.Context(), &csi.NodeStageVolumeRequest{
		VolumeId: "csi:tank:filesystem:import-any-shape", StagingTargetPath: t.TempDir(),
		VolumeContext:    map[string]string{"exportPath": "/stale/pv/path"},
		PublishContext:   map[string]string{"exportPath": "/tank/apps/imported", "nfs_root_path": "/tank"},
		VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plugin.exportPath != "/apps/imported" {
		t.Fatalf("export path = %q", plugin.exportPath)
	}
}

func TestNodeStageVolumeFilesystemRejectsMissingOrInvalidNFSRoot(t *testing.T) {
	p, err := naming.ParseVolID("csi:tank:filesystem:ordinary-id")
	if err != nil {
		t.Fatal(err)
	}
	for _, context := range []map[string]string{
		{"exportPath": "/tank/csi/fs/ordinary-id"},
		{"exportPath": "/tank/csi/fs/ordinary-id", "nfs_root_path": "/other"},
		{"exportPath": "/tank/csi/fs/ordinary-id", "nfs_root_path": "tank"},
	} {
		req := &csi.NodeStageVolumeRequest{PublishContext: context}
		ns := &NodeServer{nfsServer: "storage-a"}
		_, err := ns.stageViaPlugin(t.Context(), req, p, t.TempDir())
		if err == nil {
			t.Fatalf("publish context %v staged without valid NFS root", context)
		}
	}
}

func TestNodeStageVolumeImportedProvenanceRejectsMissingResolvedExportPath(t *testing.T) {
	p, err := naming.ParseVolID("csi:tank:filesystem:ordinary-id")
	if err != nil {
		t.Fatal(err)
	}
	_, err = fsExportPath(nil, map[string]string{"provenance": string(zfscsiv1.VolumeProvenanceImported)}, p)
	if err == nil || !strings.Contains(err.Error(), "resolved exportPath") {
		t.Fatalf("fsExportPath error = %v", err)
	}
}
