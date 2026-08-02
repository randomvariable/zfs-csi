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

package stage

import (
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

// seedFakeTarget pre-exports a subsystem+namespace and maps the initiator so
// the fake Client.Attach succeeds and returns a /dev path.
func seedFakeTarget(t *testing.T, f *transport.Fake, nqn, portal, deviceGUID, initiator string) {
	t.Helper()
	ref, err := f.Export(t.Context(), transport.ExportOptions{
		ZvolPath: "/dev/zvol/tank/vol1", DeviceGUID: deviceGUID, TargetNQN: nqn, Portal: portal, Kind: transport.KindNVMeTCP,
	})
	if err != nil {
		t.Fatalf("seed Export: %v", err)
	}
	if err := f.MapInitiator(t.Context(), ref, initiator); err != nil {
		t.Fatalf("seed MapInitiator: %v", err)
	}
}

// TestNVMePlugin_Stage_AttachFormatMount_HappyPath proves ordering: Attach
// happens, IsFormatted (false) → Format, then Mount. Device path returned.
func TestNVMePlugin_Stage_AttachFormatMount_HappyPath(t *testing.T) {
	t.Parallel()

	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.vol1", "10.0.0.1:4420", "guid-1", "nqn.node-a")
	mnt := newRecordingMount()
	p := &NVMeStagePlugin{Block: fake, Mount: mnt, Log: logr.Discard()}

	staging := "/staging/vol1"
	resp, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
		StagingPath: staging,
		FsType:      "ext4",
		MountFlags:  []string{"noatime"},
		Source: &stagepb.NodeStageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{
				TargetNqn: "nqn.vol1", Portal: "10.0.0.1:4420",
				NamespaceId: 1, DeviceGuid: "guid-1", InitiatorId: "nqn.node-a",
			},
		},
	})
	if err != nil {
		t.Fatalf("NodeStage: %v", err)
	}
	if resp.GetDevicePath() == "" {
		t.Fatal("expected non-empty device_path")
	}
	if len(mnt.formatCalls) != 1 {
		t.Fatalf("Format calls = %d, want 1", len(mnt.formatCalls))
	}
	if len(mnt.mountCalls) != 1 {
		t.Fatalf("Mount calls = %d, want 1", len(mnt.mountCalls))
	}
	if mnt.mountCalls[0].target != staging {
		t.Fatalf("mount target = %q, want %q", mnt.mountCalls[0].target, staging)
	}
	if len(mnt.mountCalls[0].opts) != 1 || mnt.mountCalls[0].opts[0] != "noatime" {
		t.Fatalf("mount opts = %v, want [noatime]", mnt.mountCalls[0].opts)
	}
}

// TestNVMePlugin_Stage_IdempotentSkipsFormat: second stage of an
// already-formatted device must skip Format (IsFormatted returns true).
func TestNVMePlugin_Stage_IdempotentSkipsFormat(t *testing.T) {
	t.Parallel()

	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.x", "1.1.1.1:4420", "guid-x", "nqn.n")
	mnt := newRecordingMount()
	p := &NVMeStagePlugin{Block: fake, Mount: mnt, Log: logr.Discard()}

	req := &stagepb.NodeStageRequest{
		StagingPath: "/staging/x", FsType: "xfs",
		Source: &stagepb.NodeStageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.x", Portal: "1.1.1.1:4420", NamespaceId: 1, DeviceGuid: "guid-x", InitiatorId: "nqn.n"},
		},
	}
	if _, err := p.NodeStage(t.Context(), req); err != nil {
		t.Fatalf("first NodeStage: %v", err)
	}
	before := len(mnt.formatCalls)
	if _, err := p.NodeStage(t.Context(), req); err != nil {
		t.Fatalf("second NodeStage: %v", err)
	}
	if len(mnt.formatCalls) != before {
		t.Fatalf("idempotent Format calls = %d, want %d (skip)", len(mnt.formatCalls), before)
	}
}

func TestFilesystemMountFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fsType    string
		formatted bool
		flags     []string
		want      []string
	}{
		{
			name:      "preformatted XFS adds nouuid",
			fsType:    "xfs",
			formatted: true,
			flags:     []string{"noatime"},
			want:      []string{"noatime", "nouuid"},
		},
		{
			name:      "existing nouuid is case insensitive",
			fsType:    "XFS",
			formatted: true,
			flags:     []string{"Nouuid", "noatime"},
			want:      []string{"Nouuid", "noatime"},
		},
		{
			name:      "fresh XFS retains UUID identity",
			fsType:    "xfs",
			formatted: false,
			flags:     []string{"noatime"},
			want:      []string{"noatime"},
		},
		{
			name:      "ext4 does not receive XFS option",
			fsType:    "ext4",
			formatted: true,
			flags:     []string{"noatime"},
			want:      []string{"noatime"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalFlags := slices.Clone(tt.flags)
			got := filesystemMountFlags(tt.fsType, tt.formatted, tt.flags)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("filesystemMountFlags() = %v, want %v", got, tt.want)
			}
			if !slices.Equal(tt.flags, originalFlags) {
				t.Fatalf("input flags mutated: got %v, want %v", tt.flags, originalFlags)
			}
		})
	}
}

// TestNVMePlugin_Unstage_UnmountsAndDetaches: detach is invoked on unstage.
func TestNVMePlugin_Unstage_UnmountsAndDetaches(t *testing.T) {
	t.Parallel()

	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.y", "2.2.2.2:4420", "guid-y", "nqn.n2")
	mnt := newRecordingMount()
	p := &NVMeStagePlugin{Block: fake, Mount: mnt, Log: logr.Discard()}

	_, err := p.NodeUnstage(t.Context(), &stagepb.NodeUnstageRequest{
		StagingPath: "/staging/y",
		Source: &stagepb.NodeUnstageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.y", Portal: "2.2.2.2:4420", NamespaceId: 1, DeviceGuid: "guid-y", InitiatorId: "nqn.n2"},
		},
	})
	if err != nil {
		t.Fatalf("NodeUnstage: %v", err)
	}
	// The raw-block bind lives at <staging>/block-device (a file inside the
	// staging dir), so unstage MUST unmount that file bind first, then the
	// staging dir. Missing the file bind leaks the NVMe device → volume pinned in
	// volumesInUse → orphaned VolumeAttachment. Both unmounts, in order.
	wantUnmounts := []string{"/staging/y/" + BlockDeviceFile, "/staging/y"}
	if len(mnt.unmountCalls) != 2 || mnt.unmountCalls[0] != wantUnmounts[0] || mnt.unmountCalls[1] != wantUnmounts[1] {
		t.Fatalf("unmount calls = %v, want %v", mnt.unmountCalls, wantUnmounts)
	}
}

// TestNVMePlugin_Unstage_UnmountsBlockDeviceBind is the regression guard for the
// raw-block unstage leak: NodeStage(block) binds the /dev node onto
// <staging>/block-device, and unstage must unmount that exact path (not just the
// staging directory) or the NVMe controller stays open and the volume is pinned
// in the node's volumesInUse, orphaning the VolumeAttachment (blocking PV delete
// and later reattach).
func TestNVMePlugin_Unstage_UnmountsBlockDeviceBind(t *testing.T) {
	t.Parallel()

	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.blk", "3.3.3.3:4420", "guid-blk", "nqn.nblk")
	mnt := newRecordingMount()
	p := &NVMeStagePlugin{Block: fake, Mount: mnt, Log: logr.Discard()}

	if _, err := p.NodeUnstage(t.Context(), &stagepb.NodeUnstageRequest{
		StagingPath: "/staging/blk",
		Source: &stagepb.NodeUnstageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.blk", Portal: "3.3.3.3:4420", NamespaceId: 1, DeviceGuid: "guid-blk", InitiatorId: "nqn.nblk"},
		},
	}); err != nil {
		t.Fatalf("NodeUnstage: %v", err)
	}
	var sawBlockBind bool
	for _, u := range mnt.unmountCalls {
		if u == "/staging/blk/"+BlockDeviceFile {
			sawBlockBind = true
		}
	}
	if !sawBlockBind {
		t.Fatalf("unstage did not unmount the block-device bind; calls = %v", mnt.unmountCalls)
	}
}

// TestNVMePlugin_Stage_ResizesRestoredFilesystem is the regression guard for the
// snapshot-restore-to-larger case: a filesystem restored from a snapshot carries
// the source's smaller fs size while the zvol is created larger, so stage must
// resize2fs an ALREADY-formatted device. A freshly-formatted device must NOT be
// resized (it already fills the device).
func TestNVMePlugin_Stage_ResizesRestoredFilesystem(t *testing.T) {
	t.Parallel()

	// Pre-formatted device (clone/restore) → resize expected.
	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.rs", "4.4.4.4:4420", "guid-rs", "nqn.nrs")
	mnt := newRecordingMount()
	// The fake's Attach returns /dev/nvme<NamespaceID>n1; NamespaceId=1 below.
	// Mark it already-formatted to simulate a restored/cloned volume that carries
	// the source's (smaller) filesystem.
	mnt.formatted["/dev/nvme1n1"] = true
	p := &NVMeStagePlugin{Block: fake, Mount: mnt, Log: logr.Discard()}

	if _, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
		StagingPath: "/staging/rs", FsType: "ext4",
		Source: &stagepb.NodeStageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.rs", Portal: "4.4.4.4:4420", NamespaceId: 1, DeviceGuid: "guid-rs", InitiatorId: "nqn.nrs"},
		},
	}); err != nil {
		t.Fatalf("NodeStage (restored): %v", err)
	}
	if len(mnt.resizeCalls) != 1 {
		t.Fatalf("restored volume: resize calls = %d, want 1 (%v)", len(mnt.resizeCalls), mnt.resizeCalls)
	}
	if len(mnt.formatCalls) != 0 {
		t.Fatalf("restored volume: should not re-format, got %v", mnt.formatCalls)
	}

	// Freshly-formatted device (new PVC) → no resize.
	fresh := transport.New()
	seedFakeTarget(t, fresh, "nqn.new", "5.5.5.5:4420", "guid-new", "nqn.nnew")
	freshMnt := newRecordingMount()
	pf := &NVMeStagePlugin{Block: fresh, Mount: freshMnt, Log: logr.Discard()}
	if _, err := pf.NodeStage(t.Context(), &stagepb.NodeStageRequest{
		StagingPath: "/staging/new", FsType: "ext4",
		Source: &stagepb.NodeStageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.new", Portal: "5.5.5.5:4420", NamespaceId: 1, DeviceGuid: "guid-new", InitiatorId: "nqn.nnew"},
		},
	}); err != nil {
		t.Fatalf("NodeStage (fresh): %v", err)
	}
	if len(freshMnt.resizeCalls) != 0 {
		t.Fatalf("fresh volume: should not resize, got %v", freshMnt.resizeCalls)
	}
}

// TestNFSPlugin_Stage_MountsNFSv4: default fsType nfs4 plus v4.2 and nconnect.
func TestNFSPlugin_Stage_MountsNFSv4(t *testing.T) {
	t.Parallel()

	mnt := newRecordingMount()
	p := &NFSStagePlugin{Mount: mnt, Log: logr.Discard()}

	if _, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
		StagingPath: "/staging/nfs",
		Source: &stagepb.NodeStageRequest_Nfs{
			Nfs: &stagepb.NFSSource{Server: "server7", ExportPath: "/tank/pvc-x"},
		},
	}); err != nil {
		t.Fatalf("NodeStage: %v", err)
	}
	if len(mnt.mountCalls) != 1 {
		t.Fatalf("mount calls = %d, want 1", len(mnt.mountCalls))
	}
	mc := mnt.mountCalls[0]
	if mc.source != "server7:/tank/pvc-x" {
		t.Fatalf("source = %q", mc.source)
	}
	if mc.fsType != "nfs4" {
		t.Fatalf("fsType = %q, want nfs4", mc.fsType)
	}
	if got, want := mc.opts, []string{"vers=4.2", "nconnect=8"}; !slices.Equal(got, want) {
		t.Fatalf("opts = %v, want %v", got, want)
	}
}

func TestNFSPlugin_Stage_MergesUserMountFlags(t *testing.T) {
	t.Parallel()

	mnt := newRecordingMount()
	p := &NFSStagePlugin{Mount: mnt, Log: logr.Discard()}

	if _, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
		StagingPath: "/staging/nfs",
		MountFlags:  []string{"noatime", "vers=4.1", "nconnect=4"},
		Source: &stagepb.NodeStageRequest_Nfs{
			Nfs: &stagepb.NFSSource{Server: "server7", ExportPath: "/tank/pvc-x"},
		},
	}); err != nil {
		t.Fatalf("NodeStage: %v", err)
	}

	if got, want := mnt.mountCalls[0].opts, []string{"noatime", "vers=4.1", "nconnect=4"}; !slices.Equal(got, want) {
		t.Fatalf("opts = %v, want %v", got, want)
	}
}

func TestNFSPlugin_Stage_TLSAddsRequiredTransportSecurity(t *testing.T) {
	t.Parallel()

	mnt := newRecordingMount()
	p := &NFSStagePlugin{Mount: mnt, Log: logr.Discard()}
	if _, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
		StagingPath: "/staging/nfs-tls",
		Source: &stagepb.NodeStageRequest_Nfs{
			Nfs: &stagepb.NFSSource{Server: "server7", ExportPath: "/tank/pvc-x", Tls: true},
		},
	}); err != nil {
		t.Fatalf("NodeStage: %v", err)
	}
	if got, want := mnt.mountCalls[0].opts, []string{"vers=4.2", "xprtsec=mtls"}; !slices.Equal(got, want) {
		t.Fatalf("opts = %v, want %v", got, want)
	}
}

func TestNFSPlugin_Stage_TLSRejectsConflictingTransportSecurity(t *testing.T) {
	t.Parallel()

	for _, flags := range [][]string{{"xprtsec=none"}, {"xprtsec=tls"}, {"xprtsec=mtls", "xprtsec=mtls"}} {
		t.Run(strings.Join(flags, ","), func(t *testing.T) {
			p := &NFSStagePlugin{Mount: newRecordingMount(), Log: logr.Discard()}
			_, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
				StagingPath: "/staging/nfs-tls",
				MountFlags:  flags,
				Source: &stagepb.NodeStageRequest_Nfs{
					Nfs: &stagepb.NFSSource{Server: "server7", ExportPath: "/tank/pvc-x", Tls: true},
				},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("NodeStage error code = %v, want InvalidArgument (err %v)", status.Code(err), err)
			}
		})
	}
}

func TestNFSPlugin_Stage_RejectsConflictingOrUnsupportedVersionFlags(t *testing.T) {
	t.Parallel()

	for _, flags := range [][]string{
		{"vers=3"},
		{"vers=4.1", "nfsvers=4.2"},
		{"nconnect=0"},
		{"nconnect=17"},
	} {
		t.Run(strings.Join(flags, ","), func(t *testing.T) {
			p := &NFSStagePlugin{Mount: newRecordingMount(), Log: logr.Discard()}
			_, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
				StagingPath: "/staging/nfs",
				MountFlags:  flags,
				Source: &stagepb.NodeStageRequest_Nfs{
					Nfs: &stagepb.NFSSource{Server: "server7", ExportPath: "/tank/pvc-x"},
				},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("NodeStage error code = %v, want %v (err %v)", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

// TestNFSPlugin_Unstage_UnmountsOnly: nfs unstage does NOT detach.
func TestNFSPlugin_Unstage_UnmountsOnly(t *testing.T) {
	t.Parallel()

	mnt := newRecordingMount()
	p := &NFSStagePlugin{Mount: mnt, Log: logr.Discard()}

	if _, err := p.NodeUnstage(t.Context(), &stagepb.NodeUnstageRequest{StagingPath: "/staging/nfs2"}); err != nil {
		t.Fatalf("NodeUnstage: %v", err)
	}
	if len(mnt.unmountCalls) != 1 {
		t.Fatalf("unmount calls = %d, want 1", len(mnt.unmountCalls))
	}
}

// TestNVMePlugin_Stage_MountErrorMappedToInternal: mount failure → codes.Internal.
func TestNVMePlugin_Stage_MountErrorMappedToInternal(t *testing.T) {
	t.Parallel()

	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.e", "9.9.9.9:4420", "guid-e", "nqn.n3")
	mnt := newRecordingMount()
	mnt.mountErr = errInjected

	p := &NVMeStagePlugin{Block: fake, Mount: mnt, Log: logr.Discard()}
	_, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
		StagingPath: "/staging/err", FsType: "ext4",
		Source: &stagepb.NodeStageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.e", Portal: "9.9.9.9:4420", NamespaceId: 1, DeviceGuid: "guid-e", InitiatorId: "nqn.n3"},
		},
	})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v, want Internal (err=%v)", got, err)
	}
}

// TestNFSPlugin_Stage_RejectsNVMeSource: source-type mismatch → InvalidArgument.
func TestNFSPlugin_Stage_RejectsNVMeSource(t *testing.T) {
	t.Parallel()

	p := &NFSStagePlugin{Mount: newRecordingMount(), Log: logr.Discard()}
	_, err := p.NodeStage(t.Context(), &stagepb.NodeStageRequest{
		StagingPath: "/staging/x",
		Source:      &stagepb.NodeStageRequest_Nvme{Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.x"}},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", got, err)
	}
}
