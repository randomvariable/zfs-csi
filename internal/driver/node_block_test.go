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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

// A volumeMode: Block publish must bind the device onto a *file* target
// (BindMountDevice), not a directory bind mount.
func TestNodePublish_BlockRoutesToDeviceBind(t *testing.T) {
	ctx := context.Background()
	mounter := &recordingMountOps{}
	server := newLoggingTestNode(&recordingLogSink{}, mounter)
	volumeID := testVolumeID(t, zfs.KindBlock)

	if _, err := server.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: "/stage/vol",
		TargetPath:        t.TempDir() + "/blockdev",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		},
	}); err != nil {
		t.Fatalf("NodePublishVolume(block) error: %v", err)
	}

	if !mounter.deviceBound {
		t.Fatalf("expected block publish to route to BindMountDevice, deviceBound=false")
	}
}

// A filesystem publish must use the directory bind mount, not the device bind.
func TestNodePublish_FilesystemRoutesToDirBind(t *testing.T) {
	ctx := context.Background()
	mounter := &recordingMountOps{}
	server := newLoggingTestNode(&recordingLogSink{}, mounter)
	volumeID := testVolumeID(t, zfs.KindBlock)

	if _, err := server.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: "/stage/vol",
		TargetPath:        t.TempDir() + "/mount",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		},
	}); err != nil {
		t.Fatalf("NodePublishVolume(fs) error: %v", err)
	}

	if mounter.deviceBound {
		t.Fatalf("expected filesystem publish to use directory BindMount, got deviceBound=true")
	}
	if !mounter.bound {
		t.Fatalf("expected directory bind to run")
	}
}

// Raw block NodeExpandVolume must skip resize2fs (no filesystem) but still
// resolve the device (for the namespace rescan).
func TestNodeExpand_BlockSkipsResize(t *testing.T) {
	ctx := context.Background()
	mounter := &recordingMountOps{}
	server := newLoggingTestNode(&recordingLogSink{}, mounter)
	volumeID := testVolumeID(t, zfs.KindBlock)
	staging := t.TempDir()

	resp, err := server.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{
		VolumeId:          volumeID,
		VolumePath:        staging,
		StagingTargetPath: staging,
		CapacityRange:     &csi.CapacityRange{RequiredBytes: 42 << 20},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		},
	})
	if err != nil {
		t.Fatalf("NodeExpandVolume(block) error: %v", err)
	}
	if resp.GetCapacityBytes() != 42<<20 {
		t.Fatalf("expected capacity 42MiB echoed, got %d", resp.GetCapacityBytes())
	}
	if mounter.resized {
		t.Fatalf("raw block expand must not call resize2fs")
	}
}

// Filesystem NodeExpandVolume must resolve a real device (not "") and resize.
func TestNodeExpand_FilesystemResizesWithDevice(t *testing.T) {
	ctx := context.Background()
	mounter := &recordingMountOps{}
	server := newLoggingTestNode(&recordingLogSink{}, mounter)
	volumeID := testVolumeID(t, zfs.KindBlock)
	staging := t.TempDir()

	if _, err := server.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{
		VolumeId:          volumeID,
		VolumePath:        staging,
		StagingTargetPath: staging,
		CapacityRange:     &csi.CapacityRange{RequiredBytes: 10 << 20},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		},
	}); err != nil {
		t.Fatalf("NodeExpandVolume(fs) error: %v", err)
	}
	if !mounter.resized {
		t.Fatalf("filesystem expand must call resize")
	}
}
