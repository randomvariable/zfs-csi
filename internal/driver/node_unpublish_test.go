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
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNodeUnpublishVolumeRemovesRawBlockTargetFile(t *testing.T) {
	target := filepath.Join(t.TempDir(), "block-target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("create block target: %v", err)
	}

	server := newLoggingTestNode(&recordingLogSink{}, &recordingMountOps{})
	if _, err := server.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{TargetPath: target}); err != nil {
		t.Fatalf("NodeUnpublishVolume returned error: %v", err)
	}

	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("block target remains after unpublish: %v", err)
	}
}

func TestNodeUnpublishVolumeMissingTargetSucceeds(t *testing.T) {
	target := filepath.Join(t.TempDir(), "missing")
	server := newLoggingTestNode(&recordingLogSink{}, &recordingMountOps{})

	if _, err := server.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{TargetPath: target}); err != nil {
		t.Fatalf("NodeUnpublishVolume returned error for missing target: %v", err)
	}
}

func TestNodeUnpublishVolumeRemovesEmptyDirectoryTarget(t *testing.T) {
	target := filepath.Join(t.TempDir(), "mount-target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatalf("create directory target: %v", err)
	}

	server := newLoggingTestNode(&recordingLogSink{}, &recordingMountOps{})
	if _, err := server.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{TargetPath: target}); err != nil {
		t.Fatalf("NodeUnpublishVolume returned error: %v", err)
	}

	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("directory target remains after unpublish: %v", err)
	}
}

func TestNodeUnpublishVolumeReturnsInternalWhenTargetRemovalFails(t *testing.T) {
	target := filepath.Join(t.TempDir(), "mount-target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatalf("create directory target: %v", err)
	}
	mounter := &recordingMountOps{onUnmount: func(path string) {
		if err := os.WriteFile(filepath.Join(path, "remaining"), nil, 0o600); err != nil {
			t.Fatalf("make target non-empty after unmount: %v", err)
		}
	}}
	server := newLoggingTestNode(&recordingLogSink{}, mounter)

	_, err := server.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{TargetPath: target})
	if status.Code(err) != codes.Internal {
		t.Fatalf("removal error code = %s, want Internal (err=%v)", status.Code(err), err)
	}
	if !mounter.unmounted {
		t.Fatal("expected unmount before target removal")
	}
}

func TestNodeUnpublishVolumeDoesNotRemoveTargetAfterUnmountFailure(t *testing.T) {
	target := filepath.Join(t.TempDir(), "block-target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("create block target: %v", err)
	}
	mounter := &recordingMountOps{unmountErr: errors.New("unmount failed")}
	server := newLoggingTestNode(&recordingLogSink{}, mounter)

	_, err := server.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{TargetPath: target})
	if status.Code(err) != codes.Internal {
		t.Fatalf("unmount error code = %s, want Internal (err=%v)", status.Code(err), err)
	}
	if _, err := os.Lstat(target); err != nil {
		t.Fatalf("target removed after failed unmount: %v", err)
	}
}
