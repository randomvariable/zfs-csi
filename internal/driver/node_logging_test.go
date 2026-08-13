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
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/observability/logging"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

func TestNodePublishUnpublishAndExpandLogOperations(t *testing.T) {
	ctx := context.Background()
	logSink := &recordingLogSink{}
	mounter := &recordingMountOps{}
	server := newLoggingTestNode(logSink, mounter)
	volumeID := testVolumeID(t, zfs.KindBlock)
	staging := t.TempDir()

	if _, err := server.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: staging,
		TargetPath:        t.TempDir() + "/mount",
		Readonly:          true,
	}); err != nil {
		t.Fatalf("NodePublishVolume returned error: %v", err)
	}

	if _, err := server.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: volumeID, TargetPath: "/pod/target"}); err != nil {
		t.Fatalf("NodeUnpublishVolume returned error: %v", err)
	}

	if _, err := server.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: volumeID, VolumePath: staging}); err != nil {
		t.Fatalf("NodeExpandVolume returned error: %v", err)
	}

	logSink.requireInfo(t, logging.OpBindMount).requireValues(t, logging.KeyVolumeID, volumeID, logging.KeySource, staging, logging.KeyReadonly, true)
	logSink.requireInfo(t, logging.OpUnmountTarget).requireValues(t, logging.KeyVolumeID, volumeID, logging.KeyTarget, "/pod/target")
	logSink.requireInfo(t, logging.OpResize).requireValues(t, logging.KeyVolumeID, volumeID, logging.KeyTarget, staging, logging.KeyFsType, defaultBlockFsType)

	if !mounter.bound || !mounter.unmounted || !mounter.resized {
		t.Fatalf("expected bind/unmount/resize to run, got bound=%t unmounted=%t resized=%t", mounter.bound, mounter.unmounted, mounter.resized)
	}
}

func TestNodeGetVolumeStatsReturnsByteAndInodeUsage(t *testing.T) {
	ctx := context.Background()
	server := newLoggingTestNode(&recordingLogSink{}, &recordingMountOps{})

	resp, err := server.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId:   testVolumeID(t, zfs.KindFilesystem),
		VolumePath: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NodeGetVolumeStats returned error: %v", err)
	}
	if got := len(resp.GetUsage()); got != 2 {
		t.Fatalf("usage entries = %d, want 2: %#v", got, resp.GetUsage())
	}
	t.Logf("NodeGetVolumeStats usage: bytes=%+v inodes=%+v", resp.GetUsage()[0], resp.GetUsage()[1])

	assertVolumeUsage(t, resp.GetUsage()[0], csi.VolumeUsage_BYTES)
	assertVolumeUsage(t, resp.GetUsage()[1], csi.VolumeUsage_INODES)
}

func TestNodeGetVolumeStatsValidatesRequiredFields(t *testing.T) {
	ctx := context.Background()
	server := newLoggingTestNode(&recordingLogSink{}, &recordingMountOps{})

	_, err := server.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{VolumePath: t.TempDir()})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty volume_id error code = %s, want InvalidArgument (err=%v)", status.Code(err), err)
	}

	_, err = server.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{VolumeId: testVolumeID(t, zfs.KindFilesystem)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty volume_path error code = %s, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestNodeGetVolumeStatsMissingPathReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	server := newLoggingTestNode(&recordingLogSink{}, &recordingMountOps{})

	_, err := server.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId:   testVolumeID(t, zfs.KindFilesystem),
		VolumePath: t.TempDir() + "/missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing path error code = %s, want NotFound (err=%v)", status.Code(err), err)
	}
}

func newLoggingTestNode(sink *recordingLogSink, mounter *recordingMountOps) *NodeServer {
	return NewNodeServer(NodeConfig{
		Log:        logr.New(sink),
		NodeID:     "node-a",
		PortalHost: "server7",
		Mounter:    mounter,
		NFSServer:  "server7",
	})
}

func testVolumeID(t *testing.T, kind zfs.VolumeKind) string {
	t.Helper()

	volumeID, err := naming.EncodeVolID("tank", kind, "pvc-a")
	if err != nil {
		t.Fatalf("encode volume id: %v", err)
	}

	return volumeID
}

func assertVolumeUsage(t *testing.T, usage *csi.VolumeUsage, unit csi.VolumeUsage_Unit) {
	t.Helper()

	if usage.GetUnit() != unit {
		t.Fatalf("usage unit = %s, want %s (usage=%#v)", usage.GetUnit(), unit, usage)
	}
	if usage.GetTotal() <= 0 {
		t.Fatalf("usage total = %d, want > 0 (usage=%#v)", usage.GetTotal(), usage)
	}
	if usage.GetAvailable() < 0 || usage.GetAvailable() > usage.GetTotal() {
		t.Fatalf("usage available = %d, want 0 <= available <= total %d (usage=%#v)", usage.GetAvailable(), usage.GetTotal(), usage)
	}
	if usage.GetUsed() < 0 {
		t.Fatalf("usage used = %d, want >= 0 (usage=%#v)", usage.GetUsed(), usage)
	}
}

type recordingMountOps struct {
	formatted   bool
	bound       bool
	deviceBound bool
	unmounted   bool
	resized     bool
	mountErr    error
	unmountErr  error
	onUnmount   func(string)
}

func (m *recordingMountOps) Format(context.Context, string, string) error {
	m.formatted = true

	return nil
}

func (m *recordingMountOps) IsFormatted(context.Context, string, string) (bool, error) {
	return false, nil
}

func (m *recordingMountOps) Mount(context.Context, string, string, string, []string) error {
	return m.mountErr
}

func (m *recordingMountOps) Unmount(_ context.Context, target string) error {
	m.unmounted = true
	if m.onUnmount != nil {
		m.onUnmount(target)
	}

	return m.unmountErr
}

func (m *recordingMountOps) IsMounted(context.Context, string, string) (bool, error) {
	return false, nil
}

func (m *recordingMountOps) Resize(context.Context, string, string, string) error {
	m.resized = true

	return nil
}

func (m *recordingMountOps) BindMount(context.Context, string, string, bool) error {
	m.bound = true

	return nil
}

func (m *recordingMountOps) BindMountDevice(context.Context, string, string, bool) error {
	m.bound = true
	m.deviceBound = true

	return nil
}

func (m *recordingMountOps) DeviceFromMount(context.Context, string) (string, error) {
	return "/dev/fake", nil
}

type recordingLogSink struct {
	entries []recordedLogEntry
}

type recordedLogEntry struct {
	msg    string
	err    error
	values map[string]any
}

func (s *recordingLogSink) Init(logr.RuntimeInfo) {}

func (s *recordingLogSink) Enabled(int) bool { return true }

func (s *recordingLogSink) Info(_ int, msg string, keysAndValues ...any) {
	s.entries = append(s.entries, recordedLogEntry{msg: msg, values: keysToMap(keysAndValues)})
}

func (s *recordingLogSink) Error(err error, msg string, keysAndValues ...any) {
	s.entries = append(s.entries, recordedLogEntry{msg: msg, err: err, values: keysToMap(keysAndValues)})
}

func (s *recordingLogSink) WithValues(keysAndValues ...any) logr.LogSink {
	child := *s
	child.entries = s.entries

	return &child
}

func (s *recordingLogSink) WithName(string) logr.LogSink { return s }

func (s *recordingLogSink) requireInfo(t *testing.T, msg string) recordedLogEntry {
	t.Helper()

	return s.requireEntry(t, msg, false)
}

func (s *recordingLogSink) requireEntry(t *testing.T, msg string, wantErr bool) recordedLogEntry {
	t.Helper()

	for _, entry := range s.entries {
		if entry.msg == msg && (entry.err != nil) == wantErr {
			return entry
		}
	}

	t.Fatalf("missing log entry msg=%q error=%t; got %#v", msg, wantErr, s.entries)

	return recordedLogEntry{}
}

func (e recordedLogEntry) requireValues(t *testing.T, want ...any) {
	t.Helper()

	for i := 0; i < len(want); i += 2 {
		key := want[i].(string)
		if got := e.values[key]; got != want[i+1] {
			t.Fatalf("log value %s = %v, want %v (entry=%#v)", key, got, want[i+1], e)
		}
	}
}

func keysToMap(keysAndValues []any) map[string]any {
	out := make(map[string]any, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			key = fmt.Sprint(keysAndValues[i])
		}
		if i+1 >= len(keysAndValues) {
			out[key] = nil
			continue
		}
		out[key] = keysAndValues[i+1]
	}

	return out
}
