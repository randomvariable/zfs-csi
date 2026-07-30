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
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

func startStageServer(t *testing.T, socketPath string, srv stagepb.StagePluginServer) func() {
	t.Helper()

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	stagepb.RegisterStagePluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()

	return func() {
		gs.Stop()
		_ = ln.Close()
	}
}

// TestNVMePlugin_Stage_OverRealGRPC grounds the runnable artifact: server +
// client actually speak gRPC over a unix socket, attach+format+mount happens
// server-side, the device path round-trips to the client.
func TestNVMePlugin_Stage_OverRealGRPC(t *testing.T) {
	t.Parallel()

	// Seed the transport fake so Attach succeeds.
	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.grpc", "3.3.3.3:4420", "guid-grpc", "nqn.grpcnode")
	mnt := newRecordingMount()
	srv := &NVMeStagePlugin{Block: fake, Mount: mnt, Log: logr.Discard(), Name: "nvmet-stage", Version: "test"}

	// Bind a unix-socket gRPC server in a temp dir.
	dir := t.TempDir()
	sock := filepath.Join(dir, "stage.sock")
	stopServer := startStageServer(t, sock, srv)
	defer stopServer()

	// Give the server a tick to accept.
	time.Sleep(50 * time.Millisecond)

	// Bounded so a hung server fails the test fast instead of blocking until
	// the global test timeout.
	rpcCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	cli, err := Dial(rpcCtx, sock, logr.Discard())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = cli.Close() }()

	resp, err := cli.Stage(rpcCtx, &stagepb.NodeStageRequest{
		StagingPath: "/staging/grpc",
		FsType:      "ext4",
		Source: &stagepb.NodeStageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.grpc", Portal: "3.3.3.3:4420", NamespaceId: 1, DeviceGuid: "guid-grpc", InitiatorId: "nqn.grpcnode"},
		},
	})
	if err != nil {
		t.Fatalf("Stage RPC: %v", err)
	}
	if resp.GetDevicePath() == "" {
		t.Fatal("device_path empty over gRPC")
	}
	if len(mnt.mountCalls) != 1 {
		t.Fatalf("server-side mount calls = %d, want 1", len(mnt.mountCalls))
	}
	t.Logf("gRPC round-trip OK: device=%s", resp.GetDevicePath())
}

// TestClient_ReconnectsAfterStageServerRestart verifies ClientConn restores a
// node-local unix-socket connection after the sidecar process restarts.
func TestClient_ReconnectsAfterStageServerRestart(t *testing.T) {
	t.Parallel()

	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.reconnect", "4.4.4.4:4420", "guid-reconnect", "nqn.reconnect-node")
	mnt := newRecordingMount()
	srv := &NVMeStagePlugin{Block: fake, Mount: mnt, Log: logr.Discard(), Name: "nvmet-stage", Version: "test"}
	dir := t.TempDir()
	sock := filepath.Join(dir, "stage.sock")
	stopServer := startStageServer(t, sock, srv)
	defer func() { stopServer() }()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, err := Dial(ctx, sock, logr.Discard())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = cli.Close() }()

	req := &stagepb.NodeStageRequest{
		StagingPath: "/staging/reconnect",
		FsType:      "ext4",
		Source: &stagepb.NodeStageRequest_Nvme{
			Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.reconnect", Portal: "4.4.4.4:4420", NamespaceId: 1, DeviceGuid: "guid-reconnect", InitiatorId: "nqn.reconnect-node"},
		},
	}
	if _, err := cli.Stage(ctx, req); err != nil {
		t.Fatalf("initial Stage RPC: %v", err)
	}

	stopServer()
	stopServer = startStageServer(t, sock, srv)

	for {
		if _, err := cli.Stage(ctx, req); err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("Stage after sidecar restart: %v", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
}
