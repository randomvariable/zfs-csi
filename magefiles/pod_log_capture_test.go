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

//go:build mage

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type notifyingWriter struct {
	bytes.Buffer
	written chan struct{}
}

func (w *notifyingWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	select {
	case w.written <- struct{}{}:
	default:
	}
	return n, err
}

var _ io.Writer = (*notifyingWriter)(nil)

func TestPodLogCaptureCommandWritesBothStreamsToArtifact(t *testing.T) {
	artifact, err := os.CreateTemp(t.TempDir(), "stern-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()

	cmd := podLogCaptureCommand(context.Background(), "/tmp/kubeconfig", "zfs-csi", artifact)
	if cmd.Stdout != artifact {
		t.Fatal("stern stdout must be written to its artifact")
	}
	if cmd.Stderr != artifact {
		t.Fatal("stern stderr must be written to its artifact")
	}
}

func TestStartPodLogCaptureProcessCleansArtifactOnStartFailure(t *testing.T) {
	logDir := t.TempDir()
	_, err := startPodLogCaptureProcess(context.Background(), logDir, "run", "/tmp/kubeconfig", "zfs-csi", podLogCaptureCommand, func(*exec.Cmd) error {
		return errors.New("stern unavailable")
	})
	if err == nil {
		t.Fatal("expected stern start failure")
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("start failure left artifacts behind: %v", entries)
	}
}

func TestPodLogCaptureArtifactLivesUnderRunDirectory(t *testing.T) {
	logDir := filepath.Join(t.TempDir(), "run-id")
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		t.Fatal(err)
	}
	_, err := startPodLogCaptureProcess(context.Background(), logDir, "run-id", "/tmp/kubeconfig", "zfs-csi", podLogCaptureCommand, func(*exec.Cmd) error {
		return errors.New("stop before launch")
	})
	if err == nil {
		t.Fatal("expected launch failure")
	}
	want := filepath.Join(logDir, "kubernetes-live-zfs-csi.log")
	if !bytes.Contains([]byte(err.Error()), []byte(want)) {
		t.Fatalf("error %q does not identify run artifact %q", err, want)
	}
}

func TestUnexpectedPodLogCaptureExitWarnsWithArtifactPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &notifyingWriter{written: make(chan struct{}, 1)}
	capture := &podLogCaptureProcess{
		namespace:    "zfs-csi",
		artifactPath: "/tmp/stern.log",
		done:         make(chan struct{}),
		waitErr:      errors.New("dns lookup failed"),
	}
	warnOnUnexpectedPodLogCaptureExit(ctx, out, capture)
	close(capture.done)
	select {
	case <-out.written:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for warning")
	}
	if got := out.String(); got == "" || !bytes.Contains([]byte(got), []byte(capture.artifactPath)) || !bytes.Contains([]byte(got), []byte("dns lookup failed")) {
		t.Fatalf("unexpected warning %q", got)
	}
}

func TestPodLogCaptureExitAfterContextCancellationDoesNotWarn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := &notifyingWriter{written: make(chan struct{}, 1)}
	capture := &podLogCaptureProcess{
		namespace:    "zfs-csi",
		artifactPath: "/tmp/stern.log",
		done:         make(chan struct{}),
	}
	warnOnUnexpectedPodLogCaptureExit(ctx, out, capture)
	cancel()
	close(capture.done)
	select {
	case <-out.written:
		t.Fatal("context cancellation must not warn")
	case <-time.After(10 * time.Millisecond):
	}
	if got := out.String(); got != "" {
		t.Fatalf("context cancellation must not warn, got %q", got)
	}
}
