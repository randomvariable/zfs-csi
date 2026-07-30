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
	"sync"
	"testing"
	"time"

	mountutils "k8s.io/mount-utils"
	testingexec "k8s.io/utils/exec/testing"
)

// unmountMounter embeds a FakeMounter but overrides Unmount to either block or
// error, so the F10 lazy-fallback path can be exercised.
type unmountMounter struct {
	*mountutils.FakeMounter
	block      bool
	unmountErr error
}

func (m *unmountMounter) Unmount(target string) error {
	if m.block {
		// Simulate a hung umount(2) on a dead NFS server.
		select {}
	}

	return m.unmountErr
}

func seededOps(t *testing.T, m *unmountMounter) *Ops {
	t.Helper()

	return New("/root", m, &testingexec.FakeExec{DisableScripts: true}).(*Ops)
}

func withLazyStub(t *testing.T) *lazyRecorder {
	t.Helper()
	rec := &lazyRecorder{}
	orig := lazyUnmount
	lazyUnmount = func(target string) error {
		rec.record(target)

		return rec.err
	}
	t.Cleanup(func() { lazyUnmount = orig })

	return rec
}

type lazyRecorder struct {
	mu      sync.Mutex
	targets []string
	err     error
}

func (r *lazyRecorder) record(t string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets = append(r.targets, t)
}

func (r *lazyRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.targets)
}

func mounted(target string) *unmountMounter {
	return &unmountMounter{
		FakeMounter: mountutils.NewFakeMounter([]mountutils.MountPoint{
			{Device: "srv:/export", Path: target, Type: "nfs4"},
		}),
	}
}

// F10: a primary unmount that ERRORS falls back to lazy MNT_DETACH.
func TestUnmount_PrimaryErrorFallsBackToLazy(t *testing.T) {
	rec := withLazyStub(t)
	m := mounted("/root/stage/vol")
	m.unmountErr = errors.New("device or resource busy")
	ops := seededOps(t, m)

	if err := ops.Unmount(context.Background(), "stage/vol"); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	if rec.count() != 1 {
		t.Fatalf("lazy unmount called %d times, want 1", rec.count())
	}
}

// F10: a primary unmount that HANGS falls back to lazy MNT_DETACH within the
// deadline instead of blocking forever.
func TestUnmount_PrimaryHangFallsBackToLazyWithinDeadline(t *testing.T) {
	orig := unmountTimeout
	unmountTimeout = 20 * time.Millisecond
	defer func() { unmountTimeout = orig }()

	rec := withLazyStub(t)
	m := mounted("/root/stage/vol")
	m.block = true
	ops := seededOps(t, m)

	done := make(chan error, 1)
	go func() { done <- ops.Unmount(context.Background(), "stage/vol") }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Unmount: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Unmount wedged on a hung primary unmount; lazy fallback did not fire")
	}
	if rec.count() != 1 {
		t.Fatalf("lazy unmount called %d times, want 1", rec.count())
	}
}

// F10: the normal (successful primary unmount) path never uses lazy detach.
func TestUnmount_NormalPathNoLazy(t *testing.T) {
	rec := withLazyStub(t)
	m := mounted("/root/stage/vol") // unmountErr nil, block false
	ops := seededOps(t, m)

	if err := ops.Unmount(context.Background(), "stage/vol"); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	if rec.count() != 0 {
		t.Fatalf("lazy unmount called %d times on the normal path, want 0", rec.count())
	}
}

// An unmounted target is a no-op and never touches lazy detach.
func TestUnmount_NotMountedIsNoOp(t *testing.T) {
	rec := withLazyStub(t)
	m := &unmountMounter{FakeMounter: mountutils.NewFakeMounter(nil)}
	ops := seededOps(t, m)

	if err := ops.Unmount(context.Background(), "stage/none"); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	if rec.count() != 0 {
		t.Fatalf("lazy unmount called on an unmounted target")
	}
}
