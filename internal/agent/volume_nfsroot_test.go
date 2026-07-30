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

package agent

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/nfsexport"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

// newRootTestDeps wires test deps that already seed the pool root dataset
// (newTestDeps seeds /tank). It exists as a named entry point for the
// explicit-root test suite so the intent is loud at the call site.
func newRootTestDeps(t *testing.T) *testDeps {
	t.Helper()
	return newTestDeps(t)
}

// greenRootProbe always succeeds: the kernel nfsd filehandle probe returns a
// valid handle. It is the test seam that lets the preflight gate pass.
func greenRootProbe(context.Context, string) error { return nil }

// rootProbeStub returns configurable probe outcomes.
type rootProbeStub struct {
	calls    atomic.Int32
	lastRoot string
	err      error
}

func (s *rootProbeStub) probe(_ context.Context, root string) error {
	s.calls.Add(1)
	s.lastRoot = root
	return s.err
}

// TestNFSReconcileDerivesPoolRootFromBackend verifies the reconciler resolves
// the pool root mountpoint via the ZFS backend (fail-closed).
func TestNFSReconcileDerivesPoolRootFromBackend(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.RootProbe = greenRootProbe

	got, err := r.derivePoolRoot(t.Context(), "tank")
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if got != "/tank" {
		t.Fatalf("root = %q, want /tank", got)
	}

	if _, err := r.derivePoolRoot(t.Context(), "missing"); !errors.Is(err, zfs.ErrNotFound) {
		t.Fatalf("missing pool err = %v, want ErrNotFound", err)
	}
}

func TestNFSReconcileClassifiesPoolRootAvailability(t *testing.T) {
	t.Run("unmounted", func(t *testing.T) {
		d := newRootTestDeps(t)
		d.zfsb.WithMounted("tank", false)
		r := d.reconciler()

		_, err := r.derivePoolRoot(t.Context(), "tank")
		if !isRootPreflightRetryable(err) {
			t.Fatalf("unmounted root = %v, want retryable classification", err)
		}
	})
	t.Run("non-filesystem", func(t *testing.T) {
		d := newRootTestDeps(t)
		if err := d.zfsb.Destroy(t.Context(), "tank"); err != nil {
			t.Fatal(err)
		}
		if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
			t.Fatal(err)
		}
		r := d.reconciler()

		_, err := r.derivePoolRoot(t.Context(), "tank")
		if !isRootPreflightTerminalConfig(err) {
			t.Fatalf("non-filesystem root = %v, want terminal config classification", err)
		}
	})
}

func TestNFSReconcileMountLossThenRecovery(t *testing.T) {
	d := newRootTestDeps(t)
	d.zfsb.WithMounted("tank", false)
	r := d.reconciler()
	r.RootProbe = greenRootProbe
	vol := nfsTestVolume("10.0.0.1/32")

	if err := r.registerNFSExportCtx(t.Context(), vol, "tank/a", "/tank/a"); !isRootPreflightRetryable(err) {
		t.Fatalf("mount loss = %v, want retryable", err)
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/a"); ok {
		t.Fatal("child registered while pool root unmounted")
	}

	d.zfsb.WithMounted("tank", true)
	if err := r.registerNFSExportCtx(t.Context(), vol, "tank/a", "/tank/a"); err != nil {
		t.Fatalf("registration after mount recovery: %v", err)
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/a"); !ok {
		t.Fatal("child missing after mount recovery")
	}
}

func TestNFSReconcileUsesCustomBackendMountpoints(t *testing.T) {
	d := newRootTestDeps(t)
	d.zfsb.WithExportPath("tank", "/srv/tank")
	d.zfsb.WithMounted("tank", true)
	d.zfsb.WithDataset("tank/csi/a", zfs.KindFilesystem, false, zfs.KeyNone)
	d.zfsb.WithExportPath("tank/csi/a", "/srv/tank/volumes/a")
	d.zfsb.WithMounted("tank/csi/a", true)
	r := d.reconciler()
	r.RootProbe = greenRootProbe

	path, err := r.mountedFilesystemPath(t.Context(), "tank/csi/a")
	if err != nil {
		t.Fatal(err)
	}
	if path != "/srv/tank/volumes/a" {
		t.Fatalf("child path = %q", path)
	}
	if err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/csi/a", path); err != nil {
		t.Fatal(err)
	}
	if root, _ := r.NFSExports.Root(); root != "/srv/tank" {
		t.Fatalf("root = %q", root)
	}
}

// TestNFSReconcileRegisterRootBeforeVolume verifies the root entry is installed
// in the MemTable before any volume entry.
func TestNFSReconcileRegisterRootBeforeVolume(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.RootProbe = greenRootProbe
	r.StatfsIdentity = func(path string) (statfsIdentityInfo, error) {
		return testStatfsIdentity(path), nil
	}

	rootEntry, _ := r.NFSExports.Root()
	if rootEntry != "" {
		t.Fatal("root leaked into fresh table")
	}
	if err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/csi/a", "/tank/csi/a"); err != nil {
		t.Fatalf("register: %v", err)
	}
	got, ok := r.NFSExports.Root()
	if !ok || got != "/tank" {
		t.Fatalf("root after register = %q ok=%v, want /tank", got, ok)
	}
	if _, ok := r.NFSExports.LookupPath(nfsexport.FsidTypeNum(), []byte{0, 0, 0, 0}); !ok {
		t.Fatal("fsid=0 root lookup missing after register")
	}
}

func TestNFSRootStructuralProbePrecedesIntentAndChild(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	var events []string
	w := &fakeNFSCacheWriter{events: &events}
	r.NFSWriter = w
	r.RootProbe = func(context.Context, string) error {
		events = append(events, "probe")
		return nil
	}
	r.registerNFSExportHook = func(string, string) { events = append(events, "child") }

	if err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/csi/a", "/tank/csi/a"); err != nil {
		t.Fatal(err)
	}
	if want := []string{"probe", "child", "install-root"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestNFSRootProbeFailureCreatesNoKernelOrUserspaceState(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	r.RootProbe = func(context.Context, string) error { return newRootPreflightRetryable(errors.New("probe")) }

	if err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/csi/a", "/tank/csi/a"); err == nil {
		t.Fatal("register unexpectedly succeeded")
	}
	if len(w.installs) != 0 || w.rootInvalidations != 0 {
		t.Fatalf("install/invalidate = %d/%d, want 0/0", len(w.installs), w.rootInvalidations)
	}
	if _, ok := r.NFSExports.Root(); ok {
		t.Fatal("userspace root survived failed probe")
	}
}

// TestNFSReconcileSecondDifferingPoolRootRejected verifies one root per host.
func TestNFSReconcileSecondDifferingPoolRootRejected(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.RootProbe = greenRootProbe

	if err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/a", "/tank/a"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// Pool "tank2" root is /tank2 — a differing root on the same host.
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank2", Kind: zfs.KindFilesystem, Capacity: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithExportPath("tank2", "/tank2")
	d.zfsb.WithMounted("tank2", true)
	err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank2/b", "/tank2/b")
	if err == nil {
		t.Fatal("second differing root admitted")
	}
}

// TestNFSReconcileDeletePreservesRootWithSurvivors verifies that withdrawing
// one of two volumes keeps the root (and its fsid=0 binding) intact.
func TestNFSReconcileDeletePreservesRootWithSurvivors(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	r.RootProbe = greenRootProbe

	if err := r.registerNFSExport(nfsTestVolume("10.0.0.1/32"), "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}
	if err := r.registerNFSExport(nfsTestVolume("10.0.0.2/32"), "tank/b", "/tank/b"); err != nil {
		t.Fatal(err)
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), nfsTestVolume("10.0.0.1/32"), "tank/a"); err != nil {
		t.Fatal(err)
	}
	if root, ok := r.NFSExports.Root(); !ok || root != "/tank" {
		t.Fatalf("root removed with survivors: root=%q ok=%v", root, ok)
	}
	if _, ok := r.NFSExports.LookupRealExport("*", "/tank/b"); !ok {
		t.Fatal("survivor /tank/b removed")
	}
	if w.rootInvalidations != 0 {
		t.Fatalf("root invalidated with survivors: %d", w.rootInvalidations)
	}
}

func TestNFSDeleteAfterRestartPreservesRootForDurableSurvivor(t *testing.T) {
	d := newRootTestDeps(t)
	deleting := nfsTestVolume("10.0.0.1/32")
	deleting.Name = "deleting"
	deleting.Spec.OwnerNode = "storage-a"
	deleting.Status.State = zfscsiv1.VolumeStateDeleting
	deleting.Status.ExportPath = "/tank/a"
	survivor := nfsTestVolume("10.0.0.2/32")
	survivor.Name = "survivor"
	survivor.Spec.OwnerNode = "storage-a"
	survivor.Status.State = zfscsiv1.VolumeStateReady
	survivor.Status.ExportPath = "/tank/b"
	for _, vol := range []*zfscsiv1.Volume{deleting, survivor} {
		vol.ObjectMeta.CreationTimestamp = metav1.Now()
		if err := d.Create(t.Context(), vol); err != nil {
			t.Fatal(err)
		}
	}
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	r.RootProbe = greenRootProbe
	if err := r.registerNFSExportCtx(t.Context(), deleting, "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}
	// Restart state: no children registered locally; survivor exists only in API.
	r.nfsEntries = nil
	r.nfsPaths = nil
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), deleting, "tank/a"); err != nil {
		t.Fatal(err)
	}
	if root, ok := r.NFSExports.Root(); !ok || root != "/tank" {
		t.Fatalf("root removed despite durable survivor: %q %v", root, ok)
	}
	if w.rootInvalidations != 0 {
		t.Fatalf("root invalidations = %d, want 0", w.rootInvalidations)
	}
}

// TestNFSReconcileDeleteLastRemovesRoot verifies that withdrawing the last
// volume removes the root entry, invalidates fsid=0, and writes a root export
// negative.
func TestNFSReconcileDeleteLastRemovesRoot(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	r.RootProbe = greenRootProbe

	if err := r.registerNFSExport(nfsTestVolume("10.0.0.1/32"), "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), nfsTestVolume("10.0.0.1/32"), "tank/a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.NFSExports.Root(); ok {
		t.Fatal("root remained after last withdrawal")
	}
	if _, ok := r.NFSExports.LookupPath(nfsexport.FsidTypeNum(), []byte{0, 0, 0, 0}); ok {
		t.Fatal("fsid=0 still resolves after last withdrawal")
	}
	if w.rootInvalidations != 1 {
		t.Fatalf("root invalidations = %d, want 1", w.rootInvalidations)
	}
}

func TestNFSRootInvalidationFailureRollsBackAndUnlocks(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{rootInvalidateErr: errors.New("root invalidate")}
	r.NFSWriter = w
	vol := nfsTestVolume("10.0.0.1/32")
	if err := r.registerNFSExport(vol, "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/a"); err == nil {
		t.Fatal("withdraw unexpectedly succeeded")
	}
	if _, ok := r.NFSExports.LookupRealExport("*", "/tank/a"); !ok {
		t.Fatal("volume entry not rolled back")
	}
	if root, ok := r.NFSExports.Root(); !ok || root != "/tank" {
		t.Fatalf("root not rolled back: %q %v", root, ok)
	}
	if !r.nfsMu.TryLock() {
		t.Fatal("nfsMu leaked locked")
	}
	r.nfsMu.Unlock()
}

// TestNFSReconcileRestartReconstructsRootFirst verifies that after a process
// restart (MemTable wiped), reconciling a single Ready volume reconstructs
// the root entry before the volume entry.
func TestNFSReconcileRestartReconstructsRootFirst(t *testing.T) {
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.RootProbe = greenRootProbe
	if err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}
	// Simulate restart: clear in-memory state but keep durable volume state.
	r.NFSExports.Replace(nil)
	r.nfsEntries = nil
	r.nfsPaths = nil
	r.clearRootStateForTest()
	// First re-registration after restart must re-install the root.
	if err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/a", "/tank/a"); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	root, ok := r.NFSExports.Root()
	if !ok || root != "/tank" {
		t.Fatalf("root not reconstructed: %q ok=%v", root, ok)
	}
}

// TestNFSReconcilePreflightProbeGatesRegistration verifies that a failing
// preflight probe blocks volume registration. A retryable probe failure must
// return a retryable error; a terminal EINVAL must be terminal.
func TestNFSReconcilePreflightProbeGatesRegistration(t *testing.T) {
	t.Run("retryable fails registration but requeues", func(t *testing.T) {
		d := newRootTestDeps(t)
		r := d.reconciler()
		stub := &rootProbeStub{err: newRootPreflightRetryable(nil)}
		r.RootProbe = stub.probe

		err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/a", "/tank/a")
		if err == nil {
			t.Fatal("register succeeded with retryable probe failure")
		}
		if !isRootPreflightRetryable(err) {
			t.Fatalf("err = %v, want retryable classification", err)
		}
		if root, ok := r.NFSExports.Root(); ok {
			t.Fatalf("root registered despite retryable probe: %q", root)
		}
	})

	t.Run("EINVAL terminal config fault", func(t *testing.T) {
		d := newRootTestDeps(t)
		r := d.reconciler()
		stub := &rootProbeStub{err: newRootPreflightTerminalConfig(errors.New("EINVAL path exists"))}
		r.RootProbe = stub.probe

		err := r.registerNFSExport(nfsTestVolume("10.0.0.0/8"), "tank/a", "/tank/a")
		if err == nil {
			t.Fatal("register succeeded with EINVAL probe failure")
		}
		if !isRootPreflightTerminalConfig(err) {
			t.Fatalf("err = %v, want terminal config classification", err)
		}
	})
}
