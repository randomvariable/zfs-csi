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
	"hash/fnv"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/crypto"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/nfsexport"
	eventsv1 "github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/transport"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

// testDeps wires a VolumeReconciler against the fake ZFS backend + fake
// transport + a recording key provider, all in-memory.
type testDeps struct {
	crclient.Client
	scheme *runtime.Scheme

	zfsb   *zfsfake.Backend
	export *fakeTransportServer
	keys   *recKeyProvider
	stager crypto.Stager
}

type fakeNFSCacheWriter struct {
	installs            []nfsexport.Entry
	installErr          error
	invalidations       []nfsexport.Entry
	invalidateClients   [][]string
	invalidateSurvivors [][]nfsexport.Entry
	invalidateErr       error
	events              *[]string
	rootInvalidations   int
	rootInvalidateErr   error
}

type fakeNFSExportFlusher struct {
	calls int
	err   error
	errs  []error
}

func (f *fakeNFSExportFlusher) Flush() error {
	f.calls++
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return f.err
}

func (w *fakeNFSCacheWriter) InstallRootPositive(entry nfsexport.Entry) error {
	if w.events != nil {
		*w.events = append(*w.events, "install-root")
	}
	w.installs = append(w.installs, entry)
	return w.installErr
}

type blockingNFSCacheWriter struct {
	fakeNFSCacheWriter
	invalidateStarted chan struct{}
	allowInvalidate   chan struct{}
	startedOnce       sync.Once
}

func (w *blockingNFSCacheWriter) InvalidateEntry(entry nfsexport.Entry, clients []string, surviving []nfsexport.Entry) error {
	w.startedOnce.Do(func() { close(w.invalidateStarted) })
	<-w.allowInvalidate
	return w.fakeNFSCacheWriter.InvalidateEntry(entry, clients, surviving)
}

func (w *fakeNFSCacheWriter) InvalidateEntry(entry nfsexport.Entry, clients []string, surviving []nfsexport.Entry) error {
	if w.events != nil {
		*w.events = append(*w.events, "invalidate")
	}
	w.invalidations = append(w.invalidations, entry)
	w.invalidateClients = append(w.invalidateClients, append([]string(nil), clients...))
	w.invalidateSurvivors = append(w.invalidateSurvivors, append([]nfsexport.Entry(nil), surviving...))
	return w.invalidateErr
}

func (w *fakeNFSCacheWriter) InvalidateRoot(root string) error {
	if w.events != nil {
		*w.events = append(*w.events, "invalidate-root")
	}
	w.rootInvalidations++
	return w.rootInvalidateErr
}

type failGetZFS struct {
	*zfsfake.Backend
	err error
}

func (b *failGetZFS) Get(context.Context, string) (zfs.DatasetInfo, error) {
	return zfs.DatasetInfo{}, b.err
}

func nfsTestVolume(cidrs ...string) *zfscsiv1.Volume {
	return &zfscsiv1.Volume{Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem, NFSExportCIDRs: cidrs,
	}}
}

func nfsTestEntry(path string, cidrs ...string) nfsexport.Entry {
	parsed := make([]netip.Prefix, 0, len(cidrs))
	for _, cidr := range cidrs {
		parsed = append(parsed, netip.MustParsePrefix(cidr))
	}
	return nfsexport.Entry{Path: path, CIDRs: parsed}
}

func TestNFSRegisterOnlyUpdatesAuthoritativeTable(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	vol := nfsTestVolume("10.0.0.0/8")

	if err := r.registerNFSExport(vol, "tank/b", "/tank/b"); err != nil {
		t.Fatal(err)
	}
	if err := r.registerNFSExport(vol, "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/a"); !ok {
		t.Fatal("/tank/a missing from MemTable")
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/b"); !ok {
		t.Fatal("/tank/b missing from MemTable")
	}
}

func TestNFSRootSquashTighteningUpdatesTableAndFlushes(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	flusher := &fakeNFSExportFlusher{}
	r.NFSFlusher = flusher
	vol := nfsTestVolume("10.0.0.0/8")
	falseValue := false
	vol.Spec.NFSRootSquash = &falseValue

	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/a"); err != nil {
		t.Fatal(err)
	}
	trueValue := true
	vol.Spec.NFSRootSquash = &trueValue
	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/a"); err != nil {
		t.Fatal(err)
	}

	entry, ok := r.NFSExports.LookupRealExport("*", "/tank/csi/a")
	if !ok || entry.NoRootSquash {
		t.Fatalf("entry = %#v, want root-squashed authoritative entry", entry)
	}
	if flusher.calls != 1 {
		t.Fatalf("flush calls = %d, want 1", flusher.calls)
	}
}

func TestNFSRootSquashUnchangedDoesNotFlush(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	flusher := &fakeNFSExportFlusher{}
	r.NFSFlusher = flusher
	vol := nfsTestVolume("10.0.0.0/8")
	trueValue := true
	vol.Spec.NFSRootSquash = &trueValue

	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/a"); err != nil {
		t.Fatal(err)
	}
	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/a"); err != nil {
		t.Fatal(err)
	}
	if flusher.calls != 0 {
		t.Fatalf("flush calls = %d, want 0", flusher.calls)
	}
}

func TestNFSRootSquashFlushFailurePropagates(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	flusher := &fakeNFSExportFlusher{err: errors.New("flush failed")}
	r.NFSFlusher = flusher
	vol := nfsTestVolume("10.0.0.0/8")
	falseValue := false
	vol.Spec.NFSRootSquash = &falseValue
	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/a"); err != nil {
		t.Fatal(err)
	}
	trueValue := true
	vol.Spec.NFSRootSquash = &trueValue
	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/a"); !errors.Is(err, flusher.err) {
		t.Fatalf("tightening error = %v, want flush failure", err)
	}
	entry, ok := r.NFSExports.LookupRealExport("*", "/tank/csi/a")
	if !ok || entry.NoRootSquash {
		t.Fatalf("entry = %#v, want fail-closed root-squashed entry", entry)
	}
}

func TestNFSRootSquashPathDriftRetriesFlushAfterFailure(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	flushErr := errors.New("flush failed")
	r.NFSFlusher = &fakeNFSExportFlusher{errs: []error{flushErr, nil}}
	vol := nfsTestVolume("10.0.0.0/8")
	falseValue := false
	vol.Spec.NFSRootSquash = &falseValue
	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/old"); err != nil {
		t.Fatal(err)
	}

	trueValue := true
	vol.Spec.NFSRootSquash = &trueValue
	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/new"); !errors.Is(err, flushErr) {
		t.Fatalf("tightening error = %v, want flush failure", err)
	}
	if got := r.nfsPaths["tank/csi/a"]; got != "/tank/csi/old" {
		t.Fatalf("path bookkeeping after failed flush = %q, want old path", got)
	}
	if entry := r.nfsEntries["/tank/csi/old"]; !entry.NoRootSquash {
		t.Fatalf("old entry bookkeeping after failed flush = %#v, want loose entry", entry)
	}

	if err := r.registerNFSExport(vol, "tank/csi/a", "/tank/csi/new"); err != nil {
		t.Fatal(err)
	}
	flusher := r.NFSFlusher.(*fakeNFSExportFlusher)
	if flusher.calls != 2 {
		t.Fatalf("flush calls = %d, want 2", flusher.calls)
	}
	if got := r.nfsPaths["tank/csi/a"]; got != "/tank/csi/new" {
		t.Fatalf("path bookkeeping after successful retry = %q, want new path", got)
	}
	if _, ok := r.nfsEntries["/tank/csi/old"]; ok {
		t.Fatal("old entry bookkeeping remained after successful retry")
	}
}

func TestNFSRegisterChangedPathEvictsOldEntry(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	r.NFSWriter = &fakeNFSCacheWriter{}
	vol := nfsTestVolume("10.0.0.2/32")
	if err := r.registerNFSExport(vol, "tank/dataset", "/tank/old"); err != nil {
		t.Fatal(err)
	}
	if err := r.registerNFSExport(vol, "tank/dataset", "/tank/new"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/old"); ok {
		t.Fatal("old path remained in MemTable")
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/new"); !ok {
		t.Fatal("new path missing from MemTable")
	}
	if got := len(r.nfsEntries); got != 1 {
		t.Fatalf("tracked entries = %d, want 1", got)
	}
}

func TestNFSWithdrawWriterContracts(t *testing.T) {
	t.Run("last entry withdraws", func(t *testing.T) {
		d := newTestDeps(t)
		r := d.reconciler()
		w := &fakeNFSCacheWriter{}
		r.NFSWriter = w
		vol := nfsTestVolume("10.0.0.1/32")
		if err := r.registerNFSExport(vol, "tank/a", "/tank/a"); err != nil {
			t.Fatal(err)
		}
		if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/a"); err != nil {
			t.Fatal(err)
		}
		if len(w.invalidations) != 1 {
			t.Fatalf("invalidation calls = %d, want 1", len(w.invalidations))
		}
	})

	t.Run("survivor stays demand-populated", func(t *testing.T) {
		d := newTestDeps(t)
		r := d.reconciler()
		w := &fakeNFSCacheWriter{}
		r.NFSWriter = w
		if err := r.registerNFSExport(nfsTestVolume("10.0.0.1/32"), "tank/a", "/tank/a"); err != nil {
			t.Fatal(err)
		}
		vol := nfsTestVolume("10.0.0.2/32")
		if err := r.registerNFSExport(vol, "tank/b", "/tank/b"); err != nil {
			t.Fatal(err)
		}
		if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/b"); err != nil {
			t.Fatal(err)
		}
		if len(w.invalidations) != 1 {
			t.Fatalf("withdraw invalidations = %d, want targeted negative only", len(w.invalidations))
		}
	})
}

func TestNFSWithdrawSerializesRegistrationWithInvalidation(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &blockingNFSCacheWriter{
		invalidateStarted: make(chan struct{}),
		allowInvalidate:   make(chan struct{}),
	}
	var events []string
	w.events = &events
	r.NFSWriter = w
	volA := nfsTestVolume("10.0.0.1/32")
	volB := nfsTestVolume("10.0.0.2/32")
	if err := r.registerNFSExport(volA, "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}

	withdrawDone := make(chan error, 1)
	go func() { withdrawDone <- r.withdrawNFSExport(t.Context(), logr.Discard(), volA, "tank/a") }()
	<-w.invalidateStarted

	registerDone := make(chan error, 1)
	registerEntered := make(chan struct{})
	allowRegister := make(chan struct{})
	r.registerNFSExportHook = func(string, string) {
		close(registerEntered)
		<-allowRegister
	}
	go func() { registerDone <- r.registerNFSExport(volB, "tank/b", "/tank/b") }()
	<-registerEntered
	close(allowRegister)
	if r.nfsMu.TryLock() {
		r.nfsMu.Unlock()
		t.Fatal("nfsMu was available while withdrawal was blocked in InvalidateEntry")
	}
	select {
	case err := <-registerDone:
		t.Fatalf("registration completed while invalidation was blocked: %v", err)
	case <-time.After(time.Second):
	}
	close(w.allowInvalidate)
	if err := <-withdrawDone; err != nil {
		t.Fatal(err)
	}
	if err := <-registerDone; err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(events, "invalidate") {
		t.Fatalf("writer events = %v, want targeted invalidation", events)
	}
}

func TestNFSInvalidationErrorsPropagate(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	if err := r.registerNFSExport(nfsTestVolume("10.0.0.1/32"), "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}
	w.invalidateErr = errors.New("invalidate failed")
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), nfsTestVolume("10.0.0.1/32"), "tank/a"); err == nil || !strings.Contains(err.Error(), "invalidate failed") {
		t.Fatalf("withdraw error = %v, want invalidation failure", err)
	}
}

func TestNFSWithdrawIsIdempotentAfterSuccessfulWithdrawal(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	r.NFSExports.Replace(nil)
	r.nfsEntries = nil
	r.nfsPaths = nil
	r.StatfsIdentity = func(string) (statfsIdentityInfo, error) {
		return statfsIdentityInfo{Low: 1, High: 2, Type: zfsSuperMagic}, nil
	}
	vol := nfsTestVolume("10.0.0.1/32")
	if err := r.registerNFSExport(vol, "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/a"); err != nil {
		t.Fatal(err)
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/a"); err != nil {
		t.Fatalf("retry after successful withdrawal = %v", err)
	}
	if len(w.invalidations) != 1 {
		t.Fatalf("retry repeated invalidation: calls=%d, want 1", len(w.invalidations))
	}
}

func TestNFSWithdrawImportedUsesTrackedBackendPathAndPropagatesGetError(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	r.NFSWriter = &fakeNFSCacheWriter{}
	vol := nfsTestVolume("10.0.0.1/32")
	vol.Spec.Provenance = zfscsiv1.VolumeProvenanceImported
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/imported", Kind: zfs.KindFilesystem, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithExportPath("tank/imported", "/tank/imported")
	if err := r.registerNFSExport(vol, "tank/imported", "/tank/imported"); err != nil {
		t.Fatal(err)
	}
	// Simulate a restart before the in-memory dataset-path index was rebuilt.
	delete(r.nfsPaths, "tank/imported")
	vol.Status.ExportPath = ""
	// The backend path, not the dynamic dataset default, identifies the entry.
	r.NFSExports.Replace([]nfsexport.Entry{nfsTestEntry("/tank/imported", "10.0.0.1/32")})
	r.nfsEntries = map[string]nfsexport.Entry{"/tank/imported": nfsTestEntry("/tank/imported", "10.0.0.1/32")}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/imported"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/imported"); ok {
		t.Fatal("tracked imported path remained")
	}

	r = d.reconciler()
	r.ZFS = &failGetZFS{Backend: d.zfsb, err: errors.New("backend get failed")}
	r.NFSExports.Replace([]nfsexport.Entry{nfsTestEntry("/tank/imported", "10.0.0.1/32")})
	r.nfsEntries = map[string]nfsexport.Entry{"/tank/imported": nfsTestEntry("/tank/imported", "10.0.0.1/32")}
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/imported"); err == nil || !strings.Contains(err.Error(), "backend get failed") {
		t.Fatalf("withdraw error = %v, want backend Get failure", err)
	}
	if len(w.invalidations) != 0 {
		t.Fatalf("backend Get error called writer: invalidations=%d", len(w.invalidations))
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/imported"); !ok {
		t.Fatal("backend Get error removed export")
	}
}

func TestNFSWithdrawReconstructionGetErrorWithEmptyEntries(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	r.nfsEntries = nil
	r.nfsPaths = nil
	vol := nfsTestVolume("10.0.0.1/32")
	r.ZFS = &failGetZFS{Backend: d.zfsb, err: errors.New("reconstruct get failed")}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/missing"); err == nil || !strings.Contains(err.Error(), "reconstruct get failed") {
		t.Fatalf("withdraw error = %v, want reconstruction Get failure", err)
	}
	if len(w.invalidations) != 0 {
		t.Fatalf("writer called after reconstruction Get error: invalidations=%d", len(w.invalidations))
	}
}

func TestNFSWithdrawRestartReconstructsIdentity(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	vol := nfsTestVolume("10.0.0.1/32")
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/restarted", Kind: zfs.KindFilesystem, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithMounted("tank/restarted", true)
	d.zfsb.WithExportPath("tank/restarted", "/tank/restarted")
	r.StatfsIdentity = func(string) (statfsIdentityInfo, error) {
		return statfsIdentityInfo{Low: 1, High: 2, Type: zfsSuperMagic}, nil
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/restarted"); err != nil {
		t.Fatal(err)
	}
	if len(w.invalidations) != 1 || w.invalidations[0].UUID != nfsexport.UUIDFromStatFS(1, 2) {
		t.Fatalf("invalidated entry = %#v", w.invalidations)
	}
	if got := w.invalidateClients[0]; !reflect.DeepEqual(got, []string{"10.0.0.1"}) {
		t.Fatalf("invalidated clients = %v", got)
	}
	if _, ok := r.NFSExports.LookupExport("*", "/tank/restarted"); ok {
		t.Fatal("restart export remained registered")
	}
}

func TestNFSWithdrawRestartUsesDurableSurvivorCIDRs(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	removed := nfsTestVolume("10.1.2.3/32")
	removed.Name = "removed"
	removed.Spec.OwnerNode = "storage-a"
	removed.Status.ExportPath = "/tank/removed"
	survivor := nfsTestVolume("10.1.2.0/24")
	survivor.Name = "survivor"
	survivor.Spec.OwnerNode = "storage-a"
	survivor.Status.ExportPath = "/tank/survivor"
	if err := d.Client.Create(t.Context(), removed); err != nil {
		t.Fatal(err)
	}
	if err := d.Client.Create(t.Context(), survivor); err != nil {
		t.Fatal(err)
	}
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/removed", Kind: zfs.KindFilesystem, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithExportPath("tank/removed", "/tank/removed")
	r.nfsEntries = nil
	r.nfsPaths = nil
	w.invalidateErr = errors.New("partial invalidation")
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), removed, "tank/removed"); err == nil {
		t.Fatal("first withdrawal succeeded despite injected invalidation failure")
	}
	w.invalidateErr = nil
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), removed, "tank/removed"); err != nil {
		t.Fatal(err)
	}
	if len(w.invalidateSurvivors) != 2 || len(w.invalidateSurvivors[1]) != 1 ||
		w.invalidateSurvivors[1][0].Path != "/tank/survivor" ||
		!w.invalidateSurvivors[1][0].CIDRs[0].Contains(netip.MustParseAddr("10.1.2.3")) {
		t.Fatalf("restart survivor entries = %#v", w.invalidateSurvivors)
	}
	if err := r.registerNFSExport(survivor, "tank/survivor", "/tank/survivor"); err != nil {
		t.Fatal(err)
	}
}

func TestNFSWithdrawPresentButUnmountedUsesPathOnly(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/unmounted", Kind: zfs.KindFilesystem, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithExportPath("tank/unmounted", "/tank/unmounted")
	d.zfsb.WithMounted("tank/unmounted", false)
	statfsCalls := 0
	r.StatfsIdentity = func(string) (statfsIdentityInfo, error) {
		statfsCalls++
		return statfsIdentityInfo{Low: 1, High: 2, Type: zfsSuperMagic}, nil
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), nfsTestVolume("10.0.0.1/32"), "tank/unmounted"); err != nil {
		t.Fatal(err)
	}
	if got := w.invalidations[0].UUID; got != ([16]byte{}) {
		t.Fatalf("invalidated UUID = %x, want path-only", got)
	}
	if statfsCalls != 0 {
		t.Fatalf("statfs calls = %d, want none for unmounted dataset", statfsCalls)
	}
}

func TestNFSWithdrawPresentInvalidConfigUsesPathOnly(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/invalid", Kind: zfs.KindFilesystem, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithExportPath("tank/invalid", "/tank/invalid")
	d.zfsb.WithMounted("tank/invalid", true)
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), nfsTestVolume("not-a-cidr"), "tank/invalid"); err != nil {
		t.Fatal(err)
	}
	if got := w.invalidations[0].UUID; got != ([16]byte{}) {
		t.Fatalf("invalidated UUID = %x, want path-only", got)
	}
}

func TestNFSWithdrawParentPathDoesNotUseParentUUID(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/child", Kind: zfs.KindFilesystem, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithExportPath("tank/child", "/tank/parent/child")
	d.zfsb.WithMounted("tank/child", true)
	r.StatfsIdentity = func(string) (statfsIdentityInfo, error) {
		return statfsIdentityInfo{Low: 9, High: 10, Type: zfsSuperMagic}, nil
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), nfsTestVolume("10.0.0.1/32"), "tank/child"); err != nil {
		t.Fatal(err)
	}
	if got := w.invalidations[0].UUID; got != ([16]byte{}) {
		t.Fatalf("invalidated UUID = %x, want path-only for mismatched path", got)
	}
}

func TestNFSWithdrawMountedPathUsesRealUUID(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	w := &fakeNFSCacheWriter{}
	r.NFSWriter = w
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/mounted", Kind: zfs.KindFilesystem, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	d.zfsb.WithMounted("tank/mounted", true)
	d.zfsb.WithExportPath("tank/mounted", "/tank/mounted")
	r.StatfsIdentity = func(string) (statfsIdentityInfo, error) {
		return statfsIdentityInfo{Low: 3, High: 4, Type: zfsSuperMagic}, nil
	}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), nfsTestVolume("10.0.0.1/32"), "tank/mounted"); err != nil {
		t.Fatal(err)
	}
	if got, want := w.invalidations[0].UUID, nfsexport.UUIDFromStatFS(3, 4); got != want {
		t.Fatalf("invalidated UUID = %x, want %x", got, want)
	}
}

func TestExplicitNFSClientsCanonicalizesMappedIPv4(t *testing.T) {
	got := explicitNFSClients([]netip.Prefix{netip.MustParsePrefix("::ffff:10.0.0.1/128")})
	want := []netip.Addr{netip.MustParseAddr("10.0.0.1")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clients = %v, want %v", got, want)
	}
}

func TestNFSWithdrawAbsentInvalidConfigProgresses(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	r.NFSWriter = &fakeNFSCacheWriter{}
	r.ZFS = &failGetZFS{Backend: d.zfsb, err: zfs.ErrNotFound}
	vol := nfsTestVolume("not-a-cidr")
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/missing"); err != nil {
		t.Fatalf("absent dataset invalid config withdrawal = %v", err)
	}
}

func TestNFSWithdrawStatfsFailureAbsentDatasetProgresses(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	r.NFSWriter = &fakeNFSCacheWriter{}
	r.StatfsIdentity = func(string) (statfsIdentityInfo, error) {
		return statfsIdentityInfo{}, errors.New("statfs failed")
	}
	r.ZFS = &failGetZFS{Backend: d.zfsb, err: zfs.ErrNotFound}
	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), nfsTestVolume("10.0.0.1/32"), "tank/missing"); err != nil {
		t.Fatalf("statfs failure on absent dataset = %v", err)
	}
}

type poolIdentityClient struct {
	crclient.Client
	deps       *testDeps
	identities map[string]string
}

func (c poolIdentityClient) Get(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	switch value := obj.(type) {
	case *zfscsiv1.Volume:
		if value.Spec.OwnerNode == "" {
			value.Spec.OwnerNode = "storage-a"
		}
		if value.Spec.PoolGUID == "" {
			value.Spec.PoolGUID = c.identity(ctx, value.Spec.Pool)
		}
	case *zfscsiv1.Snapshot:
		if value.Spec.OwnerNode == "" {
			value.Spec.OwnerNode = "storage-a"
		}
		if value.Spec.PoolGUID == "" {
			value.Spec.PoolGUID = c.identity(ctx, "tank")
		}
	}
	return nil
}

func (c poolIdentityClient) identity(ctx context.Context, pool string) string {
	if guid, ok := c.identities[pool]; ok {
		return guid
	}
	guid, _ := c.deps.zfsb.PoolGUID(ctx, pool)
	return guid
}

func (c poolIdentityClient) Create(ctx context.Context, obj crclient.Object, opts ...crclient.CreateOption) error {
	switch value := obj.(type) {
	case *zfscsiv1.Volume:
		if value.Spec.OwnerNode == "" {
			value.Spec.OwnerNode = "storage-a"
		}
		if value.Spec.PoolGUID == "" {
			value.Spec.PoolGUID = c.identity(ctx, value.Spec.Pool)
		}
	case *zfscsiv1.Snapshot:
		if value.Spec.OwnerNode == "" {
			value.Spec.OwnerNode = "storage-a"
		}
		if value.Spec.PoolGUID == "" {
			value.Spec.PoolGUID = c.identity(ctx, "tank")
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c poolIdentityClient) Update(ctx context.Context, obj crclient.Object, opts ...crclient.UpdateOption) error {
	switch value := obj.(type) {
	case *zfscsiv1.Volume:
		if value.Spec.OwnerNode == "" {
			value.Spec.OwnerNode = "storage-a"
		}
		if value.Spec.PoolGUID == "" {
			value.Spec.PoolGUID = c.identity(ctx, value.Spec.Pool)
		}
	case *zfscsiv1.Snapshot:
		if value.Spec.OwnerNode == "" {
			value.Spec.OwnerNode = "storage-a"
		}
		if value.Spec.PoolGUID == "" {
			value.Spec.PoolGUID = c.identity(ctx, "tank")
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

func newTestDeps(t *testing.T) *testDeps {
	t.Helper()

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	if err := zfscsiv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := nvmetv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	c := ctrlfake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&zfscsiv1.Volume{}, &zfscsiv1.Snapshot{}, &nvmetv1.NVMeExport{}).
		Build()

	zfsb := zfsfake.New().WithPool("tank", 1<<40)
	// Seed the pool root dataset so derivePoolRoot("tank") resolves to /tank
	// (the explicit NFSv4 root) for every reconciler under this test dep.
	if err := zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank", Kind: zfs.KindFilesystem, Capacity: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	zfsb.WithExportPath("tank", "/tank")
	zfsb.WithMounted("tank", true)
	d := &testDeps{
		Client: c,
		scheme: s,
		zfsb:   zfsb,
		export: newFakeTransportServer(),
		keys:   &recKeyProvider{fetch: map[string][]byte{}, del: map[string]bool{}},
		stager: &nopStager{},
	}
	guid, _ := zfsb.PoolGUID(t.Context(), "tank")
	d.Client = poolIdentityClient{Client: c, deps: d, identities: map[string]string{"tank": guid}}
	return d
}

func (d *testDeps) reconciler() *VolumeReconciler {
	return &VolumeReconciler{
		Client: d.Client, Scheme: d.scheme, Log: logr.Discard(),
		ZFS: d.zfsb, Export: d.export, Keys: d.keys, Stager: d.stager,
		Portal: "server7:4420", NFSServer: "server7", NodeName: "storage-a",
		Namespace: "zfs-csi-system", APIReader: d.Client,
		// The in-process nfsd responder is the sole NFS export mechanism, so it
		// is always wired (matching production). Tests assert the registered
		// entries via NFSExports.LookupExport.
		NFSExports: nfsexport.NewMemTable(),
		StatfsIdentity: func(path string) (statfsIdentityInfo, error) {
			return testStatfsIdentity(path), nil
		},
		// RootProbe is green by default so unit tests skip the live kernel
		// filehandle probe. Tests that need to exercise the preflight gate
		// inject their own stub.
		RootProbe: func(context.Context, string) error { return nil },
	}
}

func testStatfsIdentity(path string) statfsIdentityInfo {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	sum := h.Sum64()
	return statfsIdentityInfo{Low: uint32(sum), High: uint32(sum >> 32), Type: zfsSuperMagic}
}

func (d *testDeps) setReconcilerBackend(r *VolumeReconciler, backend zfs.Backend) {
	r.ZFS = backend
	if fakeBackend, ok := unwrapFakeBackend(backend); ok {
		d.useBackend(fakeBackend)
	}
}

func unwrapFakeBackend(backend zfs.Backend) (*zfsfake.Backend, bool) {
	switch value := backend.(type) {
	case *zfsfake.Backend:
		return value, true
	case *recordingZFS:
		return value.Backend, true
	default:
		return nil, false
	}
}

func (d *testDeps) useBackend(backend *zfsfake.Backend) {
	d.zfsb = backend
	base := d.Client
	if wrapped, ok := base.(poolIdentityClient); ok {
		base = wrapped.Client
	}
	identities := map[string]string{}
	if wrapped, ok := d.Client.(poolIdentityClient); ok {
		for pool, guid := range wrapped.identities {
			identities[pool] = guid
		}
	}
	for _, pool := range []string{"tank"} {
		if guid, err := backend.PoolGUID(context.Background(), pool); err == nil {
			identities[pool] = guid
		}
	}
	d.Client = poolIdentityClient{Client: base, deps: d, identities: identities}
}

func TestReconcileExportRejectsMissingPortal(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	r.Portal = ""
	vol := &zfscsiv1.Volume{Spec: zfscsiv1.VolumeSpec{Transport: zfscsiv1.TransportNVMeTCP}}
	_, err := r.reconcileExport(
		t.Context(),
		logr.Discard(),
		vol,
		naming.ParsedVolID{Pool: "tank", Kind: zfs.KindBlock, ID: "volume"},
		"/dev/zvol/tank/volume",
	)
	if err == nil || !strings.Contains(err.Error(), "portal") {
		t.Fatalf("reconcileExport error = %v, want missing portal error", err)
	}
	if len(d.export.exports) != 0 {
		t.Fatal("reconcileExport called transport Export without a portal")
	}
}

// fakeTransportServer implements transport.Server for tests.
type fakeTransportServer struct {
	exports          map[string]bool
	mapped           map[string]map[string]bool // nqn -> initiator -> mapped
	returnedPortal   string
	exportErr        error
	unexportErr      error
	unexported       []transport.TargetRef
	mapErr           error
	forceDisconnects []string // NQNs fenced via ForceDisconnect, in order
}

func newFakeTransportServer() *fakeTransportServer {
	return &fakeTransportServer{exports: map[string]bool{}, mapped: map[string]map[string]bool{}}
}

func (f *fakeTransportServer) Export(_ context.Context, opts transport.ExportOptions) (transport.TargetRef, error) {
	portal := opts.Portal
	if f.returnedPortal != "" {
		portal = f.returnedPortal
	}
	ref := transport.TargetRef{
		Kind:        opts.Kind,
		TargetNQN:   opts.TargetNQN,
		Portal:      portal,
		NamespaceID: 1,
		DeviceGUID:  opts.DeviceGUID,
	}
	if f.exportErr != nil {
		return ref, f.exportErr
	}

	if f.exports[opts.TargetNQN] {
		return ref, transport.ErrAlreadyExported
	}

	f.exports[opts.TargetNQN] = true
	f.mapped[opts.TargetNQN] = map[string]bool{}

	return ref, nil
}

func (f *fakeTransportServer) Unexport(_ context.Context, ref transport.TargetRef) error {
	f.unexported = append(f.unexported, ref)
	if f.unexportErr != nil {
		return f.unexportErr
	}
	delete(f.exports, ref.TargetNQN)
	delete(f.mapped, ref.TargetNQN)

	return nil
}

func TestReconcileDelete_TLSBlockPassesTLSRefToUnexport(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	vol := createReadyBlock(t, d, "tls-delete")
	vol.Spec.NVMeTLSEnabled = true

	_, err := r.reconcileDelete(t.Context(), logr.Discard(), vol,
		naming.ParsedVolID{Pool: "tank", Kind: zfs.KindBlock, ID: "tls-delete"},
		"tank/csi/block/tls-delete")
	if err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}
	if len(d.export.unexported) != 1 {
		t.Fatalf("Unexport calls = %d, want 1", len(d.export.unexported))
	}
	if !d.export.unexported[0].TLS {
		t.Fatal("Unexport TargetRef TLS = false, want true")
	}
}

func (f *fakeTransportServer) MapInitiator(_ context.Context, ref transport.TargetRef, id string) error {
	if f.mapErr != nil {
		return f.mapErr
	}

	if f.mapped[ref.TargetNQN] == nil {
		f.mapped[ref.TargetNQN] = map[string]bool{}
	}

	f.mapped[ref.TargetNQN][id] = true

	return nil
}

func (f *fakeTransportServer) UnmapInitiator(_ context.Context, ref transport.TargetRef, id string) error {
	if m := f.mapped[ref.TargetNQN]; m != nil {
		delete(m, id)
	}

	return nil
}

func (f *fakeTransportServer) MappedInitiators(_ context.Context, ref transport.TargetRef) ([]string, error) {
	if !f.exports[ref.TargetNQN] {
		return nil, transport.ErrNotExported
	}

	var out []string
	for k := range f.mapped[ref.TargetNQN] {
		out = append(out, k)
	}

	return out, nil
}

func (f *fakeTransportServer) ForceDisconnect(_ context.Context, ref transport.TargetRef) error {
	f.forceDisconnects = append(f.forceDisconnects, ref.TargetNQN)

	return nil
}

// recKeyProvider records DEK operations.
type recKeyProvider struct {
	gen        int
	fetch      map[string][]byte
	del        map[string]bool
	fetchCalls int
}

func (r *recKeyProvider) Generate(_ context.Context, _ string) (string, error) {
	r.gen++

	ref := "transit/key-" + itoa(r.gen)
	if r.fetch == nil {
		r.fetch = map[string][]byte{}
	}

	r.fetch[ref] = bytes32(r.gen)

	return ref, nil
}

func (r *recKeyProvider) Fetch(_ context.Context, ref string) ([]byte, error) {
	r.fetchCalls++
	if v, ok := r.fetch[ref]; ok {
		return v, nil
	}

	return nil, crypto.ErrKeyNotFound
}

func (r *recKeyProvider) Delete(_ context.Context, ref string) error {
	if r.del == nil {
		r.del = map[string]bool{}
	}

	r.del[ref] = true
	delete(r.fetch, ref)

	return nil
}

type nopStager struct{}

func (nopStager) Stage(string, []byte) (string, string, error) {
	return "file:///tmp/key", "/tmp/key", nil
}
func (nopStager) Shred(string) error { return nil }

type recordingZFS struct {
	*zfsfake.Backend
	expandCalls        []int64
	createCalls        []string
	createShareNFS     []string
	createOptions      []zfs.CreateOptions
	cloneCalls         []cloneCall
	shareCalls         []shareCall
	shareImportedCalls []shareCall
}

type cloneCall struct {
	src       string
	snap      string
	cloneName string
}

type shareCall struct {
	name     string
	shareNFS string
}

func (r *recordingZFS) Create(ctx context.Context, opts zfs.CreateOptions) error {
	r.createCalls = append(r.createCalls, opts.Name)
	r.createShareNFS = append(r.createShareNFS, opts.ShareNFS)
	r.createOptions = append(r.createOptions, opts)

	return r.Backend.Create(ctx, opts)
}

func (r *recordingZFS) Clone(ctx context.Context, src, snap, clonename string) error {
	r.cloneCalls = append(r.cloneCalls, cloneCall{src: src, snap: snap, cloneName: clonename})

	return r.Backend.Clone(ctx, src, snap, clonename)
}

func (r *recordingZFS) Share(ctx context.Context, name, shareNFS string) error {
	r.shareCalls = append(r.shareCalls, shareCall{name: name, shareNFS: shareNFS})

	return r.Backend.Share(ctx, name, shareNFS)
}

func (r *recordingZFS) ShareImported(ctx context.Context, name, shareNFS string) error {
	r.shareImportedCalls = append(r.shareImportedCalls, shareCall{name: name, shareNFS: shareNFS})

	return r.Backend.ShareImported(ctx, name, shareNFS)
}

func (r *recordingZFS) Expand(ctx context.Context, name string, capacity int64) error {
	r.expandCalls = append(r.expandCalls, capacity)

	return r.Backend.Expand(ctx, name, capacity)
}

// failDestroyZFS wraps the fake backend and forces Destroy to fail, simulating a
// dataset that can never be destroyed (e.g. dependent snapshots/clones) — the
// case that produced the delete-retry storm under conformance.
type failDestroyZFS struct {
	*zfsfake.Backend
	err error
}

func (f *failDestroyZFS) Destroy(_ context.Context, _ string) error { return f.err }

type destroyCountingZFS struct {
	*zfsfake.Backend
	destroyCalls int
}

func (z *destroyCountingZFS) Destroy(ctx context.Context, dataset string) error {
	z.destroyCalls++

	return z.Backend.Destroy(ctx, dataset)
}

type notFoundPatchClient struct {
	crclient.Client

	statusErr     error
	finalizerErr  error
	statusPatches int
	patches       int
	deleteErr     error
}

func (c *notFoundPatchClient) Status() crclient.StatusWriter {
	return notFoundStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

func (c *notFoundPatchClient) Patch(ctx context.Context, obj crclient.Object, patch crclient.Patch, opts ...crclient.PatchOption) error {
	c.patches++
	if c.finalizerErr != nil {
		c.deleteErr = c.deleteCollectedVolume(ctx, obj)

		return c.finalizerErr
	}

	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *notFoundPatchClient) deleteCollectedVolume(ctx context.Context, obj crclient.Object) error {
	// A status write may have advanced resourceVersion before this injected
	// concurrent deletion; fetch the current object instead of updating stale input.
	current := obj.DeepCopyObject().(crclient.Object)
	if err := c.Get(ctx, crclient.ObjectKeyFromObject(obj), current); err != nil {
		return err
	}
	current.SetFinalizers(nil)
	//nolint:forbidigo // The test client must use Update to reproduce deletion after a stale status write.
	if err := c.Update(ctx, current); err != nil {
		return err
	}

	return c.Delete(ctx, current)
}

type notFoundStatusWriter struct {
	crclient.SubResourceWriter
	client *notFoundPatchClient
}

func (w notFoundStatusWriter) Patch(ctx context.Context, obj crclient.Object, patch crclient.Patch, opts ...crclient.SubResourcePatchOption) error {
	w.client.statusPatches++
	if w.client.statusErr != nil {
		w.client.deleteErr = w.client.deleteCollectedVolume(ctx, obj)

		return w.client.statusErr
	}

	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

type failExistsZFS struct {
	*zfsfake.Backend
	err error
}

func (f *failExistsZFS) Exists(_ context.Context, _ string) (bool, error) { return false, f.err }

type failPoolNamesZFS struct {
	*zfsfake.Backend
	err error
}

func (f *failPoolNamesZFS) PoolNames(context.Context) ([]string, error) { return nil, f.err }

// TestReconcileDelete_FailureIsLowPriorityBackoff proves the starvation fix: a
// failing destroy returns an ERROR (rate-limited exponential backoff, not a
// fixed RequeueAfter) AND sets Result.Priority = LowPriority so the priority
// queue services fresh Volume-create events ahead of a doomed-delete storm.
func TestReconcileDelete_FailureIsLowPriorityBackoff(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	backend := &failDestroyZFS{
		Backend: zfsfake.New().WithPool("tank", 1<<40),
		err:     errors.New("dataset has dependent clones"),
	}
	d.useBackend(backend.Backend)
	r.ZFS = backend

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "dz"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
			Capacity: 1 << 30, VolumeID: "csi:tank:filesystem:dz",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "dz"}, vol); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateDeleting
	_ = d.Client.Status().Patch(context.Background(), vol, patch)

	res, err := r.Reconcile(
		context.Background(),
		reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "dz"}},
	)
	if err == nil {
		t.Fatal("expected an error (for rate-limited backoff) on destroy failure, got nil")
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("expected no fixed RequeueAfter (error drives backoff), got %v", res.RequeueAfter)
	}
	if res.Priority == nil || *res.Priority != handler.LowPriority {
		t.Fatalf(
			"expected Result.Priority=LowPriority(%d) so the doomed delete sinks below provisioning, got %v",
			handler.LowPriority,
			res.Priority,
		)
	}
}

func TestReconcileDelete_NotFoundPatchesAreIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name          string
		configure     func(*notFoundPatchClient, error)
		statusPatches int
		patches       int
	}{
		{
			name: "destroyed status",
			configure: func(c *notFoundPatchClient, err error) {
				c.statusErr = err
			},
			statusPatches: 1,
		},
		{
			name: "finalizer",
			configure: func(c *notFoundPatchClient, err error) {
				c.finalizerErr = err
			},
			// Conditions and the remaining lifecycle status fields are patched
			// separately so concurrent condition writers cannot be clobbered.
			statusPatches: 2,
			patches:       1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t)
			backend := &destroyCountingZFS{Backend: d.zfsb}
			r := d.reconciler()
			r.ZFS = backend
			client := &notFoundPatchClient{Client: d.Client}
			tc.configure(client, apierrors.NewNotFound(schema.GroupResource{Resource: "volumes"}, "gone"))
			r.Client = client

			vol := &zfscsiv1.Volume{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "gone",
					Namespace:  "default",
					Finalizers: []string{zfscsiv1.VolumeFinalizer},
				},
				Spec: zfscsiv1.VolumeSpec{
					Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
					Capacity: 1 << 30, VolumeID: "csi:tank:filesystem:gone",
				},
				Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateDeleting},
			}
			if err := d.Create(t.Context(), vol); err != nil {
				t.Fatal(err)
			}

			result, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(vol)})
			if err != nil {
				t.Fatalf("reconcile deletion race: %v", err)
			}
			if result != (reconcile.Result{}) {
				t.Fatalf("result = %#v, want no requeue", result)
			}
			if backend.destroyCalls != 1 {
				t.Fatalf("destroy calls = %d, want 1", backend.destroyCalls)
			}
			if client.statusPatches != tc.statusPatches || client.patches != tc.patches {
				t.Fatalf("status/finalizer patches = %d/%d, want %d/%d", client.statusPatches, client.patches, tc.statusPatches, tc.patches)
			}
			if client.deleteErr != nil {
				t.Fatalf("delete concurrent volume: %v", client.deleteErr)
			}

			result, err = r.Reconcile(t.Context(), reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(vol)})
			if err != nil {
				t.Fatalf("reconcile collected volume: %v", err)
			}
			if result != (reconcile.Result{}) {
				t.Fatalf("collected volume result = %#v, want no requeue", result)
			}
			if backend.destroyCalls != 1 {
				t.Fatalf("destroy calls after collected reconcile = %d, want 1", backend.destroyCalls)
			}
		})
	}
}

func TestReconcileEnsure_TransientFailureUsesRateLimitedBackoff(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	r.ZFS = &failExistsZFS{Backend: d.zfsb, err: errors.New("libzfs temporarily unavailable")}

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "ensure-failure"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:ensure-failure", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, vol); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	if err := d.Status().Patch(context.Background(), vol, patch); err != nil {
		t.Fatal(err)
	}

	result, err := r.Reconcile(
		context.Background(),
		reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}},
	)
	if err == nil {
		t.Fatal("expected transient ensure failure to return an error")
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected rate-limited error backoff, got fixed requeue %v", result.RequeueAfter)
	}
}

func TestReconcileEnsure_PoolImportCheckFailureUsesRateLimitedBackoff(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	r.ZFS = &failPoolNamesZFS{Backend: d.zfsb, err: errors.New("zpool list temporarily unavailable")}

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-check-failure"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:pool-check-failure", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, vol); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	if err := d.Status().Patch(context.Background(), vol, patch); err != nil {
		t.Fatal(err)
	}

	result, err := r.Reconcile(
		context.Background(),
		reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}},
	)
	if err == nil {
		t.Fatal("expected pool import check failure to return an error")
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected rate-limited error backoff, got fixed requeue %v", result.RequeueAfter)
	}
}

func TestReconcileMissingPoolReturnsErrorWithoutMutatingReadyStatus(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	d.useBackend(zfsfake.New())
	r.ZFS = d.zfsb

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "missing-pool"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:missing-pool", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Get(t.Context(), crclient.ObjectKey{Name: vol.Name}, vol); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	vol.Status.Conditions = []metav1.Condition{{
		Type: string(zfscsiv1.VolumeConditionReady), Status: metav1.ConditionTrue,
		Reason: "VolumeReady", Message: "seeded Ready status", LastTransitionTime: metav1.Now(),
	}}
	if err := d.Status().Patch(t.Context(), vol, patch); err != nil {
		t.Fatal(err)
	}
	before := vol.Status

	result, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: crclient.ObjectKey{Name: vol.Name}})
	if err == nil || !errors.Is(err, zfs.ErrPoolNotFound) {
		t.Fatalf("reconcile error=%v, want wrapped ErrPoolNotFound", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("result=%#v, want controller-runtime error backoff", result)
	}
	after := &zfscsiv1.Volume{}
	if err := d.Get(t.Context(), crclient.ObjectKey{Name: vol.Name}, after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after.Status) {
		t.Fatalf("status mutated while pool identity was unverifiable: before=%#v after=%#v", before, after.Status)
	}
}

// helpers.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}

	return string(b)
}

func bytes32(n int) []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(n)
	}

	return b
}

// --- tests ---

// TestReconcileCreate_BlockUnencrypted proves a Volume CR → zvol + export + Ready.
func TestReconcileCreate_BlockUnencrypted(t *testing.T) {
	d := newTestDeps(t)
	zfsb := &recordingZFS{Backend: d.zfsb}
	r := d.reconciler()
	d.setReconcilerBackend(r, zfsb)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v1"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v1", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v1"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := &zfscsiv1.Volume{}

	_ = d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "v1"}, got)
	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("state not Ready: %s", got.Status.State)
	}

	exists, _ := d.zfsb.Exists(context.Background(), "tank/csi/block/v1")
	if !exists {
		t.Fatal("zvol not created")
	}

	if !d.export.exports[d.exportRefNQN("tank", "block", "v1")] {
		t.Fatal("export not created")
	}
	if len(zfsb.createOptions) != 1 {
		t.Fatalf("create calls = %d, want 1", len(zfsb.createOptions))
	}
	if got := zfsb.createOptions[0]; got.Atime != "" || got.XAttr != "" {
		t.Fatalf("block create properties = atime=%q xattr=%q, want omitted", got.Atime, got.XAttr)
	}
}

func TestReconcileExportOwnerQualifiedIdentityDistinctAndDeterministic(t *testing.T) {
	identities := make([]transport.TargetRef, 0, 2)
	for _, tc := range []struct{ owner, poolGUID string }{{"storage-a", "1"}, {"storage-b", "2"}} {
		d := newTestDeps(t)
		d.zfsb.ReplacePool("tank", 1<<40, tc.poolGUID, "ONLINE")
		if wrapped, ok := d.Client.(poolIdentityClient); ok {
			wrapped.identities["tank"] = tc.poolGUID
		}
		r := d.reconciler()
		r.NodeName = tc.owner
		vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "same-volume"}, Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: tc.poolGUID, OwnerNode: tc.owner, Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:same-volume", Transport: zfscsiv1.TransportNVMeTCP,
		}}
		if err := d.Create(t.Context(), vol); err != nil {
			t.Fatal(err)
		}
		reconcileVol(t, r, vol.Name)
		got := getVol(t, d, vol.Name)
		identities = append(identities, transport.TargetRef{TargetNQN: got.Status.TargetNQN, DeviceGUID: got.Status.DeviceGUID})
		reconcileVol(t, r, vol.Name)
		retry := getVol(t, d, vol.Name)
		if retry.Status.TargetNQN != got.Status.TargetNQN || retry.Status.DeviceGUID != got.Status.DeviceGUID {
			t.Fatalf("retry changed transport identity: first=%#v retry=%#v", got.Status, retry.Status)
		}
	}
	if identities[0].TargetNQN == identities[1].TargetNQN || identities[0].DeviceGUID == identities[1].DeviceGUID {
		t.Fatalf("same backend coordinates collided across owners: %#v", identities)
	}
}

func TestReconcileCreate_ExportFailureMarksReadyFalseWithObservedGeneration(t *testing.T) {
	d := newTestDeps(t)
	d.export.exportErr = errors.New("boom")
	r := d.reconciler()

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v1-fail"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v1-fail", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	req := reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v1-fail"}}
	result, err := r.Reconcile(context.Background(), req)
	if err == nil {
		t.Fatal("expected export failure to return an error for rate-limited backoff")
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no fixed requeue after export failure, got %v", result.RequeueAfter)
	}

	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ObservedGeneration != got.Generation {
		t.Fatalf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
	}

	cond := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionReady))
	if cond == nil {
		t.Fatal("Ready condition missing")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != "ExportFailed" {
		t.Fatalf("Ready condition = %s/%s, want False/ExportFailed", cond.Status, cond.Reason)
	}
	if cond.ObservedGeneration != got.Generation {
		t.Fatalf("Ready observedGeneration = %d, want %d", cond.ObservedGeneration, got.Generation)
	}
}

func TestReconcileCreate_DeviceNotReadyRequeuesWithoutMarkingError(t *testing.T) {
	d := newTestDeps(t)
	d.export.exportErr = transport.ErrDeviceNotReady
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v1-device-not-ready"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v1-device-not-ready", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatalf("create volume: %v", err)
	}

	result, err := d.reconciler().Reconcile(context.Background(), reconcile.Request{
		NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, time.Second)
	}

	stored := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, stored); err != nil {
		t.Fatalf("get volume: %v", err)
	}
	if state := stored.Status.CurrentState(); state == zfscsiv1.VolumeStateError {
		t.Fatalf("device discovery wait marked volume Error")
	}
}

// TestReconcileCreate_EncryptedSkipsDEKFetchWhenDatasetExists proves the
// requeue-storm fix: on an ErrDeviceNotReady requeue the dataset already exists
// (create succeeded, export is waiting on udev), so reconcileCreate must NOT
// re-fetch the DEK from OpenBao — otherwise every pending encrypted volume
// hammers the key provider every ~1s under a 128-volume burst.
func TestReconcileCreate_EncryptedSkipsDEKFetchWhenDatasetExists(t *testing.T) {
	d := newTestDeps(t)
	d.keys.fetch["transit/ek-exists"] = bytes32(11)
	d.export.exportErr = transport.ErrDeviceNotReady // force the requeue path

	dataset := "tank/csi/block/v-enc-exists"
	// Pre-create the encrypted dataset so Exists() reports true, mirroring a
	// volume whose create already succeeded and is now bouncing on the udev wait.
	if err := d.zfsb.Create(context.Background(), zfs.CreateOptions{
		Name: dataset, Kind: zfs.KindBlock, Capacity: 1 << 30,
		Encrypted: true, KeyFormat: zfs.KeyFormatRaw, KeyLocation: "file:///tmp/preloaded",
	}); err != nil {
		t.Fatalf("pre-create dataset: %v", err)
	}

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v-enc-exists"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v-enc-exists", Transport: zfscsiv1.TransportNVMeTCP,
			EncryptionKeyRef: "transit/ek-exists",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	result, err := d.reconciler().Reconcile(context.Background(), reconcile.Request{
		NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %s, want %s (device-not-ready requeue)", result.RequeueAfter, time.Second)
	}
	if d.keys.fetchCalls != 0 {
		t.Fatalf("DEK Fetch called %d times on an existing-dataset requeue, want 0", d.keys.fetchCalls)
	}
}

// TestReconcileCreate_EncryptedBlockReloadsKeyWhenUnloaded proves the reboot-
// while-pending self-heal: an encrypted block volume whose dataset exists but
// whose key was unloaded out of band (node reboot/crash before the CR reached
// Ready) must have its key RELOADED during reconcileCreate, before export.
// Without this, waitForDevice never succeeds (no /dev node without the key) and
// the volume requeues at 1s forever without ever surfacing Error.
func TestReconcileCreate_EncryptedBlockReloadsKeyWhenUnloaded(t *testing.T) {
	d := newTestDeps(t)
	d.keys.fetch["transit/ek-reload"] = bytes32(13)

	dataset := "tank/csi/block/v-enc-reload"
	if err := d.zfsb.Create(context.Background(), zfs.CreateOptions{
		Name: dataset, Kind: zfs.KindBlock, Capacity: 1 << 30,
		Encrypted: true, KeyFormat: zfs.KeyFormatRaw, KeyLocation: "file:///tmp/preloaded",
	}); err != nil {
		t.Fatalf("pre-create dataset: %v", err)
	}
	// Simulate the reboot: key unloaded, dataset persists.
	if err := d.zfsb.UnloadKey(context.Background(), dataset); err != nil {
		t.Fatalf("unload key: %v", err)
	}

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v-enc-reload"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v-enc-reload", Transport: zfscsiv1.TransportNVMeTCP,
			EncryptionKeyRef: "transit/ek-reload",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	if _, err := d.reconciler().Reconcile(context.Background(), reconcile.Request{
		NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name, Namespace: vol.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Key must have been reloaded (fetched at least once) and now be available.
	if d.keys.fetchCalls == 0 {
		t.Fatal("key was not reloaded on an unloaded-key existing dataset (silent-loop bug)")
	}
	if ks, _ := d.zfsb.KeyStatus(context.Background(), dataset); ks != zfs.KeyAvailable {
		t.Fatalf("key status after reconcile = %s, want available", ks)
	}
}

// TestReconcileCreate_Encrypted loads + verifies key.
func TestReconcileCreate_Encrypted(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	// seed a DEK
	d.keys.fetch["transit/ek-v2"] = bytes32(7)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v2"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v2", Transport: zfscsiv1.TransportNVMeTCP,
			EncryptionKeyRef: "transit/ek-v2",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v2"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := &zfscsiv1.Volume{}

	_ = d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "v2"}, got)
	if got.Status.KeyStatus != zfscsiv1.KeyStatusAvailable {
		t.Fatalf("keystatus not Available: %s", got.Status.KeyStatus)
	}

	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("state not Ready: %s", got.Status.State)
	}
}

// TestReconcileDelete_CryptoShreds proves delete destroys dataset + crypto-shreds DEK.
func TestReconcileDelete_CryptoShreds(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	d.keys.fetch["transit/ek-v3"] = bytes32(9)
	// create first
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v3"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v3", Transport: zfscsiv1.TransportNVMeTCP,
			EncryptionKeyRef: "transit/ek-v3",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v3"}}); err != nil {
		t.Fatal(err)
	}
	// now delete
	if err := d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "v3"}, vol); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateDeleting
	_ = d.Client.Status().Patch(context.Background(), vol, patch)

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v3"}}); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}

	exists, _ := d.zfsb.Exists(context.Background(), "tank/csi/block/v3")
	if exists {
		t.Fatal("dataset should be destroyed")
	}

	if !d.keys.del["transit/ek-v3"] {
		t.Fatal("DEK not crypto-shredded")
	}
}

func TestReconcileDeleteWaitsForSnapshotDeletion(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	poolGUID, err := d.zfsb.PoolGUID(t.Context(), "tank")
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshot-source"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: poolGUID, OwnerNode: "storage-a", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:snapshot-source", Transport: zfscsiv1.TransportNVMeTCP,
		},
		Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateDeleting},
	}
	snap := &zfscsiv1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshot"},
		Spec: zfscsiv1.SnapshotSpec{
			VolumeRef: vol.Name, SourceVolumeID: vol.Spec.VolumeID, OwnerNode: "storage-a", PoolGUID: poolGUID,
		},
	}
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/csi/block/snapshot-source", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	if err := d.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Create(t.Context(), snap); err != nil {
		t.Fatal(err)
	}

	result, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(vol)})
	if err != nil {
		t.Fatalf("reconcile while Snapshot exists: %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Fatalf("requeue while Snapshot exists = %s, want 10s", result.RequeueAfter)
	}
	exists, err := d.zfsb.Exists(t.Context(), "tank/csi/block/snapshot-source")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("deleted Volume destroyed while Snapshot still exists")
	}

	if err := d.Delete(t.Context(), snap); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(vol)}); err != nil {
		t.Fatalf("reconcile after Snapshot delete: %v", err)
	}
	exists, err = d.zfsb.Exists(t.Context(), "tank/csi/block/snapshot-source")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("deleted Volume remains after Snapshot deletion")
	}
}

func TestReconcileDeleteRetainedVolumeAllowsSnapshot(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()
	poolGUID, err := d.zfsb.PoolGUID(t.Context(), "tank")
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "retained-snapshot-source", Finalizers: []string{zfscsiv1.VolumeFinalizer}},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: poolGUID, OwnerNode: "storage-a", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:retained-snapshot-source", Transport: zfscsiv1.TransportNVMeTCP,
			DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain,
		},
		Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateDeleting},
	}
	snap := &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "retained-snapshot"}, Spec: zfscsiv1.SnapshotSpec{VolumeRef: vol.Name, SourceVolumeID: vol.Spec.VolumeID}}
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{Name: "tank/csi/block/retained-snapshot-source", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	if err := d.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Create(t.Context(), snap); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(vol)}); err != nil {
		t.Fatal(err)
	}
	exists, err := d.zfsb.Exists(t.Context(), "tank/csi/block/retained-snapshot-source")
	if err != nil || !exists {
		t.Fatalf("retained dataset exists = %t, %v; want retained", exists, err)
	}
}

func TestReconcileDelete_UnexportFailurePreservesRetainedBackends(t *testing.T) {
	for _, tc := range []struct {
		name        string
		provenance  zfscsiv1.VolumeProvenance
		policy      zfscsiv1.VolumeDeletionPolicy
		dataset     string
		backendPath string
	}{
		{
			name:    "retain",
			policy:  zfscsiv1.VolumeDeletionPolicyRetain,
			dataset: "tank/csi/block/retain-unexport-failure",
		},
		{
			name:        "imported",
			provenance:  zfscsiv1.VolumeProvenanceImported,
			dataset:     "tank/imported-unexport-failure",
			backendPath: "tank/imported-unexport-failure",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t)
			if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{
				Name: tc.dataset, Kind: zfs.KindBlock, Capacity: 1 << 30,
			}); err != nil {
				t.Fatal(err)
			}
			vol := &zfscsiv1.Volume{
				ObjectMeta: metav1.ObjectMeta{
					Name:       tc.name + "-unexport-failure",
					Finalizers: []string{zfscsiv1.VolumeFinalizer},
				},
				Spec: zfscsiv1.VolumeSpec{
					Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
					VolumeID:       "csi:tank:block:" + tc.name + "-unexport-failure",
					Transport:      zfscsiv1.TransportNVMeTCP,
					Provenance:     tc.provenance,
					DeletionPolicy: tc.policy,
					BackendPath:    tc.backendPath,
				},
				Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateDeleting},
			}
			if err := d.Create(t.Context(), vol); err != nil {
				t.Fatal(err)
			}
			d.export.unexportErr = errors.New("target teardown failed")

			_, err := d.reconciler().Reconcile(t.Context(), reconcile.Request{
				NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name},
			})
			if err == nil {
				t.Fatal("delete Reconcile() succeeded after target teardown failed")
			}
			if exists, err := d.zfsb.Exists(t.Context(), tc.dataset); err != nil || !exists {
				t.Fatalf("dataset exists after failed unexport = %t, %v; want retained", exists, err)
			}
			current := &zfscsiv1.Volume{}
			if err := d.Get(t.Context(), apimachinerytypes.NamespacedName{Name: vol.Name}, current); err != nil {
				t.Fatalf("get volume: %v", err)
			}
			if !hasFinalizer(current.Finalizers, zfscsiv1.VolumeFinalizer) {
				t.Fatal("failed unexport removed volume finalizer")
			}
			if current.Status.State != zfscsiv1.VolumeStateDeleting {
				t.Fatalf("state after failed unexport = %s, want Deleting", current.Status.State)
			}
		})
	}
}

// assertResponderServes verifies the in-process nfsd responder table serves an
// export for dataset with the given CIDRs, access mode, and TLS flag. It checks
// the full entry via LookupExport (the real nfsd.export-channel lookup path).
func assertResponderServes(t *testing.T, r *VolumeReconciler, dataset string, cidrs []string, mode nfsexport.AccessMode, tls bool) {
	t.Helper()
	wantDomain := (nfsexport.Entry{}).DomainName()
	exportPath := "/" + dataset
	entry, ok := r.NFSExports.LookupExport(wantDomain, exportPath)
	if !ok {
		t.Fatalf("responder table has no export for domain %s path %s", wantDomain, exportPath)
	}
	wantCIDRs := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		wantCIDRs = append(wantCIDRs, netip.MustParsePrefix(c))
	}
	if !reflect.DeepEqual(entry.CIDRs, wantCIDRs) {
		t.Fatalf("responder export CIDRs = %v, want %v", entry.CIDRs, wantCIDRs)
	}
	if entry.AccessMode != mode {
		t.Fatalf("responder export access mode = %q, want %q", entry.AccessMode, mode)
	}
	if entry.TLS != tls {
		t.Fatalf("responder export TLS = %v, want %v", entry.TLS, tls)
	}
	identity := testStatfsIdentity(exportPath)
	wantUUID := nfsexport.UUIDFromStatFS(identity.Low, identity.High)
	if entry.UUID != wantUUID {
		t.Fatalf("responder export UUID = %x, want %x", entry.UUID, wantUUID)
	}
}

// TestReconcileCreate_FilesystemNFS proves a filesystem volume skips the export.
func TestReconcileCreate_FilesystemNFS(t *testing.T) {
	d := newTestDeps(t)
	zfsb := &recordingZFS{Backend: zfsfake.New().WithPool("tank", 1<<40)}
	zfsb.WithDataset("tank", zfs.KindFilesystem, false, zfs.KeyNone)
	zfsb.WithExportPath("tank", "/tank")
	zfsb.WithMounted("tank", true)
	r := d.reconciler()
	d.setReconcilerBackend(r, zfsb)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "f1"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
			Capacity: 1 << 30, VolumeID: "csi:tank:filesystem:f1",
			NFSExportCIDRs: []string{"10.42.0.0/16"},
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "f1"}}); err != nil {
		t.Fatal(err)
	}

	got := &zfscsiv1.Volume{}

	_ = d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "f1"}, got)
	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("state not Ready: %s", got.Status.State)
	}

	if len(d.export.exports) != 0 {
		t.Fatal("filesystem should not export a block target")
	}

	// The responder is the sole NFS export mechanism: the dataset mounts with
	// sharenfs=off (no libshare export) and the export is registered in the
	// in-process nfsd responder table with the volume's CIDRs.
	if len(zfsb.createShareNFS) != 1 || zfsb.createShareNFS[0] != "off" {
		t.Fatalf("sharenfs = %v, want off (responder serves the export)", zfsb.createShareNFS)
	}
	assertResponderServes(t, r, zfsb.createCalls[0], []string{"10.42.0.0/16"}, nfsexport.AccessRW, false)
	for prop, want := range map[string]string{"atime": "off", "xattr": "sa"} {
		got, err := zfsb.GetProperty(context.Background(), zfsb.createCalls[0], prop)
		if err != nil || got != want {
			t.Fatalf("filesystem %s = %q, %v; want %q", prop, got, err, want)
		}
	}
}

func TestReconcileCreate_FilesystemNFSUsesSpecACL(t *testing.T) {
	d := newTestDeps(t)
	zfsb := &recordingZFS{Backend: zfsfake.New().WithPool("tank", 1<<40)}
	zfsb.WithDataset("tank", zfs.KindFilesystem, false, zfs.KeyNone)
	zfsb.WithExportPath("tank", "/tank")
	zfsb.WithMounted("tank", true)
	r := d.reconciler()
	d.setReconcilerBackend(r, zfsb)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "f2"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
			Capacity: 1 << 30, VolumeID: "csi:tank:filesystem:f2",
			NFSExportCIDRs: []string{"10.42.0.0/16"}, NFSExportAccessMode: "ro",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "f2"}}); err != nil {
		t.Fatal(err)
	}

	// sharenfs=off; the responder carries the spec's read-only access mode.
	if len(zfsb.createShareNFS) != 1 || zfsb.createShareNFS[0] != "off" {
		t.Fatalf("sharenfs = %v, want off (responder serves the export)", zfsb.createShareNFS)
	}
	assertResponderServes(t, r, zfsb.createCalls[0], []string{"10.42.0.0/16"}, nfsexport.AccessRO, false)
}

func TestReconcileCreate_FilesystemRejectsUnsafeNFSIntentBeforeShareRender(t *testing.T) {
	d := newTestDeps(t)
	zfsb := &recordingZFS{Backend: zfsfake.New().WithPool("tank", 1<<40)}
	r := d.reconciler()
	d.setReconcilerBackend(r, zfsb)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "f3"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
			Capacity: 1 << 30, VolumeID: "csi:tank:filesystem:f3",
			NFSExportCIDRs: []string{"10.42.0.7/24"}, NFSExportAccessMode: "rw",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "f3"}}); err != nil {
		t.Fatal(err)
	}

	if len(zfsb.createCalls) != 0 || len(zfsb.createShareNFS) != 0 {
		t.Fatalf(
			"zfs create calls = %v with sharenfs %v, want no render before rejection",
			zfsb.createCalls,
			zfsb.createShareNFS,
		)
	}

	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "f3"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.State != zfscsiv1.VolumeStateError {
		t.Fatalf("state = %q, want Error", got.Status.State)
	}
}

func TestReconcileCreate_SnapshotCloneUsesZFSCloneAndGrowsTarget(t *testing.T) {
	d := newTestDeps(t)
	zfsb := &recordingZFS{Backend: zfsfake.New().WithPool("tank", 1<<40)}
	r := d.reconciler()
	d.setReconcilerBackend(r, zfsb)

	if err := zfsb.Create(context.Background(), zfs.CreateOptions{Name: "tank/csi/block/source", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	if err := zfsb.Snapshot(context.Background(), "tank/csi/block/source", "snap-a"); err != nil {
		t.Fatal(err)
	}
	zfsb.createCalls = nil

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "restore"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 2 << 30, VolumeID: "csi:tank:block:restore", Transport: zfscsiv1.TransportNVMeTCP,
			SourceSnapshotID: "csi:tank:block:source@snap-a",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	req := reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "restore"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile snapshot clone: %v", err)
	}

	if len(zfsb.createCalls) != 0 {
		t.Fatalf("Create calls = %v, want none for snapshot clone", zfsb.createCalls)
	}
	wantClone := cloneCall{src: "tank/csi/block/source", snap: "snap-a", cloneName: "tank/csi/block/restore"}
	if len(zfsb.cloneCalls) != 1 || zfsb.cloneCalls[0] != wantClone {
		t.Fatalf("Clone calls = %+v, want [%+v]", zfsb.cloneCalls, wantClone)
	}
	if len(zfsb.expandCalls) != 1 || zfsb.expandCalls[0] != 2<<30 {
		t.Fatalf("Expand calls = %v, want [2147483648]", zfsb.expandCalls)
	}

	info, err := zfsb.Get(context.Background(), "tank/csi/block/restore")
	if err != nil {
		t.Fatalf("get clone dataset: %v", err)
	}
	if info.Capacity != 2<<30 {
		t.Fatalf("clone capacity = %d, want %d", info.Capacity, int64(2<<30))
	}

	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.State != zfscsiv1.VolumeStateReady || got.Status.ActualCapacity != 2<<30 {
		t.Fatalf(
			"status = state %s capacity %d, want Ready/%d",
			got.Status.State,
			got.Status.ActualCapacity,
			int64(2<<30),
		)
	}
}

func TestReconcileCreate_VolumeCloneSnapshotsSourceThenClonesAndGrowsTarget(t *testing.T) {
	d := newTestDeps(t)
	zfsb := &recordingZFS{Backend: zfsfake.New().WithPool("tank", 1<<40)}
	r := d.reconciler()
	d.setReconcilerBackend(r, zfsb)

	if err := zfsb.Create(context.Background(), zfs.CreateOptions{Name: "tank/csi/block/source", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	zfsb.createCalls = nil

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "clone"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 2 << 30, VolumeID: "csi:tank:block:clone", Transport: zfscsiv1.TransportNVMeTCP,
			SourceVolumeID: "csi:tank:block:source",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	req := reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "clone"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile volume clone: %v", err)
	}

	if len(zfsb.createCalls) != 0 {
		t.Fatalf("Create calls = %v, want none for volume clone", zfsb.createCalls)
	}
	wantClone := cloneCall{src: "tank/csi/block/source", snap: "clone-clone", cloneName: "tank/csi/block/clone"}
	if len(zfsb.cloneCalls) != 1 || zfsb.cloneCalls[0] != wantClone {
		t.Fatalf("Clone calls = %+v, want [%+v]", zfsb.cloneCalls, wantClone)
	}
	if len(zfsb.expandCalls) != 1 || zfsb.expandCalls[0] != 2<<30 {
		t.Fatalf("Expand calls = %v, want [2147483648]", zfsb.expandCalls)
	}

	info, err := zfsb.Get(context.Background(), "tank/csi/block/clone")
	if err != nil {
		t.Fatalf("get clone dataset: %v", err)
	}
	if info.Capacity != 2<<30 {
		t.Fatalf("clone capacity = %d, want %d", info.Capacity, int64(2<<30))
	}
	// Block clones must NOT be shared (share is NFS/filesystem only).
	if len(zfsb.shareCalls) != 0 {
		t.Fatalf("Share calls = %+v, want none for a block clone", zfsb.shareCalls)
	}
}

// TestReconcileCreate_FilesystemVolumeCloneSharesTheClone proves the NFS clone
// path mounts+shares the clone. zfs_clone only reparents COW data, so without an
// explicit Share the clone is created but never exported and the consumer mount
// fails "No such file or directory" (seen live in AWS conformance parallel-clone
// specs). The clone path must share exactly as the create path does.
func TestReconcileCreate_FilesystemVolumeCloneSharesTheClone(t *testing.T) {
	d := newTestDeps(t)
	zfsb := &recordingZFS{Backend: zfsfake.New().WithPool("tank", 1<<40)}
	zfsb.WithDataset("tank", zfs.KindFilesystem, false, zfs.KeyNone)
	zfsb.WithExportPath("tank", "/tank")
	zfsb.WithMounted("tank", true)
	r := d.reconciler()
	d.setReconcilerBackend(r, zfsb)

	// Source dataset path uses the "fs" path component (CSIPathFilesystem), while
	// the VolumeID kind segment is "filesystem" (the API kind string).
	if err := zfsb.Create(context.Background(), zfs.CreateOptions{Name: "tank/csi/fs/source", Kind: zfs.KindFilesystem, Capacity: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	zfsb.createCalls = nil

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "fsclone"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
			Capacity: 2 << 30, VolumeID: "csi:tank:filesystem:fsclone",
			SourceVolumeID:      "csi:tank:filesystem:source",
			NFSExportCIDRs:      []string{"10.0.0.0/16"},
			NFSExportAccessMode: "rw",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	req := reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "fsclone"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile filesystem clone: %v", err)
	}

	// The clone must have been mounted with sharenfs=off (the responder serves
	// the export; libshare exports nothing).
	wantShare := shareCall{name: "tank/csi/fs/fsclone", shareNFS: "off"}
	if len(zfsb.shareCalls) != 1 || zfsb.shareCalls[0] != wantShare {
		t.Fatalf("Share calls = %+v, want [%+v] (NFS clone must be mounted with sharenfs=off)", zfsb.shareCalls, wantShare)
	}
	// And the clone dataset must carry sharenfs=off.
	got, err := zfsb.GetProperty(context.Background(), "tank/csi/fs/fsclone", "sharenfs")
	if err != nil {
		t.Fatalf("get clone sharenfs: %v", err)
	}
	if got != "off" {
		t.Fatalf("clone sharenfs = %q, want off (responder serves the export)", got)
	}
	// The clone export is registered in the responder table like any create.
	assertResponderServes(t, r, "tank/csi/fs/fsclone", []string{"10.0.0.0/16"}, nfsexport.AccessRW, false)
}

// TestReconcileEnsure_AppliesCompressionDrift proves the level-triggered ensure
// path applies a changed Spec.Compression (from ControllerModifyVolume /
// VolumeAttributesClass) to the live dataset via zfs set.
func TestReconcileEnsure_AppliesCompressionDrift(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "vc"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:vc", Transport: zfscsiv1.TransportNVMeTCP,
			Compression: "off",
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "vc"}}
	// First reconcile: create + reach Ready (compression=off applied at create).
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	// Simulate ControllerModifyVolume: change the compression spec.
	if err := d.Get(context.Background(), req.NamespacedName, vol); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Spec.Compression = "zstd-3"
	if err := d.Patch(context.Background(), vol, patch); err != nil {
		t.Fatal(err)
	}

	// Ensure pass must apply the drift via zfs set.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("ensure reconcile: %v", err)
	}

	got, err := d.zfsb.GetProperty(context.Background(), "tank/csi/block/vc", "compression")
	if err != nil {
		t.Fatalf("get compression: %v", err)
	}
	if got != "zstd-3" {
		t.Fatalf("live compression = %q, want zstd-3 (drift not applied)", got)
	}

	// A second pass must be a no-op: retries after the controller's successful
	// ModifyVolume response must not perturb status or the live property.
	beforeStatus := vol.Status
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("idempotent ensure reconcile: %v", err)
	}
	if err := d.Get(context.Background(), req.NamespacedName, vol); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vol.Status, beforeStatus) {
		t.Fatalf("idempotent compression reconcile changed status: got=%+v want=%+v", vol.Status, beforeStatus)
	}
}

// TestReconcileIdempotent_NoDoubleCreate proves re-reconciling a Ready volume
// does not error or duplicate.
func TestReconcileIdempotent_NoDoubleCreate(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v4"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v4", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v4"}}); err != nil {
		t.Fatal(err)
	}
	// second reconcile (Ready → reconcileEnsure path)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v4"}}); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	got := &zfscsiv1.Volume{}

	_ = d.Get(context.Background(), apimachinerytypes.NamespacedName{Name: "v4"}, got)
	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("state drifted: %s", got.Status.State)
	}
}

func TestReconcileEnsure_ReexportsAndRemapsBlockTargetAfterReboot(t *testing.T) {
	d := newTestDeps(t)
	r := d.reconciler()

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v5"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v5", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	req := reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v5"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}

	patch := crclient.MergeFrom(got.DeepCopy())
	got.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{NodeName: "worker1", InitiatorID: "nqn.worker1"}}
	if err := d.Status().Patch(context.Background(), got, patch); err != nil {
		t.Fatal(err)
	}

	nqn := d.exportRefNQN("tank", "block", "v5")
	delete(d.export.exports, nqn)
	delete(d.export.mapped, nqn)

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile after reboot: %v", err)
	}

	if !d.export.exports[nqn] {
		t.Fatal("target not re-exported")
	}

	if !d.export.mapped[nqn]["nqn.worker1"] {
		t.Fatal("initiator not re-mapped")
	}

	confirmed := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, confirmed); err != nil {
		t.Fatal(err)
	}
	if len(confirmed.Status.PublishedInitiators) != 1 || confirmed.Status.PublishedInitiators[0] != "nqn.worker1" {
		t.Fatalf("initiator confirmation not published: %+v", confirmed.Status.PublishedInitiators)
	}
	health := findCondition(confirmed.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy))
	if health == nil || health.Status != metav1.ConditionFalse || health.Reason != eventsv1.ReasonBackendUnhealthy {
		t.Fatalf("reboot repair must persist unhealthy status before recovery: %#v", health)
	}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile repaired target: %v", err)
	}
	confirmed = getVol(t, d, vol.Name)
	health = findCondition(confirmed.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy))
	if health == nil || health.Status != metav1.ConditionTrue || health.Reason != eventsv1.ReasonBackendRecovered {
		t.Fatalf("verified target repair must persist recovery: %#v", health)
	}
}

func TestHealthRepairHoldIsScopedAndDisabledByDefault(t *testing.T) {
	r := &VolumeReconciler{Namespace: "zfs-csi-system"}
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{
		Name:        "zfs-csi-e2e-health-volume",
		Namespace:   "zfs-csi-system",
		Annotations: map[string]string{healthRepairHoldAnnotation: "true"},
	}}
	if r.healthRepairHoldEnabled(volume) {
		t.Fatal("default health repair hold must be disabled")
	}

	r.EnableHealthRepairHold = true
	if !r.healthRepairHoldEnabled(volume) {
		t.Fatal("enabled E2E volume must be held")
	}
	wrongNamespace := volume.DeepCopy()
	wrongNamespace.Namespace = "default"
	if r.healthRepairHoldEnabled(wrongNamespace) {
		t.Fatal("wrong namespace must not hold repair")
	}
	r.Namespace = "zfs-csi"
	configuredNamespace := volume.DeepCopy()
	configuredNamespace.Namespace = "zfs-csi"
	if !r.healthRepairHoldEnabled(configuredNamespace) {
		t.Fatal("configured driver namespace must hold repair")
	}
	wrongName := volume.DeepCopy()
	wrongName.Name = "ordinary-volume"
	if r.healthRepairHoldEnabled(wrongName) {
		t.Fatal("wrong name must not hold repair")
	}
	wrongAnnotation := volume.DeepCopy()
	wrongAnnotation.Annotations[healthRepairHoldAnnotation] = "false"
	if r.healthRepairHoldEnabled(wrongAnnotation) {
		t.Fatal("wrong annotation value must not hold repair")
	}
}

func TestReconcileEnsure_DoesNotConfirmFailedMapInitiator(t *testing.T) {
	d := newTestDeps(t)
	d.export.mapErr = errors.New("map failed")
	r := d.reconciler()

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "v5-fail"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:v5-fail", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	req := reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "v5-fail"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(got.DeepCopy())
	got.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{NodeName: "worker1", InitiatorID: "nqn.worker1"}}
	if err := d.Status().Patch(context.Background(), got, patch); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("reconcile after map failure succeeded, want retryable error")
	}

	confirmed := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, confirmed); err != nil {
		t.Fatal(err)
	}
	if len(confirmed.Status.PublishedInitiators) != 0 {
		t.Fatalf("failed map should not be published: %+v", confirmed.Status.PublishedInitiators)
	}
}

func TestReconcileEnsure_ExpandsReadyVolumeAndUpdatesActualCapacity(t *testing.T) {
	d := newTestDeps(t)
	zfsb := &recordingZFS{Backend: zfsfake.New().WithPool("tank", 1<<40)}
	r := d.reconciler()
	d.setReconcilerBackend(r, zfsb)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "grow"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:grow", Transport: zfscsiv1.TransportNVMeTCP,
		},
	}
	if err := d.Create(context.Background(), vol); err != nil {
		t.Fatal(err)
	}

	req := reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: "grow"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	if err := d.Get(context.Background(), req.NamespacedName, vol); err != nil {
		t.Fatal(err)
	}
	patch := crclient.MergeFrom(vol.DeepCopy())
	vol.Spec.Capacity = 2 << 30
	if err := d.Patch(context.Background(), vol, patch); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile expand: %v", err)
	}

	if len(zfsb.expandCalls) != 1 || zfsb.expandCalls[0] != 2<<30 {
		t.Fatalf("expand calls = %v, want [2147483648]", zfsb.expandCalls)
	}

	got := &zfscsiv1.Volume{}
	if err := d.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ActualCapacity != 2<<30 {
		t.Fatalf("ActualCapacity = %d, want %d", got.Status.ActualCapacity, int64(2<<30))
	}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile idempotent expand: %v", err)
	}
	if len(zfsb.expandCalls) != 1 {
		t.Fatalf("expand called again after ActualCapacity caught up: %v", zfsb.expandCalls)
	}
}

func TestVolumeReguidReplacementRejectsMutation(t *testing.T) {
	d := newTestDeps(t)
	vol := createReadyBlock(t, d, "reguid-volume")
	d.zfsb.WithDataset("tank/csi/block/reguid-volume", zfs.KindBlock, false, zfs.KeyNone)
	d.zfsb.ReplacePool("tank", 1<<40, "2", "ONLINE")
	r := d.reconciler()

	before := getVol(t, d, vol.Name)
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: nn(vol.Name)}); err == nil || !strings.Contains(err.Error(), "GUID mismatch") {
		t.Fatalf("reconcile after reguid error=%v, want GUID mismatch", err)
	}
	after := getVol(t, d, vol.Name)
	if !reflect.DeepEqual(before.Status, after.Status) {
		t.Fatalf("reguid mismatch mutated status: before=%#v after=%#v", before.Status, after.Status)
	}
}

func (d *testDeps) exportRefNQN(pool, kind, id string) string {
	guid, _ := d.zfsb.PoolGUID(context.Background(), pool)
	nqn, _ := naming.TargetNQN("storage-a", guid, zfs.VolumeKind(kind), id)
	return nqn
}

func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}

	return nil
}

// compile-time: fakeTransportServer is a transport.Server.
var (
	_ transport.Server   = (*fakeTransportServer)(nil)
	_ crypto.KeyProvider = (*recKeyProvider)(nil)
	_ crypto.Stager      = (*nopStager)(nil)
	_ zfs.Backend        = (*recordingZFS)(nil)
)

// keep time referenced for future timestamp assertions.
var _ = time.Now
