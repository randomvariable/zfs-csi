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

package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/randomvariable/zfs-csi/internal/transport/fakefs"
)

func TestNVMETExportWaitsForDelayedZvolDevice(t *testing.T) {
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)
	n.deviceWaitTimeout = time.Second
	devicePath := "/dev/zvol/tank/csi/block/delayed"

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = fs.MkdirAll(devicePath)
	}()

	_, err := n.Export(context.Background(), ExportOptions{
		ZvolPath: devicePath, DeviceGUID: "guid", TargetNQN: "nqn.delayed", Portal: "192.0.2.10:4420",
	})
	if err != nil {
		t.Fatalf("Export after delayed zvol creation: %v", err)
	}
}

// TestNVMETExportReturnsDeviceNotReadyPromptly proves the M1 fix: when the zvol
// node never appears, Export returns ErrDeviceNotReady bounded by the SHORT
// device-wait budget (not a multi-minute block that would pin the reconciler's
// per-dataset lock and worker slot). The reconciler's 1s requeue drives retry.
func TestNVMETExportReturnsDeviceNotReadyPromptly(t *testing.T) {
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)
	n.deviceWaitTimeout = 100 * time.Millisecond

	start := time.Now()
	_, err := n.Export(context.Background(), ExportOptions{
		ZvolPath: "/dev/zvol/tank/csi/block/missing", DeviceGUID: "guid",
		TargetNQN: "nqn.missing", Portal: "192.0.2.10:4420",
	})
	if !errors.Is(err, ErrDeviceNotReady) {
		t.Fatalf("Export = %v, want ErrDeviceNotReady", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Export blocked %s, want prompt return under the wait budget", elapsed)
	}
}

func TestNVMETExportMapUnexport(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)

	// Model udev having created the zvol device node before export: Export now
	// waits (waitForDevice) for the node to exist before touching configfs,
	// since nvmet's enable write opens device_path synchronously.
	if err := fs.MkdirAll("/dev/zvol/tank/csi/block/vol"); err != nil {
		t.Fatalf("pre-create zvol node: %v", err)
	}

	ref, err := n.Export(ctx, ExportOptions{ZvolPath: "/dev/zvol/tank/csi/block/vol", DeviceGUID: "guid", TargetNQN: "nqn.test", Portal: "192.0.2.10:4420"})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	assertFile(t, fs, "/nvmet/subsystems/nqn.test/attr_allow_any_host", "0")
	assertFile(t, fs, "/nvmet/subsystems/nqn.test/namespaces/1/device_path", "/dev/zvol/tank/csi/block/vol")
	assertFile(t, fs, "/nvmet/subsystems/nqn.test/namespaces/1/device_nguid", "guid")
	assertFile(t, fs, "/nvmet/ports/1/addr_trtype", "tcp")
	assertFile(t, fs, "/nvmet/ports/1/addr_traddr", "192.0.2.10")

	if ok, _ := fs.Lexists("/nvmet/ports/1/subsystems/nqn.test"); !ok {
		t.Fatal("port subsystem link missing")
	}

	if err := n.MapInitiator(ctx, ref, "nqn.host"); err != nil {
		t.Fatalf("MapInitiator: %v", err)
	}

	// MapInitiator wraps the raw initiator id into a valid host NQN before using
	// it as the allowed_hosts entry name (the kernel rejects a bare name).
	if ok, _ := fs.Lexists("/nvmet/subsystems/nqn.test/allowed_hosts/" + hostNQN("nqn.host")); !ok {
		t.Fatal("allowed host link missing")
	}

	// MappedInitiators must reverse the wrapping so the reconciler sees raw ids.
	mapped, err := n.MappedInitiators(ctx, ref)
	if err != nil {
		t.Fatalf("MappedInitiators: %v", err)
	}
	if len(mapped) != 1 || mapped[0] != "nqn.host" {
		t.Fatalf("MappedInitiators = %#v, want [nqn.host]", mapped)
	}

	if err := n.Unexport(ctx, ref); err != nil {
		t.Fatalf("Unexport: %v", err)
	}

	if ok, _ := fs.Lexists("/nvmet/subsystems/nqn.test"); ok {
		t.Fatal("subsystem still exists after Unexport")
	}
}

// TestNVMETExportRelinksPortWhenAlreadyEnabled is the Bug SS regression: an
// already-enabled namespace whose port->subsystem link has gone missing (the
// link is a separate configfs object; a prior Export can enable the ns but fail
// or lose the link) must be RE-LINKED on the next Export pass, not skipped by an
// early return. Without the link the target is unreachable at the portal and the
// initiator connect fails NVME_SC_CONNECT_INVALID_PARAM.
func TestNVMETExportRelinksPortWhenAlreadyEnabled(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)
	if err := fs.MkdirAll("/dev/zvol/tank/csi/block/vol"); err != nil {
		t.Fatalf("pre-create zvol node: %v", err)
	}
	opts := ExportOptions{ZvolPath: "/dev/zvol/tank/csi/block/vol", DeviceGUID: "guid", TargetNQN: "nqn.test", Portal: "192.0.2.10:4420"}

	// First export: fresh namespace, must fully configure + link.
	if _, err := n.Export(ctx, opts); err != nil {
		t.Fatalf("first Export: %v", err)
	}
	link := "/nvmet/ports/1/subsystems/nqn.test"
	if ok, _ := fs.Lexists(link); !ok {
		t.Fatal("first Export did not create the port link")
	}

	// Simulate the Bug SS state: namespace stays enabled, but the port link is
	// gone (torn down out of band / prior partial export).
	if err := fs.Remove(link); err != nil {
		t.Fatalf("remove port link: %v", err)
	}
	if ok, _ := fs.Lexists(link); ok {
		t.Fatal("precondition: port link should be absent before re-export")
	}

	// Second export: the namespace is already enabled, so the immutable ns
	// attributes are skipped, but the port link MUST be re-created. Callers treat
	// ErrAlreadyExported as success.
	if _, err := n.Export(ctx, opts); err != nil && !errors.Is(err, ErrAlreadyExported) {
		t.Fatalf("re-Export: %v", err)
	}
	if ok, _ := fs.Lexists(link); !ok {
		t.Fatal("Bug SS: re-Export did not re-create the missing port link")
	}
}

// TestNVMETExportRevalidatesSizeOnReExport verifies the TARGET-side half of
// online expand: a second Export of an already-enabled namespace (what
// reconcileEnsure does every pass after reconcileExpand grows the zvol) writes
// revalidate_size=1 so the kernel re-reads the grown backing device and
// advertises the new size to initiators. Without this the target keeps
// reporting the old size and the consumer's resize2fs sees no extra space.
func TestNVMETExportRevalidatesSizeOnReExport(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)

	zvol := "/dev/zvol/tank/csi/block/vol-x"
	if err := fs.MkdirAll(zvol); err != nil {
		t.Fatalf("pre-create zvol: %v", err)
	}
	opts := ExportOptions{ZvolPath: zvol, DeviceGUID: "vol-x", TargetNQN: "nqn.test:vol-x", Portal: "192.0.2.10:4420"}

	// First export: creates + enables the namespace (not the already-enabled path).
	if _, err := n.Export(ctx, opts); err != nil {
		t.Fatalf("first Export: %v", err)
	}
	reval := "/nvmet/subsystems/nqn.test:vol-x/namespaces/1/revalidate_size"
	if ok, _ := fs.Lexists(reval); ok {
		t.Fatal("revalidate_size written on the FIRST export (should only fire on the already-enabled re-export)")
	}

	// Second export: namespace already enabled -> must write revalidate_size=1.
	_, err := n.Export(ctx, opts)
	if err != nil && !errors.Is(err, ErrAlreadyExported) {
		t.Fatalf("second Export: %v", err)
	}
	if ok, _ := fs.Lexists(reval); !ok {
		t.Fatal("revalidate_size NOT written on re-export of an already-enabled namespace (online-expand target-side bug)")
	}
}

// TestNVMETExportTwoVolumesSharerPort is the multi-volume regression for the
// "only one volume ever links" bug seen live: two DIFFERENT volumes export to
// the single shared port. The second Export must link its own subsystem even
// though the first already configured the port's addr_* attributes (which are
// immutable once in use — configurePort must skip them, not error out and bail
// before the link step). Both subsystems must end up linked.
func TestNVMETExportTwoVolumesSharePort(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)

	export := func(id string) {
		t.Helper()
		zvol := "/dev/zvol/tank/csi/block/" + id
		if err := fs.MkdirAll(zvol); err != nil {
			t.Fatalf("pre-create zvol %s: %v", id, err)
		}
		if _, err := n.Export(ctx, ExportOptions{
			ZvolPath: zvol, DeviceGUID: id, TargetNQN: "nqn.test:" + id, Portal: "192.0.2.10:4420",
		}); err != nil {
			t.Fatalf("Export %s: %v", id, err)
		}
	}

	export("vol-a")
	export("vol-b")

	for _, id := range []string{"vol-a", "vol-b"} {
		link := "/nvmet/ports/1/subsystems/nqn.test:" + id
		if ok, _ := fs.Lexists(link); !ok {
			t.Fatalf("volume %s not linked to the shared port (only-one-links bug)", id)
		}
	}
}

func TestNVMETExportTLSUsesDedicatedPort(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)
	zvol := "/dev/zvol/tank/csi/block/tls"
	if err := fs.MkdirAll(zvol); err != nil {
		t.Fatalf("pre-create zvol: %v", err)
	}

	ref, err := n.Export(ctx, ExportOptions{
		ZvolPath: zvol, DeviceGUID: "tls", TargetNQN: "nqn.test:tls",
		Portal: "192.0.2.10:4420", TLS: true,
	})
	if err != nil {
		t.Fatalf("Export TLS: %v", err)
	}
	if !ref.TLS {
		t.Fatal("TargetRef TLS = false, want true")
	}
	if ref.Portal != "192.0.2.10:4421" {
		t.Fatalf("TargetRef portal = %q, want TLS service 4421", ref.Portal)
	}
	assertFile(t, fs, "/nvmet/ports/2/addr_traddr", "192.0.2.10")
	assertFile(t, fs, "/nvmet/ports/2/addr_trsvcid", "4421")
	assertFile(t, fs, "/nvmet/ports/2/addr_tsas", "tls1.3")
	if ok, _ := fs.Lexists("/nvmet/ports/2/subsystems/nqn.test:tls"); !ok {
		t.Fatal("TLS port subsystem link missing")
	}
}

func TestNVMETExportTLSAndPlaintextUseSeparatePorts(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)

	for _, tc := range []struct {
		name string
		tls  bool
		port string
	}{
		{name: "plaintext", port: "4420"},
		{name: "tls", tls: true, port: "4421"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zvol := "/dev/zvol/tank/csi/block/" + tc.name
			if err := fs.MkdirAll(zvol); err != nil {
				t.Fatalf("pre-create zvol: %v", err)
			}
			ref, err := n.Export(ctx, ExportOptions{
				ZvolPath: zvol, DeviceGUID: tc.name, TargetNQN: "nqn.test:" + tc.name,
				Portal: "192.0.2.10:4420", TLS: tc.tls,
			})
			if err != nil {
				t.Fatalf("Export: %v", err)
			}
			if _, svc := splitPortal(ref.Portal, ""); svc != tc.port {
				t.Fatalf("TargetRef service = %q, want %q", svc, tc.port)
			}
		})
	}

	for _, port := range []string{"1", "2"} {
		if ok, _ := fs.Lexists("/nvmet/ports/" + port + "/subsystems/nqn.test:" + map[string]string{"1": "plaintext", "2": "tls"}[port]); !ok {
			t.Fatalf("subsystem not linked to port %s", port)
		}
	}
}

func TestNVMETExportRejectsInconsistentSharedPort(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	n := NewNVMETAt(fs, "/nvmet", 1)

	for _, tc := range []struct {
		nqn    string
		portal string
	}{
		{nqn: "nqn.test:first", portal: "192.0.2.10:4420"},
		{nqn: "nqn.test:second", portal: "192.0.2.10:8442"},
	} {
		if err := fs.MkdirAll("/dev/zvol/tank/csi/block/" + tc.nqn); err != nil {
			t.Fatalf("pre-create zvol: %v", err)
		}
		_, err := n.Export(ctx, ExportOptions{
			ZvolPath: "/dev/zvol/tank/csi/block/" + tc.nqn, DeviceGUID: tc.nqn,
			TargetNQN: tc.nqn, Portal: tc.portal,
		})
		if tc.nqn == "nqn.test:first" && err != nil {
			t.Fatalf("first Export: %v", err)
		}
		if tc.nqn == "nqn.test:second" && err == nil {
			t.Fatal("second Export succeeded with a different immutable port service")
		}
	}
	if ok, _ := fs.Lexists("/nvmet/ports/1/subsystems/nqn.test:second"); ok {
		t.Fatal("inconsistent target was linked to the shared port")
	}
}

func assertFile(t *testing.T, fs *fakefs.FS, p, want string) {
	t.Helper()

	got, err := fs.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", p, err)
	}

	if string(got) != want {
		t.Fatalf("ReadFile(%s) = %q, want %q", p, got, want)
	}
}

func TestNVMETForceDisconnectBouncesPortLink(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	ref := TargetRef{TargetNQN: "nqn.test", Portal: "10.0.0.1:4420", NamespaceID: 1}

	// Seed an exported+linked subsystem: the subsystem dir and the
	// port->subsystem symlink both exist, and the port already carries our
	// configured traddr (so configurePort's re-write is skipped as immutable).
	subsys := "/nvmet/subsystems/nqn.test"
	link := "/nvmet/ports/1/subsystems/nqn.test"
	if err := fs.MkdirAll(subsys); err != nil {
		t.Fatalf("MkdirAll subsys: %v", err)
	}
	if err := fs.MkdirAll("/nvmet/ports/1/subsystems"); err != nil {
		t.Fatalf("MkdirAll port: %v", err)
	}
	if err := fs.WriteFile("/nvmet/ports/1/addr_traddr", []byte("10.0.0.1")); err != nil {
		t.Fatalf("seed addr_traddr: %v", err)
	}
	for p, value := range map[string]string{
		"addr_trtype":  "tcp",
		"addr_adrfam":  "ipv4",
		"addr_trsvcid": "4420",
	} {
		if err := fs.WriteFile("/nvmet/ports/1/"+p, []byte(value)); err != nil {
			t.Fatalf("seed %s: %v", p, err)
		}
	}
	if err := fs.Symlink(subsys, link); err != nil {
		t.Fatalf("seed link: %v", err)
	}

	rec := &recordingWriter{Writer: fs}
	if err := NewNVMETAt(rec, "/nvmet", 1).ForceDisconnect(ctx, ref); err != nil {
		t.Fatalf("ForceDisconnect: %v", err)
	}

	// The link must have been removed (kernel drops all controllers) then
	// re-created (legitimate initiators reconnect). Assert the ordered pair.
	if got := rec.sequence(link); len(got) < 2 || got[0] != "remove" || got[len(got)-1] != "symlink" {
		t.Fatalf("port-link ops = %v, want remove...symlink bounce", got)
	}

	// The link must exist again after the bounce.
	if ok, _ := fs.Lexists(link); !ok {
		t.Fatal("port link absent after ForceDisconnect; want re-created")
	}
}

func TestNVMETMappedInitiatorsMissingSubsystemReturnsNotExported(t *testing.T) {
	_, err := NewNVMETAt(fakefs.New(), "/nvmet", 1).MappedInitiators(context.Background(), TargetRef{TargetNQN: "nqn.missing"})
	if !errors.Is(err, ErrNotExported) {
		t.Fatalf("MappedInitiators missing target error = %v, want ErrNotExported", err)
	}
}

func TestNVMETForceDisconnectMissingSubsystemIsNoop(t *testing.T) {
	ctx := context.Background()
	fs := fakefs.New()
	ref := TargetRef{TargetNQN: "nqn.absent", Portal: "10.0.0.1:4420", NamespaceID: 1}

	rec := &recordingWriter{Writer: fs}
	if err := NewNVMETAt(rec, "/nvmet", 1).ForceDisconnect(ctx, ref); err != nil {
		t.Fatalf("ForceDisconnect on missing subsystem: %v", err)
	}
	if len(rec.sequence("/nvmet/ports/1/subsystems/nqn.absent")) != 0 {
		t.Fatal("ForceDisconnect touched the port link for an absent subsystem; want no-op")
	}
}

// recordingWriter wraps a Writer and records the ordered sequence of mutating
// operations per path, so a test can assert e.g. that a port link was removed
// then re-created (the fence bounce).
type recordingWriter struct {
	Writer

	mu  sync.Mutex
	ops map[string][]string
}

func (w *recordingWriter) record(path, op string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ops == nil {
		w.ops = map[string][]string{}
	}
	w.ops[path] = append(w.ops[path], op)
}

func (w *recordingWriter) sequence(path string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]string(nil), w.ops[path]...)
}

func (w *recordingWriter) Remove(path string) error {
	w.record(path, "remove")

	return w.Writer.Remove(path)
}

func (w *recordingWriter) Symlink(target, link string) error {
	w.record(link, "symlink")

	return w.Writer.Symlink(target, link)
}

type errorReader struct {
	readErr error
}

func (e errorReader) MkdirAll(string) error            { return nil }
func (e errorReader) WriteFile(string, []byte) error   { return nil }
func (e errorReader) ReadFile(string) ([]byte, error)  { return nil, e.readErr }
func (e errorReader) ReadDir(string) ([]string, error) { return nil, nil }
func (e errorReader) RemoveAll(string) error           { return nil }
func (e errorReader) Remove(string) error              { return nil }
func (e errorReader) Symlink(string, string) error     { return nil }
func (e errorReader) Lexists(string) (bool, error)     { return false, nil }

var _ Writer = errorReader{}
