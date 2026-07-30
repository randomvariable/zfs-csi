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
	"strings"
	"testing"
)

var errNotFound = errors.New("not found")

// fakeHostFS is an in-memory HostFS for testing the NVMe host client. It serves
// canned file contents and records writes so tests can assert on the connect
// string and delete-controller writes.
type fakeHostFS struct {
	files   map[string]string
	writes  []fakeWrite
	readErr map[string]error
}

type fakeWrite struct {
	path string
	data string
}

func (f *fakeHostFS) WriteFile(p string, data []byte) error {
	f.writes = append(f.writes, fakeWrite{path: p, data: string(data)})
	return nil
}

func (f *fakeHostFS) ReadFile(p string) ([]byte, error) {
	if err, ok := f.readErr[p]; ok {
		return nil, err
	}
	if v, ok := f.files[p]; ok {
		return []byte(v), nil
	}
	return nil, errNotFound
}

func (f *fakeHostFS) ReadDir(d string) ([]string, error) {
	prefix := d + "/"
	var names []string
	seen := map[string]bool{}
	for k := range f.files {
		if strings.HasPrefix(k, prefix) {
			rest := strings.TrimPrefix(k, prefix)
			name := rest
			if idx := strings.IndexByte(rest, '/'); idx >= 0 {
				name = rest[:idx]
			}
			if !seen[name] {
				names = append(names, name)
				seen[name] = true
			}
		}
	}
	return names, nil
}

// TestNVMETClientAttachMultipathNamespace covers Bug Y: with NVMe multipath/ANA
// (default on modern kernels) the namespace sysfs entry under the controller is
// the per-controller form "nvme1c1n1", not "nvme1n1", and its nguid lives there
// — but the /dev node and I/O path is the HEAD "nvme1n1". findDevice must match
// the c-form entry (incl. its nguid) yet return the head /dev node.
func TestNVMETClientAttachMultipathNamespace(t *testing.T) {
	t.Parallel()

	const targetNQN = "nqn.2026-01.test:mpath"
	fs := &fakeHostFS{files: map[string]string{
		nvmeSysfsClass + "/nvme1/subsysnqn":       targetNQN + "\n",
		nvmeSysfsClass + "/nvme1":                 "",
		nvmeSysfsClass + "/nvme1/nvme1c1n1":       "",
		nvmeSysfsClass + "/nvme1/nvme1c1n1/nguid": "56fa1baf-0b97-6db6-6203-742888f1a79f\n",
	}}
	c := NewNVMETClientWithHostFS(fs)

	dev, err := c.Attach(context.Background(), TargetRef{
		Kind: KindNVMeTCP, TargetNQN: targetNQN, Portal: "10.0.0.1:4420",
		DeviceGUID: "56fa1baf0b976db66203742888f1a79f", // plain 32-hex form
	}, "node-a")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	// Must return the HEAD node, not the c-form.
	if dev != "/dev/nvme1n1" {
		t.Fatalf("device = %q, want /dev/nvme1n1 (head, not c-form)", dev)
	}
}

func TestNVMETClientAttachConnectStringTunables(t *testing.T) {
	t.Parallel()

	const targetNQN = "nqn.2026-01.test:vol"
	seed := func() *fakeHostFS {
		return &fakeHostFS{files: map[string]string{
			nvmeSysfsClass + "/nvme0/subsysnqn":     targetNQN + "\n",
			nvmeSysfsClass + "/nvme0/nvme0n1/nguid": "abcdef\n",
			nvmeSysfsClass + "/nvme0":               "",
			nvmeSysfsClass + "/nvme0/nvme0n1":       "",
		}}
	}

	t.Run("defaults retry forever", func(t *testing.T) {
		t.Parallel()
		fs := seed()
		if _, err := NewNVMETClientWithHostFS(fs).Attach(context.Background(),
			TargetRef{Kind: KindNVMeTCP, TargetNQN: targetNQN, Portal: "10.0.0.1:4420"}, ""); err != nil {
			t.Fatalf("Attach: %v", err)
		}
		if !strings.Contains(fs.writes[0].data, "ctrl_loss_tmo=-1,reconnect_delay=10") {
			t.Fatalf("connect string = %q, want default ctrl_loss_tmo=-1,reconnect_delay=10", fs.writes[0].data)
		}
	})

	t.Run("configurable via options", func(t *testing.T) {
		t.Parallel()
		fs := seed()
		c := NewNVMETClientWithHostFS(fs, WithCtrlLossTMO(120), WithReconnectDelay(5))
		if _, err := c.Attach(context.Background(),
			TargetRef{Kind: KindNVMeTCP, TargetNQN: targetNQN, Portal: "10.0.0.1:4420"}, ""); err != nil {
			t.Fatalf("Attach: %v", err)
		}
		if !strings.Contains(fs.writes[0].data, "ctrl_loss_tmo=120,reconnect_delay=5") {
			t.Fatalf("connect string = %q, want ctrl_loss_tmo=120,reconnect_delay=5", fs.writes[0].data)
		}
	})
}

func TestNVMETClientAttachTLSRequiresDedicatedPort(t *testing.T) {
	t.Parallel()

	const targetNQN = "nqn.2026-01.test:tls"
	seed := func() *fakeHostFS {
		return &fakeHostFS{files: map[string]string{
			nvmeSysfsClass + "/nvme0/subsysnqn":     targetNQN + "\n",
			nvmeSysfsClass + "/nvme0":               "",
			nvmeSysfsClass + "/nvme0/nvme0n1":       "",
			nvmeSysfsClass + "/nvme0/nvme0n1/nguid": "abcdef\n",
		}}
	}

	t.Run("connects with tls flag", func(t *testing.T) {
		fs := seed()
		if _, err := NewNVMETClientWithHostFS(fs).Attach(context.Background(), TargetRef{
			Kind: KindNVMeTCP, TargetNQN: targetNQN, Portal: "10.0.0.1:4421", TLS: true,
		}, ""); err != nil {
			t.Fatalf("Attach: %v", err)
		}
		if !strings.Contains(fs.writes[0].data, "trsvcid=4421") || !strings.Contains(fs.writes[0].data, ",tls,") {
			t.Fatalf("connect string = %q, want TLS port and bare tls option", fs.writes[0].data)
		}
		if strings.Contains(fs.writes[0].data, "tls=1") {
			t.Fatalf("connect string = %q, must not contain tls=1", fs.writes[0].data)
		}
	})

	t.Run("rejects plaintext port", func(t *testing.T) {
		_, err := NewNVMETClientWithHostFS(seed()).Attach(context.Background(), TargetRef{
			Kind: KindNVMeTCP, TargetNQN: targetNQN, Portal: "10.0.0.1:4420", TLS: true,
		}, "")
		if err == nil || !strings.Contains(err.Error(), "dedicated service 4421") {
			t.Fatalf("Attach error = %v, want TLS dedicated-port error", err)
		}
	})
}

func TestNVMETClientAttachWritesConnectStringAndDiscoversDevice(t *testing.T) {
	t.Parallel()

	const targetNQN = "nqn.2026-01.test:vol"
	fs := &fakeHostFS{files: map[string]string{
		nvmeSysfsClass + "/nvme0/subsysnqn":     targetNQN + "\n",
		nvmeSysfsClass + "/nvme1/subsysnqn":     "nqn.other\n",
		nvmeSysfsClass + "/nvme0/nvme0n1/nguid": "abcdef\n",
		nvmeSysfsClass + "/nvme0":               "", // makes nvme0 appear in ReadDir of the class
		nvmeSysfsClass + "/nvme1":               "",
		nvmeSysfsClass + "/nvme0/nvme0n1":       "",
	}}
	c := NewNVMETClientWithHostFS(fs)

	dev, err := c.Attach(context.Background(), TargetRef{
		Kind: KindNVMeTCP, TargetNQN: targetNQN, Portal: "10.0.0.1:4420",
	}, "nqn.host")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// Assert the connect string written to /dev/nvme-fabrics.
	if len(fs.writes) != 1 || fs.writes[0].path != nvmeFabricsDev {
		t.Fatalf("writes = %#v, want one write to %s", fs.writes, nvmeFabricsDev)
	}
	// The connect string carries the reconnect tunables (F2): ctrl_loss_tmo=-1
	// (retry forever) and reconnect_delay=10, appended after hostnqn/hostid.
	wantConnect := "transport=tcp,traddr=10.0.0.1,trsvcid=4420,nqn=" + targetNQN +
		",hostnqn=" + hostNQN("nqn.host") + ",hostid=" + hostID("nqn.host") +
		",ctrl_loss_tmo=-1,reconnect_delay=10"
	if fs.writes[0].data != wantConnect {
		t.Fatalf("connect string = %q, want %q", fs.writes[0].data, wantConnect)
	}

	// Assert the discovered device (matched by subsysnqn, first namespace).
	if dev != "/dev/nvme0n1" {
		t.Fatalf("device = %q, want /dev/nvme0n1", dev)
	}
}

func TestNVMETClientAttachMatchesNGUID(t *testing.T) {
	t.Parallel()

	const targetNQN = "nqn.2026-01.test:vol"
	fs := &fakeHostFS{files: map[string]string{
		nvmeSysfsClass + "/nvme0/subsysnqn":     targetNQN + "\n",
		nvmeSysfsClass + "/nvme0":               "",
		nvmeSysfsClass + "/nvme0/nvme0n1":       "",
		nvmeSysfsClass + "/nvme0/nvme0n1/nguid": "deadbeef\n",
		nvmeSysfsClass + "/nvme0/nvme0n2":       "",
		nvmeSysfsClass + "/nvme0/nvme0n2/nguid": "matching-guid\n",
	}}
	c := NewNVMETClientWithHostFS(fs)

	dev, err := c.Attach(context.Background(), TargetRef{
		Kind: KindNVMeTCP, TargetNQN: targetNQN, Portal: "10.0.0.1:4420", DeviceGUID: "matching-guid",
	}, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if dev != "/dev/nvme0n2" {
		t.Fatalf("device = %q, want /dev/nvme0n2 (NGUID match)", dev)
	}
}

func TestNVMETClientAttachNoControllerReturnsError(t *testing.T) {
	t.Parallel()

	fs := &fakeHostFS{files: map[string]string{}}
	c := NewNVMETClientWithHostFS(fs)

	_, err := c.Attach(context.Background(), TargetRef{
		Kind: KindNVMeTCP, TargetNQN: "nqn.missing", Portal: "10.0.0.1:4420",
	}, "")
	if err == nil {
		t.Fatal("expected error when no controller matches, got nil")
	}
}

func TestNVMETClientDetachWritesDeleteController(t *testing.T) {
	t.Parallel()

	const targetNQN = "nqn.2026-01.test:vol"
	fs := &fakeHostFS{files: map[string]string{
		nvmeSysfsClass + "/nvme0/subsysnqn": targetNQN + "\n",
		nvmeSysfsClass + "/nvme1/subsysnqn": "nqn.other\n",
		nvmeSysfsClass + "/nvme0":           "",
		nvmeSysfsClass + "/nvme1":           "",
	}}
	c := NewNVMETClientWithHostFS(fs)

	if err := c.Detach(context.Background(), TargetRef{TargetNQN: targetNQN}); err != nil {
		t.Fatalf("Detach: %v", err)
	}

	// Only the matching controller's delete_controller should be written "1".
	var deletes []fakeWrite
	for _, w := range fs.writes {
		if strings.HasSuffix(w.path, "/delete_controller") {
			deletes = append(deletes, w)
		}
	}
	if len(deletes) != 1 {
		t.Fatalf("delete writes = %#v, want 1", deletes)
	}
	if deletes[0].path != nvmeSysfsClass+"/nvme0/delete_controller" || deletes[0].data != "1" {
		t.Fatalf("delete write = %#v, want nvme0/delete_controller=1", deletes[0])
	}
}
