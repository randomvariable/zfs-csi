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
	"reflect"
	"testing"
)

func TestFakeTransportLifecycle(t *testing.T) {
	ctx := context.Background()
	f := New()

	ref, err := f.Export(ctx, ExportOptions{Kind: KindNVMeTCP, TargetNQN: "nqn.test", Portal: "10.0.0.1:4420", DeviceGUID: "guid"})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	t.Run("rejects duplicate export", func(t *testing.T) {
		if _, err := f.Export(ctx, ExportOptions{Kind: KindNVMeTCP, TargetNQN: "nqn.test"}); !errors.Is(err, ErrAlreadyExported) {
			t.Fatalf("Export duplicate err = %v, want ErrAlreadyExported", err)
		}
	})

	t.Run("maps initiators", func(t *testing.T) {
		assertMapInitiator(t, ctx, f, ref, "host-a")
		assertMapInitiator(t, ctx, f, ref, "host-b")

		got, err := f.MappedInitiators(ctx, ref)
		if err != nil {
			t.Fatalf("MappedInitiators: %v", err)
		}

		if want := []string{"host-a", "host-b"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("MappedInitiators = %v, want %v", got, want)
		}
	})

	t.Run("attaches and tracks active connections", func(t *testing.T) {
		dev, err := f.Attach(ctx, ref, "host-a")
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}

		if dev != "/dev/nvme1n1" {
			t.Fatalf("Attach device = %q", dev)
		}

		assertActiveConnection(t, ctx, f, ref, true)
	})

	t.Run("unmaps and manually tracks connections", func(t *testing.T) {
		if err := f.UnmapInitiator(ctx, ref, "host-a"); err != nil {
			t.Fatalf("UnmapInitiator: %v", err)
		}

		assertActiveConnection(t, ctx, f, ref, false)

		f.SetConnection(ref, "host-b", true)
		assertActiveConnection(t, ctx, f, ref, true)
	})

	t.Run("force-disconnect terminates live connections but leaves allow-list", func(t *testing.T) {
		f.SetConnection(ref, "host-b", true)
		assertActiveConnection(t, ctx, f, ref, true)

		if err := f.ForceDisconnect(ctx, ref); err != nil {
			t.Fatalf("ForceDisconnect: %v", err)
		}
		// The controller is dropped (connection cleared)...
		assertActiveConnection(t, ctx, f, ref, false)
		// ...but the allow-list entry survives (legitimate initiators reconnect).
		got, err := f.MappedInitiators(ctx, ref)
		if err != nil {
			t.Fatalf("MappedInitiators after fence: %v", err)
		}
		if want := []string{"host-b"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("allow-list after fence = %v, want %v", got, want)
		}
	})

	t.Run("detaches idempotently", func(t *testing.T) {
		if err := f.Detach(ctx, ref); err != nil {
			t.Fatalf("Detach: %v", err)
		}

		if err := f.Detach(ctx, ref); err != nil {
			t.Fatalf("Detach idempotent: %v", err)
		}
	})

	t.Run("unexports idempotently", func(t *testing.T) {
		if len(f.Targets()) != 1 {
			t.Fatalf("Targets len = %d, want 1", len(f.Targets()))
		}

		if err := f.Unexport(ctx, ref); err != nil {
			t.Fatalf("Unexport: %v", err)
		}

		if err := f.Unexport(ctx, ref); err != nil {
			t.Fatalf("Unexport idempotent: %v", err)
		}
	})
}

func assertMapInitiator(t *testing.T, ctx context.Context, f *Fake, ref TargetRef, host string) {
	t.Helper()

	if err := f.MapInitiator(ctx, ref, host); err != nil {
		t.Fatalf("MapInitiator(%q): %v", host, err)
	}
}

func assertActiveConnection(t *testing.T, ctx context.Context, f *Fake, ref TargetRef, want bool) {
	t.Helper()

	if active := f.activeConnection(ref); active != want {
		t.Fatalf("activeConnection = %v; want %v", active, want)
	}
}
