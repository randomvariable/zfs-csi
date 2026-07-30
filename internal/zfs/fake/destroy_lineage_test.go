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

package fake

import (
	"context"
	"testing"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

func TestCreate_FilesystemPerformanceProperties(t *testing.T) {
	b := New().WithPool("tank", 1<<40)
	const name = "tank/csi/filesystem/performance"
	if err := b.Create(t.Context(), zfs.CreateOptions{
		Name: name, Kind: zfs.KindFilesystem, Capacity: 1 << 30,
		Atime: "off", XAttr: "sa",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	for prop, want := range map[string]string{"atime": "off", "xattr": "sa"} {
		got, err := b.GetProperty(t.Context(), name, prop)
		if err != nil || got != want {
			t.Fatalf("%s = %q, %v; want %q", prop, got, err, want)
		}
	}
}

// TestDestroy_SourceWithDependentClonePreservesClone proves the promote-aware
// Destroy contract: destroying a clone's ORIGIN must reparent (promote) the
// clone so the origin frees, while the clone SURVIVES. This is the safety
// property that rules out `zfs destroy -R` (which would delete the live clone
// volume — silent data loss). Both destroy orders must fully clean up.
func TestDestroy_SourceWithDependentClonePreservesClone(t *testing.T) {
	ctx := context.Background()

	newLineage := func(t *testing.T) *Backend {
		t.Helper()
		b := New().WithPool("tank", 1<<40)
		if err := b.Create(ctx, zfs.CreateOptions{Name: "tank/csi/block/source", Kind: zfs.KindBlock, Capacity: 1 << 30}); err != nil {
			t.Fatalf("create source: %v", err)
		}
		if err := b.Snapshot(ctx, "tank/csi/block/source", "clone-b"); err != nil {
			t.Fatalf("snapshot source: %v", err)
		}
		if err := b.Clone(ctx, "tank/csi/block/source", "clone-b", "tank/csi/block/b"); err != nil {
			t.Fatalf("clone: %v", err)
		}

		return b
	}

	t.Run("source deleted first (promote path)", func(t *testing.T) {
		b := newLineage(t)

		// Destroying the source must succeed even though clone b depends on its
		// snapshot — the clone is promoted, not destroyed.
		if err := b.Destroy(ctx, "tank/csi/block/source"); err != nil {
			t.Fatalf("destroy source with dependent clone: %v", err)
		}
		if ok, _ := b.Exists(ctx, "tank/csi/block/source"); ok {
			t.Fatal("source should be gone after destroy")
		}
		if ok, _ := b.Exists(ctx, "tank/csi/block/b"); !ok {
			t.Fatal("clone b MUST survive the source destroy (no -R data loss)")
		}
		// The now-independent clone is itself destroyable.
		if err := b.Destroy(ctx, "tank/csi/block/b"); err != nil {
			t.Fatalf("destroy promoted clone: %v", err)
		}
		if ok, _ := b.Exists(ctx, "tank/csi/block/b"); ok {
			t.Fatal("clone should be gone after its own destroy")
		}
	})

	t.Run("clone deleted first (then source frees)", func(t *testing.T) {
		b := newLineage(t)

		if err := b.Destroy(ctx, "tank/csi/block/b"); err != nil {
			t.Fatalf("destroy clone: %v", err)
		}
		// With the clone gone, the source's snapshot has no dependents; destroying
		// the source must recursively clear its own snapshot and succeed.
		if err := b.Destroy(ctx, "tank/csi/block/source"); err != nil {
			t.Fatalf("destroy source after clone gone: %v", err)
		}
		if ok, _ := b.Exists(ctx, "tank/csi/block/source"); ok {
			t.Fatal("source should be gone")
		}
	})
}
