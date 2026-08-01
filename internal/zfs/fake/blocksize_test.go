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

package fake_test

import (
	"testing"

	"github.com/randomvariable/zfs-csi/internal/zfs"
	"github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

// TestFakeCreateRejectsUnalignedBlockCapacity mirrors the real libzfs failure
// reported in issue #1: `volsize` must be a multiple of `volblocksize`.
func TestFakeCreateRejectsUnalignedBlockCapacity(t *testing.T) {
	b := fake.New().WithPool("tank", 1<<40)

	err := b.Create(t.Context(), zfs.CreateOptions{
		Name: "tank/csi/block/unaligned", Kind: zfs.KindBlock,
		Capacity: 1<<30 + 1, VolBlockSz: "16k",
	})
	if err == nil {
		t.Fatal("Create with volsize not divisible by volblocksize must fail")
	}

	if err := b.Create(t.Context(), zfs.CreateOptions{
		Name: "tank/csi/block/aligned", Kind: zfs.KindBlock,
		Capacity: 1<<30 + 16*1024, VolBlockSz: "16k",
	}); err != nil {
		t.Fatalf("aligned Create: %v", err)
	}
}

// TestFakeCreateRejectsUnalignedDefaultBlockCapacity proves the default
// volblocksize is enforced when the property is not set explicitly.
func TestFakeCreateRejectsUnalignedDefaultBlockCapacity(t *testing.T) {
	b := fake.New().WithPool("tank", 1<<40)

	if err := b.Create(t.Context(), zfs.CreateOptions{
		Name: "tank/csi/block/default-unaligned", Kind: zfs.KindBlock, Capacity: 1<<30 + 1,
	}); err == nil {
		t.Fatal("Create with volsize not divisible by default volblocksize must fail")
	}
}

// TestFakeFilesystemCapacityNotAligned proves refquota is byte-exact: recordsize
// does not constrain dataset capacity.
func TestFakeFilesystemCapacityNotAligned(t *testing.T) {
	b := fake.New().WithPool("tank", 1<<40)

	if err := b.Create(t.Context(), zfs.CreateOptions{
		Name: "tank/csi/fs/exact", Kind: zfs.KindFilesystem,
		Capacity: 1<<30 + 1, VolBlockSz: "128k",
	}); err != nil {
		t.Fatalf("filesystem Create with unaligned refquota: %v", err)
	}
}

// TestFakeExpandRejectsUnalignedBlockCapacity proves `zfs set volsize` alignment
// is enforced on the expansion path too.
func TestFakeExpandRejectsUnalignedBlockCapacity(t *testing.T) {
	b := fake.New().WithPool("tank", 1<<40)
	const name = "tank/csi/block/grow"
	if err := b.Create(t.Context(), zfs.CreateOptions{
		Name: name, Kind: zfs.KindBlock, Capacity: 1 << 30, VolBlockSz: "16k",
	}); err != nil {
		t.Fatal(err)
	}

	if err := b.Expand(t.Context(), name, 1<<30+1); err == nil {
		t.Fatal("Expand to a volsize not divisible by volblocksize must fail")
	}
	if err := b.Expand(t.Context(), name, 1<<30+16*1024); err != nil {
		t.Fatalf("aligned Expand: %v", err)
	}
}
