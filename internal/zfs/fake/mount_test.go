package fake

import (
	"context"
	"testing"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

func TestMountTruthMatchesCreateAndShare(t *testing.T) {
	b := New().WithPool("tank", 1<<40)
	ctx := context.Background()

	if err := b.Create(ctx, zfs.CreateOptions{Name: "tank/block", Kind: zfs.KindBlock, Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Create(ctx, zfs.CreateOptions{Name: "tank/fs", Kind: zfs.KindFilesystem, Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Create(ctx, zfs.CreateOptions{Name: "tank/nfs", Kind: zfs.KindFilesystem, Capacity: 1, ShareNFS: "off"}); err != nil {
		t.Fatal(err)
	}

	for name, want := range map[string]bool{"tank/block": false, "tank/fs": false, "tank/nfs": true} {
		info, err := b.Get(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mounted != want {
			t.Errorf("%s mounted = %v, want %v", name, info.Mounted, want)
		}
	}
	if err := b.Share(ctx, "tank/fs", "off"); err != nil {
		t.Fatal(err)
	}
	if info, _ := b.Get(ctx, "tank/fs"); !info.Mounted {
		t.Fatal("Share did not mount filesystem")
	}
	if err := b.ShareImported(ctx, "tank/block", "off"); err != nil {
		t.Fatal(err)
	}
	if info, _ := b.Get(ctx, "tank/block"); info.Mounted {
		t.Fatal("ShareImported mounted block volume")
	}
}

func TestCloneStartsUnmounted(t *testing.T) {
	ctx := context.Background()
	b := New().WithPool("tank", 1<<40)
	if err := b.Create(ctx, zfs.CreateOptions{Name: "tank/source", Kind: zfs.KindFilesystem, Capacity: 1, ShareNFS: "off"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Snapshot(ctx, "tank/source", "snap"); err != nil {
		t.Fatal(err)
	}
	if err := b.Clone(ctx, "tank/source", "snap", "tank/clone"); err != nil {
		t.Fatal(err)
	}
	info, err := b.Get(ctx, "tank/clone")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mounted {
		t.Fatal("clone was mounted before Share")
	}
}

func TestWithMountedNoOpsForMissingAndNonFilesystem(t *testing.T) {
	ctx := context.Background()
	b := New().WithPool("tank", 1<<40)
	if err := b.Create(ctx, zfs.CreateOptions{Name: "tank/volume", Kind: zfs.KindBlock, Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	b.WithMounted("tank/missing", true).WithMounted("tank/volume", true)
	info, err := b.Get(ctx, "tank/volume")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mounted {
		t.Fatal("WithMounted changed non-filesystem dataset")
	}
}
