package nfsexport

import (
	"net/netip"
	"testing"
)

func mustPrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func rootTank() Entry {
	return Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
}

func TestMemTableMatchesCIDRsReactively(t *testing.T) {
	e := Entry{Path: "/tank/csi/fs/a", UUID: [16]byte{1}, CIDRs: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}}
	tbl := NewMemTable(rootTank(), e)
	if domain, _, ok := tbl.LookupClient(netip.MustParseAddr("10.1.2.3")); !ok || domain != "*" {
		t.Fatalf("lookup = %q, %v", domain, ok)
	}
	if _, _, ok := tbl.LookupClient(netip.MustParseAddr("192.0.2.1")); ok {
		t.Fatal("unauthorized client matched")
	}
}

func TestMemTableLookupUUID16(t *testing.T) {
	e := Entry{Path: "/tank/csi/fs/a", UUID: [16]byte{1, 2, 3}}
	if got, ok := NewMemTable(rootTank(), e).LookupPath(fsidTypeUUID16, e.UUID[:]); !ok || got.Path != e.Path {
		t.Fatalf("uuid lookup = %#v, %v", got, ok)
	}
}

// TestMemTableFSIDNumZeroResolvesOnlyToExplicitRoot replaces the legacy
// synthesis: a table without a Root entry fails closed for fsid=0, and a table
// with an explicit /tank Root resolves fsid=0 to that entry, never to "/".
func TestMemTableFSIDNumZeroResolvesOnlyToExplicitRoot(t *testing.T) {
	fsid := []byte{0, 0, 0, 0}
	if _, ok := NewMemTable().LookupPath(fsidTypeNum, fsid); ok {
		t.Fatal("empty table matched fsid=0")
	}
	nonRootOnly := NewMemTable(Entry{Path: "/tank/a", UUID: [16]byte{1}})
	if _, ok := nonRootOnly.LookupPath(fsidTypeNum, fsid); ok {
		t.Fatal("table without Root entry matched fsid=0")
	}
	got, ok := NewMemTable(rootTank()).LookupPath(fsidTypeNum, fsid)
	if !ok {
		t.Fatal("explicit root not found")
	}
	if got.Path != "/tank" {
		t.Fatalf("fsid=0 path = %q, want /tank", got.Path)
	}
	if !got.Root {
		t.Fatal("fsid=0 entry lacks Root flag")
	}
}

// TestMemTableRejectsEntryOutsideRoot enforces the component-boundary check
// including the /tank2 prefix attack: /tank2 must not be admitted as a sibling
// of /tank.
func TestMemTableRejectsEntryOutsideRoot(t *testing.T) {
	tbl := NewMemTable(rootTank())
	cases := []struct {
		name string
		path string
		ok   bool
	}{
		{"direct child", "/tank/csi", true},
		{"nested child", "/tank/csi/fs/vol", true},
		{"prefix attack /tank2", "/tank2", false},
		{"prefix attack /tank2/x", "/tank2/evil", false},
		{"unrelated /var", "/var/lib", false},
		{"collides with root", "/tank", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tbl.Upsert(Entry{Path: tc.path, UUID: [16]byte{1}})
			if tc.ok && err != nil {
				t.Fatalf("Upsert(%q) failed: %v", tc.path, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("Upsert(%q) admitted outside root", tc.path)
			}
		})
	}
}

// TestMemTableSecondDifferingRootRejected enforces one root per host nfsd.
func TestMemTableSecondDifferingRootRejected(t *testing.T) {
	tbl := NewMemTable(rootTank())
	if err := tbl.Upsert(Entry{Path: "/tank2", Root: true}); err == nil {
		t.Fatal("second differing root admitted")
	}
}

// TestMemTableHostRootRejected ensures the host overlayfs "/" can never become
// the NFS root.
func TestMemTableHostRootRejected(t *testing.T) {
	if err := NewMemTable().Upsert(Entry{Path: "/", Root: true, AccessMode: AccessRO}); err == nil {
		t.Fatal("host / admitted as Root")
	}
}

func TestMemTableUpsertBelowRootRejectsMissingRoot(t *testing.T) {
	tbl := NewMemTable()
	if err := tbl.UpsertBelowRoot(Entry{Path: "/tank/child"}); err == nil {
		t.Fatal("expected missing root rejection")
	}
	if _, ok := tbl.LookupExport("*", "/tank/child"); ok {
		t.Fatal("child became visible without root")
	}
}

func TestMemTableUpsertBelowRootAcceptsMatchingRoot(t *testing.T) {
	tbl := NewMemTable(Entry{Path: "/tank", Root: true, AccessMode: AccessRO})
	if err := tbl.UpsertBelowRoot(Entry{Path: "/tank/child"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.LookupExport("*", "/tank/child"); !ok {
		t.Fatal("child missing below registered root")
	}
}

// TestMemTablePseudoAncestorsOnlyBelowRoot verifies pseudo synthesis happens
// strictly below the registered root.
func TestMemTablePseudoAncestorsOnlyBelowRoot(t *testing.T) {
	tbl := NewMemTable(rootTank(), Entry{Path: "/tank/csi/fs/vol", UUID: [16]byte{1, 2, 3}})

	// Below root: synthesized.
	for _, p := range []string{"/tank/csi", "/tank/csi/fs"} {
		if _, ok := tbl.LookupExport("*", p); !ok {
			t.Fatalf("pseudo ancestor %q not synthesized", p)
		}
	}

	// At or above root: never synthesized, never returned.
	for _, p := range []string{"/", "/tank", "/var"} {
		if _, ok := tbl.LookupExport("*", p); p == "/tank" && ok {
			// /tank IS the root entry — real, not synthesized.
		} else if ok {
			t.Fatalf("pseudo/lookup for %q should not match", p)
		}
	}

	// /tank resolves as the real Root entry, not as a pseudo.
	e, ok := tbl.LookupExport("*", "/tank")
	if !ok || !e.Root || e.IsPseudo() {
		t.Fatalf("/tank lookup = %#v ok=%v (want real Root)", e, ok)
	}
}

func TestMemTableLookupRealExportRejectsSyntheticParent(t *testing.T) {
	tbl := NewMemTable(rootTank(), Entry{Path: "/tank/csi/a", UUID: [16]byte{1}})
	if got, ok := tbl.LookupExport("*", "/tank/csi"); !ok || !got.IsPseudo() {
		t.Fatalf("synthetic ancestor = %#v, %v", got, ok)
	}
	if _, ok := tbl.LookupRealExport("*", "/tank/csi"); ok {
		t.Fatal("synthetic ancestor returned as real export")
	}
	if got, ok := tbl.LookupRealExport("*", "/tank/csi/a"); !ok || got.IsPseudo() {
		t.Fatalf("real export = %#v, %v", got, ok)
	}
}

// TestMemTableRemoveRootClearsIdentity verifies that withdrawing the root
// entry removes the fsid=0 binding.
func TestMemTableRemoveRootClearsIdentity(t *testing.T) {
	tbl := NewMemTable(rootTank())
	if _, ok := tbl.LookupPath(fsidTypeNum, []byte{0, 0, 0, 0}); !ok {
		t.Fatal("root not found before removal")
	}
	if _, ok := tbl.RemoveRoot(); !ok {
		t.Fatal("RemoveRoot returned false")
	}
	if _, ok := tbl.LookupPath(fsidTypeNum, []byte{0, 0, 0, 0}); ok {
		t.Fatal("fsid=0 still resolved after root removal")
	}
}
