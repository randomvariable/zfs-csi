package nfsexport

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/netip"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type AccessMode string

const (
	AccessRW AccessMode = "rw"
	AccessRO AccessMode = "ro"
)

var pseudoNamespace = [16]byte{0x39, 0xc6, 0xb5, 0xc1, 0x3f, 0x24, 0x4f, 0x4e, 0x97, 0x7c, 0x7f, 0xe6, 0x54, 0x6b, 0x8a, 0x25}

// Entry is one real filesystem export. UUID must be the 16-byte identity
// derived by the caller from statfs.f_fsid.
type Entry struct {
	Path string
	// FSID is retained as a compatibility key for existing reconcilers. New
	// callers must provide UUID; FSID is never used for non-root wire identity.
	FSID       uint32
	UUID       [16]byte
	CIDRs      []netip.Prefix
	AccessMode AccessMode
	TLS        bool
	// Root marks the explicit host filesystem used as the NFSv4 pseudo-root.
	// Root identity must never be inferred from Path == "/".
	Root bool

	// pseudo marks synthetic NFSv4 namespace entries returned by lookups.
	// It is intentionally not part of the caller-facing export contract.
	pseudo bool
}

// IsPseudo reports whether entry was synthesized for an NFSv4 namespace path.
func (e Entry) IsPseudo() bool { return e.pseudo }

func (e Entry) DomainName() string { return "*" }

// exportFlags returns the nfsd.export flags for a real (non-pseudo) entry.
// The explicit root entry marshals with the full V4ROOT flag set including
// FSID, READONLY, ROOTSQUASH, INSECURE_PORT, and NOSUBTREECHECK.
func (e Entry) exportFlags() int {
	if e.Root {
		return nfsexpV4Root | nfsexpFSID | nfsexpReadOnly | nfsexpRootSquash | nfsexpInsecurePort | nfsexpNoSubtreeChk
	}
	flags := nfsexpRootSquash | nfsexpNoSubtreeChk
	if e.AccessMode == AccessRO {
		flags |= nfsexpReadOnly
	}
	return flags
}

func pseudoUUID(path string) [16]byte {
	h := sha1.New()
	_, _ = h.Write(pseudoNamespace[:])
	_, _ = h.Write([]byte(path))
	sum := h.Sum(nil)
	var out [16]byte
	copy(out[:], sum[:16])
	out[6] = (out[6] & 0x0f) | 0x50
	out[8] = (out[8] & 0x3f) | 0x80
	return out
}

// PseudoUUID returns UUIDv5 identity for an NFSv4 pseudo-filesystem path.
func PseudoUUID(path string) [16]byte { return pseudoUUID(path) }

// UUIDFromStatFS converts OpenZFS statfs.f_fsid words to nfs-utils' UUID16.
// The two words are rendered as eight hexadecimal bytes, followed by zeros.
func UUIDFromStatFS(low, high uint32) [16]byte {
	var out [16]byte
	var text [16]byte
	for i, v := range []uint32{low, high} {
		s := fmt.Sprintf("%08x", v)
		copy(text[i*8:], s)
	}
	_, _ = hex.Decode(out[:8], text[:])
	return out
}

func uuidBytes(u [16]byte) []byte { return append([]byte(nil), u[:]...) }

// fhBytes is the raw fsid representation used by nfsd.fh's cache channel.
func fhBytes(u [16]byte) []byte {
	return uuidBytes(u)
}

type ExportTable interface {
	LookupClient(netip.Addr) (string, Entry, bool)
	LookupPath(int, []byte) (Entry, bool)
	LookupExport(string, string) (Entry, bool)
}

// MemTable is the authoritative in-process export table. At most one entry
// may carry Root=true; that entry's path is the table root and is the only
// identity that resolves to fsid=0.
type MemTable struct {
	mu      sync.RWMutex
	entries map[string]Entry
	root    string
}

// NewMemTable seeds a table. The first Root entry wins; subsequent conflicting
// Root entries are rejected silently (use Upsert directly to surface errors).
func NewMemTable(entries ...Entry) *MemTable {
	t := &MemTable{entries: make(map[string]Entry)}
	for _, e := range entries {
		_ = t.Upsert(e)
	}
	return t
}

// Replace wholesale swaps the table contents. It is intended for tests that
// install a known full state; production code uses Upsert for incremental
// validation. At most one entry in the slice may carry Root.
func (t *MemTable) Replace(entries []Entry) {
	cp := make(map[string]Entry, len(entries))
	root := ""
	for _, e := range entries {
		e.Path = filepath.Clean(e.Path)
		if e.Root {
			root = e.Path
		}
		cp[e.Path] = e
	}
	t.mu.Lock()
	t.entries = cp
	t.root = root
	t.mu.Unlock()
}

// Root returns the registered root path and a presence flag.
func (t *MemTable) Root() (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.root, t.root != ""
}

// Upsert inserts or updates a single entry. The first Root entry establishes
// the table root; once root is set, a different Root path is rejected, as is
// any non-root entry whose path lies outside the root (component-boundary
// check).
func (t *MemTable) Upsert(e Entry) error {
	e.Path = filepath.Clean(e.Path)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.upsertLocked(e, false)
}

// UpsertBelowRoot inserts a production child export only when an explicit root
// is already present. Upsert retains rootless support for reconstruction and
// focused table tests; normal reconciler registration uses this stricter API.
func (t *MemTable) UpsertBelowRoot(e Entry) error {
	e.Path = filepath.Clean(e.Path)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.upsertLocked(e, true)
}

func (t *MemTable) upsertLocked(e Entry, requireRoot bool) error {
	if requireRoot && t.root == "" {
		return fmt.Errorf("nfsexport: child export %q requires explicit root", e.Path)
	}
	if err := validateEntry(t.root, e); err != nil {
		return err
	}
	if e.Root {
		if t.root != "" && t.root != e.Path {
			return fmt.Errorf("nfsexport: root already %q; cannot accept %q", t.root, e.Path)
		}
		t.root = e.Path
	}
	if t.entries == nil {
		t.entries = make(map[string]Entry)
	}
	t.entries[e.Path] = e
	return nil
}

// validateEntry enforces the table invariants without taking the mutex. The
// root argument is the currently-registered root ("" if none). When the entry
// is itself a Root entry, it must be absolute and must not be the host "/".
// When root is already registered, a non-root entry must lie strictly below
// the root on a path-component boundary. When no root is registered, any
// absolute path is accepted (test/migration mode).
func validateEntry(root string, e Entry) error {
	path := filepath.Clean(e.Path)
	if !filepath.IsAbs(path) {
		return fmt.Errorf("nfsexport: export path %q is not absolute", e.Path)
	}
	if e.Root {
		if path == "/" {
			return fmt.Errorf("nfsexport: host root cannot be NFS root")
		}
		if path != e.Path || e.AccessMode != AccessRO || e.TLS || len(e.CIDRs) != 0 || e.UUID != ([16]byte{}) || e.FSID != 0 {
			return fmt.Errorf("nfsexport: malformed desired root entry for %q", e.Path)
		}
		return nil
	}
	if root == "" {
		return nil
	}
	if path == root {
		return fmt.Errorf("nfsexport: export path %q collides with root", path)
	}
	if !strings.HasPrefix(path, root+string(filepath.Separator)) {
		return fmt.Errorf("nfsexport: export path %q is outside root %q", path, root)
	}
	return nil
}

// Remove deletes the entry matching key (path string or FSID uint32). Removing
// the root entry clears the table's root identity. Returns whether anything
// was removed.
func (t *MemTable) Remove(key any) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for path, e := range t.entries {
		if path == key || (e.FSID != 0 && e.FSID == key) {
			delete(t.entries, path)
			if e.Root {
				t.root = ""
			}
			return true
		}
	}
	return false
}

// RemoveRoot deletes the root entry (if any) and clears root identity. It
// returns the removed root entry and a presence flag.
func (t *MemTable) RemoveRoot() (Entry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.root == "" {
		return Entry{}, false
	}
	e, ok := t.entries[t.root]
	if !ok {
		t.root = ""
		return Entry{}, false
	}
	delete(t.entries, t.root)
	t.root = ""
	return e, true
}

func (t *MemTable) LookupClient(addr netip.Addr) (string, Entry, bool) {
	if addr.Is4In6() {
		addr = addr.Unmap()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	bestBits := -1
	var best Entry
	for _, e := range t.entries {
		for _, p := range e.CIDRs {
			if p.Contains(addr) && p.Bits() > bestBits {
				best, bestBits = e, p.Bits()
			}
		}
	}
	return best.DomainName(), best, bestBits >= 0
}

func (t *MemTable) LookupPath(ft int, fsid []byte) (Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if ft == fsidTypeNum && len(fsid) == 4 && binary.BigEndian.Uint32(fsid) == 0 {
		// fsid=0 resolves only to the explicit root entry. There is no
		// synthesis: a table without a registered Root entry fails closed.
		if e, ok := t.entries[t.root]; ok && e.Root {
			return e, true
		}
		return Entry{}, false
	}
	if ft != fsidTypeUUID16 && ft != fsidTypeUUID16Inum {
		return Entry{}, false
	}
	if len(fsid) != keyLen(ft) {
		return Entry{}, false
	}
	if ft == fsidTypeUUID16Inum {
		return Entry{}, false
	}
	key := fsid
	for _, e := range t.entries {
		if string(e.UUID[:]) == string(key) {
			return e, true
		}
		for _, path := range pathComponentsBelow(t.root, e.Path) {
			u := pseudoUUID(path)
			if string(u[:]) == string(key) {
				return Entry{Path: path, UUID: u, pseudo: true}, true
			}
		}
	}
	return Entry{}, false
}

func (t *MemTable) LookupExport(domain, path string) (Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if domain != "*" {
		return Entry{}, false
	}
	path = filepath.Clean(path)
	if e, ok := t.entries[path]; ok {
		return e, true
	}
	for _, e := range t.entries {
		for _, component := range pathComponentsBelow(t.root, e.Path) {
			if component == path && path != e.Path {
				u := pseudoUUID(path)
				return Entry{Path: path, UUID: u, AccessMode: AccessRO, pseudo: true}, true
			}
		}
	}
	return Entry{}, false
}

// LookupRealExport returns only an explicitly registered export. It never
// synthesizes NFSv4 namespace entries.
func (t *MemTable) LookupRealExport(domain, path string) (Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if domain != "*" {
		return Entry{}, false
	}
	e, ok := t.entries[filepath.Clean(path)]
	return e, ok && !e.IsPseudo()
}

func sortedCIDRs(in []netip.Prefix) []netip.Prefix {
	out := append([]netip.Prefix(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// pathComponents returns every prefix of path starting from "/". Used by the
// channel writer when invalidating kernel cache entries along the export path.
func pathComponents(path string) []string {
	if path == "/" {
		return []string{"/"}
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	out := []string{"/"}
	cur := ""
	for _, p := range parts {
		cur += "/" + p
		out = append(out, cur)
	}
	return out
}

// pathComponentsBelow returns the strict descendants of root on the path to
// child. It returns nil if root is empty, child equals root, or child is not
// actually below root. Pseudo synthesis is only ever generated for these
// descendants — never for the root itself and never for "/".
func pathComponentsBelow(root, child string) []string {
	if root == "" || child == root || !strings.HasPrefix(child, root+string(filepath.Separator)) {
		return nil
	}
	rel := strings.TrimPrefix(child, root+string(filepath.Separator))
	out := make([]string, 0, strings.Count(rel, "/")+1)
	cur := root
	for _, part := range strings.Split(rel, "/") {
		cur += "/" + part
		out = append(out, cur)
	}
	return out
}

// FSIDFromString is deprecated compatibility for callers migrating to UUID16.
func FSIDFromString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func clientAddrFromString(s string) (netip.Addr, error) {
	a, err := netip.ParseAddr(s)
	if err == nil && a.Is4In6() {
		a = a.Unmap()
	}
	return a, err
}

func (e Entry) fsidBytes() []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], e.FSID)
	return b[:]
}
