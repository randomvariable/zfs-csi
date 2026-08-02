package nfsexport

import (
	"net/netip"
	"strconv"
	"strings"
	"testing"
)

func TestMarshalExportAnswerGoldenLines(t *testing.T) {
	const exp = int64(123)
	cases := []struct {
		name       string
		entry      Entry
		path, want string
	}{
		{"explicit root", Entry{Path: "/tank", Root: true, AccessMode: AccessRO}, "/tank", "* /tank 123 74759 65534 65534 0 secinfo 1 1 74759\n"},
		{"pseudo non-root", Entry{Path: "/tank/csi", UUID: [16]byte{1, 2}, pseudo: true}, "/tank/csi", "* /tank/csi 123 66563 65534 65534 0 secinfo 1 1 66563 uuid \\x01020000000000000000000000000000\n"},
		{"real ro", Entry{Path: "/tank/a", UUID: [16]byte{1, 2}, AccessMode: AccessRO}, "/tank/a", "* /tank/a 123 1029 65534 65534 0 secinfo 1 1 1029 uuid \\x01020000000000000000000000000000\n"},
		{"real rw", Entry{Path: "/tank/a", UUID: [16]byte{1, 2}}, "/tank/a", "* /tank/a 123 1028 65534 65534 0 secinfo 1 1 1028 uuid \\x01020000000000000000000000000000\n"},
		{"mtls", Entry{Path: "/tank/a", UUID: [16]byte{1, 2}, TLS: true}, "/tank/a", "* /tank/a 123 1028 65534 65534 0 secinfo 1 1 1028 uuid \\x01020000000000000000000000000000 xprtsec 1 4\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := marshalExportAnswer(tc.entry, "*", tc.path, exp, false); got != tc.want {
				t.Fatalf("wire = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestMarshalExportAnswerRootFlags verifies the explicit root entry marshals
// with the full root flag set (V4ROOT|FSID|READONLY|ROOTSQUASH|INSECURE_PORT|
// NOSUBTREECHECK) and that AUTH_SYS secinfo flavor flags mirror the export
// flags exactly so a flavor lookup cannot widen authorization.
func TestMarshalExportAnswerRootFlags(t *testing.T) {
	const exp = int64(123)
	out := marshalExportAnswer(Entry{Path: "/tank", Root: true}, "*", "/tank", exp, false)
	fields := strings.Fields(out)
	flags, err := strconv.Atoi(fields[3])
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	want := nfsexpV4Root | nfsexpFSID | nfsexpReadOnly | nfsexpRootSquash | nfsexpInsecurePort | nfsexpNoSubtreeChk
	if flags != want {
		t.Fatalf("root flags = %#x, want %#x", flags, want)
	}
	flavorFlags, err := strconv.Atoi(fields[10])
	if err != nil {
		t.Fatalf("parse flavor flags: %v", err)
	}
	if flavorFlags != flags {
		t.Fatalf("AUTH_SYS flavor flags = %#x, export flags = %#x", flavorFlags, flags)
	}
	for _, forbidden := range []string{"uuid", "xprtsec"} {
		if strings.Contains(out, forbidden) {
			t.Fatalf("root marshal leaked %q: %q", forbidden, out)
		}
	}
}

func TestMarshalExportAnswerCanDisableRootSquash(t *testing.T) {
	e := Entry{Path: "/tank/migration", UUID: [16]byte{1}, NoRootSquash: true}
	line := marshalExportAnswer(e, "*", e.Path, 123, false)
	fields := strings.Fields(line)
	if len(fields) < 4 {
		t.Fatalf("short answer: %q", line)
	}
	flags, err := strconv.Atoi(fields[3])
	if err != nil {
		t.Fatal(err)
	}
	if flags&nfsexpRootSquash != 0 {
		t.Fatalf("flags %#x unexpectedly contain root_squash", flags)
	}
}

func TestMarshalExportAnswerSecinfoRetainsAuthorizationFlags(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry Entry
		ro    bool
	}{
		{name: "read-only", entry: Entry{Path: "/tank/a", AccessMode: AccessRO}, ro: true},
		{name: "read-write", entry: Entry{Path: "/tank/a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := strings.Fields(marshalExportAnswer(tc.entry, "*", tc.entry.Path, 123, false))
			flags, err := strconv.Atoi(fields[3])
			if err != nil {
				t.Fatal(err)
			}
			flavorFlags, err := strconv.Atoi(fields[10])
			if err != nil {
				t.Fatal(err)
			}
			if flavorFlags != flags {
				t.Fatalf("AUTH_SYS flavor flags = %#x, export flags = %#x", flavorFlags, flags)
			}
			if flags&nfsexpRootSquash == 0 || flavorFlags&nfsexpRootSquash == 0 {
				t.Fatalf("ROOTSQUASH missing from export %#x or flavor %#x", flags, flavorFlags)
			}
			if (flags&nfsexpReadOnly != 0) != tc.ro || (flavorFlags&nfsexpReadOnly != 0) != tc.ro {
				t.Fatalf("READONLY mismatch for export %#x or flavor %#x", flags, flavorFlags)
			}
		})
	}
}

func TestResponderWorldDomainAndAuthSys(t *testing.T) {
	e := Entry{Path: "/tank/csi/fs/vol", UUID: [16]byte{1}, CIDRs: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}, AccessMode: AccessRW}
	r := NewResponder(NewMemTable(rootTank(), e))
	got, err := r.Answer(ChannelAuthUnixIP, "nfsd 10.1.2.3\n")
	if err != nil {
		t.Fatal(err)
	}
	if got[len(got)-2:] != "*\n" {
		t.Fatalf("auth domain = %q, want world domain", got)
	}
	got, err = r.Answer(ChannelExport, "* /tank/csi/fs/vol\n")
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Fatal("empty export answer")
	}
}

func TestResponderFirstClientOnDemandOrdering(t *testing.T) {
	e := Entry{Path: "/tank/a", UUID: [16]byte{1}, CIDRs: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}}
	r := NewResponder(NewMemTable(rootTank(), e))

	auth, err := r.Answer(ChannelAuthUnixIP, "nfsd 10.1.2.3\n")
	if err != nil || !strings.HasSuffix(auth, " *\n") {
		t.Fatalf("authorized auth answer = %q, %v", auth, err)
	}
	export, err := r.Answer(ChannelExport, "* /tank/a\n")
	if err != nil || len(strings.Fields(export)) <= 3 {
		t.Fatalf("export answer = %q, %v", export, err)
	}
	fh, err := r.Answer(ChannelExpKey, "* 6 \\x01000000000000000000000000000000\n")
	if err != nil || !strings.HasSuffix(fh, " /tank/a\n") {
		t.Fatalf("fh answer = %q, %v", fh, err)
	}
	denied, err := r.Answer(ChannelAuthUnixIP, "nfsd 192.0.2.1\n")
	if err != nil || len(strings.Fields(denied)) != 3 {
		t.Fatalf("unauthorized auth answer = %q, %v", denied, err)
	}
}

func TestMemTableUUID16InumFailsClosed(t *testing.T) {
	e := Entry{Path: "/tank/a", UUID: [16]byte{1, 2, 3}}
	fsid := append(append([]byte(nil), e.UUID[:]...), make([]byte, 8)...)
	if _, ok := NewMemTable(rootTank(), e).LookupPath(fsidTypeUUID16Inum, fsid); ok {
		t.Fatal("inode-bearing fsid unexpectedly matched")
	}
}

// TestResponderRootExpKeyFollowsExplicitRoot verifies the fsid=0 expkey upcall
// resolves to the explicit /tank root when registered, and fails closed when
// no root is present (no synthesis).
func TestResponderRootExpKeyFollowsExplicitRoot(t *testing.T) {
	withRoot := NewResponder(NewMemTable(rootTank()))
	positive, err := withRoot.Answer(ChannelExpKey, "* 1 \\x00000000")
	if err != nil || !strings.HasSuffix(positive, " /tank\n") {
		t.Fatalf("root positive = %q, %v", positive, err)
	}
	empty := NewResponder(NewMemTable())
	negative, err := empty.Answer(ChannelExpKey, "* 1 \\x00000000")
	if err != nil || strings.Contains(negative, " /") {
		t.Fatalf("root negative = %q, %v", negative, err)
	}
}
func TestUUIDFromStatFS(t *testing.T) {
	got := UUIDFromStatFS(0x1705c40d, 0x0034a626)
	want := [16]byte{0x17, 0x05, 0xc4, 0x0d, 0x00, 0x34, 0xa6, 0x26}
	if got != want {
		t.Fatalf("statfs UUID = %x, want %x", got, want)
	}
}
