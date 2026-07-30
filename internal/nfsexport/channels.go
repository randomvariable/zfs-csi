package nfsexport

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

// Channel identifies one of the three sunrpc cache channels the responder
// services.
type Channel int

const (
	// ChannelAuthUnixIP is /proc/net/rpc/auth.unix.ip/channel.
	ChannelAuthUnixIP Channel = iota
	// ChannelExpKey is /proc/net/rpc/nfsd.fh/channel (expkey).
	ChannelExpKey
	// ChannelExport is /proc/net/rpc/nfsd.export/channel.
	ChannelExport
)

// defaultExpiry is how far in the future answered cache entries are valid. The
// kernel refreshes entries at roughly half their age, so a modest lifetime
// keeps client-IP/domain bindings fresh without excessive upcall churn.
const defaultExpiry = 30 * time.Minute

// nowFunc is overridable in tests.
var nowFunc = time.Now

func expiryUnix() int64 {
	return nowFunc().Add(defaultExpiry).Unix()
}

// --- auth.unix.ip ---------------------------------------------------------

// ipMapRequest is a kernel request on auth.unix.ip: "<class> <ipaddr>".
// ip_map_request (net/sunrpc/svcauth_unix.c:161).
type ipMapRequest struct {
	Class string
	Addr  netip.Addr
}

func parseIPMapRequest(toks []token) (ipMapRequest, error) {
	if len(toks) < 2 {
		return ipMapRequest{}, fmt.Errorf("%w: auth.unix.ip request needs class+addr", ErrMalformedQword)
	}
	addr, err := netip.ParseAddr(toks[1].String())
	if err != nil {
		return ipMapRequest{}, fmt.Errorf("%w: bad ip %q", ErrMalformedQword, toks[1].String())
	}
	return ipMapRequest{Class: toks[0].String(), Addr: addr}, nil
}

// marshalIPMapAnswer renders the positive answer
// "<class> <ipaddr> <expiry> <domain>" per ip_map_parse
// (net/sunrpc/svcauth_unix.c:203-...). A negative answer omits the domain
// token, leaving an empty trailing field (dom==NULL).
func marshalIPMapAnswer(class string, addr netip.Addr, expiry int64, domain string, negative bool) string {
	var b strings.Builder
	appendQword(&b, class)
	appendQword(&b, addr.String())
	appendQwordInt(&b, expiry)
	if !negative {
		appendQword(&b, domain)
	}
	return finishLine(&b)
}

// --- nfsd.fh / expkey -----------------------------------------------------

// expKeyRequest is a kernel request on nfsd.fh:
// "<domain> <fsidtype> <\xfsid>". expkey_request (fs/nfsd/export.c writer).
type expKeyRequest struct {
	Domain   string
	FSIDType int
	FSID     []byte
}

func parseExpKeyRequest(toks []token) (expKeyRequest, error) {
	if len(toks) < 3 {
		return expKeyRequest{}, fmt.Errorf("%w: nfsd.fh request needs domain+fsidtype+fsid", ErrMalformedQword)
	}
	ft, err := strconv.Atoi(toks[1].String())
	if err != nil {
		return expKeyRequest{}, fmt.Errorf("%w: bad fsidtype %q", ErrMalformedQword, toks[1].String())
	}
	return expKeyRequest{Domain: toks[0].String(), FSIDType: ft, FSID: toks[2].Raw}, nil
}

// marshalExpKeyAnswer renders "<domain> <fsidtype> <\xfsid> <expiry> <path>"
// per expkey_parse (fs/nfsd/export.c:81-160). A negative answer omits the
// path token (empty path => CACHE_NEGATIVE).
func marshalExpKeyAnswer(domain string, fsidType int, fsid []byte, expiry int64, path string, negative bool) string {
	var b strings.Builder
	appendQword(&b, domain)
	appendQwordInt(&b, int64(fsidType))
	appendQwordHex(&b, fsid)
	appendQwordInt(&b, expiry)
	if !negative {
		appendQword(&b, path)
	}
	return finishLine(&b)
}

// --- nfsd.export ----------------------------------------------------------

// exportRequest is a kernel request on nfsd.export: "<domain> <path>".
// svc_export_request (fs/nfsd/export.c writer).
type exportRequest struct {
	Domain string
	Path   string
}

func parseExportRequest(toks []token) (exportRequest, error) {
	if len(toks) < 2 {
		return exportRequest{}, fmt.Errorf("%w: nfsd.export request needs domain+path", ErrMalformedQword)
	}
	return exportRequest{Domain: toks[0].String(), Path: toks[1].String()}, nil
}

// marshalExportAnswer renders the positive nfsd.export answer per
// svc_export_parse (fs/nfsd/export.c:1309-1400):
//
//	<domain> <path> <expiry> <flags> <anonuid> <anongid> <fsid> [secinfo ...] [uuid ...] [xprtsec ...]
//
// A negative answer emits "<domain> <path> <expiry>" with no flags token,
// which svc_export_parse treats as CACHE_NEGATIVE (get_int -> -ENOENT at the
// flags step, export.c:1356-1359).
//
// Root behavior is keyed on the entry's explicit Root state, never on a
// literal "/" path. The root entry carries numeric fsid=0, the full
// V4ROOT|FSID|READONLY|ROOTSQUASH|INSECURE_PORT|NOSUBTREECHECK flag set, and
// no UUID — matching what nfsd.export expects of a pseudo-root established by
// nfs-utils' exportfs. AUTH_SYS secinfo flavor flags mirror the export flags
// exactly so a flavor lookup cannot widen authorization (#3696).
func marshalExportAnswer(e Entry, domain, path string, expiry int64, negative bool) string {
	var b strings.Builder
	appendQword(&b, domain)
	appendQword(&b, path)
	appendQwordInt(&b, expiry)
	if negative {
		return finishLine(&b)
	}
	flags := e.exportFlags()
	if e.pseudo {
		flags = nfsexpReadOnly | nfsexpInsecurePort | nfsexpNoSubtreeChk | nfsexpV4Root
	}
	appendQwordInt(&b, int64(flags))
	// anon uid/gid: root_squash maps to the kernel default anon (65534). The
	// values are only consulted for squashed identities.
	appendQwordInt(&b, 65534)
	appendQwordInt(&b, 65534)
	// numeric fsid: root uses 0 (FSID_NUM with nfsexpFSID); pseudo/real use 0
	// here because their UUID carries identity via the nfsd.fh channel.
	appendQwordInt(&b, 0)
	appendQword(&b, "secinfo")
	appendQwordInt(&b, 1)
	appendQwordInt(&b, 1)
	// Kernel nfsexp_flags returns full export flags as AUTH_SYS flavor flags.
	appendQwordInt(&b, int64(flags))
	if e.Root {
		// Root carries numeric fsid=0 and no UUID identity.
		return finishLine(&b)
	}
	appendQword(&b, "uuid")
	appendQwordHex(&b, e.UUID[:])
	if !e.pseudo && e.TLS {
		appendQword(&b, "xprtsec")
		appendQwordInt(&b, 1)
		appendQwordInt(&b, xprtsecMTLS)
	}
	return finishLine(&b)
}

// finishLine trims the trailing separator space appendQword leaves and
// terminates the line with '\n' (the kernel parsers require the final byte to
// be a newline; svc_export_parse:1318 / expkey_parse:92 / ip_map_parse).
func finishLine(b *strings.Builder) string {
	s := strings.TrimRight(b.String(), " ")
	return s + "\n"
}
