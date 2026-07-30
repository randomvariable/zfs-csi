// Package nfsexport implements an in-process responder for the Linux sunrpc
// cache channels that nfsd uses to resolve NFS export authorization
// (auth.unix.ip, nfsd.fh/expkey, nfsd.export).
//
// It exists because OpenZFS `sharenfs` cannot express `xprtsec=mtls` in any
// grammar, so TLS datasets are exported with `sharenfs=off` and this package
// answers the kernel's export/auth upcalls directly — with no os/exec and no
// dependency on rpc.mountd or exportfs.
//
// This file implements the qword wire codec used by every cache channel. The
// encoding mirrors the kernel writer/reader in net/sunrpc/cache.c:
//   - qword_add:    string_escape_str(str, ESCAPE_OCTAL, "\\ \n\t") + ' '
//     i.e. backslash, space, newline and tab are escaped as \NNN octal; every
//     token is followed by a single space.
//   - qword_addhex: "\x" prefix followed by lowercase hex bytes, then ' '.
//   - qword_get:    reverse of the above, splitting on spaces, unescaping
//     \NNN octal and \xHH... hex, terminating at newline.
package nfsexport

import (
	"errors"
	"fmt"
	"strings"
)

// ErrMalformedQword indicates a request line could not be tokenized per the
// sunrpc cache qword framing.
var ErrMalformedQword = errors.New("nfsexport: malformed qword line")

// needsOctalEscape reports whether a byte must be octal-escaped by qword_add.
// The kernel escapes backslash, space, newline and tab, plus any byte that
// string_escape_str with ESCAPE_OCTAL would escape (non-printable / non-ASCII).
func needsOctalEscape(b byte) bool {
	switch b {
	case '\\', ' ', '\n', '\t':
		return true
	}
	// ESCAPE_OCTAL escapes bytes outside the printable ASCII range.
	return b < 0x20 || b > 0x7e
}

// appendQword appends one string token to b using qword_add semantics
// (octal-escaped, trailing space).
func appendQword(b *strings.Builder, tok string) {
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		if needsOctalEscape(c) {
			// \NNN three-digit octal, matching string_escape_str ESCAPE_OCTAL.
			fmt.Fprintf(b, "\\%03o", c)
			continue
		}
		b.WriteByte(c)
	}
	b.WriteByte(' ')
}

// appendQwordHex appends a raw byte slice as a "\x<hex>" token with a trailing
// space, matching qword_addhex.
func appendQwordHex(b *strings.Builder, raw []byte) {
	b.WriteString("\\x")
	const hexdigits = "0123456789abcdef"
	for _, c := range raw {
		b.WriteByte(hexdigits[c>>4])
		b.WriteByte(hexdigits[c&0x0f])
	}
	b.WriteByte(' ')
}

// appendQwordInt appends a base-10 integer token (qword_add of the decimal
// string).
func appendQwordInt(b *strings.Builder, v int64) {
	appendQword(b, fmt.Sprintf("%d", v))
}

// parseQwords tokenizes one cache-channel request line into its decoded tokens.
// It mirrors the kernel qword_get: tokens are space-separated; a leading "\x"
// marks a hex blob; otherwise \NNN octal and \xHH byte escapes are decoded.
// The trailing newline (if present) is stripped before tokenizing.
//
// Each returned token carries whether it was hex-encoded so callers that need
// the raw bytes (e.g. the expkey fsid) can recover them exactly.
func parseQwords(line string) ([]token, error) {
	line = strings.TrimSuffix(line, "\n")
	var out []token
	i := 0
	for i < len(line) {
		// Skip run of separating spaces.
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}
		tok, next, err := parseOneQword(line, i)
		if err != nil {
			return nil, err
		}
		out = append(out, tok)
		i = next
	}
	return out, nil
}

// token is a single decoded qword. Raw always holds the decoded bytes; Hex
// records whether the source token used the "\x" hex form (so an all-hex fsid
// is distinguishable from a decimal string that happens to be hex digits).
type token struct {
	Raw []byte
	Hex bool
}

func (t token) String() string { return string(t.Raw) }

func parseOneQword(line string, start int) (token, int, error) {
	// Hex blob form: "\x" followed by an even number of hex digits.
	if strings.HasPrefix(line[start:], "\\x") {
		j := start + 2
		var raw []byte
		for j+1 < len(line)+1 && j < len(line) && line[j] != ' ' {
			hi, ok1 := hexVal(line[j])
			if !ok1 || j+1 >= len(line) {
				return token{}, 0, fmt.Errorf("%w: bad hex token at %d", ErrMalformedQword, start)
			}
			lo, ok2 := hexVal(line[j+1])
			if !ok2 {
				return token{}, 0, fmt.Errorf("%w: bad hex token at %d", ErrMalformedQword, start)
			}
			raw = append(raw, hi<<4|lo)
			j += 2
		}
		return token{Raw: raw, Hex: true}, j, nil
	}

	// Plain (octal-escaped) token.
	var raw []byte
	j := start
	for j < len(line) && line[j] != ' ' {
		if line[j] == '\\' {
			if j+1 >= len(line) {
				return token{}, 0, fmt.Errorf("%w: dangling escape at %d", ErrMalformedQword, j)
			}
			switch line[j+1] {
			case 'x':
				// \xHH single-byte hex escape inside a plain token.
				if j+3 >= len(line) {
					return token{}, 0, fmt.Errorf("%w: short \\x escape at %d", ErrMalformedQword, j)
				}
				hi, ok1 := hexVal(line[j+2])
				lo, ok2 := hexVal(line[j+3])
				if !ok1 || !ok2 {
					return token{}, 0, fmt.Errorf("%w: bad \\x escape at %d", ErrMalformedQword, j)
				}
				raw = append(raw, hi<<4|lo)
				j += 4
			default:
				// \NNN octal escape (exactly three octal digits per kernel writer).
				if j+3 >= len(line) {
					return token{}, 0, fmt.Errorf("%w: short octal escape at %d", ErrMalformedQword, j)
				}
				var v byte
				for k := 1; k <= 3; k++ {
					d := line[j+k]
					if d < '0' || d > '7' {
						return token{}, 0, fmt.Errorf("%w: bad octal escape at %d", ErrMalformedQword, j)
					}
					v = v<<3 | (d - '0')
				}
				raw = append(raw, v)
				j += 4
			}
			continue
		}
		raw = append(raw, line[j])
		j++
	}
	return token{Raw: raw, Hex: false}, j, nil
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
