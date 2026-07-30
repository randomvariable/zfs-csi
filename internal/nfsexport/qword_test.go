package nfsexport

import (
	"bytes"
	"strings"
	"testing"
)

func TestAppendQwordEscaping(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string // token bytes without the trailing space
	}{
		{"plain", "nfsd", "nfsd"},
		{"path", "/tank/csi/fs/vol1", "/tank/csi/fs/vol1"},
		{"space", "a b", `a\040b`},
		{"backslash", `a\b`, `a\134b`},
		{"tab", "a\tb", `a\011b`},
		{"newline", "a\nb", `a\012b`},
		{"nonprint", string([]byte{0x01}), `\001`},
		{"highbyte", string([]byte{0xff}), `\377`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			appendQword(&b, tc.in)
			got := b.String()
			want := tc.want + " "
			if got != want {
				t.Fatalf("appendQword(%q) = %q, want %q", tc.in, got, want)
			}
		})
	}
}

func TestAppendQwordHex(t *testing.T) {
	var b strings.Builder
	appendQwordHex(&b, []byte{0x00, 0x01, 0xab, 0xff})
	got := b.String()
	want := `\x0001abff `
	if got != want {
		t.Fatalf("appendQwordHex = %q, want %q", got, want)
	}
}

func TestParseQwordsRoundTrip(t *testing.T) {
	// Encode a mix of plain, escaped and hex tokens, then decode.
	var b strings.Builder
	appendQword(&b, "nfsd")
	appendQword(&b, "a b")                 // space -> \040
	appendQwordHex(&b, []byte{0xde, 0xad}) // hex blob
	appendQword(&b, "/tank/x")
	line := finishLine(&b)

	toks, err := parseQwords(line)
	if err != nil {
		t.Fatalf("parseQwords: %v", err)
	}
	if len(toks) != 4 {
		t.Fatalf("got %d tokens, want 4: %#v", len(toks), toks)
	}
	if toks[0].String() != "nfsd" {
		t.Errorf("tok0 = %q", toks[0].String())
	}
	if toks[1].String() != "a b" {
		t.Errorf("tok1 = %q, want %q", toks[1].String(), "a b")
	}
	if toks[2].Hex != true || !bytes.Equal(toks[2].Raw, []byte{0xde, 0xad}) {
		t.Errorf("tok2 = %#v, want hex deadbytes", toks[2])
	}
	if toks[3].String() != "/tank/x" {
		t.Errorf("tok3 = %q", toks[3].String())
	}
}

func TestParseQwordsOctalEscape(t *testing.T) {
	// The kernel writer emits three-digit octal; ensure we decode it.
	toks, err := parseQwords(`a\040b \134c` + "\n")
	if err != nil {
		t.Fatalf("parseQwords: %v", err)
	}
	if len(toks) != 2 {
		t.Fatalf("got %d tokens, want 2", len(toks))
	}
	if toks[0].String() != "a b" {
		t.Errorf("tok0 = %q, want %q", toks[0].String(), "a b")
	}
	if toks[1].String() != `\c` {
		t.Errorf("tok1 = %q, want %q", toks[1].String(), `\c`)
	}
}

func TestParseQwordsHexByteEscape(t *testing.T) {
	// \xHH inside a plain token decodes to one byte.
	toks, err := parseQwords(`p\x2fq` + "\n")
	if err != nil {
		t.Fatalf("parseQwords: %v", err)
	}
	if len(toks) != 1 || toks[0].String() != "p/q" {
		t.Fatalf("got %#v, want single token p/q", toks)
	}
}

func TestParseQwordsMalformed(t *testing.T) {
	bad := []string{
		`\x0` + "\n",  // odd hex nibbles
		`a\` + "\n",   // dangling escape
		`a\09` + "\n", // bad octal digit
	}
	for _, in := range bad {
		if _, err := parseQwords(in); err == nil {
			t.Errorf("parseQwords(%q) = nil err, want error", in)
		}
	}
}

func TestFinishLineTrimsAndTerminates(t *testing.T) {
	var b strings.Builder
	appendQword(&b, "x")
	got := finishLine(&b)
	if got != "x\n" {
		t.Fatalf("finishLine = %q, want %q", got, "x\n")
	}
}
