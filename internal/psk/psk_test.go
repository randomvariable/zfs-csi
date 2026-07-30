// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package psk

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"strings"
	"testing"
)

const (
	host = "nqn.psk-test-host"
	sub  = "nqn.psk-test-subsys"
)

var key32 = mustHex("5512DBB6737D0106F65975B773DFB011FFC344BCF442E2DD6D8BC4870B5D5B03")
var key48 = append(append([]byte(nil), key32...), mustHex("FFC344BCF442E2DD6D8BC4870B5D5B03")...)

func TestInterchangeKAT(t *testing.T) {
	cases := []struct {
		hmacID int
		key    []byte
		wire   string
	}{
		{HMACNone, key32, "NVMeTLSkey-1:00:VRLbtnN9AQb2WXW3c9+wEf/DRLz0QuLdbYvEhwtdWwNf9LrZ:"},
		{HMACSHA256, key32, "NVMeTLSkey-1:01:VRLbtnN9AQb2WXW3c9+wEf/DRLz0QuLdbYvEhwtdWwNf9LrZ:"},
		{HMACSHA384, key48, "NVMeTLSkey-1:02:VRLbtnN9AQb2WXW3c9+wEf/DRLz0QuLdbYvEhwtdWwP/w0S89ELi3W2LxIcLXVsDn8kXZQ==:"},
	}
	for _, tc := range cases {
		got, err := (Interchange{HMAC: tc.hmacID, Key: tc.key}).Format()
		if err != nil || got != tc.wire {
			t.Fatalf("Format() = %q, %v; want %q", got, err, tc.wire)
		}
		parsed, err := Parse(tc.wire)
		if err != nil || parsed.HMAC != tc.hmacID || !bytes.Equal(parsed.Key, tc.key) {
			t.Fatalf("Parse() = %#v, %v", parsed, err)
		}
	}
}

func TestV1IdentityKAT(t *testing.T) {
	cases := []struct {
		ic   Interchange
		want string
	}{
		{Interchange{HMAC: HMACSHA256, Key: key32}, "NVMe1R01 nqn.psk-test-host nqn.psk-test-subsys iSbjiStwJ/1TrTvDlt2fjFmzvsytOJelidNnA+X5lEU="},
		{Interchange{HMAC: HMACSHA384, Key: key48}, "NVMe1R02 nqn.psk-test-host nqn.psk-test-subsys QhW2+Rp6RzHlNtCslyRxMnwJ11tKKhz8JCAQpQ+XUD8f9td1VeH5h53yz2wKJG1a"},
	}
	for _, tc := range cases {
		material, err := Derive(tc.ic, host, sub, Version1)
		if err != nil || material.Identity != tc.want {
			t.Fatalf("Derive(v1) identity = %q, %v; want %q", material.Identity, err, tc.want)
		}
	}
}

func TestDefaultV0AndIndependentGolden(t *testing.T) {
	ic := Interchange{HMAC: HMACSHA256, Key: key32}
	got, err := Derive(ic, host, sub, DefaultVersion)
	if err != nil {
		t.Fatal(err)
	}
	if got.Identity != "NVMe0R01 "+host+" "+sub {
		t.Fatalf("default identity = %q", got.Identity)
	}
	retained := manualExpand(sha256.New, key32, "tls13 HostNQN", []byte(host), 32)
	want := manualExpand(sha256.New, retained, "tls13 nvme-tls-psk", []byte(got.Identity), 32)
	if !bytes.Equal(got.TLSPSK, want) {
		t.Fatalf("v0 TLS PSK = %x, want independent RFC5869 calculation %x", got.TLSPSK, want)
	}
}

func TestDeriveDeterministicAndNoneResolution(t *testing.T) {
	for _, ic := range []Interchange{{HMAC: HMACNone, Key: key32}, {HMAC: HMACNone, Key: key48}, {HMAC: HMACSHA256, Key: key32}, {HMAC: HMACSHA384, Key: key48}} {
		a, err := Derive(ic, host, sub, Version0)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Derive(ic, host, sub, Version0)
		if err != nil || !bytes.Equal(a.TLSPSK, b.TLSPSK) {
			t.Fatalf("derivation not deterministic: %v", err)
		}
		if len(a.TLSPSK) != len(ic.Key) {
			t.Fatalf("TLS PSK length = %d, want %d", len(a.TLSPSK), len(ic.Key))
		}
	}
}

func TestParseRejectionMatrix(t *testing.T) {
	good, err := (Interchange{HMAC: HMACSHA256, Key: key32}).Format()
	if err != nil {
		t.Fatal(err)
	}
	badCRC := append([]byte(nil), key32...)
	badCRC[0] ^= 1
	raw := append(badCRC, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(raw[len(badCRC):], crc32.ChecksumIEEE(key32))
	badCRCString := "NVMeTLSkey-1:01:" + base64.StdEncoding.EncodeToString(raw) + ":"
	for _, tc := range []struct {
		name string
		in   string
		want error
	}{
		{"bad version", strings.Replace(good, "-1:", "-2:", 1), ErrInterchange},
		{"no final colon", good[:len(good)-1], ErrInterchange},
		{"extra field", good + ":", ErrInterchange},
		{"bad base64", "NVMeTLSkey-1:01:!!!!:", ErrInterchange},
		{"bad padding", "NVMeTLSkey-1:02:" + strings.TrimSuffix("VRLbtnN9AQb2WXW3c9+wEf/DRLz0QuLdbYvEhwtdWwP/w0S89ELi3W2LxIcLXVsDn8kXZQ==", "=") + ":", ErrInterchange},
		{"bad CRC", badCRCString, ErrCRC},
		{"bad hmac", strings.Replace(good, ":01:", ":03:", 1), ErrInterchange},
		{"hmac/key mismatch", strings.Replace(good, ":01:", ":02:", 1), ErrHMACKeyLen},
		{"invalid length", "NVMeTLSkey-1:01:AAAA:", ErrInterchange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Parse(%q) error = %v, want %v", tc.in, err, tc.want)
			}
		})
	}
}

func TestGenerateRoundTripsAndUsesInjectedReader(t *testing.T) {
	for _, hmacID := range []int{HMACNone, HMACSHA256, HMACSHA384} {
		reader := bytes.NewReader(bytes.Repeat([]byte{byte(hmacID + 1)}, 48))
		ic, err := Generate(reader, hmacID)
		if err != nil {
			t.Fatal(err)
		}
		if len(ic.Key) != map[int]int{HMACNone: 32, HMACSHA256: 32, HMACSHA384: 48}[hmacID] {
			t.Fatalf("generated length = %d", len(ic.Key))
		}
		wire, _ := ic.Format()
		back, err := Parse(wire)
		if err != nil || !bytes.Equal(back.Key, ic.Key) {
			t.Fatalf("generated round trip failed: %v", err)
		}
	}
	if _, err := Generate(strings.NewReader("short"), HMACSHA256); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short reader error = %v", err)
	}
}

func TestValidationErrors(t *testing.T) {
	for _, tc := range []struct {
		ic Interchange
	}{
		{Interchange{HMAC: HMACSHA256, Key: key48}},
		{Interchange{HMAC: HMACSHA384, Key: key32}},
		{Interchange{HMAC: 3, Key: key32}},
		{Interchange{HMAC: HMACNone, Key: []byte{1}}},
	} {
		if _, err := tc.ic.Format(); !errors.Is(err, ErrHMACKeyLen) {
			t.Fatalf("Format error = %v", err)
		}
	}
	if _, err := Derive(Interchange{HMAC: HMACSHA256, Key: key32}, "", sub, Version0); !errors.Is(err, ErrNQN) {
		t.Fatal(err)
	}
	if _, err := Derive(Interchange{HMAC: HMACSHA256, Key: key32}, host, sub, 2); !errors.Is(err, ErrVersion) {
		t.Fatal(err)
	}
}

func manualExpand(newHash func() hash.Hash, secret []byte, label string, context []byte, length int) []byte {
	salt := make([]byte, newHash().Size())
	extract := hmac.New(newHash, salt)
	_, _ = extract.Write(secret)
	prk := extract.Sum(nil)
	info := make([]byte, 0, 2+1+len(label)+1+len(context))
	info = binary.BigEndian.AppendUint16(info, uint16(length))
	info = append(info, byte(len(label)))
	info = append(info, label...)
	info = append(info, byte(len(context)))
	info = append(info, context...)
	expand := hmac.New(newHash, prk)
	_, _ = expand.Write(info)
	_, _ = expand.Write([]byte{1})
	return expand.Sum(nil)[:length]
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

var _ = sha512.New384
