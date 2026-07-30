// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

// Package psk implements pure NVMe/TCP TLS PSK interchange and derivation.
// It intentionally has no Kubernetes, keyring, kernel, or transport I/O.
package psk

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

const (
	HMACNone   = 0
	HMACSHA256 = 1
	HMACSHA384 = 2

	Version0       = 0
	Version1       = 1
	DefaultVersion = Version0

	keyLenSHA256  = 32
	keyLenSHA384  = 48
	prefix        = "NVMeTLSkey-1:"
	retainedLabel = "tls13 HostNQN"
	tlsPSKLabel   = "tls13 nvme-tls-psk"
)

var (
	ErrInterchange = errors.New("psk: malformed NVMeTLSkey interchange")
	ErrCRC         = errors.New("psk: interchange CRC-32 mismatch")
	ErrHMACKeyLen  = errors.New("psk: hmac and key length mismatch")
	ErrNQN         = errors.New("psk: hostNQN and subsysNQN required")
	ErrVersion     = errors.New("psk: unsupported derivation version")
)

// Interchange is the configured PSK, without its interchange CRC trailer.
type Interchange struct {
	HMAC int
	Key  []byte
}

// Material is the final TLS PSK and exact Linux key description.
type Material struct {
	Identity string
	TLSPSK   []byte
	Cipher   int
	Version  int
}

// Generate returns a fresh configured PSK for the requested HMAC transform.
// reader is injected so callers can supply their own entropy source in tests.
func Generate(reader io.Reader, hmacID int) (Interchange, error) {
	length, err := keyLengthForHMAC(hmacID)
	if err != nil {
		return Interchange{}, err
	}
	key := make([]byte, length)
	if _, err := io.ReadFull(reader, key); err != nil {
		return Interchange{}, fmt.Errorf("psk: generate configured key: %w", err)
	}
	return Interchange{HMAC: hmacID, Key: key}, nil
}

// Format renders the exact libnvme non-compat interchange representation.
func (i Interchange) Format() (string, error) {
	if err := i.validate(); err != nil {
		return "", err
	}
	raw := make([]byte, len(i.Key)+4)
	copy(raw, i.Key)
	binary.LittleEndian.PutUint32(raw[len(i.Key):], crc32.ChecksumIEEE(i.Key))
	return fmt.Sprintf("%s%02x:%s:", prefix, i.HMAC, base64.StdEncoding.EncodeToString(raw)), nil
}

// Parse accepts only the exact standard-base64, padded interchange form.
func Parse(s string) (Interchange, error) {
	if len(s) != 65 && len(s) != 89 {
		return Interchange{}, ErrInterchange
	}
	if len(s) < len(prefix)+4 || s[:len(prefix)] != prefix || s[len(s)-1] != ':' {
		return Interchange{}, ErrInterchange
	}
	fields := s[len(prefix) : len(s)-1]
	if len(fields) < 4 || fields[2] != ':' {
		return Interchange{}, ErrInterchange
	}
	var hmacID int
	switch fields[:2] {
	case "00":
		hmacID = HMACNone
	case "01":
		hmacID = HMACSHA256
	case "02":
		hmacID = HMACSHA384
	default:
		return Interchange{}, ErrInterchange
	}
	b64 := fields[3:]
	if b64 == "" {
		return Interchange{}, ErrInterchange
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || base64.StdEncoding.EncodeToString(raw) != b64 {
		return Interchange{}, ErrInterchange
	}
	if len(raw) != keyLenSHA256+4 && len(raw) != keyLenSHA384+4 {
		return Interchange{}, ErrInterchange
	}
	key := raw[:len(raw)-4]
	if crc32.ChecksumIEEE(key) != binary.LittleEndian.Uint32(raw[len(key):]) {
		return Interchange{}, ErrCRC
	}
	ic := Interchange{HMAC: hmacID, Key: append([]byte(nil), key...)}
	if err := ic.validate(); err != nil {
		return Interchange{}, err
	}
	return ic, nil
}

// Derive applies the libnvme non-compat retained then TLS-PSK derivation.
func Derive(ic Interchange, hostNQN, subsysNQN string, version int) (Material, error) {
	if hostNQN == "" || subsysNQN == "" {
		return Material{}, ErrNQN
	}
	if version != Version0 && version != Version1 {
		return Material{}, ErrVersion
	}
	if err := ic.validate(); err != nil {
		return Material{}, err
	}
	cipher, err := resolveCipher(ic.HMAC, len(ic.Key))
	if err != nil {
		return Material{}, err
	}
	h, length, err := hashForCipher(cipher)
	if err != nil {
		return Material{}, err
	}

	retained := append([]byte(nil), ic.Key...)
	if ic.HMAC != HMACNone {
		retained, err = expandLabel(h, ic.Key, retainedLabel, []byte(hostNQN), length)
		if err != nil {
			return Material{}, fmt.Errorf("psk: derive retained: %w", err)
		}
	}

	identity := identity(version, cipher, hostNQN, subsysNQN, "")
	context := identity
	if version == Version1 {
		digest := digestFor(cipher, retained, hostNQN, subsysNQN)
		identity = identityForV1(cipher, hostNQN, subsysNQN, digest)
		context = fmt.Sprintf("%02x %s", cipher, digest)
	}
	tlsPSK, err := expandLabel(h, retained, tlsPSKLabel, []byte(context), length)
	if err != nil {
		return Material{}, fmt.Errorf("psk: derive TLS PSK: %w", err)
	}
	return Material{Identity: identity, TLSPSK: tlsPSK, Cipher: cipher, Version: version}, nil
}

func (i Interchange) validate() error {
	_, err := resolveCipher(i.HMAC, len(i.Key))
	return err
}

func keyLengthForHMAC(hmacID int) (int, error) {
	switch hmacID {
	case HMACNone, HMACSHA256:
		return keyLenSHA256, nil
	case HMACSHA384:
		return keyLenSHA384, nil
	default:
		return 0, ErrHMACKeyLen
	}
}

func resolveCipher(hmacID, keyLength int) (int, error) {
	switch hmacID {
	case HMACNone:
		if keyLength == keyLenSHA256 {
			return HMACSHA256, nil
		}
		if keyLength == keyLenSHA384 {
			return HMACSHA384, nil
		}
	case HMACSHA256:
		if keyLength == keyLenSHA256 {
			return HMACSHA256, nil
		}
	case HMACSHA384:
		if keyLength == keyLenSHA384 {
			return HMACSHA384, nil
		}
	}
	return 0, ErrHMACKeyLen
}

func hashForCipher(cipher int) (func() hash.Hash, int, error) {
	switch cipher {
	case HMACSHA256:
		return sha256.New, keyLenSHA256, nil
	case HMACSHA384:
		return sha512.New384, keyLenSHA384, nil
	default:
		return nil, 0, ErrHMACKeyLen
	}
}

func expandLabel(newHash func() hash.Hash, secret []byte, label string, context []byte, length int) ([]byte, error) {
	info := hkdfLabel(length, label, context)
	// NVMe's non-compat path passes an explicit all-zero HashLen salt.
	salt := make([]byte, newHash().Size())
	return hkdf.Key(newHash, secret, salt, string(info), length)
}

func hkdfLabel(length int, label string, context []byte) []byte {
	b := make([]byte, 0, 2+1+len(label)+1+len(context))
	b = binary.BigEndian.AppendUint16(b, uint16(length))
	b = append(b, byte(len(label)))
	b = append(b, label...)
	b = append(b, byte(len(context)))
	b = append(b, context...)
	return b
}

func digestFor(cipher int, retained []byte, hostNQN, subsysNQN string) string {
	newHash, _, _ := hashForCipher(cipher)
	mac := hmac.New(newHash, retained)
	_, _ = mac.Write([]byte(hostNQN + " " + subsysNQN + " NVMe-over-Fabrics"))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func identity(version, cipher int, hostNQN, subsysNQN, digest string) string {
	if version == Version1 {
		return identityForV1(cipher, hostNQN, subsysNQN, digest)
	}
	return fmt.Sprintf("NVMe0R%02x %s %s", cipher, hostNQN, subsysNQN)
}

func identityForV1(cipher int, hostNQN, subsysNQN, digest string) string {
	return fmt.Sprintf("NVMe1R%02x %s %s %s", cipher, hostNQN, subsysNQN, digest)
}
