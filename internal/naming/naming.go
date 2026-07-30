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

// Package naming centralises CSI volume/snapshot id encoding and ZFS dataset
// path derivation. The volume id is opaque to the CSI consumer but parseable
// by us so the controller, agent, and node never disagree about where a volume
// lives.
//
// Volume id grammar (stable, round-trippable):
//
//	csi:<pool>:<kind>:<id>
//
// where:
//   - pool   ∈ [a-z][a-z0-9_.-]{0,62}      (ZFS pool name)
//   - kind   ∈ {block, filesystem}
//   - id     ∈ [a-z0-9][a-z0-9-]{0,62}      (volume leaf name; k8s-name-safe)
//
// Dataset path derived from a volume id:
//
//	<pool>/csi/<kind>/<id>
//
// e.g. csi:tank:block:abc-123 → tank/csi/block/abc-123
//
//	csi:tank:filesystem:def-456 → tank/csi/fs/def-456   (note: "filesystem" → "fs" in path)
package naming

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const (
	// VolIDPrefix is the scheme-like prefix for all zfs-csi volume ids.
	VolIDPrefix = "csi"
	// CSIPathBlock is the on-pool path component for block volumes.
	CSIPathBlock = "block"
	// CSIPathFilesystem is the on-pool path component for filesystem volumes.
	CSIPathFilesystem = "fs"
	// CSIPathRoot is the parent dataset under each pool.
	CSIPathRoot = "csi"
)

var (
	ErrInvalidPool = errors.New("naming: invalid pool")
	ErrInvalidID   = errors.New("naming: invalid id")
	ErrMalformedID = errors.New("naming: malformed id")
	ErrUnknownKind = errors.New("naming: unknown volume kind")

	poolRE       = regexp.MustCompile(`^[a-z][a-z0-9_.\-]{0,62}$`)
	idRE         = regexp.MustCompile(`^[a-z0-9][a-z0-9\-]{0,62}$`)
	targetNQNRE  = regexp.MustCompile(`^nqn\.2026-01\.csi\.randomvariable:zfs:[0-9a-f]{32}:(block|filesystem):[a-z0-9][a-z0-9-]{0,62}$`)
	deviceGUIDRE = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// SanitizeLeaf normalizes an arbitrary CSI volume/snapshot name into a form
// that satisfies idRE: lowercased, [a-z0-9-] only, trimmed, ≤ 63 chars. Names
// exceeding 63 chars after sanitization are truncated and suffixed with a
// deterministic hash so round-trip is stable + collision-resistant. Empty/all-
// invalid input yields "x".
//
// This is the single canonical sanitizer; callers must NOT pre-sanitize.
func SanitizeLeaf(name string) string {
	out := strings.ToLower(name)

	var b strings.Builder

	for _, r := range out {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}

	s := strings.Trim(b.String(), "-")
	if s == "" {
		return "x"
	}

	if len(s) <= maxIDLen {
		return s
	}

	// Truncate + hash suffix: <prefix>-<hash> = 63 chars total.
	// prefix = 44 chars, hyphen, hash = 18 hex chars (9 bytes of sha256).
	h := sha256.Sum256([]byte(name))

	return s[:maxIDLen-hashSuffixLen-1] + "-" + hex.EncodeToString(h[:9])
}

const (
	maxIDLen      = 63
	hashSuffixLen = 18
)

// KindPathComponent maps a zfs.VolumeKind to its on-pool path component.
func KindPathComponent(k zfs.VolumeKind) (string, error) {
	switch k {
	case zfs.KindBlock:
		return CSIPathBlock, nil
	case zfs.KindFilesystem:
		return CSIPathFilesystem, nil
	}

	return "", fmt.Errorf("%w: %q", ErrUnknownKind, k)
}

// KindFromPath maps an on-pool path component back to a VolumeKind.
func KindFromPath(p string) (zfs.VolumeKind, error) {
	switch p {
	case CSIPathBlock:
		return zfs.KindBlock, nil
	case CSIPathFilesystem:
		return zfs.KindFilesystem, nil
	}

	return "", fmt.Errorf("%w: path component %q", ErrUnknownKind, p)
}

// EncodeVolID builds a CSI volume id from its parts. The id is sanitized to
// accept arbitrary CSI names (uppercase, special chars, long); pool + kind are
// validated.
func EncodeVolID(pool string, kind zfs.VolumeKind, id string) (string, error) {
	if !poolRE.MatchString(pool) {
		return "", fmt.Errorf("%w: pool name %q", ErrInvalidPool, pool)
	}

	if _, err := KindPathComponent(kind); err != nil {
		return "", err
	}

	leaf := SanitizeLeaf(id)

	return strings.Join([]string{VolIDPrefix, pool, string(kind), leaf}, ":"), nil
}

// ParsedVolID is the decoded form of a CSI volume id.
type ParsedVolID struct {
	Pool string
	Kind zfs.VolumeKind
	ID   string
}

// ParseVolID decodes a CSI volume id. Returns an error if malformed.
func ParseVolID(volID string) (ParsedVolID, error) {
	parts := strings.Split(volID, ":")
	if len(parts) != 4 || parts[0] != VolIDPrefix {
		return ParsedVolID{}, fmt.Errorf("%w: volume id %q", ErrMalformedID, volID)
	}

	pool, kindStr, id := parts[1], parts[2], parts[3]
	if !poolRE.MatchString(pool) {
		return ParsedVolID{}, fmt.Errorf("%w: in volume id %q", ErrInvalidPool, volID)
	}

	if !idRE.MatchString(id) {
		return ParsedVolID{}, fmt.Errorf("%w: in volume id %q", ErrInvalidID, volID)
	}

	kind := zfs.VolumeKind(kindStr)
	if _, err := KindPathComponent(kind); err != nil {
		return ParsedVolID{}, fmt.Errorf("%w: in volume id %q", ErrUnknownKind, volID)
	}

	return ParsedVolID{Pool: pool, Kind: kind, ID: id}, nil
}

// DatasetPath returns the full ZFS dataset name for a parsed volume id.
func (p ParsedVolID) DatasetPath() string {
	pc, _ := KindPathComponent(p.Kind)

	return p.Pool + "/" + CSIPathRoot + "/" + pc + "/" + p.ID
}

// ImportID derives a stable Kubernetes/CSI identity from an administrator-
// selected backend path. The readable suffix aids diagnosis while the hash
// prevents equal leaves in different dataset trees from colliding.
func ImportID(backendPath string) string {
	const hashChars = 20
	sum := sha256.Sum256([]byte(backendPath))
	hash := hex.EncodeToString(sum[:])[:hashChars]
	leaves := strings.Split(strings.Trim(backendPath, "/"), "/")
	leaf := SanitizeLeaf(leaves[len(leaves)-1])
	const prefix = "import-"
	maxLeaf := 63 - len(prefix) - 1 - hashChars
	if len(leaf) > maxLeaf {
		leaf = strings.Trim(leaf[:maxLeaf], "-")
	}
	if leaf == "" {
		return prefix + hash
	}
	return prefix + leaf + "-" + hash
}

// SnapIDPrefix/Snap encoding ------------------------------------------------

// EncodeSnapID builds a CSI snapshot id: csi:<pool>:<kind>:<id>@<snap>. Both
// the volume id and the snap name are sanitized to accept arbitrary CSI input.
func EncodeSnapID(pool string, kind zfs.VolumeKind, id, snap string) (string, error) {
	base, err := EncodeVolID(pool, kind, id)
	if err != nil {
		return "", err
	}

	return base + "@" + SanitizeLeaf(snap), nil
}

// ParseSnapID splits a snapshot id into its volume + snapshot parts.
func ParseSnapID(snapID string) (p ParsedVolID, snap string, err error) {
	idx := strings.LastIndex(snapID, "@")
	if idx < 0 {
		return ParsedVolID{}, "", fmt.Errorf("%w: snapshot id %q", ErrMalformedID, snapID)
	}

	p, err = ParseVolID(snapID[:idx])
	if err != nil {
		return ParsedVolID{}, "", err
	}

	snap = snapID[idx+1:]
	if !idRE.MatchString(snap) {
		return ParsedVolID{}, "", fmt.Errorf("%w: snapshot name in id %q", ErrInvalidID, snapID)
	}

	return p, snap, nil
}

// SnapshotDatasetPath returns tank/csi/block/<id>@<snap>.
func SnapshotDatasetPath(p ParsedVolID, snap string) string {
	return p.DatasetPath() + "@" + snap
}

// TargetNQN builds a bounded NVMe subsystem NQN from immutable owner-qualified
// pool identity. Hashing owner+pool GUID avoids exposing unbounded Kubernetes
// names while retaining deterministic, globally distinct wire identity.
func TargetNQN(ownerNode, poolGUID string, kind zfs.VolumeKind, id string) (string, error) {
	if _, err := KindPathComponent(kind); err != nil {
		return "", err
	}

	if ownerNode == "" || !canonicalPoolGUID(poolGUID) || !idRE.MatchString(id) {
		return "", fmt.Errorf("%w: for target nqn", ErrInvalidID)
	}

	ownerPool := sha256.Sum256([]byte(ownerNode + "\x00" + poolGUID))
	return fmt.Sprintf("nqn.2026-01.csi.randomvariable:zfs:%s:%s:%s", hex.EncodeToString(ownerPool[:16]), kind, id), nil
}

// DeviceGUID builds a deterministic 16-byte NVMe namespace globally-unique
// identifier (NGUID) for a volume, rendered as 32 lowercase hex digits. The
// kernel nvmet target's namespaces/<n>/device_nguid attribute accepts ONLY a
// 16-byte identifier (32 hex chars, optionally dash-separated); a human-readable
// string like "zfs-csi-tank-block-<id>" is rejected with EINVAL. Deriving it
// from a SHA-256 of immutable owner-qualified pool coordinates keeps it
// deterministic without needing to mint and persist a random UUID.
func DeviceGUID(ownerNode, poolGUID string, kind zfs.VolumeKind, id string) (string, error) {
	if _, err := KindPathComponent(kind); err != nil {
		return "", err
	}
	if ownerNode == "" || !canonicalPoolGUID(poolGUID) || !idRE.MatchString(id) {
		return "", fmt.Errorf("%w: for device guid", ErrInvalidID)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%s", ownerNode, poolGUID, kind, id)))
	return hex.EncodeToString(sum[:16]), nil
}

// ValidateTargetNQN verifies the exact bounded grammar emitted by TargetNQN.
func ValidateTargetNQN(nqn string) error {
	// NVMe NQNs are limited to 223 bytes by the protocol.
	if len(nqn) > 223 || !targetNQNRE.MatchString(nqn) {
		return fmt.Errorf("%w: target nqn", ErrInvalidID)
	}
	return nil
}

// ValidateDeviceGUID verifies the lowercase 16-byte NGUID encoding accepted by nvmet.
func ValidateDeviceGUID(guid string) error {
	if !deviceGUIDRE.MatchString(guid) {
		return fmt.Errorf("%w: device guid", ErrInvalidID)
	}
	return nil
}

func canonicalPoolGUID(guid string) bool {
	value, err := strconv.ParseUint(guid, 10, 64)
	return err == nil && value != 0 && strconv.FormatUint(value, 10) == guid
}
