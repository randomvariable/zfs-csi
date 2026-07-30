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

package naming

import (
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

func TestVolIDRoundTripProperties(t *testing.T) {
	t.Run("valid volume ids round-trip", rapid.MakeCheck(func(t *rapid.T) {
		pool := rapid.StringMatching(`[a-z][a-z0-9_.\-]{0,62}`).Draw(t, "pool")
		kind := rapid.SampledFrom([]zfs.VolumeKind{zfs.KindBlock, zfs.KindFilesystem}).Draw(t, "kind")
		id := rapid.StringMatching(`[a-z0-9][a-z0-9\-]{0,62}`).Draw(t, "id")

		volID, err := EncodeVolID(pool, kind, id)
		if err != nil {
			t.Fatalf("EncodeVolID(%q, %q, %q): %v", pool, kind, id, err)
		}

		parsed, err := ParseVolID(volID)
		if err != nil {
			t.Fatalf("ParseVolID(%q): %v", volID, err)
		}

		// EncodeVolID sanitizes the id (trim, lowercase), so the parsed id is
		// the sanitized form, not the raw input.
		wantID := SanitizeLeaf(id)
		if parsed.Pool != pool || parsed.Kind != kind || parsed.ID != wantID {
			t.Fatalf("ParseVolID(EncodeVolID(...)) = %#v, want pool=%q kind=%q id=%q", parsed, pool, kind, wantID)
		}
	}))

	t.Run("arbitrary strings never parse as a different valid id", rapid.MakeCheck(func(t *rapid.T) {
		input := rapid.StringOf(rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-:@/\\x00 "))).Draw(t, "input")

		parsed, err := ParseVolID(input)
		if err != nil {
			return
		}

		reencoded, err := EncodeVolID(parsed.Pool, parsed.Kind, parsed.ID)
		if err != nil {
			t.Fatalf("EncodeVolID(%#v) after successful parse: %v", parsed, err)
		}

		// EncodeVolID sanitizes the id, so a parsed id with trailing/leading
		// hyphens or uppercase will be normalized on re-encode. Assert
		// idempotency instead: re-encoding the re-encoded value is stable.
		if reencoded != input {
			reparsed, err := ParseVolID(reencoded)
			if err != nil {
				t.Fatalf("ParseVolID(reencoded %q): %v", reencoded, err)
			}
			re2, err := EncodeVolID(reparsed.Pool, reparsed.Kind, reparsed.ID)
			if err != nil || re2 != reencoded {
				t.Fatalf("non-idempotent encode: input=%q parsed=%#v reencoded=%q re2=%q", input, parsed, reencoded, re2)
			}
		}
	}))
}

func TestOwnerQualifiedTransportIdentity(t *testing.T) {
	nqnA, err := TargetNQN("storage-a", "1234", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	nqnB, err := TargetNQN("storage-b", "5678", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	guidA, err := DeviceGUID("storage-a", "1234", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	guidB, err := DeviceGUID("storage-b", "5678", zfs.KindBlock, "same-volume")
	if err != nil {
		t.Fatal(err)
	}
	if nqnA == nqnB || guidA == guidB {
		t.Fatalf("owner-qualified identities collide: nqn=%q guid=%q", nqnA, guidA)
	}
	nqnRetry, _ := TargetNQN("storage-a", "1234", zfs.KindBlock, "same-volume")
	guidRetry, _ := DeviceGUID("storage-a", "1234", zfs.KindBlock, "same-volume")
	if nqnRetry != nqnA || guidRetry != guidA || len(guidA) != 32 || len(nqnA) > 223 {
		t.Fatalf("transport identity not deterministic/canonical: nqn=%q/%q guid=%q/%q", nqnA, nqnRetry, guidA, guidRetry)
	}
}

func TestOwnerQualifiedTransportIdentityRejectsMissingOrMalformedIdentity(t *testing.T) {
	for _, tc := range []struct{ owner, poolGUID string }{{"", "1"}, {"storage-a", ""}, {"storage-a", "01"}, {"storage-a", "0"}} {
		if _, err := TargetNQN(tc.owner, tc.poolGUID, zfs.KindBlock, "volume"); err == nil {
			t.Fatalf("TargetNQN(%q, %q) accepted malformed identity", tc.owner, tc.poolGUID)
		}
		if _, err := DeviceGUID(tc.owner, tc.poolGUID, zfs.KindBlock, "volume"); err == nil {
			t.Fatalf("DeviceGUID(%q, %q) accepted malformed identity", tc.owner, tc.poolGUID)
		}
	}
}

func TestNamingDerivedIdentifierProperties(t *testing.T) {
	t.Run("derived names preserve volume parts", rapid.MakeCheck(func(t *rapid.T) {
		pool := rapid.StringMatching(`[a-z][a-z0-9_.\-]{0,62}`).Draw(t, "pool")
		kind := rapid.SampledFrom([]zfs.VolumeKind{zfs.KindBlock, zfs.KindFilesystem}).Draw(t, "kind")
		id := rapid.StringMatching(`[a-z0-9][a-z0-9\-]{0,62}`).Draw(t, "id")

		parsed := ParsedVolID{Pool: pool, Kind: kind, ID: id}

		pathPart, err := KindPathComponent(kind)
		if err != nil {
			t.Fatalf("KindPathComponent(%q): %v", kind, err)
		}

		if got, want := parsed.DatasetPath(), fmt.Sprintf("%s/csi/%s/%s", pool, pathPart, id); got != want {
			t.Fatalf("DatasetPath() = %q, want %q", got, want)
		}

		targetNQN, err := TargetNQN("storage-a", "1234", kind, id)
		if err != nil {
			t.Fatalf("TargetNQN(%q, %q, %q): %v", pool, kind, id, err)
		}

		if len(targetNQN) > 223 || !strings.HasPrefix(targetNQN, "nqn.2026-01.csi.randomvariable:zfs:") || !strings.HasSuffix(targetNQN, fmt.Sprintf(":%s:%s", kind, id)) {
			t.Fatalf("TargetNQN() = %q, invalid canonical form", targetNQN)
		}
	}))
}

func TestEncodeSnapID_AcceptsUppercaseAndLongNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		snap string
	}{
		{"uppercase", "MySnapshot.NAME"},
		{"long", strings.Repeat("a", 200)},
		{"special_chars", "snap.with spaces!and@symbols"},
		{"mixed", "CSI-Sanity.Snapshot_Test_1234567890123456789012345678901234567890"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			snapID, err := EncodeSnapID("tank", zfs.KindBlock, "vol-abc", tc.snap)
			if err != nil {
				t.Fatalf("EncodeSnapID(%q): %v", tc.snap, err)
			}

			// Must round-trip: parse must succeed and recover the sanitized snap.
			p, snap, err := ParseSnapID(snapID)
			if err != nil {
				t.Fatalf("ParseSnapID(%q): %v", snapID, err)
			}
			if p.Pool != "tank" || p.Kind != zfs.KindBlock || p.ID != "vol-abc" {
				t.Fatalf("parsed vol parts wrong: %#v", p)
			}
			if snap != SanitizeLeaf(tc.snap) {
				t.Fatalf("parsed snap = %q, want %q (sanitized)", snap, SanitizeLeaf(tc.snap))
			}
			// The snap component must satisfy idRE (parseable).
			if !idRE.MatchString(snap) {
				t.Fatalf("sanitized snap %q fails idRE", snap)
			}
		})
	}
}
