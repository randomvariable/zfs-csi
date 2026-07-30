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

package openbao

import (
	"regexp"
	"strings"
	"testing"
)

// The Volume CRD validates encryptionKeyRef against this pattern; a ref that
// fails it makes CreateVolume fail at CR creation (real live bug). encodeRef
// must produce a compliant ref even when the Transit ciphertext contains bytes
// that base64-StdEncoding would render as '+', '/', or '='.
func TestEncodeRefMatchesCRDPattern(t *testing.T) {
	pattern := regexp.MustCompile(`^(transit|kv)/[a-zA-Z0-9._/-]{1,255}$`)

	// A ciphertext whose bytes force StdEncoding to emit +, /, and = padding.
	ciphertext := "vault:v1:" + strings.Repeat("\xfb\xff\xbf", 20)
	ref := encodeRef("zfs-vol-abc123", ciphertext)

	if !pattern.MatchString(ref) {
		t.Fatalf("encodeRef produced a ref that violates the CRD pattern: %q", ref)
	}

	// And it must round-trip.
	name, ct, err := parseRef(ref)
	if err != nil {
		t.Fatalf("parseRef(%q) error: %v", ref, err)
	}
	if name != "zfs-vol-abc123" || ct != ciphertext {
		t.Fatalf("round-trip mismatch: name=%q ct=%q", name, ct)
	}
}
