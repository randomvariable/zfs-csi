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

package zfs_test

import (
	"math"
	"testing"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

// TestParseBlockSize covers the syntax the Volume CRD already accepts
// (^[0-9]+[kKmMgG]?$) plus the rejection cases the CRD pattern cannot express:
// zero and int64 overflow.
func TestParseBlockSize(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
		ok   bool
	}{
		{in: "512", want: 512, ok: true},
		{in: "4096", want: 4096, ok: true},
		{in: "8k", want: 8 * 1024, ok: true},
		{in: "16K", want: 16 * 1024, ok: true},
		{in: "128k", want: 128 * 1024, ok: true},
		{in: "1m", want: 1024 * 1024, ok: true},
		{in: "1M", want: 1024 * 1024, ok: true},
		{in: "1g", want: 1024 * 1024 * 1024, ok: true},
		{in: "1G", want: 1024 * 1024 * 1024, ok: true},
		{in: "", ok: false},
		{in: "0", ok: false},
		{in: "0k", ok: false},
		{in: "k", ok: false},
		{in: "16kk", ok: false},
		{in: "16 k", ok: false},
		{in: " 16k", ok: false},
		{in: "-16k", ok: false},
		{in: "16t", ok: false},
		{in: "0x10", ok: false},
		{in: "16.5k", ok: false},
		{in: "9223372036854775808", ok: false},  // int64 overflow
		{in: "9223372036854775807k", ok: false}, // multiplier overflow
		{in: "99999999999999999999", ok: false}, // int64 overflow
		{in: "8796093022208m", ok: false},       // multiplier overflow
	} {
		got, err := zfs.ParseBlockSize(tc.in)
		if tc.ok {
			if err != nil {
				t.Errorf("ParseBlockSize(%q) error = %v, want %d", tc.in, err, tc.want)

				continue
			}
			if got != tc.want {
				t.Errorf("ParseBlockSize(%q) = %d, want %d", tc.in, got, tc.want)
			}

			continue
		}
		if err == nil {
			t.Errorf("ParseBlockSize(%q) = %d, want error", tc.in, got)
		}
	}
}

// TestCanonicalVolBlockSize proves the zvol volblocksize contract: OpenZFS only
// accepts powers of two between 512 bytes and 128 KiB, and equivalent spellings
// must canonicalise to one persisted form.
func TestCanonicalVolBlockSize(t *testing.T) {
	for _, tc := range []struct {
		in        string
		want      string
		wantBytes int64
		ok        bool
	}{
		{in: "512", want: "512", wantBytes: 512, ok: true},
		{in: "1024", want: "1k", wantBytes: 1024, ok: true},
		{in: "1k", want: "1k", wantBytes: 1024, ok: true},
		{in: "1K", want: "1k", wantBytes: 1024, ok: true},
		{in: "8k", want: "8k", wantBytes: 8 * 1024, ok: true},
		{in: "16K", want: "16k", wantBytes: 16 * 1024, ok: true},
		{in: "16k", want: "16k", wantBytes: 16 * 1024, ok: true},
		{in: "16384", want: "16k", wantBytes: 16 * 1024, ok: true},
		{in: "128k", want: "128k", wantBytes: 128 * 1024, ok: true},
		{in: "128K", want: "128k", wantBytes: 128 * 1024, ok: true},
		{in: "131072", want: "128k", wantBytes: 128 * 1024, ok: true},
		{in: "", ok: false},                     // inherit is not a canonical zvol value
		{in: "0", ok: false},                    // rejected by ParseBlockSize
		{in: "256", ok: false},                  // below SPA_MINBLOCKSIZE
		{in: "511", ok: false},                  // below minimum and not a power of two
		{in: "768", ok: false},                  // in range but not a power of two
		{in: "12k", ok: false},                  // not a power of two
		{in: "100k", ok: false},                 // not a power of two
		{in: "256k", ok: false},                 // above the supported zvol maximum
		{in: "1m", ok: false},                   // above the supported zvol maximum
		{in: "1M", ok: false},                   // above the supported zvol maximum
		{in: "1g", ok: false},                   // above the supported zvol maximum
		{in: "16t", ok: false},                  // unsupported suffix
		{in: "abc", ok: false},                  // malformed
		{in: "-16k", ok: false},                 // malformed
		{in: "9223372036854775807k", ok: false}, // multiplier overflow
	} {
		got, gotBytes, err := zfs.CanonicalVolBlockSize(tc.in)
		if !tc.ok {
			if err == nil {
				t.Errorf("CanonicalVolBlockSize(%q) = %q/%d, want error", tc.in, got, gotBytes)
			}

			continue
		}
		if err != nil {
			t.Errorf("CanonicalVolBlockSize(%q) error = %v, want %q", tc.in, err, tc.want)

			continue
		}
		if got != tc.want || gotBytes != tc.wantBytes {
			t.Errorf("CanonicalVolBlockSize(%q) = %q/%d, want %q/%d", tc.in, got, gotBytes, tc.want, tc.wantBytes)
		}
		// A canonical value must be a fixed point: re-canonicalising cannot drift.
		if again, _, err := zfs.CanonicalVolBlockSize(got); err != nil || again != got {
			t.Errorf("CanonicalVolBlockSize(%q) = %q, %v; want stable %q", got, again, err, got)
		}
	}
}

// TestDefaultVolBlockSizeValueIsCanonical ties the explicit default the driver
// persists to the byte value capacity alignment uses, so create and expand
// cannot diverge.
func TestDefaultVolBlockSizeValueIsCanonical(t *testing.T) {
	got, gotBytes, err := zfs.CanonicalVolBlockSize(zfs.DefaultVolBlockSizeValue)
	if err != nil {
		t.Fatalf("CanonicalVolBlockSize(%q) error = %v", zfs.DefaultVolBlockSizeValue, err)
	}
	if got != zfs.DefaultVolBlockSizeValue || gotBytes != zfs.DefaultVolBlockSize {
		t.Fatalf("default %q resolves to %q/%d, want %q/%d",
			zfs.DefaultVolBlockSizeValue, got, gotBytes, zfs.DefaultVolBlockSizeValue, zfs.DefaultVolBlockSize)
	}
	if effective, err := zfs.EffectiveBlockSize(zfs.DefaultVolBlockSizeValue); err != nil || effective != zfs.DefaultVolBlockSize {
		t.Fatalf("EffectiveBlockSize(%q) = %d, %v; want %d", zfs.DefaultVolBlockSizeValue, effective, err, zfs.DefaultVolBlockSize)
	}
}

// TestAlignUp proves capacity rounds up to the next block multiple and that
// arithmetic overflow is reported rather than silently wrapping negative.
func TestAlignUp(t *testing.T) {
	for _, tc := range []struct {
		capacity  int64
		blockSize int64
		want      int64
		ok        bool
	}{
		{capacity: 1 << 30, blockSize: 16 * 1024, want: 1 << 30, ok: true},
		{capacity: 1<<30 + 1, blockSize: 16 * 1024, want: 1<<30 + 16*1024, ok: true},
		{capacity: 1, blockSize: 16 * 1024, want: 16 * 1024, ok: true},
		{capacity: 16*1024 - 1, blockSize: 16 * 1024, want: 16 * 1024, ok: true},
		{capacity: 100, blockSize: 512, want: 512, ok: true},
		{capacity: 0, blockSize: 512, ok: false},
		{capacity: -1, blockSize: 512, ok: false},
		{capacity: 512, blockSize: 0, ok: false},
		{capacity: math.MaxInt64 - 1, blockSize: 16 * 1024, ok: false},
	} {
		got, err := zfs.AlignUp(tc.capacity, tc.blockSize)
		if tc.ok {
			if err != nil || got != tc.want {
				t.Errorf("AlignUp(%d, %d) = %d, %v; want %d", tc.capacity, tc.blockSize, got, err, tc.want)
			}

			continue
		}
		if err == nil {
			t.Errorf("AlignUp(%d, %d) = %d, want error", tc.capacity, tc.blockSize, got)
		}
	}
}
