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

package nfs

import (
	"errors"
	"reflect"
	"testing"
)

func TestNormalizeExportIntentRequiresCIDRs(t *testing.T) {
	_, _, err := NormalizeExportIntent(nil, "")
	if !errors.Is(err, ErrCIDRsRequired) {
		t.Fatalf("NormalizeExportIntent error = %v, want ErrCIDRsRequired", err)
	}
}

func TestParseExportCIDRsParameterCanonicalizesSet(t *testing.T) {
	got, err := ParseExportCIDRsParameter(" fd00:10:244::/64,10.42.0.0/16 ")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.42.0.0/16", "fd00:10:244::/64"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseExportCIDRsParameter = %q, want %q", got, want)
	}
}

func TestParseExportCIDRsAcceptsFullPrefixRanges(t *testing.T) {
	for _, cidrs := range [][]string{{"0.0.0.0/0"}, {"192.0.2.1/32"}, {"::/0"}, {"2001:db8::1/128"}} {
		if _, err := ParseExportCIDRs(cidrs); err != nil {
			t.Errorf("ParseExportCIDRs(%q): %v", cidrs, err)
		}
	}
}

func TestParseExportCIDRsRejectsUnsafeInput(t *testing.T) {
	for name, cidrs := range map[string][]string{
		"empty":           {""},
		"doubled comma":   {"10.0.0.0/8", "", "192.0.2.0/24"},
		"unmasked v4":     {"10.42.0.1/16"},
		"unmasked v6":     {"2001:db8::1/64"},
		"mapped v4":       {"::ffff:192.0.2.0/120"},
		"zone":            {"fe80::%eth0/64"},
		"raw options":     {"rw=@0.0.0.0/0"},
		"bracketed input": {"[2001:db8::]/64"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseExportCIDRs(cidrs); !errors.Is(err, ErrInvalidCIDR) {
				t.Fatalf("error = %v, want ErrInvalidCIDR", err)
			}
		})
	}
}

func TestParseExportCIDRsDeduplicates(t *testing.T) {
	got, err := ParseExportCIDRsParameter("10.0.0.0/8,10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"10.0.0.0/8"}) {
		t.Fatalf("got %q", got)
	}
}
