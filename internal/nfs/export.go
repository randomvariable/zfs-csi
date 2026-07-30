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

// Package nfs centralises the typed NFS export policy accepted by zfs-csi.
package nfs

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
)

const (
	// DefaultExportAccessMode grants read-write access unless a StorageClass asks for read-only.
	DefaultExportAccessMode = "rw"
)

var (
	ErrCIDRsRequired     = errors.New("at least one nfs export cidr is required")
	ErrInvalidCIDR       = errors.New("nfs export cidr must be a canonical IPv4 or IPv6 prefix")
	ErrInvalidAccessMode = errors.New("nfs export access mode must be rw|ro")
)

// ParseExportCIDRs validates and canonicalizes the allowed client networks.
// CIDRs are set-like API intent, so canonical output is sorted and unique.
func ParseExportCIDRs(raw []string) ([]netip.Prefix, error) {
	if len(raw) == 0 {
		return nil, ErrCIDRsRequired
	}
	prefixes := make([]netip.Prefix, 0, len(raw))
	seen := make(map[netip.Prefix]struct{}, len(raw))
	for _, value := range raw {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%w: empty list element", ErrInvalidCIDR)
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil || prefix.Addr().Is4In6() || prefix != prefix.Masked() {
			return nil, fmt.Errorf("%w: got %q", ErrInvalidCIDR, value)
		}
		if _, ok := seen[prefix]; ok {
			continue
		}
		seen[prefix] = struct{}{}
		prefixes = append(prefixes, prefix)
	}
	slices.SortFunc(prefixes, func(a, b netip.Prefix) int {
		return strings.Compare(a.String(), b.String())
	})
	return prefixes, nil
}

// ParseExportCIDRsParameter parses the comma-separated StorageClass wire value.
func ParseExportCIDRsParameter(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrCIDRsRequired
	}
	parts := strings.Split(raw, ",")
	prefixes, err := ParseExportCIDRs(parts)
	if err != nil {
		return nil, err
	}
	return prefixStrings(prefixes), nil
}

// NormalizeExportIntent validates typed export intent and applies mode defaults.
func NormalizeExportIntent(cidrs []string, accessMode string) ([]string, string, error) {
	prefixes, err := ParseExportCIDRs(cidrs)
	if err != nil {
		return nil, "", err
	}
	if accessMode == "" {
		accessMode = DefaultExportAccessMode
	}

	if accessMode != "rw" && accessMode != "ro" {
		return nil, "", fmt.Errorf("%w: got %q", ErrInvalidAccessMode, accessMode)
	}

	return prefixStrings(prefixes), accessMode, nil
}

func prefixStrings(prefixes []netip.Prefix) []string {
	result := make([]string, len(prefixes))
	for i := range prefixes {
		result[i] = prefixes[i].String()
	}
	return result
}
