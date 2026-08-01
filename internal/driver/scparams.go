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

package driver

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/randomvariable/zfs-csi/internal/nfs"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const defaultNFSExportAccessMode = nfs.DefaultExportAccessMode

var (
	ErrPoolRequired             = errors.New("sc param \"pool\" is required")
	errUnsupportedType          = errors.New("sc param \"type\" must be block|filesystem")
	errUnsupportedFSType        = errors.New("sc param \"fsType\" must be ext4|xfs")
	errUnsupportedTransport     = errors.New("sc param \"transport\" must be nvme-tcp")
	errInvalidEncrypted         = errors.New("sc param \"encrypted\" must be bool")
	errInvalidNFSTLS            = errors.New("sc param \"nfsTLS\" must be bool")
	errNFSTLSRequiresFilesystem = errors.New("sc param \"nfsTLS\" requires type=filesystem")
	errInvalidNVMeTLS           = errors.New("sc param \"nvmeTLS\" must be bool")
	errNVMeTLSRequiresBlock     = errors.New("sc param \"nvmeTLS\" requires type=block and transport=nvme-tcp")
	errInvalidCompression       = errors.New("mutable param \"compression\" must be on|off|lz4|gzip|zstd|zstd-<1-9>|zstd-<1-9>-fast")
	errUnsupportedMutableParam  = errors.New("unsupported mutable parameter")
	errInvalidBlockSize         = errors.New("sc param \"blocksize\" must be a positive size: digits with an optional k/K/m/M/g/G suffix (base 1024)")
	errInvalidVolBlockSize      = errors.New("sc param \"blocksize\" for type=block must be a power of two between 512 and 131072 bytes")
)

// parseSCParams maps a CSI parameters map (from the StorageClass) onto scParams.
// Keys are case-insensitive. Unknown keys are ignored.
func parseSCParams(params map[string]string) (scParams, error) {
	p := scParams{
		FsType:              "xfs",
		Transport:           "nvme-tcp",
		Type:                "block",
		NFSExportAccessMode: defaultNFSExportAccessMode,
	}

	if v := getSCParam(params, "pool"); v != "" {
		p.Pool = v
	} else {
		return p, ErrPoolRequired
	}

	if v := getSCParam(params, "type"); v != "" {
		p.Type = strings.ToLower(v)
	}

	if v := getSCParam(params, "fsType"); v != "" {
		p.FsType = v
	}

	if v := getSCParam(params, "blocksize"); v != "" {
		p.BlockSize = v
	}

	if v := getSCParam(params, "compression"); v != "" {
		p.Compression = v
	}

	if v := getSCParam(params, "transport"); v != "" {
		p.Transport = strings.ToLower(v)
	}

	if v := getSCParam(params, "encrypted"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return p, fmt.Errorf("%w: got %q", errInvalidEncrypted, v)
		}

		p.Encrypted = b
	}

	if v, ok := lookupSCParam(params, "nfsExportCIDRs"); ok {
		cidrs, err := nfs.ParseExportCIDRsParameter(v)
		if err != nil {
			return p, fmt.Errorf("nfs export parameters: %w", err)
		}
		p.NFSExportCIDRs = cidrs
	}

	if v := getSCParam(params, "nfsExportAccessMode"); v != "" {
		p.NFSExportAccessMode = strings.ToLower(v)
	}

	if v := getSCParam(params, "nfsTLS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return p, fmt.Errorf("%w: got %q", errInvalidNFSTLS, v)
		}
		p.NFSTLSEnabled = b
		p.NFSTLSSpecified = true
	}

	if v := getSCParam(params, "nvmeTLS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return p, fmt.Errorf("%w: got %q", errInvalidNVMeTLS, v)
		}
		p.NVMeTLSEnabled = b
		p.NVMeTLSSpecified = true
	}

	if err := validateSCParams(p); err != nil {
		return p, err
	}

	// Block volumes carry an explicit, canonical volblocksize on the Volume CR so
	// that create-time and expand-time alignment always agree and never depend on
	// the ZFS build's default. Equivalent spellings ("16K", "16k", "16384")
	// canonicalise to the same value, so idempotent retries compare equal.
	if p.Type == "block" {
		if p.BlockSize == "" {
			p.BlockSize = zfs.DefaultVolBlockSizeValue
		} else {
			canonical, _, err := zfs.CanonicalVolBlockSize(p.BlockSize)
			if err != nil {
				return p, fmt.Errorf("%w: got %q (%v)", errInvalidVolBlockSize, p.BlockSize, err)
			}
			p.BlockSize = canonical
		}
	}

	return p, nil
}

func getSCParam(params map[string]string, key string) string {
	value, _ := lookupSCParam(params, key)
	return value
}

func lookupSCParam(params map[string]string, key string) (string, bool) {
	for k, v := range params {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

// compressionPattern mirrors the Volume CRD's compression validation
// (api/v1alpha1: on|off|lz4|gzip|zstd|zstd-<1-9>|zstd-<1-9>-fast).
var compressionPattern = regexp.MustCompile(`^(on|off|lz4|gzip|zstd|zstd-[1-9]|zstd-[1-9]-fast)$`)

// mutableCompression extracts + validates the "compression" mutable parameter
// from a VolumeAttributesClass parameter map. Returns "" (no error) when the
// parameter is absent, so callers can no-op. An empty-string value is treated as
// absent (a VAC cannot un-set a property to inherit via ModifyVolume here).
func mutableCompression(params map[string]string) (string, error) {
	v := getSCParam(params, "compression")
	if v == "" {
		return "", nil
	}
	if !compressionPattern.MatchString(v) {
		return "", fmt.Errorf("%w: got %q", errInvalidCompression, v)
	}

	return v, nil
}

// applyMutableParams merges the sole supported VolumeAttributesClass parameter
// into StorageClass intent. CSI passes VAC parameters separately from StorageClass
// parameters, so mutable compression wins when both name the property.
func applyMutableParams(base scParams, params map[string]string) (scParams, error) {
	if err := validateMutableParams(params); err != nil {
		return base, err
	}
	compression, err := mutableCompression(params)
	if err != nil {
		return base, err
	}
	if compression != "" {
		base.Compression = compression
	}

	return base, nil
}

// validateMutableParams rejects keys this driver cannot apply after creation.
// Validation is case-insensitive to match StorageClass parameter handling.
func validateMutableParams(params map[string]string) error {
	for key := range params {
		if !strings.EqualFold(key, "compression") {
			return fmt.Errorf("%w: %q", errUnsupportedMutableParam, key)
		}
	}

	return nil
}

func validateSCParams(p scParams) error {
	if p.Type != "block" && p.Type != "filesystem" {
		return fmt.Errorf("%w: got %q", errUnsupportedType, p.Type)
	}

	if p.FsType != "ext4" && p.FsType != "xfs" {
		return fmt.Errorf("%w: got %q", errUnsupportedFSType, p.FsType)
	}

	if p.Transport != "nvme-tcp" {
		return fmt.Errorf("%w: got %q", errUnsupportedTransport, p.Transport)
	}

	// Reject block sizes the driver cannot use for capacity alignment before any
	// Volume CR is written. The CRD pattern permits "0" and unbounded digits, so
	// zero and overflow are only caught here.
	if p.BlockSize != "" {
		if _, err := zfs.ParseBlockSize(p.BlockSize); err != nil {
			return fmt.Errorf("%w: got %q (%v)", errInvalidBlockSize, p.BlockSize, err)
		}
	}
	if p.Type != "block" && p.NVMeTLSSpecified {
		return errNVMeTLSRequiresBlock
	}

	if p.Type != "filesystem" {
		if p.NFSTLSSpecified {
			return errNFSTLSRequiresFilesystem
		}
		return nil
	}

	if _, _, err := nfs.NormalizeExportIntent(p.NFSExportCIDRs, p.NFSExportAccessMode); err != nil {
		return fmt.Errorf("nfs export parameters: %w", err)
	}

	return nil
}
