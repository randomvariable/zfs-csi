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

package zfs

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// DefaultVolBlockSize is the block size the driver aligns zvol capacity to when
// no explicit volblocksize is requested. OpenZFS's own default volblocksize is
// 16 KiB (8 KiB before OpenZFS 2.2), and every supported default is a power of
// two no larger than this, so a capacity aligned to 16 KiB is a legal volsize
// under any of them.
const DefaultVolBlockSize int64 = 16 * 1024

// DefaultVolBlockSizeValue is the canonical string form of DefaultVolBlockSize.
// The driver persists this explicitly on block Volume CRs instead of leaving
// volBlockSize empty, so create-time and expand-time alignment can never
// diverge from an unset (and version-dependent) ZFS default.
const DefaultVolBlockSizeValue = "16k"

// MinVolBlockSize and MaxVolBlockSize bound the volblocksize values OpenZFS
// accepts for a zvol: a power of two between 512 bytes (SPA_MINBLOCKSIZE) and
// 128 KiB (SPA_OLD_MAXBLOCKSIZE, the largest volblocksize zvol_create honours).
const (
	MinVolBlockSize int64 = 512
	MaxVolBlockSize int64 = 128 * 1024
)

var (
	// ErrInvalidBlockSize reports a volblocksize/recordsize string the driver
	// cannot interpret as a positive byte count.
	ErrInvalidBlockSize = errors.New("zfs: invalid block size")
	// ErrCapacityOverflow reports capacity arithmetic that would exceed int64.
	ErrCapacityOverflow = errors.New("zfs: capacity overflow")
)

// ParseBlockSize converts the volblocksize/recordsize syntax the Volume CRD
// accepts (`^[0-9]+[kKmMgG]?$`, base 1024) into bytes. It additionally rejects
// what the CRD pattern cannot express: a zero result and int64 overflow.
func ParseBlockSize(value string) (int64, error) {
	if value == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidBlockSize)
	}

	digits := value
	multiplier := int64(1)

	switch value[len(value)-1] {
	case 'k', 'K':
		digits, multiplier = value[:len(value)-1], 1024
	case 'm', 'M':
		digits, multiplier = value[:len(value)-1], 1024*1024
	case 'g', 'G':
		digits, multiplier = value[:len(value)-1], 1024*1024*1024
	}

	// strconv.ParseInt accepts signs and underscores; the CRD pattern does not.
	if digits == "" || strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidBlockSize, value)
	}

	base, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q: %w", ErrInvalidBlockSize, value, err)
	}
	if base <= 0 {
		return 0, fmt.Errorf("%w: %q must be positive", ErrInvalidBlockSize, value)
	}
	if base > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("%w: %q overflows int64", ErrInvalidBlockSize, value)
	}

	return base * multiplier, nil
}

// EffectiveBlockSize resolves the block size a zvol capacity must be a multiple
// of. An empty value means the volume inherits the ZFS default.
func EffectiveBlockSize(value string) (int64, error) {
	if value == "" {
		return DefaultVolBlockSize, nil
	}

	return ParseBlockSize(value)
}

// CanonicalVolBlockSize validates a requested zvol volblocksize against what
// OpenZFS actually accepts and returns the canonical string form plus the byte
// value. Accepted input syntax is unchanged (digits with an optional
// k/K/m/M/g/G suffix, base 1024); the additional constraints are OpenZFS's own:
// the value must be a power of two between 512 bytes and 128 KiB. Canonical
// output is plain digits below 1 KiB ("512") and lower-case k units at or above
// it ("1k", "16k", "128k"), so equivalent spellings such as "16K", "16k" and
// "16384" all persist identically.
func CanonicalVolBlockSize(value string) (string, int64, error) {
	size, err := ParseBlockSize(value)
	if err != nil {
		return "", 0, err
	}
	if size&(size-1) != 0 {
		return "", 0, fmt.Errorf("%w: %q (%d bytes) must be a power of two", ErrInvalidBlockSize, value, size)
	}
	if size < MinVolBlockSize || size > MaxVolBlockSize {
		return "", 0, fmt.Errorf("%w: %q (%d bytes) must be between %d and %d bytes",
			ErrInvalidBlockSize, value, size, MinVolBlockSize, MaxVolBlockSize)
	}
	if size < 1024 {
		return strconv.FormatInt(size, 10), size, nil
	}

	return strconv.FormatInt(size/1024, 10) + "k", size, nil
}

// AlignUp rounds capacity up to the next multiple of blockSize. ZFS rejects a
// `volsize` that is not a multiple of `volblocksize`, so every zvol capacity the
// driver persists, reserves, or reports must pass through here.
func AlignUp(capacity, blockSize int64) (int64, error) {
	if capacity <= 0 {
		return 0, fmt.Errorf("%w: capacity %d must be positive", ErrInvalidBlockSize, capacity)
	}
	if blockSize <= 0 {
		return 0, fmt.Errorf("%w: block size %d must be positive", ErrInvalidBlockSize, blockSize)
	}

	remainder := capacity % blockSize
	if remainder == 0 {
		return capacity, nil
	}
	padding := blockSize - remainder
	if capacity > math.MaxInt64-padding {
		return 0, fmt.Errorf("%w: aligning %d to %d exceeds int64", ErrCapacityOverflow, capacity, blockSize)
	}

	return capacity + padding, nil
}
