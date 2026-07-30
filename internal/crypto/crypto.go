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

// Package crypto defines the per-volume Data-Encryption-Key (DEK) provider
// surface. The OpenBao implementation lives in internal/crypto/openbao; tests
// can use small in-process fakes.
//
// The flow (PLAN §6): at create + every import, the storage-agent fetches the
// DEK from the provider, stages it on tmpfs, hands the tmpfs path to zfs.Backend
// (create with keylocation=file://<tmpfs> / LoadKey), then shreds the tmpfs file.
// The key never touches disk.
package crypto

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	rawDEKSize   = 32
	tmpfsDirPerm = 0o700
	keyFilePerm  = 0o600
	shredBufSize = 32 * 1024
)

// KeyProvider mints, fetches, and crypto-shreds per-volume DEKs.
type KeyProvider interface {
	// Generate creates a fresh 32-byte raw DEK for a volume, stores it under the
	// provider, and returns a reference string (e.g. "transit/zfs-vol-<id>").
	Generate(ctx context.Context, volumeID string) (ref string, err error)
	// Fetch retrieves the raw DEK bytes for a reference. The caller stages these
	// to tmpfs and never persists them.
	Fetch(ctx context.Context, ref string) (rawKey []byte, err error)
	// Delete crypto-shreds the DEK (Transit: delete key version / KV: destroy path).
	// After this the volume's data is unrecoverable (PLAN §6 crypto-shred).
	Delete(ctx context.Context, ref string) error
}

// Sentinel errors.
var (
	ErrKeyNotFound       = errSentinel("crypto: key reference not found")
	ErrInvalidRawKeySize = errors.New("crypto: invalid raw key size")
	ErrTmpfsDirRequired  = errors.New("crypto: tmpfs dir is required")
)

type errSentinel string

func (e errSentinel) Error() string { return string(e) }

// IsKeyNotFound reports whether err means a provider reference is absent.
func IsKeyNotFound(err error) bool { return errors.Is(err, ErrKeyNotFound) }

// Stager stages raw key material to a tmpfs file and shreds it after use.
// Implemented in this package (internal/crypto/keyfile) — not a provider.
type Stager interface {
	// Stage writes rawKey to a tmpfs path and returns the file://<path> location
	// suitable for ZFS keylocation, plus the bare path (for shredding).
	Stage(volumeID string, rawKey []byte) (location string, path string, err error)
	// Shred securely removes the staged keyfile (best-effort overwrite + unlink).
	Shred(path string) error
}

// FileStager stages raw keys in a caller-provided tmpfs directory.
type FileStager struct {
	tmpfsDir string
}

// NewFileStager returns a Stager rooted at tmpfsDir (for example /run/zfs).
func NewFileStager(tmpfsDir string) *FileStager {
	return &FileStager{tmpfsDir: tmpfsDir}
}

// Stage writes rawKey to <tmpfsDir>/<volumeID> and returns the ZFS file:// URI
// and the bare path used by Shred.
func (s *FileStager) Stage(volumeID string, rawKey []byte) (location string, path string, err error) {
	if len(rawKey) != rawDEKSize {
		return "", "", fmt.Errorf("%w: must be %d bytes, got %d", ErrInvalidRawKeySize, rawDEKSize, len(rawKey))
	}

	if s.tmpfsDir == "" {
		return "", "", ErrTmpfsDirRequired
	}

	if err := os.MkdirAll(s.tmpfsDir, tmpfsDirPerm); err != nil {
		return "", "", fmt.Errorf("crypto: create tmpfs dir: %w", err)
	}

	path = filepath.Join(s.tmpfsDir, filepath.Base(volumeID))
	path = filepath.Clean(path)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, keyFilePerm)
	if err != nil {
		return "", "", fmt.Errorf("crypto: stage key: %w", err)
	}

	defer func() {
		if closeErr := f.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("crypto: close staged key: %w", closeErr)
		}
	}()

	if _, err = f.Write(rawKey); err != nil {
		_ = os.Remove(path)

		return "", "", fmt.Errorf("crypto: write staged key: %w", err)
	}

	return "file://" + path, path, nil
}

// Shred overwrites a staged key three times and removes it. Remove still runs
// if an overwrite fails so the tmpfs path is not left behind unnecessarily.
func (s *FileStager) Shred(path string) error {
	if path == "" {
		return nil
	}

	info, statErr := os.Stat(path)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return nil
		}

		return fmt.Errorf("crypto: stat staged key: %w", statErr)
	}

	var overwriteErr error
	if !info.IsDir() {
		overwriteErr = overwriteFile(path, info.Size())
	}

	removeErr := os.Remove(path)

	if overwriteErr != nil {
		return overwriteErr
	}

	if removeErr != nil && !os.IsNotExist(removeErr) {
		return fmt.Errorf("crypto: remove staged key: %w", removeErr)
	}

	return nil
}

// ShredAll shreds every staged file directly under the stager directory.
func (s *FileStager) ShredAll() error {
	entries, err := os.ReadDir(s.tmpfsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("crypto: read tmpfs dir: %w", err)
	}

	var errs []error

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if err := s.Shred(filepath.Join(s.tmpfsDir, entry.Name())); err != nil {
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("crypto: shred staged files: %w", err)
	}

	return nil
}

func overwriteFile(path string, size int64) error {
	if size <= 0 {
		return nil
	}

	patterns := []byte{0x00, 0xff, 0x00}
	buf := make([]byte, shredBufSize)

	for _, p := range patterns {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			return fmt.Errorf("crypto: open staged key for overwrite: %w", err)
		}

		for i := range buf {
			buf[i] = p
		}

		remaining := size
		for remaining > 0 {
			n := min(int64(len(buf)), remaining)
			if _, err := f.Write(buf[:n]); err != nil {
				_ = f.Close()

				return fmt.Errorf("crypto: overwrite staged key: %w", err)
			}

			remaining -= n
		}

		if err := f.Sync(); err != nil {
			_ = f.Close()

			return fmt.Errorf("crypto: sync staged key overwrite: %w", err)
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("crypto: close staged key overwrite: %w", err)
		}
	}

	return nil
}

var _ Stager = (*FileStager)(nil)
