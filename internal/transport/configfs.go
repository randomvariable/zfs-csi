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

package transport

import (
	"fmt"
	"os"
	"path/filepath"
)

// Writer is the minimal configfs filesystem surface used by target backends.
// Tests provide an in-memory implementation; production uses realWriter.
type Writer interface {
	MkdirAll(path string) error
	WriteFile(path string, content []byte) error
	ReadFile(path string) ([]byte, error)
	// ReadDir lists the immediate child entry names in a directory.
	// Returns os.ErrNotExist (via Lexists==false) for a missing dir; returns
	// an empty slice for an empty dir. Symlinks are listed by link name.
	ReadDir(path string) ([]string, error)
	RemoveAll(path string) error
	// Remove deletes a single entry (unlink for a file/symlink, rmdir for an
	// empty directory). Unlike RemoveAll it does NOT recurse — this is required
	// for configfs, whose directories are torn down only by rmdir of the
	// (emptied) directory itself; recursively unlinking their attribute files
	// fails with EPERM.
	Remove(path string) error
	Symlink(target, link string) error
	Lexists(path string) (bool, error)
}

// NewRealWriter returns a Writer backed by the host filesystem.
func NewRealWriter() Writer { return realWriter{} }

type realWriter struct{}

func (realWriter) MkdirAll(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("mkdirall %s: %w", path, err)
	}

	return nil
}

func (realWriter) WriteFile(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir parent for %s: %w", path, err)
	}

	if err := os.WriteFile(path, content, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	return nil
}

func (realWriter) ReadFile(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	return b, nil
}

func (realWriter) ReadDir(p string) ([]string, error) {
	entries, err := os.ReadDir(p)
	if err != nil {
		return nil, fmt.Errorf("readdir %s: %w", p, err)
	}

	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}

	return out, nil
}

func (realWriter) RemoveAll(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("removeall %s: %w", path, err)
	}

	return nil
}

func (realWriter) Remove(path string) error {
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove %s: %w", path, err)
	}

	return nil
}

func (realWriter) Symlink(target, link string) error {
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		return fmt.Errorf("mkdir parent for symlink %s: %w", link, err)
	}

	if err := os.Symlink(target, link); err != nil {
		return fmt.Errorf("symlink %s -> %s: %w", link, target, err)
	}

	return nil
}

func (realWriter) Lexists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, fmt.Errorf("lstat %s: %w", path, err)
}
