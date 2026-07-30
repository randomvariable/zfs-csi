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

package crypto

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStagerStageAndShred(t *testing.T) {
	dir := t.TempDir()
	stager := NewFileStager(dir)
	raw := bytes.Repeat([]byte{0x42}, rawDEKSize)

	location, path, err := stager.Stage("csi:tank:block:vol-1", raw)
	if err != nil {
		t.Fatalf("Stage() error = %v", err)
	}

	if location != "file://"+path {
		t.Fatalf("location = %q, want file://%s", location, path)
	}

	if filepath.Dir(path) != dir {
		t.Fatalf("path %q not under %q", path, dir)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	if !bytes.Equal(got, raw) {
		t.Fatal("staged bytes mismatch")
	}

	if err := stager.Shred(path); err != nil {
		t.Fatalf("Shred() error = %v", err)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged key still exists, stat err = %v", err)
	}
}

func TestFileStagerRejectsWrongKeyLength(t *testing.T) {
	_, _, err := NewFileStager(t.TempDir()).Stage("vol", []byte("too short"))
	if err == nil {
		t.Fatal("Stage() error = nil, want length error")
	}
}

func TestFileStagerShredAll(t *testing.T) {
	dir := t.TempDir()
	stager := NewFileStager(dir)
	raw := bytes.Repeat([]byte{0x24}, rawDEKSize)

	_, path1, err := stager.Stage("vol-1", raw)
	if err != nil {
		t.Fatalf("Stage(vol-1) error = %v", err)
	}

	_, path2, err := stager.Stage("vol-2", raw)
	if err != nil {
		t.Fatalf("Stage(vol-2) error = %v", err)
	}

	if err := stager.ShredAll(); err != nil {
		t.Fatalf("ShredAll() error = %v", err)
	}

	for _, path := range []string{path1, path2} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s still exists, stat err = %v", path, err)
		}
	}
}
