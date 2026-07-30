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

package transport_test

import (
	"testing"

	"github.com/randomvariable/zfs-csi/internal/transport/fakefs"
)

func TestFakeFSMkdirWriteReadSymlinkRemove(t *testing.T) {
	fs := fakefs.New()
	if err := fs.MkdirAll("/a/b/c"); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	if err := fs.WriteFile("/a/b/c/value", []byte("hello")); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := fs.Symlink("c/value", "/a/b/link"); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	data, err := fs.ReadFile("/a/b/link")
	if err != nil {
		t.Fatalf("ReadFile through symlink: %v", err)
	}

	if string(data) != "hello" {
		t.Fatalf("ReadFile = %q, want hello", data)
	}

	if ok, err := fs.Lexists("/a/b/link"); err != nil || !ok {
		t.Fatalf("Lexists link = %v, %v; want true, nil", ok, err)
	}

	if err := fs.RemoveAll("/a/b/c"); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	if _, err := fs.ReadFile("/a/b/link"); err == nil {
		t.Fatal("ReadFile through dangling symlink succeeded")
	}

	if ok, err := fs.Lexists("/a/b/link"); err != nil || !ok {
		t.Fatalf("Lexists dangling link = %v, %v; want true, nil", ok, err)
	}
}
