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

package fakefs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

type fatalHelper interface {
	Helper()
	Fatalf(format string, args ...any)
}

func TestFSWriteReadRemoveProperties(t *testing.T) {
	t.Run("written file data is copied and removable", rapid.MakeCheck(func(t *rapid.T) {
		fs := New()
		filePath := fakePath().Draw(t, "filePath")
		content := []byte(rapid.StringOf(rapid.RuneFrom([]rune("abcXYZ012:-_"))).Draw(t, "content"))

		if err := fs.WriteFile(filePath, content); err != nil {
			t.Fatalf("WriteFile(%q): %v", filePath, err)
		}

		assertWriteCopiesBytes(t, fs, filePath, content)

		want := []byte(rapid.StringOf(rapid.RuneFrom([]rune("abcXYZ012:-_"))).Draw(t, "replacement"))
		if err := fs.WriteFile(filePath, want); err != nil {
			t.Fatalf("second WriteFile(%q): %v", filePath, err)
		}

		assertReadReturnsCopy(t, fs, filePath, want)

		if err := fs.RemoveAll(path.Dir(filePath)); err != nil {
			t.Fatalf("RemoveAll(%q): %v", path.Dir(filePath), err)
		}

		if ok, err := fs.Lexists(filePath); err != nil || ok {
			t.Fatalf("Lexists(%q) after RemoveAll = %v, %v; want false, nil", filePath, ok, err)
		}
	}))
}

func assertWriteCopiesBytes(t fatalHelper, fs *FS, filePath string, content []byte) {
	t.Helper()

	if len(content) > 0 {
		content[0] ^= 0xff
	}

	got, err := fs.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", filePath, err)
	}

	if bytes.Equal(got, content) && len(content) > 0 {
		t.Fatalf("ReadFile(%q) reflected caller mutation: got %q", filePath, got)
	}
}

func assertReadReturnsCopy(t fatalHelper, fs *FS, filePath string, want []byte) {
	t.Helper()

	got, err := fs.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", filePath, err)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("ReadFile(%q) = %q, want %q", filePath, got, want)
	}

	if len(got) > 0 {
		got[0] ^= 0xff
	}

	got, err = fs.ReadFile(filePath)
	if err != nil {
		t.Fatalf("second ReadFile(%q): %v", filePath, err)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("ReadFile(%q) after mutating returned bytes = %q, want %q", filePath, got, want)
	}
}

func TestFSSymlinkReadProperties(t *testing.T) {
	t.Run("relative symlink resolves from link directory", rapid.MakeCheck(func(t *rapid.T) {
		fs := New()
		dir := fakePath().Draw(t, "dir")
		fileName := fakeName().Draw(t, "fileName")

		linkName := fakeName().Draw(t, "linkName")
		if linkName == fileName {
			linkName += "-link"
		}

		filePath := path.Join(dir, fileName)
		linkPath := path.Join(dir, linkName)
		content := []byte(rapid.StringOf(rapid.RuneFrom([]rune("data-0123456789"))).Draw(t, "content"))

		if err := fs.WriteFile(filePath, content); err != nil {
			t.Fatalf("WriteFile(%q): %v", filePath, err)
		}

		if err := fs.Symlink(fileName, linkPath); err != nil {
			t.Fatalf("Symlink(%q, %q): %v", fileName, linkPath, err)
		}

		got, err := fs.ReadFile(linkPath)
		if err != nil {
			t.Fatalf("ReadFile(%q): %v", linkPath, err)
		}

		if !bytes.Equal(got, content) {
			t.Fatalf("ReadFile(%q) = %q, want %q", linkPath, got, content)
		}

		if ok, err := fs.Lexists(linkPath); err != nil || !ok {
			t.Fatalf("Lexists(%q) = %v, %v; want true, nil", linkPath, ok, err)
		}

		if err := fs.RemoveAll(filePath); err != nil {
			t.Fatalf("RemoveAll(%q): %v", filePath, err)
		}

		if _, err := fs.ReadFile(linkPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("ReadFile(%q) after target removal error = %v, want os.ErrNotExist", linkPath, err)
		}

		if ok, err := fs.Lexists(linkPath); err != nil || !ok {
			t.Fatalf("Lexists(%q) dangling = %v, %v; want true, nil", linkPath, ok, err)
		}
	}))
}

func TestFSReadDirProperties(t *testing.T) {
	t.Run("ReadDir returns immediate children only", rapid.MakeCheck(func(t *rapid.T) {
		fs := New()
		dir := fakePath().Draw(t, "dir")
		children := rapid.SliceOfN(fakeName(), 1, 12).Draw(t, "children")
		wantSet := make(map[string]struct{})

		for i, child := range children {
			name := fmt.Sprintf("%s-%d", child, i)

			wantSet[name] = struct{}{}
			if rapid.Bool().Draw(t, "nested") {
				if err := fs.WriteFile(path.Join(dir, name, "nested"), []byte(name)); err != nil {
					t.Fatalf("WriteFile nested child: %v", err)
				}
			} else if err := fs.WriteFile(path.Join(dir, name), []byte(name)); err != nil {
				t.Fatalf("WriteFile child: %v", err)
			}
		}

		got, err := fs.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%q): %v", dir, err)
		}

		sort.Strings(got)

		want := sortedKeys(wantSet)
		if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
			t.Fatalf("ReadDir(%q) = %v, want %v", dir, got, want)
		}
	}))
}

func fakePath() *rapid.Generator[string] {
	return rapid.Map(rapid.SliceOfN(fakeName(), 1, 5), func(parts []string) string {
		return "/" + path.Join(parts...)
	})
}

func fakeName() *rapid.Generator[string] {
	return rapid.StringMatching(`[a-z][a-z0-9\-]{0,8}`)
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}

	sort.Strings(out)

	return out
}
