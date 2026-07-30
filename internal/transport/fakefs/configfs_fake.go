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

// Package fakefs provides an in-memory configfs Writer for transport tests.
package fakefs

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
)

var (
	errNotAFile            = errors.New("fakefs: not a file")
	errPathComponentNotDir = errors.New("fakefs: path component is not a directory")
	errSymlinkLoop         = errors.New("fakefs: symlink loop")
)

type nodeKind int

const (
	maxSymlinkDepth = 16

	dirNode nodeKind = iota
	fileNode
	symlinkNode
)

type node struct {
	kind   nodeKind
	data   []byte
	target string
}

// FS is a concurrency-safe in-memory configfs tree.
type FS struct {
	mu    sync.Mutex
	nodes map[string]node
}

// New returns an empty filesystem with / present.
func New() *FS { return &FS{nodes: map[string]node{"/": {kind: dirNode}}} }

func (fs *FS) MkdirAll(p string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.mkdirAllLocked(clean(p))
}

func (fs *FS) WriteFile(p string, content []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	p = clean(p)
	if err := fs.mkdirAllLocked(path.Dir(p)); err != nil {
		return err
	}

	data := append([]byte(nil), content...)
	fs.nodes[p] = node{kind: fileNode, data: data}

	return nil
}

func (fs *FS) ReadFile(p string) ([]byte, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	n, err := fs.resolveLocked(clean(p), 0)
	if err != nil {
		return nil, err
	}

	if n.kind != fileNode {
		return nil, errNotAFile
	}

	return append([]byte(nil), n.data...), nil
}

func (fs *FS) RemoveAll(p string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	p = clean(p)
	if p == "/" {
		fs.nodes = map[string]node{"/": {kind: dirNode}}

		return nil
	}

	for name := range fs.nodes {
		if name == p || strings.HasPrefix(name, p+"/") {
			delete(fs.nodes, name)
		}
	}

	return nil
}

// Remove deletes a single entry, modelling a configfs rmdir. On real configfs a
// directory (subsystem/namespace) is torn down by rmdir even though it still
// "contains" kernel-managed attribute files and default-group subdirs — the
// kernel cascades those away. os.Remove (rmdir) succeeds there whereas
// os.RemoveAll fails EPERM trying to unlink the attribute files; that is exactly
// the distinction Unexport relies on. The fake therefore cascades the managed
// subtree (like a successful configfs rmdir) but still returns ErrNotExist for a
// missing node so idempotent teardown is observable.
func (fs *FS) Remove(p string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	p = clean(p)
	if _, ok := fs.nodes[p]; !ok {
		return fmt.Errorf("remove %s: %w", p, os.ErrNotExist)
	}
	for name := range fs.nodes {
		if name == p || strings.HasPrefix(name, p+"/") {
			delete(fs.nodes, name)
		}
	}

	return nil
}

func (fs *FS) Symlink(target, link string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	link = clean(link)
	if err := fs.mkdirAllLocked(path.Dir(link)); err != nil {
		return err
	}

	fs.nodes[link] = node{kind: symlinkNode, target: target}

	return nil
}

func (fs *FS) Lexists(p string) (bool, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	_, ok := fs.nodes[clean(p)]

	return ok, nil
}

// ReadDir lists the immediate child entry names of a directory.
func (fs *FS) ReadDir(p string) ([]string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	p = clean(p)
	if _, ok := fs.nodes[p]; !ok {
		return nil, os.ErrNotExist
	}

	prefix := p
	if prefix != "/" {
		prefix += "/"
	}

	var out []string

	for name := range fs.nodes {
		if name == p || !strings.HasPrefix(name, prefix) {
			continue
		}

		rest := strings.TrimPrefix(name, prefix)
		// immediate child only: no further "/"
		if strings.Contains(rest, "/") {
			continue
		}

		out = append(out, rest)
	}

	return out, nil
}

// compile-time assertion: FS implements transport.Writer (via struct methods
// matching the interface; import cycle avoided by keeping this package
// self-contained — the assertion lives in the transport package's test).
var _ = clean

func (fs *FS) mkdirAllLocked(p string) error {
	p = clean(p)
	if p == "/" {
		return nil
	}

	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")

	cur := ""
	for _, part := range parts {
		cur += "/" + part
		if n, ok := fs.nodes[cur]; ok && n.kind != dirNode {
			return errPathComponentNotDir
		}

		fs.nodes[cur] = node{kind: dirNode}
	}

	return nil
}

func (fs *FS) resolveLocked(p string, depth int) (node, error) {
	if depth > maxSymlinkDepth {
		return node{}, errSymlinkLoop
	}

	n, ok := fs.nodes[p]
	if !ok {
		return node{}, os.ErrNotExist
	}

	if n.kind != symlinkNode {
		return n, nil
	}

	target := n.target
	if !strings.HasPrefix(target, "/") {
		target = path.Join(path.Dir(p), target)
	}

	return fs.resolveLocked(clean(target), depth+1)
}

func clean(p string) string {
	if p == "" {
		return "/"
	}

	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	return path.Clean(p)
}
