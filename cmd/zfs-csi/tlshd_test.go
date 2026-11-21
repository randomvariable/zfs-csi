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

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fakeNodeGetter struct {
	node *corev1.Node
	err  error
}

func (f fakeNodeGetter) GetNode(context.Context, string) (*corev1.Node, error) { return f.node, f.err }

func TestStorageOwnerNode(t *testing.T) {
	selectors := filepath.Join(t.TempDir(), "selectors.json")
	if err := os.WriteFile(selectors, []byte(`[
		{"kubernetes.io/arch": "amd64", "kubernetes.io/hostname": "server7"},
		{"kubernetes.io/arch": "amd64", "kubernetes.io/hostname": "server25"}
	]`), 0o600); err != nil {
		t.Fatal(err)
	}
	node := func(hostname string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			"kubernetes.io/arch":     "amd64",
			"kubernetes.io/hostname": hostname,
		}}}
	}

	for _, tc := range []struct {
		name    string
		getter  fakeNodeGetter
		want    bool
		wantErr bool
	}{
		{name: "owner", getter: fakeNodeGetter{node: node("server25")}, want: true},
		// Sharing one label of an owner's selector, here the arch, is not enough.
		{name: "client", getter: fakeNodeGetter{node: node("server21")}},
		{name: "node lookup fails", getter: fakeNodeGetter{err: errors.New("forbidden")}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := storageOwnerNode(context.Background(), "node", selectors, tc.getter)
			if (err != nil) != tc.wantErr {
				t.Fatalf("storageOwnerNode() error = %v, wantErr %t", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("storageOwnerNode() = %t, want %t", got, tc.want)
			}
		})
	}
}
