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
	"encoding/json"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/randomvariable/zfs-csi/internal/reachability"
)

// storageOwnerNode reports whether the labels of Node nodeName satisfy the
// nodeSelector of any storage owner listed in selectorsPath, a JSON array of
// label maps. On such a node the storage agent's tlshd answers every TLS
// handshake in the host network namespace, so the node plugin must not start
// a second one.
func storageOwnerNode(ctx context.Context, nodeName, selectorsPath string, getter reachability.NodeGetter) (bool, error) {
	if nodeName == "" {
		return false, fmt.Errorf("NODE_NAME is required")
	}
	data, err := os.ReadFile(selectorsPath)
	if err != nil {
		return false, fmt.Errorf("read storage owner selectors: %w", err)
	}
	var selectors []map[string]string
	if err := json.Unmarshal(data, &selectors); err != nil {
		return false, fmt.Errorf("parse storage owner selectors %s: %w", selectorsPath, err)
	}
	node, err := getter.GetNode(ctx, nodeName)
	if err != nil {
		return false, fmt.Errorf("get Node %q: %w", nodeName, err)
	}
	for _, selector := range selectors {
		if len(selector) > 0 && labels.SelectorFromSet(selector).Matches(labels.Set(node.Labels)) {
			return true, nil
		}
	}
	return false, nil
}
