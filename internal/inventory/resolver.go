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

package inventory

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

// Resolver resolves immutable pool identities from fresh StorageNode inventory.
type Resolver struct {
	Client crclient.Client
	Now    func() time.Time
}

func (r Resolver) ResolvePoolGUID(ctx context.Context, ownerNode, pool string) (string, error) {
	if r.Client == nil {
		return "", fmt.Errorf("StorageNode inventory client is not configured")
	}
	node := &zfscsiv1.StorageNode{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: ownerNode}, node); err != nil {
		return "", fmt.Errorf("get StorageNode %q: %w", ownerNode, err)
	}
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	if !Eligible(node, now) {
		return "", fmt.Errorf("StorageNode %q inventory is not eligible", ownerNode)
	}
	guid := ""
	for _, candidate := range node.Status.Pools {
		if candidate.Name != pool || !candidate.Ready {
			continue
		}
		if guid != "" {
			return "", fmt.Errorf("StorageNode %q has ambiguous ready pool name %q", ownerNode, pool)
		}
		value, err := strconv.ParseUint(candidate.GUID, 10, 64)
		if err != nil || value == 0 || strconv.FormatUint(value, 10) != candidate.GUID {
			return "", fmt.Errorf("StorageNode %q pool %q has invalid GUID %q", ownerNode, pool, candidate.GUID)
		}
		guid = candidate.GUID
	}
	if guid == "" {
		return "", fmt.Errorf("StorageNode %q has no ready pool named %q", ownerNode, pool)
	}
	return guid, nil
}
