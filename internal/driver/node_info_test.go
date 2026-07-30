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

package driver

import (
	"reflect"
	"testing"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"

	"github.com/randomvariable/zfs-csi/internal/reachability"
)

// TestNodeGetInfo_MaxVolumesPerNode verifies the per-node attach ceiling is
// reported when configured (>0) and omitted (0 => "no limit") otherwise, per
// the CSI spec.
func TestNodeGetInfo_MaxVolumesPerNode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		max  int64
		want int64
	}{
		{name: "reported when positive", max: 128, want: 128},
		{name: "omitted when zero", max: 0, want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ns := NewNodeServer(NodeConfig{
				Log:               logr.Discard(),
				NodeID:            "node-a",
				MaxVolumesPerNode: tc.max,
				NetworkDomain:     "fabric-a",
			})

			resp, err := ns.NodeGetInfo(t.Context(), &csi.NodeGetInfoRequest{})
			if err != nil {
				t.Fatalf("NodeGetInfo: %v", err)
			}
			if resp.GetMaxVolumesPerNode() != tc.want {
				t.Fatalf("MaxVolumesPerNode = %d, want %d", resp.GetMaxVolumesPerNode(), tc.want)
			}
			if resp.GetNodeId() != "node-a" {
				t.Fatalf("NodeId = %q, want node-a", resp.GetNodeId())
			}
			wantTopology := map[string]string{reachability.TopologyKeyNetworkDomain: "fabric-a"}
			if !reflect.DeepEqual(resp.GetAccessibleTopology().GetSegments(), wantTopology) {
				t.Fatalf("AccessibleTopology = %v, want %v", resp.GetAccessibleTopology().GetSegments(), wantTopology)
			}
			if len(resp.GetAccessibleTopology().GetSegments()) != 1 {
				t.Fatalf("AccessibleTopology has %d segments, want exactly 1", len(resp.GetAccessibleTopology().GetSegments()))
			}
		})
	}
}
