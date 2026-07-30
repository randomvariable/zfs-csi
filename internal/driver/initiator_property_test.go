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
	"slices"
	"testing"

	"pgregory.net/rapid"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

// removeInitiator and upsertInitiator are the pure list mutators behind the F5
// optimistic-lock unpublish and the single-writer publish. Both mutate the input
// slice IN PLACE (removeInitiator via in[:0], upsertInitiator via in[i]=m), which
// is a subtle aliasing trap: a lost-update or a duplicated/dropped node here is
// exactly the failover corruption F5 exists to prevent. These properties assert
// their invariants across arbitrary initiator lists.

// drawInitiators builds a list of MappedInitiators with UNIQUE node names (the
// production invariant: upsert keys on NodeName, so the list never holds two
// entries for one node). InitiatorID is derived so we can assert entries survive
// intact, not just by node name.
func drawInitiators(t *rapid.T, label string) []zfscsiv1.MappedInitiator {
	nodes := rapid.SliceOfNDistinct(
		rapid.SampledFrom([]string{"node-a", "node-b", "node-c", "node-d", "node-e"}),
		0, 5,
		func(s string) string { return s },
	).Draw(t, label)

	out := make([]zfscsiv1.MappedInitiator, len(nodes))
	for i, n := range nodes {
		out[i] = zfscsiv1.MappedInitiator{NodeName: n, InitiatorID: "nqn." + n}
	}

	return out
}

func hasNode(list []zfscsiv1.MappedInitiator, node string) bool {
	return slices.ContainsFunc(list, func(m zfscsiv1.MappedInitiator) bool {
		return m.NodeName == node
	})
}

func entryFor(list []zfscsiv1.MappedInitiator, node string) (zfscsiv1.MappedInitiator, bool) {
	for _, m := range list {
		if m.NodeName == node {
			return m, true
		}
	}

	return zfscsiv1.MappedInitiator{}, false
}

func TestRemoveInitiatorProperties(t *testing.T) {
	node := rapid.SampledFrom([]string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-absent"})

	t.Run("removed node is absent and others are preserved", rapid.MakeCheck(func(t *rapid.T) {
		in := drawInitiators(t, "in")
		target := node.Draw(t, "target")

		// Snapshot the entries we expect to survive (every non-target node).
		var wantSurvivors []zfscsiv1.MappedInitiator
		for _, m := range in {
			if m.NodeName != target {
				wantSurvivors = append(wantSurvivors, m)
			}
		}

		got := removeInitiator(slices.Clone(in), target)

		if hasNode(got, target) {
			t.Fatalf("removeInitiator left target %q in %v", target, got)
		}
		if len(got) != len(wantSurvivors) {
			t.Fatalf("survivor count = %d, want %d (in=%v got=%v)", len(got), len(wantSurvivors), in, got)
		}
		for _, w := range wantSurvivors {
			e, ok := entryFor(got, w.NodeName)
			if !ok || e != w {
				t.Fatalf("survivor %v missing/altered in %v", w, got)
			}
		}
	}))

	t.Run("idempotent: removing twice equals removing once", rapid.MakeCheck(func(t *rapid.T) {
		in := drawInitiators(t, "in")
		target := node.Draw(t, "target")

		once := removeInitiator(slices.Clone(in), target)
		twice := removeInitiator(removeInitiator(slices.Clone(in), target), target)

		if !slices.Equal(once, twice) {
			t.Fatalf("not idempotent: once=%v twice=%v", once, twice)
		}
	}))
}

func TestUpsertInitiatorProperties(t *testing.T) {
	t.Run("upsert yields exactly one entry for the node, equal to input", rapid.MakeCheck(func(t *rapid.T) {
		in := drawInitiators(t, "in")
		node := rapid.SampledFrom([]string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-new"}).Draw(t, "node")
		m := zfscsiv1.MappedInitiator{NodeName: node, InitiatorID: rapid.SampledFrom([]string{"nqn.x", "nqn.y", "nqn.z"}).Draw(t, "iid")}

		hadNode := hasNode(in, node)
		got := upsertInitiator(slices.Clone(in), m)

		// Exactly one entry for the node, and it equals m (upsert overwrites).
		count := 0
		for _, e := range got {
			if e.NodeName == node {
				count++
				if e != m {
					t.Fatalf("upserted entry = %v, want %v", e, m)
				}
			}
		}
		if count != 1 {
			t.Fatalf("node %q appears %d times after upsert, want 1: %v", node, count, got)
		}

		// Length grows by exactly 1 for a new node, stays equal for an existing one.
		wantLen := len(in)
		if !hadNode {
			wantLen++
		}
		if len(got) != wantLen {
			t.Fatalf("len after upsert = %d, want %d (hadNode=%v)", len(got), wantLen, hadNode)
		}
	}))

	t.Run("upsert preserves every other node unchanged", rapid.MakeCheck(func(t *rapid.T) {
		in := drawInitiators(t, "in")
		node := rapid.SampledFrom([]string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-new"}).Draw(t, "node")
		m := zfscsiv1.MappedInitiator{NodeName: node, InitiatorID: "nqn.upserted"}

		got := upsertInitiator(slices.Clone(in), m)

		for _, orig := range in {
			if orig.NodeName == node {
				continue
			}
			e, ok := entryFor(got, orig.NodeName)
			if !ok || e != orig {
				t.Fatalf("bystander %v altered/dropped by upsert: %v", orig, got)
			}
		}
	}))

	// Metamorphic round-trip: removing a just-upserted node leaves no trace of it.
	t.Run("remove after upsert removes the node", rapid.MakeCheck(func(t *rapid.T) {
		in := drawInitiators(t, "in")
		node := rapid.SampledFrom([]string{"node-a", "node-b", "node-c", "node-new"}).Draw(t, "node")
		m := zfscsiv1.MappedInitiator{NodeName: node, InitiatorID: "nqn.rt"}

		got := removeInitiator(upsertInitiator(slices.Clone(in), m), node)
		if hasNode(got, node) {
			t.Fatalf("remove after upsert left %q behind: %v", node, got)
		}
	}))
}
