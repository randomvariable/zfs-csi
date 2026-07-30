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

package agent

import (
	"slices"
	"testing"

	"pgregory.net/rapid"
)

// fenceIsReplacement decides whether an F1 fence (ForceDisconnect, which drops
// ALL controllers of the subsystem) fires. A wrong answer is safety-critical in
// both directions: a false negative on a genuine single-writer failover leaves a
// zombie writer attached (split-brain corruption), and a false positive on a
// multi-node scale-down bounces a legitimate surviving co-tenant (spurious I/O
// stall). The example test covers hand-picked transitions; these properties
// assert the invariant across arbitrary initiator sets.
//
// The caller guarantees len(desired)==1, so every case draws exactly one desired
// initiator plus an arbitrary prior-live set.
func TestFenceIsReplacementProperties(t *testing.T) {
	// An alphabet small enough that random prior-live sets frequently DO and
	// don't contain the desired initiator, exercising both branches densely.
	initiator := rapid.SampledFrom([]string{
		"nqn.a", "nqn.b", "nqn.c", "nqn.d", "nqn.e",
	})

	drawDesired := func(t *rapid.T) (map[string]string, string) {
		id := initiator.Draw(t, "desired")

		return map[string]string{id: "node-" + id}, id
	}
	drawPriorLive := func(t *rapid.T) []string {
		return rapid.SliceOfN(initiator, 0, 5).Draw(t, "priorLive")
	}

	// Definitional: fires exactly when the sole desired initiator is NOT already
	// live. This is the whole contract — a replacement introduces a NEW initiator.
	t.Run("fires iff desired initiator absent from prior-live", rapid.MakeCheck(func(t *rapid.T) {
		desired, id := drawDesired(t)
		priorLive := drawPriorLive(t)

		got := fenceIsReplacement(desired, priorLive)
		want := !slices.Contains(priorLive, id)
		if got != want {
			t.Fatalf("fenceIsReplacement(%v, %v) = %v, want %v", desired, priorLive, got, want)
		}
	}))

	// Safety (no survivor bounce): if the desired initiator is among the prior
	// live set, the fence must NEVER fire — that is a scale-down survivor, and
	// bouncing it is the exact ROX/RWX co-tenant outage F1's scoping forbids.
	t.Run("never fires when desired initiator was already live", rapid.MakeCheck(func(t *rapid.T) {
		desired, id := drawDesired(t)
		// Force the desired initiator into the prior-live set at a random position.
		priorLive := drawPriorLive(t)
		priorLive = append(priorLive, id)
		rapid.Permutation(priorLive).Draw(t, "shuffle")

		if fenceIsReplacement(desired, priorLive) {
			t.Fatalf("fenced a scale-down survivor: desired=%v priorLive=%v", desired, priorLive)
		}
	}))

	// Metamorphic: adding the desired initiator to a prior-live set that did NOT
	// contain it must flip a fire into a no-fire. A genuine failover ([A]->[B])
	// becomes a subset-shrink ([A,B]->[B]) the moment B was already present.
	t.Run("adding desired to prior-live flips fire to no-fire", rapid.MakeCheck(func(t *rapid.T) {
		desired, id := drawDesired(t)
		priorLive := drawPriorLive(t)

		if slices.Contains(priorLive, id) {
			// Only meaningful when it currently fires; drop the desired id so it does.
			priorLive = slices.DeleteFunc(priorLive, func(s string) bool { return s == id })
		}
		if !fenceIsReplacement(desired, priorLive) {
			t.Fatalf("precondition: expected fire for desired=%v priorLive=%v", desired, priorLive)
		}

		withDesired := append(slices.Clone(priorLive), id)
		if fenceIsReplacement(desired, withDesired) {
			t.Fatalf("adding desired %q to prior-live did not suppress the fence: %v", id, withDesired)
		}
	}))

	// Empty prior-live is a fresh attach: caller gates fence on orphanUnmapped
	// (false here), but the pure function must still report "replacement" (the
	// desired initiator is trivially absent), never panicking on an empty slice.
	t.Run("empty prior-live is always a replacement", rapid.MakeCheck(func(t *rapid.T) {
		desired, _ := drawDesired(t)
		if !fenceIsReplacement(desired, nil) {
			t.Fatalf("empty prior-live should be a replacement: desired=%v", desired)
		}
	}))
}
