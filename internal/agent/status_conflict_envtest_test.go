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

//go:build envtest

package agent

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	eventsv1 "github.com/randomvariable/zfs-csi/internal/observability/events"
	testenv "github.com/randomvariable/zfs-csi/internal/testutil/envtest"
)

// TestEnvtestStatusConflict_DisjointFieldsSurviveConcurrentWrites proves that two
// managers writing their own disjoint status fields (controller: mappedInitiators,
// agent: publishedInitiators + actualCapacity) never lose each other's last write.
//
// On the PRE-FIX code this would fail because:
//   - the controller's MergeFrom baseline included speculative writes to
//     targetNQN/portal (agent-owned fields), which could blank them on conflict;
//   - the Confirmed field on MappedInitiator was mutated by BOTH writers on the
//     same array entry, producing a classic lost-update.
//
// Post-fix: every status field has exactly one writer, so JSON merge patch
// bodies are disjoint and never clobber.
func TestEnvtestStatusConflict_DisjointFieldsSurviveConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	const volName = "conflict-vol"
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: volName},
		Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
			Pool:     "tank",
			VolName:  volName,
			Type:     zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30,
			VolumeID: "csi:tank:block:conflict-vol",
		}),
	}
	if err := h.Client.Create(ctx, vol); err != nil {
		t.Fatalf("create Volume: %v", err)
	}

	// Set the agent-owned fields so we can prove they are never blanked.
	//nolint:forbidigo // The conflict test needs Status().Update to validate that API's lost-update behavior.
	if err := h.Client.Status().Update(ctx, &zfscsiv1.Volume{
		ObjectMeta: vol.ObjectMeta,
		Status: zfscsiv1.VolumeStatus{
			State:          zfscsiv1.VolumeStateReady,
			TargetNQN:      "nqn.test/target",
			Portal:         "server7:4420",
			ActualCapacity: 1 << 30,
		},
	}); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	const rounds = 50
	var agentCount int64

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer C (controller role): repeatedly patches status.mappedInitiators
	// with optimistic lock + retry on 409.
	go func() {
		defer wg.Done()
		for range rounds {
			v := &zfscsiv1.Volume{}
			if err := h.Client.Get(ctx, types.NamespacedName{Name: volName}, v); err != nil {
				t.Errorf("controller get: %v", err)
				return
			}
			patch := crclient.MergeFromWithOptions(v.DeepCopy(), crclient.MergeFromWithOptimisticLock{})
			v.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{
				NodeName: "node-a", InitiatorID: "nqn.node-a",
			}}
			if err := h.Client.Status().Patch(ctx, v, patch); err != nil {
				// Conflict is expected under concurrent writes — retry once.
				v2 := &zfscsiv1.Volume{}
				if err := h.Client.Get(ctx, types.NamespacedName{Name: volName}, v2); err != nil {
					t.Errorf("controller re-get: %v", err)
					return
				}
				patch2 := crclient.MergeFromWithOptions(v2.DeepCopy(), crclient.MergeFromWithOptimisticLock{})
				v2.Status.MappedInitiators = []zfscsiv1.MappedInitiator{{
					NodeName: "node-a", InitiatorID: "nqn.node-a",
				}}
				_ = h.Client.Status().Patch(ctx, v2, patch2)
			}
		}
	}()

	// Writer A (agent role): repeatedly patches status.publishedInitiators + bumps
	// actualCapacity.
	go func() {
		defer wg.Done()
		for range rounds {
			v := &zfscsiv1.Volume{}
			if err := h.Client.Get(ctx, types.NamespacedName{Name: volName}, v); err != nil {
				t.Errorf("agent get: %v", err)
				return
			}
			patch := crclient.MergeFrom(v.DeepCopy())
			v.Status.PublishedInitiators = []string{"nqn.node-a"}
			v.Status.ActualCapacity = v.Status.ActualCapacity + 1
			if err := h.Client.Status().Patch(ctx, v, patch); err != nil {
				t.Errorf("agent patch: %v", err)
				return
			}
			atomic.AddInt64(&agentCount, 1)
		}
	}()

	wg.Wait()

	// Assert: all agent patches succeeded.
	if got := atomic.LoadInt64(&agentCount); got != rounds {
		t.Fatalf("agent successful patches = %d, want %d", got, rounds)
	}

	// Assert: controller's field survived.
	final := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: volName}, final); err != nil {
		t.Fatalf("final get: %v", err)
	}

	if len(final.Status.MappedInitiators) == 0 || final.Status.MappedInitiators[0].NodeName != "node-a" {
		t.Fatalf("controller field lost: mappedInitiators = %+v", final.Status.MappedInitiators)
	}

	// Assert: agent's fields survived.
	found := false
	for _, p := range final.Status.PublishedInitiators {
		if p == "nqn.node-a" {
			found = true
		}
	}
	if !found {
		t.Fatalf("agent field lost: publishedInitiators = %+v", final.Status.PublishedInitiators)
	}

	if final.Status.ActualCapacity <= 1<<30 {
		t.Fatalf("agent field lost: actualCapacity = %d (expected growth)", final.Status.ActualCapacity)
	}

	// Assert: agent-owned fields were NEVER blanked by the controller.
	if final.Status.TargetNQN == "" {
		t.Fatalf("agent field blanked by controller: targetNQN is empty")
	}
	if final.Status.Portal == "" {
		t.Fatalf("agent field blanked by controller: portal is empty")
	}

	t.Logf("final state: mappedInitiators=%d publishedInitiators=%v actualCapacity=%d targetNQN=%q portal=%q",
		len(final.Status.MappedInitiators), final.Status.PublishedInitiators,
		final.Status.ActualCapacity, final.Status.TargetNQN, final.Status.Portal)
}

func TestEnvtestRecordBackendHealthWarning_RetriesConflictWithoutMutatingCaller(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "health-conflict"},
		Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
			Pool: "tank", VolName: "health-conflict", Type: zfscsiv1.VolumeTypeBlock,
			Capacity: 1 << 30, VolumeID: "csi:tank:block:health-conflict",
		}),
	}
	if err := h.Client.Create(ctx, vol); err != nil {
		t.Fatalf("create Volume: %v", err)
	}

	caller := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, caller); err != nil {
		t.Fatalf("get Volume: %v", err)
	}
	r := &VolumeReconciler{Client: &statusConflictOnceClient{Client: h.Client}, Log: logr.Discard()}
	if err := r.recordBackendHealthWarning(ctx, caller, "target export is missing"); err != nil {
		t.Fatalf("record backend health warning: %v", err)
	}
	if health := findCondition(caller.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy)); health != nil {
		t.Fatalf("caller object mutated: BackendHealthy = %#v", health)
	}

	got := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}, got); err != nil {
		t.Fatalf("get persisted Volume: %v", err)
	}
	if health := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionBackendHealthy)); health == nil ||
		health.Status != metav1.ConditionFalse || health.Reason != eventsv1.ReasonBackendUnhealthy {
		t.Fatalf("BackendHealthy = %#v, want False/%s", health, eventsv1.ReasonBackendUnhealthy)
	}
	if got.Status.TargetNQN != "nqn.test/concurrent" {
		t.Fatalf("concurrent status field lost: targetNQN = %q", got.Status.TargetNQN)
	}
	if ready := findCondition(got.Status.Conditions, string(zfscsiv1.VolumeConditionReady)); ready == nil ||
		ready.Status != metav1.ConditionTrue || ready.Reason != "ConcurrentWriter" {
		t.Fatalf("concurrent status condition lost: Ready = %#v", ready)
	}
	if got.Status.State != zfscsiv1.VolumeStateReady {
		t.Fatalf("concurrent status field lost: state = %q", got.Status.State)
	}
}

func TestEnvtestPatchConditions_ConcurrentNonOwnedConditionComparison(t *testing.T) {
	for _, tt := range []struct {
		name             string
		mutateConcurrent func(*metav1.Condition)
		wantErr          bool
	}{
		{
			name: "timestamp only",
			mutateConcurrent: func(condition *metav1.Condition) {
				condition.LastTransitionTime = metav1.NewTime(condition.LastTransitionTime.Add(time.Second))
			},
		},
		{
			name: "semantic change",
			mutateConcurrent: func(condition *metav1.Condition) {
				condition.Reason = "ConcurrentWriter"
			},
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			h := testenv.Start(t)
			defer h.Stop(t)
			testenv.CRDsInstalled(ctx, t, h)

			vol := &zfscsiv1.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "condition-conflict"},
				Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{
					Pool: "tank", VolName: "condition-conflict", Type: zfscsiv1.VolumeTypeBlock,
					Capacity: 1 << 30, VolumeID: "csi:tank:block:condition-conflict",
				}),
			}
			if err := h.Client.Create(ctx, vol); err != nil {
				t.Fatalf("create Volume: %v", err)
			}

			seed := &zfscsiv1.Volume{}
			key := types.NamespacedName{Name: vol.Name, Namespace: vol.Namespace}
			if err := h.Client.Get(ctx, key, seed); err != nil {
				t.Fatalf("get Volume for seed: %v", err)
			}
			seedPatch := crclient.MergeFrom(seed.DeepCopy())
			seed.Status.Conditions = []metav1.Condition{{
				Type: "External", Status: metav1.ConditionTrue, Reason: "Initial", Message: "managed elsewhere",
				ObservedGeneration: seed.Generation, LastTransitionTime: metav1.Now(),
			}}
			if err := h.Client.Status().Patch(ctx, seed, seedPatch); err != nil {
				t.Fatalf("seed condition: %v", err)
			}

			before := &zfscsiv1.Volume{}
			if err := h.Client.Get(ctx, key, before); err != nil {
				t.Fatalf("get Volume before update: %v", err)
			}
			after := before.DeepCopy()
			after.Status.Conditions = setCondition(after.Status.Conditions, after.Generation, "External", metav1.ConditionFalse, "Desired", "agent update")

			concurrent := before.DeepCopy()
			concurrentPatch := crclient.MergeFrom(concurrent.DeepCopy())
			condition := conditionByType(concurrent.Status.Conditions, "External")
			if condition == nil {
				t.Fatal("missing seeded External condition")
			}
			tt.mutateConcurrent(condition)
			if err := h.Client.Status().Patch(ctx, concurrent, concurrentPatch); err != nil {
				t.Fatalf("concurrent condition update: %v", err)
			}

			err := patchStatusWithConditions(ctx, h.Client, before, after)
			if (err != nil) != tt.wantErr {
				t.Fatalf("patchStatusWithConditions error = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestEnvtestRecordBackendHealthWarning_NotFoundIsBestEffort(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.CRDsInstalled(ctx, t, h)

	caller := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "already-gone"}}
	r := &VolumeReconciler{Client: h.Client, Log: logr.Discard()}
	if err := r.recordBackendHealthWarning(ctx, caller, "target export is missing"); err != nil {
		t.Fatalf("warning for deleted volume: %v", err)
	}
}

// statusConflictOnceClient advances the status resource version before the first
// patch, forcing the reconciler to exercise its real API-server conflict retry.
type statusConflictOnceClient struct {
	crclient.Client
	patched atomic.Bool
}

func (c *statusConflictOnceClient) Status() crclient.StatusWriter {
	return statusConflictOnceWriter{SubResourceWriter: c.Client.Status(), client: c.Client, patched: &c.patched}
}

type statusConflictOnceWriter struct {
	crclient.SubResourceWriter
	client  crclient.Client
	patched *atomic.Bool
}

func (w statusConflictOnceWriter) Patch(ctx context.Context, obj crclient.Object, patch crclient.Patch, options ...crclient.SubResourcePatchOption) error {
	if w.patched.CompareAndSwap(false, true) {
		current := &zfscsiv1.Volume{}
		key := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
		if err := w.client.Get(ctx, key, current); err != nil {
			return err
		}
		concurrentPatch := crclient.MergeFrom(current.DeepCopy())
		current.Status.State = zfscsiv1.VolumeStateReady
		current.Status.TargetNQN = "nqn.test/concurrent"
		current.Status.Conditions = []metav1.Condition{{
			Type: string(zfscsiv1.VolumeConditionReady), Status: metav1.ConditionTrue,
			Reason: "ConcurrentWriter", Message: "preserve unrelated condition", LastTransitionTime: metav1.Now(),
		}}
		if err := w.client.Status().Patch(ctx, current, concurrentPatch); err != nil {
			return err
		}
	}

	return w.SubResourceWriter.Patch(ctx, obj, patch, options...)
}
