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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// conditionObject is deliberately compatible with Cluster API's condition Setter
// contract. The patch semantics below are locally derived from Cluster API's
// Apache-2.0 licensed util/patch/conditions implementation.
type conditionObject interface {
	crclient.Object
	GetConditions() []metav1.Condition
	SetConditions([]metav1.Condition)
}

// setCondition adds or updates a condition in the slice (idempotent by Type).
func setCondition(conds []metav1.Condition, generation int64, condType string, status metav1.ConditionStatus, reason, msg string) []metav1.Condition {
	meta.SetStatusCondition(&conds, metav1.Condition{
		Type:               condType,
		Status:             status,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            msg,
	})

	return conds
}

// patchStatusWithConditions applies changed condition types to a fresh object
// before issuing the ordinary status merge patch. Kubernetes merge patches replace
// a conditions array wholesale; this per-type merge preserves types managed by
// other reconcilers and uses an optimistic-lock retry for real API conflicts.
//
// ownedTypes documents the types this caller alone owns. A concurrent edit to an
// owned type is intentionally overwritten; a concurrent edit to a type changed
// by this patch but not owned is rejected rather than silently discarded.
func patchStatusWithConditions(ctx context.Context, c crclient.Client, before, after conditionObject, ownedTypes ...string) error {
	if err := patchConditions(ctx, c, before, after, ownedTypes...); err != nil {
		return err
	}

	// Generate a status-only patch without conditions. The retry below rebases the
	// resourceVersion while retaining this narrow field-level diff, so unrelated
	// status fields and condition types cannot be clobbered by a stale writer.
	withoutConditionsBefore := before.DeepCopyObject().(conditionObject)
	withoutConditionsAfter := after.DeepCopyObject().(conditionObject)
	withoutConditionsAfter.SetConditions(withoutConditionsBefore.GetConditions())
	data, err := crclient.MergeFrom(withoutConditionsBefore).Data(withoutConditionsAfter)
	if err != nil {
		return fmt.Errorf("create non-condition status patch: %w", err)
	}
	if bytes.Equal(data, []byte("{}")) {
		return c.Get(ctx, crclient.ObjectKeyFromObject(after), after)
	}

	var patch map[string]any
	if err := json.Unmarshal(data, &patch); err != nil {
		return fmt.Errorf("decode non-condition status patch: %w", err)
	}
	key := crclient.ObjectKeyFromObject(after)
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := before.DeepCopyObject().(conditionObject)
		if err := c.Get(ctx, key, latest); err != nil {
			return err
		}
		metadata, _ := patch["metadata"].(map[string]any)
		if metadata == nil {
			metadata = map[string]any{}
			patch["metadata"] = metadata
		}
		metadata["resourceVersion"] = latest.GetResourceVersion()
		data, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		return c.Status().Patch(ctx, latest, crclient.RawPatch(types.MergePatchType, data))
	}); err != nil {
		return err
	}
	return nil
}

func patchConditions(ctx context.Context, c crclient.Client, before, after conditionObject, ownedTypes ...string) error {
	changes := changedConditionTypes(before.GetConditions(), after.GetConditions())
	if len(changes) == 0 {
		return nil
	}

	owned := make(map[string]struct{}, len(ownedTypes))
	for _, conditionType := range ownedTypes {
		owned[conditionType] = struct{}{}
	}
	key := crclient.ObjectKeyFromObject(after)

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := before.DeepCopyObject().(conditionObject)
		if err := c.Get(ctx, key, latest); err != nil {
			return err
		}
		patch := crclient.MergeFromWithOptions(latest.DeepCopyObject().(crclient.Object), crclient.MergeFromWithOptimisticLock{})
		for _, conditionType := range changes {
			oldCondition := conditionByType(before.GetConditions(), conditionType)
			latestCondition := conditionByType(latest.GetConditions(), conditionType)
			desiredCondition := conditionByType(after.GetConditions(), conditionType)
			if !conditionsEqual(oldCondition, latestCondition) && !conditionsEqual(latestCondition, desiredCondition) {
				if _, ok := owned[conditionType]; !ok {
					return fmt.Errorf("condition %q was concurrently modified", conditionType)
				}
			}
			conditions := latest.GetConditions()
			if desiredCondition == nil {
				conditions = removeCondition(conditions, conditionType)
			} else {
				meta.SetStatusCondition(&conditions, *desiredCondition)
			}
			latest.SetConditions(conditions)
		}
		return c.Status().Patch(ctx, latest, patch)
	})
}

func changedConditionTypes(before, after []metav1.Condition) []string {
	types := map[string]struct{}{}
	for _, condition := range before {
		types[condition.Type] = struct{}{}
	}
	for _, condition := range after {
		types[condition.Type] = struct{}{}
	}
	changes := make([]string, 0, len(types))
	for conditionType := range types {
		if !conditionsEqual(conditionByType(before, conditionType), conditionByType(after, conditionType)) {
			changes = append(changes, conditionType)
		}
	}
	sort.Strings(changes)
	return changes
}

func conditionByType(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func conditionsEqual(left, right *metav1.Condition) bool {
	if left == nil || right == nil {
		return left == right
	}

	// Match Cluster API's HasSameState semantics: transition timestamps record
	// when a state was observed, not whether its semantic state changed.
	return left.Type == right.Type &&
		left.Status == right.Status &&
		left.Reason == right.Reason &&
		left.Message == right.Message &&
		left.ObservedGeneration == right.ObservedGeneration
}

func removeCondition(conditions []metav1.Condition, conditionType string) []metav1.Condition {
	result := conditions[:0]
	for _, condition := range conditions {
		if condition.Type != conditionType {
			result = append(result, condition)
		}
	}
	return result
}
