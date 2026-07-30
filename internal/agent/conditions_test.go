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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestChangedConditionTypes(t *testing.T) {
	before := []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}, {Type: "Other", Status: metav1.ConditionTrue}}
	after := []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}, {Type: "New", Status: metav1.ConditionTrue}}
	got := changedConditionTypes(before, after)
	want := []string{"New", "Other", "Ready"}
	if len(got) != len(want) {
		t.Fatalf("changed condition types = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("changed condition types = %v, want %v", got, want)
		}
	}
}

func TestConditionsEqualIgnoresLastTransitionTime(t *testing.T) {
	before := &metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready", Message: "available", ObservedGeneration: 3,
		LastTransitionTime: metav1.NewTime(time.Unix(1, 0)),
	}
	after := before.DeepCopy()
	after.LastTransitionTime = metav1.NewTime(time.Unix(2, 0))

	if !conditionsEqual(before, after) {
		t.Fatal("timestamp-only condition update is a semantic change")
	}
}

func TestConditionsEqualDetectsSemanticChange(t *testing.T) {
	before := &metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready", Message: "available", ObservedGeneration: 3,
	}
	after := before.DeepCopy()
	after.Reason = "BackendUnavailable"

	if conditionsEqual(before, after) {
		t.Fatal("reason change is not a semantic change")
	}
}

func TestSetConditionPreservesUnrelatedTypes(t *testing.T) {
	conditions := []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
	got := setCondition(conditions, 2, "BackendHealthy", metav1.ConditionFalse, "Unavailable", "target missing")
	if conditionByType(got, "Ready") == nil || conditionByType(got, "Ready").Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition was not preserved: %#v", got)
	}
	if health := conditionByType(got, "BackendHealthy"); health == nil || health.Status != metav1.ConditionFalse || health.ObservedGeneration != 2 {
		t.Fatalf("BackendHealthy condition = %#v", health)
	}
}
