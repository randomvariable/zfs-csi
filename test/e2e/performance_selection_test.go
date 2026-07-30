// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && performance

package e2e

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPerformanceWorkerReadyMirrorsSchedulingSemantics(t *testing.T) {
	ready := corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue}
	cases := []struct {
		name string
		node corev1.Node
		want bool
	}{
		{name: "worker", node: corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{ready}}}, want: true},
		{name: "not-ready", node: corev1.Node{}, want: false},
		{name: "unschedulable", node: corev1.Node{Spec: corev1.NodeSpec{Unschedulable: true}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{ready}}}},
		{name: "control-plane", node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{ready}}}},
		{name: "master", node: corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"node-role.kubernetes.io/master": ""}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{ready}}}},
		{name: "no-schedule", node: corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{ready}}}},
		{name: "no-execute", node: corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoExecute}}}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{ready}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := performanceWorkerReady(&tc.node); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
