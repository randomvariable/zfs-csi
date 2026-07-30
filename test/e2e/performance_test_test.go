// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && performance

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWaitForPerformanceObjectDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "diagnostic", Namespace: "default"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { time.Sleep(20 * time.Millisecond); _ = c.Delete(context.Background(), pod) }()
	if err := waitForPerformanceObjectDeleted(ctx, c, client.ObjectKeyFromObject(pod), time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestWaitForPerformanceObjectDeletedTimesOut(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "leak", Namespace: "default"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	err := waitForPerformanceObjectDeleted(context.Background(), c, client.ObjectKeyFromObject(pod), 10*time.Millisecond)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("expected leak timeout, got %v", err)
	}
}
