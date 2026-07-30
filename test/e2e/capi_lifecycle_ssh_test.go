// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestConformanceSSHBastionResolvesCAPAPublicAddress(t *testing.T) {
	awsCluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2",
		"kind":       "AWSCluster",
		"metadata": map[string]any{
			"name":      "raws-dev",
			"namespace": "zfs-csi-e2e-images",
		},
		"status": map[string]any{
			"bastion": map[string]any{"publicIp": "203.0.113.10"},
		},
	}}
	client := fake.NewClientBuilder().WithRuntimeObjects(awsCluster).Build()

	got, err := conformanceSSHBastion(context.Background(), client, "zfs-csi-e2e-images", "raws-dev", "aws")
	if err != nil {
		t.Fatal(err)
	}
	if got != "203.0.113.10:22" {
		t.Fatalf("bastion = %q, want 203.0.113.10:22", got)
	}
}

func TestConformanceSSHBastionIsEmptyOutsideAWS(t *testing.T) {
	got, err := conformanceSSHBastion(context.Background(), nil, "", "", "kubevirt")
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("bastion = %q, want empty", got)
	}
}
