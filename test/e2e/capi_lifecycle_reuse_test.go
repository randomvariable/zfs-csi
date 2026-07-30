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

//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRetainedClusterCreateDecision(t *testing.T) {
	key := types.NamespacedName{Namespace: "e2e", Name: "raws-dev"}
	healthy := retainedCluster(key)
	deleting := retainedCluster(key)
	now := metav1.NewTime(time.Now())
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"test.cluster.x-k8s.io/retain"}
	failed := retainedCluster(key)
	failed.Status.Phase = "Failed"

	tests := []struct {
		name       string
		objects    []client.Object
		wantCreate bool
		wantErr    string
	}{
		{name: "absent creates", wantCreate: true},
		{name: "healthy retained cluster skips create", objects: []client.Object{healthy}},
		{name: "deleting cluster fails", objects: []client.Object{deleting}, wantErr: "is deleting"},
		{name: "failed cluster fails", objects: []client.Object{failed}, wantErr: "phase is"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newSchemeForTest(t)
			if err := clusterv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()
			cluster, create, err := retainedClusterCreateDecision(context.Background(), c, key)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if create != tc.wantCreate {
				t.Fatalf("create = %t, want %t", create, tc.wantCreate)
			}
			if !create && cluster == nil {
				t.Fatal("reuse decision did not return the existing cluster")
			}
		})
	}
}

func retainedCluster(key types.NamespacedName) *clusterv1.Cluster {
	return &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Status: clusterv1.ClusterStatus{
			Phase: string(clusterv1.ClusterPhaseProvisioned),
			Conditions: []metav1.Condition{{
				Type:   clusterv1.ClusterAvailableCondition,
				Status: metav1.ConditionTrue,
				Reason: clusterv1.ClusterAvailableReason,
			}},
		},
	}
}
