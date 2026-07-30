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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCleanupOnlyRejectsUnsupportedSubstrateBeforeLifecycleSeams(t *testing.T) {
	fixture := filepath.Join("data", "infrastructure-kubevirt", "two-owner.yaml")
	root := repositoryRootForTestDriver(t, fixture)
	body, err := os.ReadFile(filepath.Join(root, "test", "e2e", fixture))
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "machineDeploymentSuffix: md-0", "machineDeploymentSuffix: workers-unknown", 1))
	path := filepath.Join(t.TempDir(), "unsupported-cleanup.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(e2econfig.Env[e2econfig.CleanupOnlyKey], "1")
	t.Setenv(e2econfig.Env[e2econfig.InfrastructureProviderKey], "kubevirt")
	t.Setenv(e2econfig.Env[e2econfig.InfrastructureConfigKey], path)
	if err := e2econfig.Init(); err != nil {
		t.Fatal(err)
	}
	cleanupOnly, err := validateCAPILifecycleSubstrate()
	if err == nil || !strings.Contains(err.Error(), "does not match rendered substrate") {
		t.Fatalf("validateCAPILifecycleSubstrate() error = %v, want substrate rejection", err)
	}
	if cleanupOnly {
		t.Fatal("unsupported substrate reached cleanup-only lifecycle branch")
	}
}

func TestCAPIWorkerMachineCountUsesExplicitConsumerReplicas(t *testing.T) {
	fixture := filepath.Join("data", "infrastructure-kubevirt", "two-owner.yaml")
	root := repositoryRootForTestDriver(t, fixture)
	t.Setenv(e2econfig.Env[e2econfig.InfrastructureProviderKey], "kubevirt")
	t.Setenv(e2econfig.Env[e2econfig.InfrastructureConfigKey], filepath.Join(root, "test", "e2e", fixture))
	if err := e2econfig.Init(); err != nil {
		t.Fatal(err)
	}
	got, err := capiWorkerMachineCount(true, "kubevirt")
	if err != nil {
		t.Fatal(err)
	}
	if got != 2 {
		t.Fatalf("worker machine count = %d, want fixture replica total 2", got)
	}
}

func TestKubeVirtControlPlaneLBRouteDaemonSetUsesMutablePreflightImage(t *testing.T) {
	scheme := newSchemeForTest(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	if err := ensureKubeVirtControlPlaneLBRoutesObject(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	var daemonSet appsv1.DaemonSet
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "kube-system", Name: "zfs-csi-e2e-control-plane-lb"}, &daemonSet); err != nil {
		t.Fatal(err)
	}
	container := daemonSet.Spec.Template.Spec.Containers[0]
	if container.ImagePullPolicy != "Always" {
		t.Fatalf("route image pull policy = %q, want Always", container.ImagePullPolicy)
	}
	wantRoute := fmt.Sprintf("ip route replace %s via %s dev vlan200", siteMgmtSubnet(), siteVLANGateway())
	if !strings.Contains(strings.Join(container.Args, " "), wantRoute) {
		t.Fatal("route DaemonSet lacks deterministic LoadBalancer route")
	}
}

func TestRunCAPIWorkloadCleanupContinuesAfterFailures(t *testing.T) {
	var calls []string
	inventoryErr := errors.New("inventory unavailable")
	deleteErr := errors.New("delete timed out")
	scanErr := errors.New("orphan detector failed")
	assertBoundedContext := func(ctx context.Context) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("cleanup operation received an unbounded context")
		}
	}

	err := runCAPIWorkloadCleanup(capiCleanupOperations{
		inventory: func(ctx context.Context) error {
			assertBoundedContext(ctx)
			calls = append(calls, "inventory")
			return inventoryErr
		},
		delete: func(ctx context.Context) error {
			assertBoundedContext(ctx)
			calls = append(calls, "delete")
			return deleteErr
		},
		orphanScan: func(ctx context.Context) error {
			assertBoundedContext(ctx)
			calls = append(calls, "scan")
			return scanErr
		},
	})
	if got, want := strings.Join(calls, ","), "inventory,delete,scan"; got != want {
		t.Fatalf("cleanup order = %q, want %q", got, want)
	}
	for _, want := range []error{inventoryErr, deleteErr, scanErr} {
		if !errors.Is(err, want) {
			t.Errorf("cleanup error does not retain %v: %v", want, err)
		}
	}
}

func TestRunCAPIWorkloadCleanupSkipsMissingOperations(t *testing.T) {
	called := false
	err := runCAPIWorkloadCleanup(capiCleanupOperations{
		orphanScan: func(context.Context) error {
			called = true
			return nil
		},
	})
	if err != nil || !called {
		t.Fatalf("cleanup err=%v, orphan scan called=%t", err, called)
	}
}
