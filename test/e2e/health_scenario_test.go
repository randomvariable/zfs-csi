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
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

func TestHealthConsumerCreatesReadinessMarker(t *testing.T) {
	pod := healthConsumerPod("default", "health-consumer", "claim")
	container := pod.Spec.Containers[0]
	command := strings.Join(container.Command, " ")
	for _, want := range []string{"printf health > /data/proof", "test \"$(cat /data/proof)\" = health", "touch " + scenarioReadyFile, "exec sleep 3600"} {
		if !strings.Contains(command, want) {
			t.Fatalf("health consumer command missing %q: %q", want, container.Command)
		}
	}
	if probe := container.ReadinessProbe; probe == nil || probe.Exec == nil ||
		!reflect.DeepEqual(probe.Exec.Command, []string{"test", "-f", scenarioReadyFile}) {
		t.Fatalf("health consumer readiness probe = %+v", probe)
	}
	// Placement follows the shared smoke contract (nil on non-static lanes,
	// consumer-group pin on static) — never a node-specific pin.
	if got := pod.Spec.NodeSelector; !reflect.DeepEqual(got, smokeConsumerNodeSelector()) {
		t.Fatalf("health consumer selector = %v, want smoke placement contract %v", got, smokeConsumerNodeSelector())
	}
	if len(pod.Spec.Containers) != 1 || container.Name != "consumer" {
		t.Fatalf("health consumer containers = %+v", pod.Spec.Containers)
	}
}

func TestDestroyTargetCommandDisablesNamespacesBeforeRemoval(t *testing.T) {
	command := destroyTargetCommand("nqn.2026-01.csi:volume")
	for _, want := range []string{"rm -f", "allowed_hosts/*", "rm -f \"$host\"", "echo 0 >", "rmdir \"$ns\"", "rmdir '/sys/kernel/config/nvmet/subsystems/nqn.2026-01.csi:volume'", "test ! -e"} {
		if !strings.Contains(command, want) {
			t.Fatalf("target teardown command missing %q: %s", want, command)
		}
	}
	if strings.Index(command, "rm -f \"$host\"") > strings.Index(command, "rmdir \"$ns\"") {
		t.Fatal("allowed hosts must be removed before namespaces")
	}
}

func TestStorageArgsEnableHealthRepairHold(t *testing.T) {
	if !storageArgsEnableHealthRepairHold([]string{"--e2e-enable-health-repair-hold=true"}) {
		t.Fatal("enabled argument not detected")
	}
	if storageArgsEnableHealthRepairHold([]string{"--mode=storage"}) {
		t.Fatal("ordinary storage arguments must not enable health hold")
	}
}

func TestWaitForBackendHealthTransitionRejectsInitialResourceVersion(t *testing.T) {
	c := newScriptedVolumeClient(t, backendHealthVolume("10", metav1.ConditionFalse, "BackendUnhealthy"))
	setBackendHealthPollInterval(t, time.Millisecond)

	err := waitForBackendHealthTransition(context.Background(), c, "volume", "10", metav1.ConditionFalse, 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same-version condition error = %v, want deadline exceeded", err)
	}
}

func TestWaitForBackendHealthTransitionAcceptsNewerResourceVersion(t *testing.T) {
	c := newScriptedVolumeClient(t,
		backendHealthVolume("10", metav1.ConditionFalse, "BackendUnhealthy"),
		backendHealthVolume("11", metav1.ConditionFalse, "BackendUnhealthy"),
	)
	setBackendHealthPollInterval(t, time.Millisecond)

	if err := waitForBackendHealthTransition(context.Background(), c, "volume", "10", metav1.ConditionFalse, time.Second); err != nil {
		t.Fatalf("wait for newer healthy condition: %v", err)
	}
	if c.calls != 2 {
		t.Fatalf("Get calls = %d, want 2", c.calls)
	}
}

func TestWaitForBackendHealthTransitionTimesOutForWrongCondition(t *testing.T) {
	c := newScriptedVolumeClient(t, backendHealthVolume("11", metav1.ConditionTrue, "BackendRecovered"))
	setBackendHealthPollInterval(t, time.Millisecond)

	err := waitForBackendHealthTransition(context.Background(), c, "volume", "10", metav1.ConditionFalse, 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wrong condition error = %v, want deadline exceeded", err)
	}
	if c.calls < 2 {
		t.Fatalf("Get calls = %d, want multiple polls", c.calls)
	}
}

func TestWaitForBackendHealthTransitionPropagatesGetError(t *testing.T) {
	want := errors.New("get volume")
	c := newScriptedVolumeClient(t)
	c.err = want

	err := waitForBackendHealthTransition(context.Background(), c, "volume", "10", metav1.ConditionFalse, time.Second)
	if !errors.Is(err, want) {
		t.Fatalf("Get error = %v, want %v", err, want)
	}
}

type scriptedVolumeClient struct {
	client.Client
	volumes []zfscsiv1.Volume
	err     error
	calls   int
}

func newScriptedVolumeClient(t *testing.T, volumes ...zfscsiv1.Volume) *scriptedVolumeClient {
	t.Helper()
	return &scriptedVolumeClient{
		Client:  fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).Build(),
		volumes: volumes,
	}
}

func (c *scriptedVolumeClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	c.calls++
	if c.err != nil {
		return c.err
	}
	index := c.calls - 1
	if index >= len(c.volumes) {
		index = len(c.volumes) - 1
	}
	*obj.(*zfscsiv1.Volume) = *c.volumes[index].DeepCopy()
	return nil
}

func backendHealthVolume(resourceVersion string, status metav1.ConditionStatus, reason string) zfscsiv1.Volume {
	return zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "volume", ResourceVersion: resourceVersion},
		Status: zfscsiv1.VolumeStatus{Conditions: []metav1.Condition{{
			Type:   string(zfscsiv1.VolumeConditionBackendHealthy),
			Status: status,
			Reason: reason,
		}}},
	}
}

func setBackendHealthPollInterval(t *testing.T, interval time.Duration) {
	t.Helper()
	previous := backendHealthPollInterval
	backendHealthPollInterval = interval
	t.Cleanup(func() { backendHealthPollInterval = previous })
}
