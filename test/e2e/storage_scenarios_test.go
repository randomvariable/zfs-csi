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
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

func TestEphemeralScenarioPodUsesClaimTemplateAndReadinessMarker(t *testing.T) {
	pod := ephemeralScenarioPod("default", "consumer", scenarioStorageClass, "worker-a", "sentinel-a")
	if pod.Spec.NodeName != "" || pod.Spec.NodeSelector[corev1.LabelHostname] != "worker-a" {
		t.Fatalf("pod does not use scheduler hostname selection: %+v", pod.Spec)
	}
	volume := pod.Spec.Volumes[0]
	if volume.Ephemeral == nil || volume.Ephemeral.VolumeClaimTemplate == nil || volume.PersistentVolumeClaim != nil {
		t.Fatalf("volume is not generic ephemeral: %+v", volume)
	}
	if got := *volume.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName; got != scenarioStorageClass {
		t.Fatalf("storage class = %q", got)
	}
	probe := pod.Spec.Containers[0].ReadinessProbe
	if probe == nil || probe.Exec == nil ||
		!reflect.DeepEqual(probe.Exec.Command, []string{"test", "-f", scenarioReadyFile}) {
		t.Fatalf("readiness probe = %+v", probe)
	}
	if got := pod.Spec.Containers[0].Env[0].Value; got != "sentinel-a" {
		t.Fatalf("sentinel = %q", got)
	}
}

// An empty node argument must not fabricate a node-specific selector; the pod
// only carries the shared smoke placement contract (nil on non-static lanes,
// the consumer-group pin on static).
func TestScenarioPodWithoutNodeLeavesSchedulingUnconstrained(t *testing.T) {
	pod := scenarioPod("default", "consumer", "claim", "", "exec sleep 3600", "")
	if got := pod.Spec.NodeSelector; !reflect.DeepEqual(got, smokeConsumerNodeSelector()) {
		t.Fatalf("empty node selector = %v, want smoke placement contract %v", got, smokeConsumerNodeSelector())
	}
}

func TestDiscoverEphemeralPVCsUsesPodOwnerUID(t *testing.T) {
	podA := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "default", UID: "pod-a"}}
	podB := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default", UID: "pod-b"}}
	pvcA := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-data",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Pod", UID: podA.UID}},
		},
	}
	pvcB := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "b-data",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Pod", UID: podB.UID}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(pvcB, pvcA).Build()
	got, err := discoverEphemeralPVCs(t.Context(), c, []*corev1.Pod{podA, podB})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Name != pvcA.Name || got[1].Name != pvcB.Name {
		t.Fatalf("discovered PVCs = %s, %s", got[0].Name, got[1].Name)
	}
}

func TestValidateDistinctVolumeIdentities(t *testing.T) {
	valid := []volumeIdentity{
		{pvName: "pv-a", volumeHandle: "vol-a", targetNQN: "nqn-a", deviceGUID: "guid-a"},
		{pvName: "pv-b", volumeHandle: "vol-b", targetNQN: "nqn-b", deviceGUID: "guid-b"},
	}
	if err := validateDistinctVolumeIdentities(valid); err != nil {
		t.Fatal(err)
	}
	invalid := append([]volumeIdentity(nil), valid...)
	invalid[1].targetNQN = invalid[0].targetNQN
	if err := validateDistinctVolumeIdentities(invalid); err == nil {
		t.Fatal("accepted duplicate target NQN")
	}
}

func TestPodCommandSucceededRequiresRunningAndReady(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if !podCommandSucceeded(pod) {
		t.Fatal("ready pod did not satisfy marker predicate")
	}
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	if podCommandSucceeded(pod) {
		t.Fatal("running but unready pod satisfied marker predicate")
	}
}

func TestVolumeLimitScenarioUsesMinimalBoundedProof(t *testing.T) {
	if scenarioNodeLimit != 2 {
		t.Fatalf("node limit = %d, want 2", scenarioNodeLimit)
	}
	for name, timeout := range map[string]time.Duration{
		"limit":    volumeLimitTimeout,
		"attach":   volumeLimitAttachTimeout,
		"recovery": volumeLimitRecoveryTimeout,
	} {
		if timeout > 3*time.Minute {
			t.Fatalf("%s timeout = %s, want at most 3m", name, timeout)
		}
	}
}

func TestPodTerminalErrorReportsTerminalContainerState(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "consumer",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason:  "CreateContainerConfigError",
				Message: "missing secret",
			}},
		}}},
	}
	err := podTerminalError(pod)
	if err == nil || !strings.Contains(err.Error(), "CreateContainerConfigError") {
		t.Fatalf("error = %v, want terminal container state", err)
	}
}

func TestCreateAndWaitForScenarioPodsRejectsMismatchedInputs(t *testing.T) {
	err := createAndWaitForScenarioPods(
		t.Context(),
		fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).Build(),
		[]*corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "claim"}}},
		nil,
		time.Second,
	)
	if err == nil || !strings.Contains(err.Error(), "PVC/pod count mismatch") {
		t.Fatalf("error = %v, want PVC/pod count mismatch", err)
	}
}

func TestVolumePVNamesPreservesIdentityOrder(t *testing.T) {
	got := volumePVNames([]volumeIdentity{{pvName: "pv-a"}, {pvName: "pv-b"}})
	if !reflect.DeepEqual(got, []string{"pv-a", "pv-b"}) {
		t.Fatalf("PV names = %v", got)
	}
}

func TestRunVolumeLimitScenarioPropagatesAndRestoresOverrides(t *testing.T) {
	var got []map[string]string
	scheme := newSchemeForTest(t)
	if err := addDriverTypes(scheme); err != nil {
		t.Fatal(err)
	}
	csiNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-a"},
		Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{
				{
					Name:        zfsCSIDriverName,
					NodeID:      "worker-a",
					Allocatable: &storagev1.VolumeNodeResources{Count: ptr.To(defaultMaxVolumesPerNode)},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(csiNode).Build()
	installer := func(_ context.Context, overrides map[string]string) error {
		got = append(got, overrides)
		if overrides["node.maxVolumesPerNode"] == fmt.Sprint(defaultMaxVolumesPerNode) {
			current := &storagev1.CSINode{}
			if err := c.Get(t.Context(), client.ObjectKey{Name: "worker-a"}, current); err != nil {
				return err
			}
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.Drivers[0].Allocatable.Count = ptr.To(defaultMaxVolumesPerNode)
			return c.Patch(t.Context(), current, patch)
		}
		return nil
	}
	for _, workload := range readyDriverWorkloads(false) {
		if err := c.Create(t.Context(), workload); err != nil {
			t.Fatal(err)
		}
	}
	// Exercise the install/restore contract without a cluster by cancelling after
	// the low-limit install. The deferred restore must still use the same path.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_ = runVolumeLimitScenario(ctx, c, "default", scenarioStorageClass, "worker-a", installer)
	want := []map[string]string{maxVolumeOverride(scenarioNodeLimit), maxVolumeOverride(defaultMaxVolumesPerNode)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("install overrides = %#v, want %#v", got, want)
	}
}

func TestPodUnschedulableCausePredicates(t *testing.T) {
	for _, test := range []struct {
		name    string
		cause   unschedulableCause
		message string
		want    bool
	}{
		{name: "CSI limit", cause: unschedulableVolumeLimit, message: "node(s) exceed max volume count for CSI driver zfs.csi.randomvariable.co.uk", want: true},
		{name: "RWOP conflict", cause: unschedulableRWOPConflict, message: "volume uses the ReadWriteOncePod access mode and is already in use", want: true},
		{name: "taint is not limit", cause: unschedulableVolumeLimit, message: "node(s) had untolerated taint", want: false},
		{name: "pressure is not RWOP", cause: unschedulableRWOPConflict, message: "Insufficient memory", want: false},
		{name: "affinity is not limit", cause: unschedulableVolumeLimit, message: "node(s) didn't match Pod's node affinity", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod"},
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					Conditions: []corev1.PodCondition{
						{
							Type:    corev1.PodScheduled,
							Status:  corev1.ConditionFalse,
							Reason:  corev1.PodReasonUnschedulable,
							Message: test.message,
						},
					},
				},
			}
			got, _ := podUnschedulableForCause(pod, nil, test.cause)
			if got != test.want {
				t.Fatalf("match = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPodUnschedulableCauseUsesEvents(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod"}, Status: corev1.PodStatus{Phase: corev1.PodPending}}
	events := []corev1.Event{
		{
			InvolvedObject: corev1.ObjectReference{UID: pod.UID},
			Reason:         "FailedScheduling",
			Message:        "volume uses the ReadWriteOncePod access mode and is already in use",
		},
	}
	if got, _ := podUnschedulableForCause(pod, events, unschedulableRWOPConflict); !got {
		t.Fatal("expected matching scheduling event")
	}
}

func TestValidateBackendExports(t *testing.T) {
	identities := []volumeIdentity{
		{
			pvName:       "pv-a",
			volumeHandle: "vol-a",
			targetNQN:    "nqn-a",
			deviceGUID:   "guid-a",
			zvolPath:     "/dev/zvol/a",
			datasetPath:  "tank/a",
		},
		{
			pvName:       "pv-b",
			volumeHandle: "vol-b",
			targetNQN:    "nqn-b",
			deviceGUID:   "guid-b",
			zvolPath:     "/dev/zvol/b",
			datasetPath:  "tank/b",
		},
	}
	volumes := []client.Object{
		volumeForIdentity("a", identities[0]), volumeForIdentity("b", identities[1]),
	}
	for _, test := range []struct {
		name    string
		enabled bool
		exports []client.Object
		wantErr bool
	}{
		{name: "disabled no exports"},
		{name: "enabled zero exports", enabled: true, wantErr: true},
		{name: "enabled valid exports", enabled: true, exports: []client.Object{exportForIdentity("a", identities[0]), exportForIdentity("b", identities[1])}},
		{name: "enabled malformed", enabled: true, exports: []client.Object{&nvmetv1.NVMeExport{ObjectMeta: metav1.ObjectMeta{Name: "bad"}}, exportForIdentity("b", identities[1])}, wantErr: true},
		{name: "enabled duplicate", enabled: true, exports: []client.Object{exportForIdentity("a", identities[0]), exportForIdentity("duplicate", identities[0])}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := append([]client.Object(nil), volumes...)
			objects = append(objects, test.exports...)
			if test.enabled {
				objects = append(
					objects,
					&appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{Name: "nvmet-controller", Namespace: zfsCSINamespace},
					},
				)
			}
			scheme := newSchemeForTest(t)
			if err := addDriverTypes(scheme); err != nil {
				t.Fatal(err)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			err := validateBackendIdentities(t.Context(), c, append([]volumeIdentity(nil), identities...))
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func volumeForIdentity(name string, identity volumeIdentity) client.Object {
	return &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       zfscsiv1.VolumeSpec{VolumeID: identity.volumeHandle},
		Status: zfscsiv1.VolumeStatus{
			TargetNQN:   identity.targetNQN,
			DeviceGUID:  identity.deviceGUID,
			ZvolPath:    identity.zvolPath,
			DatasetPath: identity.datasetPath,
		},
	}
}

func exportForIdentity(name string, identity volumeIdentity) client.Object {
	return &nvmetv1.NVMeExport{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nvmetv1.NVMeExportSpec{
			TargetNQN:   identity.targetNQN,
			DeviceGUID:  identity.deviceGUID,
			DevicePath:  identity.zvolPath,
			Portal:      "127.0.0.1:4420",
			NamespaceID: 1,
		},
	}
}

func TestSelectScenarioNodeRejectsStorageAndControlPlaneNodes(t *testing.T) {
	ready := []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	c := fakeClientForScenario(
		t,
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "storage",
				Labels: map[string]string{e2eStorageRoleLabel: e2eStorageRoleValue},
			},
			Status: corev1.NodeStatus{Conditions: ready},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "cp"},
			Spec:       corev1.NodeSpec{Taints: []corev1.Taint{{Key: "node-role.kubernetes.io/control-plane"}}},
			Status:     corev1.NodeStatus{Conditions: ready},
		},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Status: corev1.NodeStatus{Conditions: ready}},
	)
	got, err := selectScenarioNode(t.Context(), c)
	if err != nil || got != "worker" {
		t.Fatalf("selected node = %q, %v", got, err)
	}
}

func fakeClientForScenario(t *testing.T, nodes ...*corev1.Node) client.Client {
	t.Helper()
	objects := make([]client.Object, 0, len(nodes))
	for _, node := range nodes {
		objects = append(objects, node)
	}
	return fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(objects...).Build()
}
