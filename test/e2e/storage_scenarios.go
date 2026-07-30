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
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

const (
	scenarioStorageClass = "zfs-tank-nvme"
	// Two attachments prove the limit and release/recovery path without adding a
	// third slow provisioning cycle to the scheduler acceptance scenario.
	scenarioNodeLimit          = int32(2)
	defaultMaxVolumesPerNode   = int32(128)
	scenarioPollInterval       = 2 * time.Second
	scenarioPodTimeout         = 10 * time.Minute
	scenarioUnschedulableWait  = 2 * time.Minute
	scenarioReadyFile          = "/tmp/zfs-csi-ready"
	volumeLimitTimeout         = 3 * time.Minute
	volumeLimitAttachTimeout   = 2 * time.Minute
	volumeLimitRecoveryTimeout = 3 * time.Minute
)

type scenarioDriverInstaller func(context.Context, map[string]string) error

type unschedulableCause int

const (
	unschedulableVolumeLimit unschedulableCause = iota
	unschedulableRWOPConflict
)

func runStorageFeatureScenarios(
	ctx context.Context,
	c client.Client,
	namespace string,
	install scenarioDriverInstaller,
) error {
	node, err := selectScenarioNode(ctx, c)
	if err != nil {
		return err
	}
	if err := runSameNodeAccessModeScenarios(ctx, c, namespace, scenarioStorageClass, node); err != nil {
		return fmt.Errorf("#61 same-node access modes: %w", err)
	}
	if err := runTwoVolumeSameNodeScenario(ctx, c, namespace, scenarioStorageClass, node); err != nil {
		return fmt.Errorf("#67 two-volume same-node regression: %w", err)
	}
	if err := runVolumeLimitScenario(ctx, c, namespace, scenarioStorageClass, node, install); err != nil {
		return fmt.Errorf("#58 scheduler volume limit: %w", err)
	}
	return nil
}

func runSameNodeAccessModeScenarios(ctx context.Context, c client.Client, namespace, storageClass, node string) error {
	if err := runSamePVTwoPodScenario(ctx, c, namespace, storageClass, node, corev1.ReadWriteOnce, "rwo", false); err != nil {
		return err
	}
	return runSamePVTwoPodScenario(ctx, c, namespace, storageClass, node, corev1.ReadWriteOncePod, "rwop", true)
}

func runSamePVTwoPodScenario(
	ctx context.Context,
	c client.Client,
	namespace, storageClass, node string,
	mode corev1.PersistentVolumeAccessMode,
	suffix string,
	rejectSecond bool,
) (retErr error) {
	prefix := "zfs-csi-e2e-" + suffix
	pvc := pvcObject(namespace, prefix, storageClass, mode)
	first := scenarioPod(
		namespace,
		prefix+"-first",
		pvc.Name,
		node,
		`printf '%s\n' "$EXPECTED" > /data/proof; touch `+scenarioReadyFile+`; exec sleep 3600`,
		suffix+"-sentinel",
	)
	second := scenarioPod(
		namespace,
		prefix+"-second",
		pvc.Name,
		node,
		`until grep -qxF -- "$EXPECTED" /data/proof; do sleep 1; done; touch `+scenarioReadyFile+`; exec sleep 3600`,
		suffix+"-sentinel",
	)
	objects := []client.Object{second, first, pvc}
	if err := resetScenarioObjects(ctx, c, objects); err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, cleanupScenarioObjects(c, objects)) }()

	if err := c.Create(ctx, pvc); err != nil {
		return err
	}
	if err := c.Create(ctx, first); err != nil {
		return err
	}
	if err := waitForPodReady(ctx, c, keyOf(first), scenarioPodTimeout); err != nil {
		return fmt.Errorf("first %s pod did not complete its sentinel command: %w", suffix, err)
	}
	if err := c.Create(ctx, second); err != nil {
		return err
	}
	if !rejectSecond {
		if err := waitForPodReady(ctx, c, keyOf(second), scenarioPodTimeout); err != nil {
			return fmt.Errorf("ordinary RWO second pod did not read the same-node sentinel: %w", err)
		}
		return assertSingleBoundVolume(ctx, c, pvc)
	}
	if err := waitForPodUnschedulableCause(ctx, c, keyOf(second), unschedulableRWOPConflict, scenarioUnschedulableWait); err != nil {
		return fmt.Errorf("RWOP second pod was not rejected: %w", err)
	}
	if err := deleteIfExists(ctx, c, first); err != nil {
		return err
	}
	if err := waitForPodReady(ctx, c, keyOf(second), scenarioPodTimeout); err != nil {
		return fmt.Errorf("RWOP second pod did not read the sentinel after first-pod removal: %w", err)
	}
	return assertSingleBoundVolume(ctx, c, pvc)
}

func runTwoVolumeSameNodeScenario(
	ctx context.Context,
	c client.Client,
	namespace, storageClass, node string,
) (retErr error) {
	const prefix = "zfs-csi-e2e-two-volume"
	pods := []*corev1.Pod{
		ephemeralScenarioPod(namespace, prefix+"-a", storageClass, node, "a"),
		ephemeralScenarioPod(namespace, prefix+"-b", storageClass, node, "b"),
	}
	objects := []client.Object{pods[1], pods[0]}
	if err := resetScenarioObjects(ctx, c, objects); err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, cleanupScenarioObjects(c, objects)) }()
	for _, pod := range pods {
		if err := c.Create(ctx, pod); err != nil {
			return err
		}
	}
	for _, pod := range pods {
		if err := waitForPodReady(ctx, c, keyOf(pod), scenarioPodTimeout); err != nil {
			return fmt.Errorf("pod %s did not prove isolated write/read: %w", pod.Name, err)
		}
	}
	for i := range pods {
		if err := c.Get(ctx, keyOf(pods[i]), pods[i]); err != nil {
			return err
		}
	}
	pvcs, err := discoverEphemeralPVCs(ctx, c, pods)
	if err != nil {
		return err
	}
	identities := make([]volumeIdentity, 0, len(pvcs))
	for _, pvc := range pvcs {
		identity, err := boundVolumeIdentity(ctx, c, pvc)
		if err != nil {
			return err
		}
		identities = append(identities, identity)
	}
	if err := validateDistinctPersistentIdentities(identities); err != nil {
		return err
	}
	if err := waitForAttachedPVsOnNode(ctx, c, node, []string{identities[0].pvName, identities[1].pvName}, 2*time.Minute); err != nil {
		return err
	}
	return validateBackendIdentities(ctx, c, identities)
}

func runVolumeLimitScenario(
	ctx context.Context,
	c client.Client,
	namespace, storageClass, node string,
	install scenarioDriverInstaller,
) (retErr error) {
	if install == nil {
		return errors.New("driver installer is required")
	}
	if err := install(ctx, maxVolumeOverride(scenarioNodeLimit)); err != nil {
		return err
	}
	defer func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), volumeLimitTimeout)
		defer cancel()
		if err := install(restoreCtx, maxVolumeOverride(defaultMaxVolumesPerNode)); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("restore driver volume limit: %w", err))
			return
		}
		if err := waitForCSINodeLimit(restoreCtx, c, node, defaultMaxVolumesPerNode, volumeLimitTimeout); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("restore CSINode allocatable count: %w", err))
			return
		}
		if err := waitForDriverReady(restoreCtx, c); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("restore driver workload readiness: %w", err))
		}
	}()
	if err := waitForCSINodeLimit(ctx, c, node, scenarioNodeLimit, volumeLimitTimeout); err != nil {
		return err
	}

	var pvcs []*corev1.PersistentVolumeClaim
	var pods []*corev1.Pod
	var objects []client.Object
	for i := 0; i < int(scenarioNodeLimit)+1; i++ {
		name := fmt.Sprintf("zfs-csi-e2e-limit-%d", i)
		pvc := pvcObject(namespace, name, storageClass, corev1.ReadWriteOnce)
		pod := scenarioPod(namespace, name, pvc.Name, node, `touch `+scenarioReadyFile+`; exec sleep 3600`, "")
		pvcs, pods = append(pvcs, pvc), append(pods, pod)
		objects = append(objects, pod, pvc)
	}
	if err := resetScenarioObjects(ctx, c, objects); err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, cleanupScenarioObjects(c, objects)) }()
	if err := createAndWaitForScenarioPods(ctx, c, pvcs[:scenarioNodeLimit], pods[:scenarioNodeLimit], volumeLimitTimeout); err != nil {
		return fmt.Errorf("within-limit pods: %w", err)
	}
	attached := make([]volumeIdentity, 0, scenarioNodeLimit)
	for i := 0; i < int(scenarioNodeLimit); i++ {
		identity, err := boundVolumeIdentity(ctx, c, pvcs[i])
		if err != nil {
			return err
		}
		attached = append(attached, identity)
	}
	if err := validateDistinctPersistentIdentities(attached); err != nil {
		return fmt.Errorf("within-limit volumes: %w", err)
	}
	if err := waitForAttachedPVsOnNode(ctx, c, node, volumePVNames(attached), volumeLimitAttachTimeout); err != nil {
		return err
	}
	if err := validateBackendIdentities(ctx, c, attached); err != nil {
		return fmt.Errorf("within-limit backend identities: %w", err)
	}
	extra := int(scenarioNodeLimit)
	if err := c.Create(ctx, pvcs[extra]); err != nil {
		return err
	}
	if err := c.Create(ctx, pods[extra]); err != nil {
		return err
	}
	if err := waitForPodUnschedulableCause(ctx, c, keyOf(pods[extra]), unschedulableVolumeLimit, scenarioUnschedulableWait); err != nil {
		return fmt.Errorf("N+1 pod: %w", err)
	}
	if err := deleteIfExists(ctx, c, pods[0]); err != nil {
		return err
	}
	if err := deleteIfExists(ctx, c, pvcs[0]); err != nil {
		return err
	}
	if err := waitForVolumeReleased(ctx, c, attached[0], volumeLimitRecoveryTimeout); err != nil {
		return fmt.Errorf("released attachment/backend did not disappear: %w", err)
	}
	if err := waitForPodReady(ctx, c, keyOf(pods[extra]), volumeLimitRecoveryTimeout); err != nil {
		return fmt.Errorf("N+1 pod did not recover after releasing one attachment: %w", err)
	}
	recovered, err := boundVolumeIdentity(ctx, c, pvcs[extra])
	if err != nil {
		return err
	}
	if err := validateDistinctPersistentIdentities([]volumeIdentity{attached[1], recovered}); err != nil {
		return fmt.Errorf("recovered volume identity: %w", err)
	}
	if err := validateBackendIdentities(ctx, c, []volumeIdentity{attached[1], recovered}); err != nil {
		return fmt.Errorf("recovered backend identities: %w", err)
	}
	return assertProvisionedByDriver(ctx, c, keyOf(pvcs[extra]))
}

func createAndWaitForScenarioPods(
	ctx context.Context,
	c client.Client,
	pvcs []*corev1.PersistentVolumeClaim,
	pods []*corev1.Pod,
	timeout time.Duration,
) error {
	if len(pvcs) != len(pods) {
		return fmt.Errorf("PVC/pod count mismatch: %d PVCs, %d pods", len(pvcs), len(pods))
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make([]error, len(pods))
	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := c.Create(ctx, pvcs[i]); err != nil {
				errs[i] = fmt.Errorf("create PVC %s: %w", pvcs[i].Name, err)
				cancel()
				return
			}
			if err := c.Create(ctx, pods[i]); err != nil {
				errs[i] = fmt.Errorf("create pod %s: %w", pods[i].Name, err)
				cancel()
				return
			}
			if err := waitForPodReady(ctx, c, keyOf(pods[i]), timeout); err != nil {
				errs[i] = fmt.Errorf("pod %s: %w", pods[i].Name, err)
				cancel()
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func volumePVNames(identities []volumeIdentity) []string {
	names := make([]string, 0, len(identities))
	for _, identity := range identities {
		names = append(names, identity.pvName)
	}
	return names
}

func maxVolumeOverride(limit int32) map[string]string {
	return map[string]string{"node.maxVolumesPerNode": fmt.Sprint(limit)}
}

func scenarioPod(namespace, name, claim, node, command, expected string) *corev1.Pod {
	pod := smokePod(namespace, name, claim, "", command)
	if node != "" {
		pod.Spec.NodeSelector = map[string]string{corev1.LabelHostname: node}
	}
	pod.Spec.Containers[0].ReadinessProbe = scenarioReadinessProbe()
	if expected != "" {
		pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "EXPECTED", Value: expected}}
	}
	return pod
}

func ephemeralScenarioPod(namespace, name, storageClass, node, sentinel string) *corev1.Pod {
	pod := scenarioPod(
		namespace,
		name,
		"",
		node,
		`printf '%s\n' "$EXPECTED" > /data/proof; grep -qxF -- "$EXPECTED" /data/proof; touch `+scenarioReadyFile+`; exec sleep 3600`,
		sentinel,
	)
	pod.Spec.Volumes[0].PersistentVolumeClaim = nil
	pod.Spec.Volumes[0].Ephemeral = &corev1.EphemeralVolumeSource{
		VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		}},
	}
	return pod
}

func scenarioReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: []string{"test", "-f", scenarioReadyFile}},
		},
		PeriodSeconds:    1,
		FailureThreshold: 600,
	}
}

func podCommandSucceeded(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func waitForPodReady(ctx context.Context, c client.Client, key types.NamespacedName, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pod := &corev1.Pod{}
		if err := c.Get(ctx, key, pod); err != nil {
			if err := client.IgnoreNotFound(err); err != nil {
				return err
			}
		} else if err := podTerminalError(pod); err != nil {
			return err
		} else if podCommandSucceeded(pod) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(scenarioPollInterval):
		}
	}
	return errors.New("timed out waiting for pod readiness")
}

func podTerminalError(pod *corev1.Pod) error {
	if pod.Status.Phase == corev1.PodFailed {
		return fmt.Errorf("pod failed: %s", podTerminationSummary(pod))
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range statuses {
			if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
				return fmt.Errorf("container %s terminated: %s", status.Name, podTerminationSummary(pod))
			}
			if status.State.Waiting != nil && terminalContainerWaitingReason(status.State.Waiting.Reason) {
				return fmt.Errorf("container %s cannot start: %s: %s", status.Name, status.State.Waiting.Reason, status.State.Waiting.Message)
			}
		}
	}
	return nil
}

func terminalContainerWaitingReason(reason string) bool {
	switch reason {
	case "CreateContainerConfigError", "CreateContainerError", "ErrImagePull", "ImageInspectError", "InvalidImageName", "RunContainerError":
		return true
	default:
		return false
	}
}

func podTerminationSummary(pod *corev1.Pod) string {
	var summaries []string
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Terminated != nil {
			summaries = append(
				summaries,
				fmt.Sprintf(
					"%s exit=%d reason=%s message=%s",
					status.Name,
					status.State.Terminated.ExitCode,
					status.State.Terminated.Reason,
					status.State.Terminated.Message,
				),
			)
		}
	}
	return strings.Join(summaries, "; ")
}

func discoverEphemeralPVCs(
	ctx context.Context,
	c client.Client,
	pods []*corev1.Pod,
) ([]*corev1.PersistentVolumeClaim, error) {
	result := make([]*corev1.PersistentVolumeClaim, 0, len(pods))
	for _, pod := range pods {
		var found *corev1.PersistentVolumeClaim
		err := pollScenario(ctx, 3*time.Minute, func() (bool, error) {
			list := &corev1.PersistentVolumeClaimList{}
			if err := c.List(ctx, list, client.InNamespace(pod.Namespace)); err != nil {
				return false, err
			}
			for i := range list.Items {
				pvc := &list.Items[i]
				for _, owner := range pvc.OwnerReferences {
					if owner.UID == pod.UID && owner.Kind == "Pod" {
						found = pvc.DeepCopy()
						return true, nil
					}
				}
			}
			return false, nil
		})
		if err != nil {
			return nil, fmt.Errorf("discover ephemeral PVC for pod %s: %w", pod.Name, err)
		}
		result = append(result, found)
	}
	return result, nil
}

type volumeIdentity struct {
	pvName       string
	volumeHandle string
	targetNQN    string
	deviceGUID   string
	datasetPath  string
	zvolPath     string
}

func validateDistinctVolumeIdentities(identities []volumeIdentity) error {
	if len(identities) != 2 {
		return fmt.Errorf("got %d identities, want 2", len(identities))
	}
	if identities[0].pvName == identities[1].pvName || identities[0].volumeHandle == identities[1].volumeHandle ||
		identities[0].targetNQN == identities[1].targetNQN ||
		identities[0].deviceGUID == identities[1].deviceGUID {
		return fmt.Errorf("volume identities are not distinct: %+v", identities)
	}
	return nil
}

func validateDistinctPersistentIdentities(identities []volumeIdentity) error {
	if len(identities) != 2 {
		return fmt.Errorf("got %d identities, want 2", len(identities))
	}
	if identities[0].pvName == identities[1].pvName || identities[0].volumeHandle == identities[1].volumeHandle {
		return fmt.Errorf("PV identities are not distinct: %+v", identities)
	}
	return nil
}

func validateBackendIdentities(ctx context.Context, c client.Client, identities []volumeIdentity) error {
	volumes := &zfscsiv1.VolumeList{}
	if err := c.List(ctx, volumes); err != nil {
		return err
	}
	byHandle := map[string]zfscsiv1.Volume{}
	for i := range volumes.Items {
		byHandle[volumes.Items[i].Spec.VolumeID] = volumes.Items[i]
	}
	for i := range identities {
		volume, ok := byHandle[identities[i].volumeHandle]
		if !ok {
			return fmt.Errorf("no Volume CR for handle %s", identities[i].volumeHandle)
		}
		identities[i].targetNQN = volume.Status.TargetNQN
		identities[i].deviceGUID = volume.Status.DeviceGUID
		identities[i].datasetPath = volume.Status.DatasetPath
		identities[i].zvolPath = volume.Status.ZvolPath
		if identities[i].targetNQN == "" || identities[i].deviceGUID == "" || volume.Status.ZvolPath == "" {
			return fmt.Errorf("volume CR %s lacks target identity", volume.Name)
		}
	}
	if err := validateDistinctVolumeIdentities(identities); err != nil {
		return err
	}
	if identities[0].datasetPath == identities[1].datasetPath || identities[0].zvolPath == identities[1].zvolPath {
		return fmt.Errorf("volume CR backing paths are not distinct: %+v", identities)
	}
	enabled, err := nvmetControllerEnabled(ctx, c)
	if err != nil {
		return err
	}
	exports := &nvmetv1.NVMeExportList{}
	if err := c.List(ctx, exports); err != nil {
		return err
	}
	if !enabled {
		// Direct configfs mode has no NVMeExport CRs. Volume CR target NQN, device
		// GUID, dataset and zvol path remain the authoritative backend proof.
		return nil
	}
	if len(exports.Items) == 0 {
		return errors.New("nvmet-controller is enabled but no NVMeExport CRs exist")
	}
	found := map[string]nvmetv1.NVMeExport{}
	for i := range exports.Items {
		export := exports.Items[i]
		if export.Spec.TargetNQN == "" || export.Spec.DevicePath == "" || export.Spec.DeviceGUID == "" ||
			export.Spec.NamespaceID < 1 {
			return fmt.Errorf("NVMeExport %s has malformed target identity", export.Name)
		}
		if _, duplicate := found[export.Spec.TargetNQN]; duplicate {
			return fmt.Errorf("duplicate NVMeExport target NQN %s", export.Spec.TargetNQN)
		}
		found[export.Spec.TargetNQN] = export
	}
	for _, identity := range identities {
		export, ok := found[identity.targetNQN]
		if !ok {
			return fmt.Errorf("no NVMeExport CR for target %s", identity.targetNQN)
		}
		if export.Spec.DevicePath != identity.zvolPath || export.Spec.DeviceGUID != identity.deviceGUID {
			return fmt.Errorf("NVMeExport %s does not match Volume CR identity", export.Name)
		}
	}
	return nil
}

func nvmetControllerEnabled(ctx context.Context, c client.Client) (bool, error) {
	for _, obj := range []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "nvmet-controller", Namespace: zfsCSINamespace}},
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "nvmet-controller", Namespace: zfsCSINamespace}},
	} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			return true, nil
		} else if !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return false, nil
}

func selectScenarioNode(ctx context.Context, c client.Client) (string, error) {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return "", err
	}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Labels[e2eStorageRoleLabel] == e2eStorageRoleValue ||
			hasTaintKey(node.Spec.Taints, corev1.TaintNodeUnschedulable) ||
			hasTaintKey(node.Spec.Taints, "node-role.kubernetes.io/control-plane") {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				return node.Name, nil
			}
		}
	}
	return "", errors.New("no ready schedulable non-storage node found")
}

func hasTaintKey(taints []corev1.Taint, key string) bool {
	for _, taint := range taints {
		if taint.Key == key {
			return true
		}
	}
	return false
}

func waitForPodUnschedulableCause(
	ctx context.Context,
	c client.Client,
	key types.NamespacedName,
	cause unschedulableCause,
	timeout time.Duration,
) error {
	return pollScenario(ctx, timeout, func() (bool, error) {
		pod := &corev1.Pod{}
		if err := c.Get(ctx, key, pod); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		events := &corev1.EventList{}
		if err := c.List(ctx, events, client.InNamespace(key.Namespace)); err != nil {
			return false, err
		}
		matched, detail := podUnschedulableForCause(pod, events.Items, cause)
		if !matched && detail != "" {
			return false, errors.New(detail)
		}
		return matched, nil
	})
}

func podUnschedulableForCause(pod *corev1.Pod, events []corev1.Event, cause unschedulableCause) (bool, string) {
	if pod.Status.Phase != corev1.PodPending {
		return false, ""
	}
	var messages []string
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse &&
			condition.Reason == corev1.PodReasonUnschedulable {
			messages = append(messages, condition.Message)
		}
	}
	for _, event := range events {
		if event.InvolvedObject.UID == pod.UID &&
			(event.Reason == "FailedScheduling" || event.Reason == corev1.PodReasonUnschedulable) {
			messages = append(messages, event.Message)
		}
	}
	for _, message := range messages {
		if unschedulableMessageMatches(message, cause) {
			return true, strings.Join(messages, " | ")
		}
	}
	if len(messages) > 0 {
		return false, fmt.Sprintf("pod is unschedulable for wrong cause: %s", strings.Join(messages, " | "))
	}
	return false, ""
}

func unschedulableMessageMatches(message string, cause unschedulableCause) bool {
	message = strings.ToLower(message)
	switch cause {
	case unschedulableVolumeLimit:
		return strings.Contains(message, "max volume count") ||
			strings.Contains(message, "node(s) exceed max volume count") ||
			(strings.Contains(message, "csi") && strings.Contains(message, "volume limit"))
	case unschedulableRWOPConflict:
		return strings.Contains(message, "readwriteoncepod") || strings.Contains(message, "read-write-once-pod") ||
			strings.Contains(message, "volume uses the readwriteoncepod access mode") ||
			strings.Contains(message, "exclusive") && strings.Contains(message, "volume")
	default:
		return false
	}
}

func boundVolumeIdentity(
	ctx context.Context,
	c client.Client,
	pvc *corev1.PersistentVolumeClaim,
) (volumeIdentity, error) {
	var result volumeIdentity
	err := pollScenario(ctx, 3*time.Minute, func() (bool, error) {
		current := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, keyOf(pvc), current); err != nil {
			return false, err
		}
		if current.Status.Phase != corev1.ClaimBound || current.Spec.VolumeName == "" {
			return false, nil
		}
		pv := &corev1.PersistentVolume{}
		if err := c.Get(ctx, types.NamespacedName{Name: current.Spec.VolumeName}, pv); err != nil {
			return false, err
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != zfsCSIDriverName || pv.Spec.CSI.VolumeHandle == "" {
			return false, fmt.Errorf("PV %s lacks a zfs-csi volume handle", pv.Name)
		}
		result = volumeIdentity{pvName: pv.Name, volumeHandle: pv.Spec.CSI.VolumeHandle}
		return true, nil
	})
	return result, err
}

func assertSingleBoundVolume(ctx context.Context, c client.Client, pvc *corev1.PersistentVolumeClaim) error {
	identity, err := boundVolumeIdentity(ctx, c, pvc)
	if err != nil {
		return err
	}
	if identity.pvName == "" || identity.volumeHandle == "" {
		return errors.New("same-volume scenario did not retain one bound zfs-csi volume")
	}
	return nil
}

func waitForAttachedPVsOnNode(
	ctx context.Context,
	c client.Client,
	node string,
	pvNames []string,
	timeout time.Duration,
) error {
	want := make(map[string]struct{}, len(pvNames))
	for _, name := range pvNames {
		want[name] = struct{}{}
	}
	return pollScenario(ctx, timeout, func() (bool, error) {
		list := &storagev1.VolumeAttachmentList{}
		if err := c.List(ctx, list); err != nil {
			return false, err
		}
		seen := map[string]struct{}{}
		for i := range list.Items {
			va := &list.Items[i]
			if va.Spec.Attacher == zfsCSIDriverName && va.Spec.NodeName == node && va.Status.Attached &&
				va.Spec.Source.PersistentVolumeName != nil {
				if _, ok := want[*va.Spec.Source.PersistentVolumeName]; ok {
					seen[*va.Spec.Source.PersistentVolumeName] = struct{}{}
				}
			}
		}
		return len(seen) == len(want), nil
	})
}

func waitForCSINodeLimit(ctx context.Context, c client.Client, node string, limit int32, timeout time.Duration) error {
	return pollScenario(ctx, timeout, func() (bool, error) {
		info := &storagev1.CSINode{}
		if err := c.Get(ctx, types.NamespacedName{Name: node}, info); err != nil {
			return false, err
		}
		for _, driver := range info.Spec.Drivers {
			if driver.Name == zfsCSIDriverName && driver.Allocatable != nil && driver.Allocatable.Count != nil {
				return *driver.Allocatable.Count == limit, nil
			}
		}
		return false, nil
	})
}

func waitForVolumeReleased(ctx context.Context, c client.Client, identity volumeIdentity, timeout time.Duration) error {
	return pollScenario(ctx, timeout, func() (bool, error) {
		pvGone := apierrors.IsNotFound(
			c.Get(ctx, types.NamespacedName{Name: identity.pvName}, &corev1.PersistentVolume{}),
		)
		attachments := &storagev1.VolumeAttachmentList{}
		if err := c.List(ctx, attachments); err != nil {
			return false, err
		}
		for i := range attachments.Items {
			if attachments.Items[i].Spec.Source.PersistentVolumeName != nil &&
				*attachments.Items[i].Spec.Source.PersistentVolumeName == identity.pvName {
				return false, nil
			}
		}
		volumes := &zfscsiv1.VolumeList{}
		if err := c.List(ctx, volumes); err != nil {
			return false, err
		}
		for i := range volumes.Items {
			if volumes.Items[i].Spec.VolumeID == identity.volumeHandle {
				return false, nil
			}
		}
		return pvGone, nil
	})
}

func resetScenarioObjects(ctx context.Context, c client.Client, objects []client.Object) error {
	for _, obj := range objects {
		if err := deleteIfExists(ctx, c, obj); err != nil {
			return err
		}
	}
	return waitForScenarioObjectsGone(ctx, c, objects, 3*time.Minute)
}

func cleanupScenarioObjects(c client.Client, objects []client.Object) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, obj := range objects {
		if err := deleteIfExists(ctx, c, obj); err != nil {
			return err
		}
	}
	return waitForScenarioObjectsGone(ctx, c, objects, 3*time.Minute)
}

func waitForScenarioObjectsGone(
	ctx context.Context,
	c client.Client,
	objects []client.Object,
	timeout time.Duration,
) error {
	return pollScenario(ctx, timeout, func() (bool, error) {
		for _, obj := range objects {
			current := obj.DeepCopyObject().(client.Object)
			err := c.Get(ctx, client.ObjectKeyFromObject(obj), current)
			if err == nil {
				return false, nil
			}
			if !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		return true, nil
	})
}

func pollScenario(ctx context.Context, timeout time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if ok {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(scenarioPollInterval):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("timed out: %w", lastErr)
	}
	return errors.New("timed out waiting for scenario condition")
}
