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
	"os/exec"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

const (
	healthScenarioTimeout      = 5 * time.Minute
	healthRepairHoldAnnotation = "zfs.csi.randomvariable.co.uk/test-hold-health-repair"
)

var backendHealthPollInterval = scenarioPollInterval

// runDurableHealthScenario removes the live configfs target for a mounted NVMe
// volume, then proves the agent persists an abnormal condition before repairing
// it and later records recovery. The storage-agent pod receives the mutation;
// no workload scheduling or access-mode behavior is exercised here.
func runDurableHealthScenario(ctx context.Context, c client.Client, kubeconfig, namespace string, storage storageNode) (retErr error) {
	const prefix = "zfs-csi-e2e-health"
	if err := healthRepairHoldGateEnabled(ctx, c, storage.Name); err != nil {
		return err
	}
	pvc := pvcObject(namespace, prefix, "zfs-tank-nvme", corev1.ReadWriteOnce)
	pod := healthConsumerPod(namespace, prefix+"-consumer", pvc.Name)
	objects := []client.Object{pod, pvc}
	if err := resetScenarioObjects(ctx, c, objects); err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, cleanupScenarioObjects(c, objects)) }()
	if err := c.Create(ctx, pvc); err != nil {
		return err
	}
	if err := c.Create(ctx, pod); err != nil {
		return err
	}
	if err := waitForPodRunning(ctx, c, keyOf(pod), scenarioPodTimeout); err != nil {
		return fmt.Errorf("health consumer not running: %w", err)
	}
	identity, err := boundVolumeIdentity(ctx, c, pvc)
	if err != nil {
		return err
	}
	volume, err := volumeForHandle(ctx, c, identity.volumeHandle)
	if err != nil {
		return err
	}
	if volume.Status.TargetNQN == "" {
		return fmt.Errorf("volume %s has no target NQN", volume.Name)
	}
	// Hold only this E2E fault's repair. This makes the persisted false state
	// observable before recovery rather than relying on a one-second requeue.
	base := volume.DeepCopy()
	annotations := volume.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[healthRepairHoldAnnotation] = "true"
	volume.SetAnnotations(annotations)
	if err := c.Patch(ctx, volume, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("hold backend health repair: %w", err)
	}

	storagePod, err := storageAgentPod(ctx, c, storage.Name)
	if err != nil {
		return err
	}
	if err := execInPod(ctx, kubeconfig, storagePod, destroyTargetCommand(volume.Status.TargetNQN)); err != nil {
		return fmt.Errorf("remove configfs target: %w", err)
	}
	// Metadata changes wake the level-triggered reconciler without modifying the
	// desired volume contract, so it detects and repairs the missing target.
	base = volume.DeepCopy()
	annotations = volume.GetAnnotations()
	annotations["zfs.csi.randomvariable.co.uk/health-check"] = time.Now().UTC().Format(time.RFC3339Nano)
	volume.SetAnnotations(annotations)
	if err := c.Patch(ctx, volume, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("trigger backend health reconcile: %w", err)
	}
	watchFrom := volume.ResourceVersion
	if err := waitForBackendHealthTransition(ctx, c, volume.Name, watchFrom, metav1.ConditionFalse, healthScenarioTimeout); err != nil {
		return fmt.Errorf("wait for persisted unhealthy backend condition: %w", err)
	}
	if err := waitForHealthEvent(ctx, c, volume.Name, "BackendUnhealthy", healthScenarioTimeout); err != nil {
		return fmt.Errorf("wait for BackendUnhealthy event: %w", err)
	}
	// The hold makes this a durable observation, not a race against the next
	// repair pass. A second read after the normal poll interval rejects a brief
	// false condition that could otherwise make the scenario pass spuriously.
	time.Sleep(1 * time.Minute)
	if err := assertBackendHealth(ctx, c, volume.Name, metav1.ConditionFalse, "BackendUnhealthy"); err != nil {
		return fmt.Errorf("unhealthy condition did not remain durable: %w", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: volume.Name}, volume); err != nil {
		return err
	}
	base = volume.DeepCopy()
	annotations = volume.GetAnnotations()
	delete(annotations, healthRepairHoldAnnotation)
	volume.SetAnnotations(annotations)
	if err := c.Patch(ctx, volume, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("release backend health repair: %w", err)
	}
	if err := waitForBackendHealthTransition(ctx, c, volume.Name, watchFrom, metav1.ConditionTrue, healthScenarioTimeout); err != nil {
		return fmt.Errorf("wait for repaired backend condition: %w", err)
	}
	if err := waitForHealthEvent(ctx, c, volume.Name, "BackendRecovered", healthScenarioTimeout); err != nil {
		return fmt.Errorf("wait for BackendRecovered event: %w", err)
	}
	if err := verifyHealthIO(ctx, kubeconfig, namespace, pod.Name); err != nil {
		return err
	}
	return nil
}

func healthConsumerPod(namespace, name, claim string) *corev1.Pod {
	return scenarioPod(
		namespace,
		name,
		claim,
		"",
		"printf health > /data/proof; test \"$(cat /data/proof)\" = health; touch "+scenarioReadyFile+"; exec sleep 3600",
		"",
	)
}

func healthRepairHoldGateEnabled(ctx context.Context, c client.Client, node string) error {
	pod, err := storageAgentPod(ctx, c, node)
	if err != nil {
		return err
	}
	storagePod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Name: pod, Namespace: zfsCSINamespace}, storagePod); err != nil {
		return err
	}
	for _, container := range storagePod.Spec.Containers {
		if container.Name == "storage" && storageArgsEnableHealthRepairHold(container.Args) {
			return nil
		}
	}
	return fmt.Errorf("storage agent health repair hold gate is not enabled")
}

func storageArgsEnableHealthRepairHold(args []string) bool {
	for _, arg := range args {
		if arg == "--e2e-enable-health-repair-hold=true" {
			return true
		}
	}
	return false
}

func destroyTargetCommand(nqn string) string {
	subsystem := "/sys/kernel/config/nvmet/subsystems/" + nqn
	link := "/sys/kernel/config/nvmet/ports/1/subsystems/" + nqn
	return fmt.Sprintf(`
if test -e %s; then
  rm -f %s
  for host in %s/allowed_hosts/*; do
    test -e "$host" || continue
    rm -f "$host"
  done
  for ns in %s/namespaces/*; do
    test -e "$ns" || continue
    echo 0 > "$ns/enable"
    rmdir "$ns"
  done
  rmdir %s
fi
test ! -e %s
test ! -e %s
`, shellQuote(subsystem), shellQuote(link), shellQuote(subsystem), shellQuote(subsystem), shellQuote(subsystem), shellQuote(subsystem), shellQuote(link))
}

func volumeForHandle(ctx context.Context, c client.Client, handle string) (*zfscsiv1.Volume, error) {
	list := &zfscsiv1.VolumeList{}
	if err := c.List(ctx, list); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Spec.VolumeID == handle {
			return &list.Items[i], nil
		}
	}
	return nil, fmt.Errorf("volume for handle %q not found", handle)
}

func waitForBackendHealthTransition(ctx context.Context, c client.Client, name, resourceVersion string, want metav1.ConditionStatus, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(backendHealthPollInterval)
	defer ticker.Stop()
	lastResourceVersion := "<not-read>"
	lastHealth := "<missing>"
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := waitCtx.Err(); err != nil {
			return fmt.Errorf("wait for BackendHealthy=%s timed out: triggerResourceVersion=%q lastResourceVersion=%q lastCondition=%s: %w",
				want, resourceVersion, lastResourceVersion, lastHealth, err)
		}

		current := &zfscsiv1.Volume{}
		if err := c.Get(waitCtx, types.NamespacedName{Name: name}, current); err != nil {
			return err
		}
		lastResourceVersion = current.ResourceVersion
		lastHealth = backendHealthConditionReport(current)
		// The trigger update itself may retain an old condition. Only a later
		// object version proves the reconciler observed the requested state.
		if current.ResourceVersion != resourceVersion && hasBackendHealth(current, want) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func backendHealthConditionReport(volume *zfscsiv1.Volume) string {
	for _, condition := range volume.Status.Conditions {
		if condition.Type == string(zfscsiv1.VolumeConditionBackendHealthy) {
			return fmt.Sprintf("%s/%s/%q", condition.Status, condition.Reason, condition.Message)
		}
	}

	return "<missing>"
}

func hasBackendHealth(volume *zfscsiv1.Volume, want metav1.ConditionStatus) bool {
	for _, condition := range volume.Status.Conditions {
		if condition.Type == string(zfscsiv1.VolumeConditionBackendHealthy) && condition.Status == want && condition.Reason == healthReason(want) {
			return true
		}
	}
	return false
}

func assertBackendHealth(ctx context.Context, c client.Client, name string, want metav1.ConditionStatus, reason string) error {
	volume := &zfscsiv1.Volume{}
	if err := c.Get(ctx, types.NamespacedName{Name: name}, volume); err != nil {
		return err
	}
	for _, condition := range volume.Status.Conditions {
		if condition.Type == string(zfscsiv1.VolumeConditionBackendHealthy) {
			if condition.Status == want && condition.Reason == reason && condition.Message != "" {
				return nil
			}
			return fmt.Errorf("BackendHealthy=%s/%s/%q, want %s/%s/non-empty", condition.Status, condition.Reason, condition.Message, want, reason)
		}
	}
	return fmt.Errorf("BackendHealthy condition missing")
}

func healthReason(status metav1.ConditionStatus) string {
	if status == metav1.ConditionFalse {
		return "BackendUnhealthy"
	}
	return "BackendRecovered"
}

func waitForHealthEvent(ctx context.Context, c client.Client, volumeName, reason string, timeout time.Duration) error {
	return pollScenario(ctx, timeout, func() (bool, error) {
		events := &eventsv1.EventList{}
		if err := c.List(ctx, events, client.InNamespace(zfsCSINamespace)); err != nil {
			return false, err
		}
		for i := range events.Items {
			event := &events.Items[i]
			if event.Regarding.Name == volumeName && event.Reason == reason {
				return true, nil
			}
		}
		return false, nil
	})
}

func verifyHealthIO(ctx context.Context, kubeconfig, namespace, pod string) error {
	return execWorkloadPod(ctx, kubeconfig, namespace, pod, "test \"$(cat /data/proof)\" = health && printf recovered > /data/proof && test \"$(cat /data/proof)\" = recovered")
}

func storageAgentPod(ctx context.Context, c client.Client, node string) (string, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(zfsCSINamespace), client.MatchingLabels{"app.kubernetes.io/component": "storage"}); err != nil {
		return "", err
	}
	for i := range pods.Items {
		if pods.Items[i].Spec.NodeName == node && pods.Items[i].Status.Phase == corev1.PodRunning {
			return pods.Items[i].Name, nil
		}
	}
	return "", fmt.Errorf("running storage agent on %s not found", node)
}

func execInPod(ctx context.Context, kubeconfig, pod, command string) error {
	args := []string{"--kubeconfig", kubeconfig, "-n", zfsCSINamespace, "exec", pod, "-c", "storage", "--", "sh", "-ceu", command}
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl exec %s: %w\n%s", pod, err, out)
	}
	return nil
}

func execWorkloadPod(ctx context.Context, kubeconfig, namespace, pod, command string) error {
	args := []string{"--kubeconfig", kubeconfig, "-n", namespace, "exec", pod, "-c", "consumer", "--", "sh", "-ceu", command}
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl exec workload %s: %w\n%s", pod, err, out)
	}
	return nil
}
