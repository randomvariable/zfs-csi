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

package performance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvmetv1alpha1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfsv1alpha1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
)

type Runner struct {
	Client                                            client.Client
	Clientset                                         kubernetes.Interface
	Namespace, Pool, ConsumerNode, SecondConsumerNode string
	DriverNamespace                                   string
	NFSExportCIDRs                                    []string
	Artifacts                                         Artifacts
	Environment                                       Environment
	PollInterval                                      time.Duration
	samples                                           []Sample
	runID                                             string
	lifecycleHook                                     func(stage string) error
}

func (r *Runner) Run(ctx context.Context) (Run, error) {
	if !LiveEnabled() {
		return Run{}, fmt.Errorf("live performance execution requires ZFS_CSI_PERF=1")
	}
	image, err := FioImage()
	if err != nil {
		return Run{}, err
	}
	r.Environment.FioImage = image
	fingerprint, err := Fingerprint(r.Environment)
	if err != nil {
		return Run{}, err
	}
	if r.Environment.Fingerprint != "" && r.Environment.Fingerprint != fingerprint {
		return Run{}, fmt.Errorf("environment fingerprint stale after fio image population")
	}
	r.Environment.Fingerprint = fingerprint
	if r.PollInterval == 0 {
		r.PollInterval = 500 * time.Millisecond
	}
	if err := r.Artifacts.Init(); err != nil {
		return Run{}, err
	}
	scenarios := DefaultScenarios()
	if err := ValidateScenarios(scenarios); err != nil {
		return Run{}, err
	}
	r.runID = safeName(fmt.Sprintf("p%d", time.Now().UTC().UnixNano()))
	scenarios = uniqueStorageClasses(scenarios, r.runID)
	run := Run{
		SchemaVersion: 1,
		RunID:         r.runID,
		StartedAt:     time.Now().UTC(),
		Environment:   r.Environment,
		Scenarios:     scenarios,
		Valid:         true,
	}
	if err := r.Artifacts.WriteRun(run); err != nil {
		return run, err
	}
	for i := range run.Scenarios {
		if run.Scenarios[i].Transport == "nfs" && r.SecondConsumerNode == "" {
			run.Scenarios[i].Available, run.Scenarios[i].SkipReason = false, "NFS comparison requires two ready non-storage workers"
		}
		scenario := run.Scenarios[i]
		if !scenario.Available {
			event := Event{
				Time:     time.Now().UTC(),
				Scenario: scenario.Name,
				Boundary: "skip",
				Details:  map[string]string{"reason": scenario.SkipReason},
			}
			if err := r.Artifacts.AppendEvent(event); err != nil {
				return run, err
			}
			continue
		}
		for _, variant := range []Variant{scenario.Control, scenario.Candidate} {
			if err := r.ensureStorageClass(ctx, scenario.Transport, variant); err != nil {
				return run, err
			}
			if err := r.runLifecycle(ctx, scenario, variant); err != nil {
				run.Valid, run.InvalidReason = false, err.Error()
				_ = r.Artifacts.WriteRun(run)
				return run, err
			}
			if err := r.runFio(ctx, scenario, variant); err != nil {
				return run, err
			}
		}
	}
	runnable := 0
	for _, scenario := range run.Scenarios {
		if scenario.Available {
			runnable++
		}
	}
	if runnable == 0 {
		run.Valid, run.InvalidReason = false, "no runnable performance scenarios"
		_ = r.Artifacts.WriteRun(run)
		return run, errors.New(run.InvalidReason)
	}
	if err := EnvironmentStable(r.Environment.Fingerprint, r.samples); err != nil {
		run.Valid, run.InvalidReason = false, err.Error()
	}
	run.Assessments = AssessSamples(run.Scenarios, r.samples)
	for _, assessment := range run.Assessments {
		if !assessment.Valid || !assessment.Passed {
			run.Valid = false
			if run.InvalidReason == "" {
				reason := assessment.InvalidReason
				if reason == "" {
					reason = "regression threshold exceeded"
				}
				run.InvalidReason = assessment.Scenario + "/" + assessment.Metric + ": " + reason
			}
		}
	}
	return run, r.Artifacts.WriteRun(run)
}

func (r *Runner) runLifecycle(ctx context.Context, scenario Scenario, variant Variant) error {
	for i := 0; i < WarmupCycles+MeasuredCycles; i++ {
		if err := r.runLifecycleIteration(ctx, scenario, variant, i, i < WarmupCycles); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) runLifecycleIteration(
	ctx context.Context,
	scenario Scenario,
	variant Variant,
	i int,
	warmup bool,
) (err error) {
	name := safeName(fmt.Sprintf("perf-%s-%s-%s-%02d", r.runID, scenario.Name, variant.Name, i))
	pvc := PVC(r.Namespace, name, variant.StorageClass, scenario.Transport)
	setRunLabels(&pvc.ObjectMeta, r.runID)
	pod := consumerPod(r.Namespace, name, pvc.Name, r.ConsumerNode)
	setRunLabels(&pod.ObjectMeta, r.runID)
	pods := []*corev1.Pod{pod}
	if scenario.Transport == "nfs" {
		second := consumerPod(r.Namespace, name+"-second", pvc.Name, r.SecondConsumerNode)
		setRunLabels(&second.ObjectMeta, r.runID)
		pods = append(pods, second)
	}
	created := false
	defer func() {
		if created {
			err = errors.Join(err, r.cleanupLifecycleIteration(pvc, pods, "", ""))
		}
	}()
	started := time.Now().UTC()
	r.event(scenario, variant, i, "create-start", name, nil)
	if err := r.Client.Create(ctx, pvc); err != nil {
		return err
	}
	created = true
	if err := r.callLifecycleHook("after-pvc-create"); err != nil {
		return err
	}
	for _, consumer := range pods {
		if err := r.Client.Create(ctx, consumer); err != nil {
			return err
		}
		if err := r.callLifecycleHook("after-pod-create"); err != nil {
			return err
		}
	}
	for _, consumer := range pods {
		if err := r.waitReadyBoundary(ctx, scenario.Transport, consumer, pvc); err != nil {
			return err
		}
	}
	attach := time.Since(started)
	r.event(scenario, variant, i, "ready-and-storage-visible", name, nil)
	sample := Sample{
		Scenario:       scenario.Name,
		Variant:        variant.Name,
		Phase:          "attach",
		Iteration:      i,
		Warmup:         warmup,
		StartedAt:      started,
		DurationMillis: float64(attach.Microseconds()) / 1000,
		Valid:          true,
		Fingerprint:    r.Environment.Fingerprint,
	}
	if err := r.recordSample(sample); err != nil {
		return err
	}
	started = time.Now().UTC()
	r.event(scenario, variant, i, "delete-start", name, nil)
	for _, consumer := range pods {
		if err := r.Client.Delete(ctx, consumer); err != nil {
			return err
		}
	}
	for _, consumer := range pods {
		if err := r.waitDetachBoundary(ctx, scenario.Transport, types.NamespacedName{Namespace: consumer.Namespace, Name: consumer.Name}, pvc); err != nil {
			return err
		}
	}
	r.event(scenario, variant, i, "consumers-and-mounts-gone", name, nil)
	pvName, volumeHandle, identityErr := r.resolvePVCIdentity(ctx, pvc)
	if identityErr != nil {
		return identityErr
	}
	if err := r.Client.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if volumeHandle == "" {
		return fmt.Errorf("PVC %s resolved no CSI volumeHandle", pvc.Name)
	}
	if err := r.callLifecycleHook("before-backend-wait"); err != nil {
		return err
	}
	if err := r.waitStorageDeleted(ctx, pvc, pvName, volumeHandle); err != nil {
		return err
	}
	detach := time.Since(started)
	r.event(
		scenario,
		variant,
		i,
		"pvc-pv-attachment-backend-gone",
		name,
		map[string]string{"pv": pvName, "volumeHandle": volumeHandle},
	)
	sample = Sample{
		Scenario:       scenario.Name,
		Variant:        variant.Name,
		Phase:          "detach",
		Iteration:      i,
		Warmup:         warmup,
		StartedAt:      started,
		DurationMillis: float64(detach.Microseconds()) / 1000,
		Valid:          true,
		Fingerprint:    r.Environment.Fingerprint,
	}
	if err := r.recordSample(sample); err != nil {
		return err
	}
	created = false
	return nil
}

func (r *Runner) runFio(ctx context.Context, scenario Scenario, variant Variant) (err error) {
	name := safeName("fio-" + scenario.Name + "-" + variant.Name)
	pvc := PVC(r.Namespace, name, variant.StorageClass, scenario.Transport)
	if err := r.Client.Create(ctx, pvc); err != nil {
		return err
	}
	pvName, volumeHandle, err := r.waitPVCIdentity(ctx, pvc)
	if err != nil {
		_ = r.Client.Delete(context.Background(), pvc)
		return err
	}
	defer func() { err = errors.Join(err, r.cleanupFioPVC(pvc, pvName, volumeHandle)) }()
	for _, workload := range Workloads {
		for _, phase := range []struct {
			name string
			runs int
		}{{"precondition", 1}, {"warmup", 1}, {"measured", FioMeasuredRuns}} {
			for i := 0; i < phase.runs; i++ {
				jobName := safeName(fmt.Sprintf("%s-%s-%s-%d", name, workload.Name, phase.name, i))
				job := FioJobManifest(
					r.Namespace,
					jobName,
					pvc.Name,
					r.Environment.FioImage,
					r.ConsumerNode,
					workload,
					phase.name,
				)
				raw, runErr := r.runFioJob(ctx, job)
				if runErr != nil {
					return runErr
				}
				if phase.name == "measured" {
					path, err := r.Artifacts.WriteFio(jobName, raw)
					if err != nil {
						return err
					}
					op := "read"
					if strings.Contains(workload.RW, "write") {
						op = "write"
					}
					iops, bw, p99, err := ParseFio(raw, op)
					if err != nil {
						return err
					}
					sample := Sample{
						Scenario:      scenario.Name,
						Variant:       variant.Name,
						Phase:         workload.Name,
						Iteration:     i,
						StartedAt:     job.CreationTimestamp.Time,
						IOPS:          iops,
						ThroughputMiB: bw,
						P99Millis:     p99,
						Valid:         true,
						RawArtifact:   path,
						Fingerprint:   r.Environment.Fingerprint,
					}
					if err := r.recordSample(sample); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (r *Runner) runFioJob(ctx context.Context, job *batchv1.Job) (raw []byte, err error) {
	if err = r.Client.Create(ctx, job); err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, r.deleteJobAndWait(job)) }()
	if err = r.waitJob(ctx, job); err != nil {
		return nil, err
	}
	return r.jobLogs(ctx, job)
}

func (r *Runner) deleteJobAndWait(job *batchv1.Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := r.Client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil &&
		!apierrors.IsNotFound(err) {
		return err
	}
	return r.poll(ctx, 2*time.Minute, func() (bool, error) {
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(job), &batchv1.Job{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func (r *Runner) waitPVCIdentity(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) (pvName, volumeHandle string, err error) {
	err = r.poll(ctx, 10*time.Minute, func() (bool, error) {
		current := &corev1.PersistentVolumeClaim{}
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pvc), current); err != nil {
			return false, err
		}
		if current.Spec.VolumeName == "" {
			return false, nil
		}
		pv := &corev1.PersistentVolume{}
		if err := r.Client.Get(ctx, client.ObjectKey{Name: current.Spec.VolumeName}, pv); err != nil {
			return false, err
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
			return false, nil
		}
		pvName, volumeHandle = pv.Name, pv.Spec.CSI.VolumeHandle
		return true, nil
	})
	return
}

func (r *Runner) cleanupFioPVC(pvc *corev1.PersistentVolumeClaim, pvName, volumeHandle string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := r.Client.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return r.waitStorageDeleted(ctx, pvc, pvName, volumeHandle)
}

func (r *Runner) resolvePVCIdentity(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, string, error) {
	current := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pvc), current); err != nil {
		return "", "", err
	}
	if current.Spec.VolumeName == "" {
		return "", "", fmt.Errorf("PVC %s has no bound PV", pvc.Name)
	}
	pv := &corev1.PersistentVolume{}
	if err := r.Client.Get(ctx, client.ObjectKey{Name: current.Spec.VolumeName}, pv); err != nil {
		return "", "", err
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
		return pv.Name, "", fmt.Errorf("PV %s has no CSI volumeHandle", pv.Name)
	}
	return pv.Name, pv.Spec.CSI.VolumeHandle, nil
}

func (r *Runner) cleanupLifecycleIteration(
	pvc *corev1.PersistentVolumeClaim,
	pods []*corev1.Pod,
	pvName, volumeHandle string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var cleanupErr error
	for _, pod := range pods {
		if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			cleanupErr = errors.Join(cleanupErr, err)
		}
		cleanupErr = errors.Join(cleanupErr, r.waitObjectGone(ctx, pod, 2*time.Minute))
	}
	if pvName == "" || volumeHandle == "" {
		resolvedPV, resolvedHandle, err := r.resolvePVCIdentity(ctx, pvc)
		if err == nil {
			pvName, volumeHandle = resolvedPV, resolvedHandle
		}
	}
	if err := r.Client.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		cleanupErr = errors.Join(cleanupErr, err)
	}
	if pvName != "" && volumeHandle != "" {
		cleanupErr = errors.Join(cleanupErr, r.waitStorageDeleted(ctx, pvc, pvName, volumeHandle))
	} else {
		cleanupErr = errors.Join(cleanupErr, r.waitNamespaceRunInventoryGone(ctx))
	}
	return cleanupErr
}

func (r *Runner) waitObjectGone(ctx context.Context, obj client.Object, timeout time.Duration) error {
	return r.poll(ctx, timeout, func() (bool, error) {
		probe := obj.DeepCopyObject().(client.Object)
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(obj), probe)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func (r *Runner) waitNamespaceRunInventoryGone(ctx context.Context) error {
	return r.poll(ctx, 10*time.Minute, func() (bool, error) {
		selector := client.MatchingLabels{"zfs-csi.randomvariable.co.uk/performance-run": r.runID}
		pods := &corev1.PodList{}
		if err := r.Client.List(ctx, pods, client.InNamespace(r.Namespace), selector); err != nil {
			return false, err
		}
		pvcs := &corev1.PersistentVolumeClaimList{}
		if err := r.Client.List(ctx, pvcs, client.InNamespace(r.Namespace), selector); err != nil {
			return false, err
		}
		return len(pods.Items) == 0 && len(pvcs.Items) == 0, nil
	})
}

func (r *Runner) callLifecycleHook(stage string) error {
	if r.lifecycleHook != nil {
		return r.lifecycleHook(stage)
	}
	return nil
}

func setRunLabels(meta *metav1.ObjectMeta, runID string) {
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	meta.Labels["zfs-csi.randomvariable.co.uk/performance-run"] = runID
}

func (r *Runner) ensureStorageClass(ctx context.Context, transport string, variant Variant) error {
	sc, err := StorageClassFor(variant, transport, r.Pool, r.NFSExportCIDRs)
	if err != nil {
		return err
	}
	if err := r.Client.Create(ctx, sc); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing := &storagev1.StorageClass{}
		if getErr := r.Client.Get(ctx, client.ObjectKey{Name: sc.Name}, existing); getErr != nil {
			return getErr
		}
		if !equalStorageClassSpec(existing, sc) {
			return fmt.Errorf("StorageClass %s exists with a different spec", sc.Name)
		}
	}
	return nil
}

func (r *Runner) waitStorageDeleted(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	pvName, volumeHandle string,
) error {
	return r.poll(ctx, 10*time.Minute, func() (bool, error) {
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}); err == nil {
			return false, nil
		} else if !apierrors.IsNotFound(err) {
			return false, err
		}
		if pvName != "" {
			if err := r.Client.Get(ctx, client.ObjectKey{Name: pvName}, &corev1.PersistentVolume{}); err == nil {
				return false, nil
			} else if !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		attachments := &storagev1.VolumeAttachmentList{}
		if err := r.Client.List(ctx, attachments); err != nil {
			return false, err
		}
		for _, va := range attachments.Items {
			if va.Spec.Source.PersistentVolumeName != nil && *va.Spec.Source.PersistentVolumeName == pvName {
				return false, nil
			}
		}
		volumes := &zfsv1alpha1.VolumeList{}
		if err := r.Client.List(ctx, volumes); err != nil {
			return false, err
		}
		for _, volume := range volumes.Items {
			if volume.Spec.VolumeID == volumeHandle {
				return false, nil
			}
		}
		exports := &nvmetv1alpha1.NVMeExportList{}
		if err := r.Client.List(ctx, exports); err != nil {
			return false, err
		}
		parsed, targetErr := naming.ParseVolID(volumeHandle)
		if targetErr != nil {
			return false, targetErr
		}
		devicePath := "/dev/zvol/" + parsed.DatasetPath()
		for _, export := range exports.Items {
			if export.Spec.DevicePath == devicePath {
				return false, nil
			}
		}
		return true, nil
	})
}

func (r *Runner) waitReadyBoundary(
	ctx context.Context,
	transport string,
	pod *corev1.Pod,
	pvc *corev1.PersistentVolumeClaim,
) error {
	return r.poll(ctx, 10*time.Minute, func() (bool, error) {
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
			return false, err
		}
		ready := pod.Status.Phase == corev1.PodRunning
		if !ready {
			return false, nil
		}
		if transport == "nfs" {
			return true, nil
		}
		return r.hasAttachment(ctx, pvc)
	})
}

func (r *Runner) waitDetachBoundary(
	ctx context.Context,
	transport string,
	pod types.NamespacedName,
	pvc *corev1.PersistentVolumeClaim,
) error {
	return r.poll(ctx, 10*time.Minute, func() (bool, error) {
		probe := &corev1.Pod{}
		err := r.Client.Get(ctx, pod, probe)
		gone := apierrors.IsNotFound(err)
		if err != nil && !gone {
			return false, err
		}
		if !gone {
			return false, nil
		}
		if transport == "nfs" {
			return true, nil
		}
		attached, err := r.hasAttachment(ctx, pvc)
		return !attached, err
	})
}

func (r *Runner) hasAttachment(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	current := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pvc), current); err != nil {
		return false, err
	}
	if current.Spec.VolumeName == "" {
		return false, nil
	}
	list := &storagev1.VolumeAttachmentList{}
	if err := r.Client.List(ctx, list); err != nil {
		return false, err
	}
	for _, va := range list.Items {
		if va.Spec.Source.PersistentVolumeName != nil &&
			*va.Spec.Source.PersistentVolumeName == current.Spec.VolumeName &&
			va.Status.Attached {
			return true, nil
		}
	}
	return false, nil
}

func (r *Runner) waitJob(ctx context.Context, job *batchv1.Job) error {
	return r.poll(ctx, 10*time.Minute, func() (bool, error) {
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
			return false, err
		}
		if job.Status.Failed > 0 {
			return false, fmt.Errorf("fio job %s failed", job.Name)
		}
		return job.Status.Succeeded > 0, nil
	})
}

func (r *Runner) jobLogs(ctx context.Context, job *batchv1.Job) (logs []byte, retErr error) {
	pods, err := r.Clientset.CoreV1().
		Pods(job.Namespace).
		List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + job.Name})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) != 1 {
		return nil, fmt.Errorf("expected one pod for job %s, got %d", job.Name, len(pods.Items))
	}
	stream, err := r.Clientset.CoreV1().
		Pods(job.Namespace).
		GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{Container: "fio"}).
		Stream(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, stream.Close()) }()
	return io.ReadAll(stream)
}

func (r *Runner) poll(ctx context.Context, timeout time.Duration, check func() (bool, error)) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(r.PollInterval)
	defer ticker.Stop()
	for {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("performance boundary timed out after %s", timeout)
		case <-ticker.C:
		}
	}
}

func (r *Runner) event(s Scenario, v Variant, i int, boundary, object string, details map[string]string) {
	_ = r.Artifacts.AppendEvent(
		Event{
			Time:      time.Now().UTC(),
			Scenario:  s.Name,
			Variant:   v.Name,
			Iteration: i,
			Boundary:  boundary,
			Object:    object,
			Details:   details,
		},
	)
}

func (r *Runner) recordSample(sample Sample) error {
	r.samples = append(r.samples, sample)
	return r.Artifacts.AppendSample(sample)
}

func Fingerprint(env Environment) (string, error) {
	env.Fingerprint = ""
	sort.Slice(env.Nodes, func(i, j int) bool { return env.Nodes[i].Name < env.Nodes[j].Name })
	b, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func consumerPod(namespace, name, pvc, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			Affinity:      requiredNodeAffinity(node),
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:         "hold",
					Image:        "registry.k8s.io/pause:3.10",
					VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc},
					},
				},
			},
		},
	}
}

func uniqueStorageClasses(scenarios []Scenario, suffix string) []Scenario {
	out := append([]Scenario(nil), scenarios...)
	for i := range out {
		if out[i].Available {
			out[i].Control.StorageClass = safeName(out[i].Control.StorageClass + "-" + suffix)
			out[i].Candidate.StorageClass = safeName(out[i].Candidate.StorageClass + "-" + suffix)
		}
	}
	return out
}

func equalStorageClassSpec(a, b *storagev1.StorageClass) bool {
	ja, _ := json.Marshal(struct {
		P map[string]string
		M []string
	}{a.Parameters, a.MountOptions})
	jb, _ := json.Marshal(struct {
		P map[string]string
		M []string
	}{b.Parameters, b.MountOptions})
	return string(ja) == string(jb) && a.Provisioner == b.Provisioner
}

func safeName(value string) string {
	value = strings.ToLower(value)
	replacer := strings.NewReplacer("_", "-", ".", "-", "/", "-")
	value = replacer.Replace(value)
	if len(value) > 63 {
		value = value[:63]
	}
	return strings.Trim(value, "-")
}
