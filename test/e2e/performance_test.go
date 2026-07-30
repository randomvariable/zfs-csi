// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && performance

package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
	perf "github.com/randomvariable/zfs-csi/test/performance"
)

// RunPerformanceAcceptance is the reporting integration hook. The lifecycle
// orchestrator calls it only after provisioning and correctness scenarios have
// completed; keeping that call outside this file avoids coupling performance
// ownership to the shared Ordered spec.
func RunPerformanceAcceptance(ctx context.Context, artifactDir string, workload framework.ClusterProxy, workloadClient client.Client, storage storageNode, driverImage string) error {
	if !perf.LiveEnabled() {
		return fmt.Errorf("live performance execution requires ZFS_CSI_PERF=1")
	}
	consumers, err := performanceConsumerNodes(ctx, workloadClient, storage.Name)
	if err != nil {
		return err
	}
	consumer := consumers[0]
	second := ""
	if len(consumers) > 1 {
		second = consumers[1]
	}
	config, err := clientcmd.BuildConfigFromFlags("", workload.GetKubeconfigPath())
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	fioImage, err := perf.FioImage()
	if err != nil {
		return err
	}
	diagnosticNodes := append([]string{storage.Name}, consumers...)
	env, err := collectLiveEnvironment(ctx, workloadClient, clientset, "default", diagnosticNodes, strings.TrimSpace(os.Getenv("ZFS_CSI_PERF_DIAGNOSTIC_IMAGE")), perf.Environment{GitCommit: strings.TrimSpace(os.Getenv("ZFS_CSI_PERF_GIT_COMMIT")), DriverImage: driverImage, FioImage: fioImage, Kubernetes: e2econfig.KubernetesVersion(), Values: map[string]string{"provider": e2econfig.InfrastructureProvider(), "pool": storagePoolFromEnv()}})
	if err != nil {
		return err
	}
	runner := &perf.Runner{Client: workloadClient, Clientset: clientset, Namespace: "default", DriverNamespace: zfsCSINamespace, Pool: storagePoolFromEnv(), NFSExportCIDRs: e2econfig.NFSExportCIDRs(), ConsumerNode: consumer, SecondConsumerNode: second, Artifacts: perf.Artifacts{Root: filepath.Join(artifactDir, "performance")}, Environment: env}
	perfCtx, cancel := context.WithTimeout(context.Background(), 8*time.Hour)
	defer cancel()
	_, err = runner.Run(perfCtx)
	return err
}

// RegisterPerformanceAcceptance wires the single performance Ginkgo case. It
// is available only in e2e+performance builds, so ordinary E2E registration is
// unchanged and cannot accidentally create benchmark workloads.
func RegisterPerformanceAcceptance(run func() error) {
	It("records one auditable performance acceptance result", Label("performance"), func() {
		if !perf.LiveEnabled() {
			Skip("performance execution is opt-in; set ZFS_CSI_PERF=1")
		}
		Expect(run()).To(Succeed())
	})
}

func collectLiveEnvironment(ctx context.Context, c client.Client, clientset kubernetes.Interface, namespace string, nodes []string, image string, base perf.Environment) (perf.Environment, error) {
	if image == "" || !strings.Contains(image, "@sha256:") {
		return perf.Environment{}, fmt.Errorf("ZFS_CSI_PERF_DIAGNOSTIC_IMAGE must be pinned by digest")
	}
	diagnostics := map[string]perf.NodeEnvironment{}
	for i, node := range nodes {
		facts, err := collectNodeDiagnostic(ctx, c, clientset, namespace, fmt.Sprintf("perf-env-%d-", i), node, image)
		if err != nil {
			return perf.Environment{}, err
		}
		diagnostics[node] = facts
	}
	pool, err := perf.PoolEnvironmentFromEnv(storagePoolFromEnv())
	if err != nil {
		return perf.Environment{}, err
	}
	return perf.CollectEnvironment(ctx, c, base, diagnostics, pool)
}

func collectNodeDiagnostic(ctx context.Context, c client.Client, clientset kubernetes.Interface, namespace, generateName, node, image string) (facts perf.NodeEnvironment, err error) {
	pod := perf.DiagnosticPod(namespace, "", node, image)
	pod.GenerateName = generateName
	if err = c.Create(ctx, pod); err != nil {
		return facts, err
	}
	defer func() {
		cleanupErr := c.Delete(context.Background(), pod)
		if cleanupErr != nil && !apierrors.IsNotFound(cleanupErr) {
			err = errors.Join(err, cleanupErr)
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		err = errors.Join(err, waitForPerformanceObjectDeleted(cleanupCtx, c, client.ObjectKeyFromObject(pod), 2*time.Minute))
	}()
	if err = waitForPodSucceeded(ctx, c, client.ObjectKeyFromObject(pod), 5*time.Minute); err != nil {
		return facts, err
	}
	raw, err := clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "diagnostic"}).DoRaw(ctx)
	if err != nil {
		return facts, err
	}
	return perf.ParseDiagnosticFacts(string(raw))
}

func waitForPerformanceObjectDeleted(ctx context.Context, c client.Client, key client.ObjectKey, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := c.Get(ctx, key, &corev1.Pod{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("pod %s deletion timed out", key)
		case <-ticker.C:
		}
	}
}

func performanceConsumerNodes(ctx context.Context, c client.Client, storageName string) ([]string, error) {
	var nodes corev1.NodeList
	if err := c.List(ctx, &nodes); err != nil {
		return nil, err
	}
	var names []string
	for _, node := range nodes.Items {
		if node.Name != storageName && performanceWorkerReady(&node) {
			names = append(names, node.Name)
		}
	}
	sort.Strings(names)
	if len(names) < 1 {
		return nil, fmt.Errorf("performance suite requires one ready schedulable worker")
	}
	return names, nil
}

func performanceWorkerReady(node *corev1.Node) bool {
	_, controlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
	_, master := node.Labels["node-role.kubernetes.io/master"]
	if node.Spec.Unschedulable || controlPlane || master {
		return false
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
