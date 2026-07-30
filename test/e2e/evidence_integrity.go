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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

// e2eRunMetadata is deliberately limited to reproducibility evidence. It must
// never contain credential values, kubeconfigs, or process environments.
type e2eRunMetadata struct {
	RecordedAt        string            `json:"recorded_at"`
	RunID             string            `json:"run_id"`
	Provider          string            `json:"provider"`
	Cluster           string            `json:"cluster"`
	KubernetesVersion string            `json:"kubernetes_version"`
	Environment       map[string]string `json:"environment"`
	GitCommit         string            `json:"git_commit"`
	GitDirty          bool              `json:"git_dirty"`
	GinkgoSeed        int64             `json:"ginkgo_seed"`
	DriverImage       string            `json:"driver_image"`
	Chart             chartEvidence     `json:"chart"`
	TestDriverSHA256  map[string]string `json:"testdriver_sha256"`
}

type chartEvidence struct {
	Reference string            `json:"reference"`
	Overrides map[string]string `json:"overrides"`
}

// driverImageHelmValues accepts both mutable tags and immutable OCI digests.
func driverImageHelmValues(ref string) (repository, tag, digest string, err error) {
	ref = strings.TrimSpace(ref)
	if repository, digest, ok := strings.Cut(ref, "@"); ok {
		if repository == "" || !isSHA256Digest(digest) {
			return "", "", "", fmt.Errorf("invalid digest-pinned driver image %q", ref)
		}
		return repository, "", digest, nil
	}
	lastSlash := strings.LastIndexByte(ref, '/')
	lastColon := strings.LastIndexByte(ref, ':')
	if lastColon > lastSlash {
		return ref[:lastColon], ref[lastColon+1:], "", nil
	}
	if ref == "" {
		return "", "", "", errors.New("driver image must not be empty")
	}
	return ref, "latest", "", nil
}

func isSHA256Digest(digest string) bool {
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	return err == nil
}

func hashFiles(paths []string) (map[string]string, error) {
	hashes := make(map[string]string, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		sum := sha256.Sum256(contents)
		hashes[path] = hex.EncodeToString(sum[:])
	}
	return hashes, nil
}

func chartOverrides(image string, node storageNode) map[string]string {
	repository, tag, digest, err := driverImageHelmValues(image)
	if err != nil {
		return nil
	}
	return chartOverridesForImageValues(repository, tag, digest, node)
}

func chartOverridesForImageValues(repository, tag, digest string, node storageNode) map[string]string {
	overrides := map[string]string{
		"namespace":                       zfsCSINamespace,
		"image.repository":                repository,
		"image.tag":                       tag,
		"image.digest":                    digest,
		"image.pullPolicy":                "Always",
		"controller.enabled":              "true",
		"storage.enabled":                 "true",
		"node.enabled":                    "true",
		"storageClasses.tankNVMe.enabled": "true",
		"storageNode.name":                node.Name,
		"network.portalHost":              node.PortalHost,
		"network.nfsServer":               node.NFSServer,
		"storageClasses.tankNVMe.pool":    e2econfig.PoolName(),
		"storageClasses.tankNFS.pool":     e2econfig.PoolName(),
		"storageNode.poolMountRoot":       "/" + e2econfig.PoolName(),
	}
	overrides["storageClasses.tankNFS.enabled"] = "true"
	overrides["storageClasses.tankNFS.nfsExportCIDRs"] = strings.Join(e2econfig.NFSExportCIDRs(), ",")
	if e2econfig.EncryptionEnabled() {
		overrides["encryption.enabled"] = "true"
	}
	return overrides
}

func safeEvidenceEnvironment() map[string]string {
	keys := []string{"AWS_REGION", "AWS_PROFILE", "AWS_IDENTITY_KIND", "AWS_IDENTITY_NAME", "E2E_FLAVOR", "E2E_KUBERNETES_VERSION"}
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			values[key] = value
		}
	}
	return values
}

func gitEvidence(dir string) (commit string, dirty bool) {
	commit = "unknown"
	if out, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output(); err == nil {
		commit = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "-C", dir, "status", "--porcelain").Output(); err == nil {
		dirty = strings.TrimSpace(string(out)) != ""
	}
	return commit, dirty
}

func writeRunMetadata(path string, metadata e2eRunMetadata) error {
	body, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run metadata: %w", err)
	}
	return os.WriteFile(path, append(body, '\n'), 0o640)
}

func newRunMetadata(runID, cluster, image string, testDrivers []string, node storageNode, seed int64, repoRoot string) (e2eRunMetadata, error) {
	hashes, err := hashFiles(testDrivers)
	if err != nil {
		return e2eRunMetadata{}, err
	}
	commit, dirty := gitEvidence(repoRoot)
	return e2eRunMetadata{
		RecordedAt:        time.Now().UTC().Format(time.RFC3339),
		RunID:             runID,
		Provider:          e2econfig.InfrastructureProvider(),
		Cluster:           cluster,
		KubernetesVersion: e2econfig.KubernetesVersion(),
		Environment:       safeEvidenceEnvironment(),
		GitCommit:         commit,
		GitDirty:          dirty,
		GinkgoSeed:        seed,
		DriverImage:       image,
		Chart: chartEvidence{
			Reference: e2econfig.ChartReference(),
			Overrides: chartOverrides(image, node),
		},
		TestDriverSHA256: hashes,
	}, nil
}

// writePreTeardownInventory records Kubernetes and backend state while the
// workload API remains reachable. This is intentionally before CAPI teardown.
func writePreTeardownInventory(ctx context.Context, path, kubeconfig string, workload client.Client) error {
	if workload == nil {
		return nil
	}
	contents := collectDriverDiagnosticsWithLogs(ctx, workload, kubeconfig)
	writeErr := os.WriteFile(path, []byte(contents), 0o640)
	// Container logs are best-effort evidence. Collection failures are recorded in
	// artifacts and must not replace the test or teardown failure being diagnosed.
	captureKubernetesContainerLogs(ctx, filepath.Dir(path), kubeconfig, workload, runKubectlDiagnostic)
	return writeErr
}

func captureKubernetesContainerLogs(ctx context.Context, artifactDir, kubeconfig string, workload client.Client, runner diagnosticRunner) {
	if workload == nil || runner == nil || kubeconfig == "" {
		return
	}
	logDir := filepath.Join(artifactDir, "kubernetes-logs")
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		return
	}

	pods := &corev1.PodList{}
	if err := workload.List(ctx, pods); err != nil {
		_ = os.WriteFile(filepath.Join(logDir, "collection-errors.log"), []byte(fmt.Sprintf("list pods: %v\n", err)), 0o640)
		return
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		left, right := pods.Items[i], pods.Items[j]
		return left.Namespace+"\x00"+left.Name < right.Namespace+"\x00"+right.Name
	})

	var collectionErrors strings.Builder
	for i := range pods.Items {
		pod := &pods.Items[i]
		containers := make([]string, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers)+len(pod.Spec.EphemeralContainers))
		for _, container := range pod.Spec.InitContainers {
			containers = append(containers, container.Name)
		}
		for _, container := range pod.Spec.Containers {
			containers = append(containers, container.Name)
		}
		for _, container := range pod.Spec.EphemeralContainers {
			containers = append(containers, container.Name)
		}
		sort.Strings(containers)
		for _, container := range containers {
			base := kubernetesLogArtifactName(pod.Namespace, pod.Name, container)
			args := []string{"--kubeconfig", kubeconfig, "-n", pod.Namespace, "logs", pod.Name, "-c", container, "--timestamps=true", "--tail=-1"}
			out, err := runner(ctx, "kubectl", args...)
			currentPath := filepath.Join(logDir, base+"__current.log")
			if writeErr := os.WriteFile(currentPath, out, 0o640); writeErr != nil {
				fmt.Fprintf(&collectionErrors, "%s current write: %v\n", base, writeErr)
			}
			if err != nil {
				fmt.Fprintf(&collectionErrors, "%s current: %v\n%s\n", base, err, out)
			}

			previousArgs := append(append([]string(nil), args...), "--previous=true")
			previous, previousErr := runner(ctx, "kubectl", previousArgs...)
			if previousErr == nil || len(previous) > 0 {
				if writeErr := os.WriteFile(filepath.Join(logDir, base+"__previous.log"), previous, 0o640); writeErr != nil {
					fmt.Fprintf(&collectionErrors, "%s previous write: %v\n", base, writeErr)
				}
			}
		}
	}
	if collectionErrors.Len() > 0 {
		_ = os.WriteFile(filepath.Join(logDir, "collection-errors.log"), []byte(collectionErrors.String()), 0o640)
	}
}

func kubernetesLogArtifactName(namespace, pod, container string) string {
	return strings.Join([]string{sanitizeLogIdentity(namespace), sanitizeLogIdentity(pod), sanitizeLogIdentity(container)}, "__")
}

func sanitizeLogIdentity(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, value)
}

func runAWSOrphanScan(ctx context.Context, artifactDir, kubeconfig string) error {
	if e2econfig.InfrastructureProvider() != "aws" {
		return nil
	}
	repoRoot, err := filepath.Abs(".")
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	script := filepath.Join(repoRoot, "test", "e2e", "aws", "reaper", "orphan-detector.sh")
	cmd := exec.CommandContext(ctx, "bash", script)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	out, runErr := cmd.CombinedOutput()
	writeErr := os.WriteFile(filepath.Join(artifactDir, "post-teardown-aws-orphans.txt"), out, 0o640)
	return errors.Join(runErr, writeErr)
}
