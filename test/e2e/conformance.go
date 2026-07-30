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
	stderrors "errors"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/onsi/ginkgo/v2"
	"github.com/pkg/errors"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/test/framework/ginkgoextensions"
	"sigs.k8s.io/cluster-api/test/infrastructure/container"
)

func conformanceTestDriverFiles(tlsOnly, encryption, tls bool) ([]string, error) {
	if tlsOnly {
		if !tls {
			return nil, errors.New("TLS-only conformance requires transport TLS")
		}
		return []string{"zfs-csi-nvme-tls.yaml", "zfs-csi-nfs-tls.yaml"}, nil
	}
	files := []string{"zfs-csi-nvme.yaml", "zfs-csi-nfs.yaml"}
	if encryption {
		files = append(files, "zfs-csi-nvme-encrypted.yaml")
	}
	if tls {
		files = append(files, "zfs-csi-nvme-tls.yaml", "zfs-csi-nfs-tls.yaml")
	}
	return files, nil
}

const (
	conformanceStandardImage = "registry.k8s.io/conformance"
	// conformanceContainerNetwork inherits the workstation network namespace so
	// the container reaches the workload apiserver at the exact address the
	// workstation used (AWS ELB FQDN / KubeVirt VLAN200 IP). "host" avoids the
	// NAT/DNS indirection of a bridge and needs no kind network. Linux-only;
	// the workstation + CI are Linux. If an HTTP(S)_PROXY env var is set, the
	// underlying container pkg tries to inspect this network's subnets and would
	// fail on "host" — run conformance without proxy env, or override.
	conformanceContainerNetwork = "host"
	// conformanceDefaultFocus selects only the external-storage specs for the
	// zfs-csi drivers. Both testdrivers use a DriverInfo.Name suffixed with the
	// co.uk domain, so this one anchor matches BOTH [Driver: ...] trees.
	conformanceDefaultFocus = `External.Storage.*co\.uk`
	// conformanceDefaultSkip is EMPTY: the focus (External.Storage.*co.uk) already
	// scopes the run to the external-storage suite for our drivers, so everything
	// that matches is storage-related by construction. We deliberately do NOT skip
	// [Disruptive]/[Serial]/[Slow]/[Feature:] — those are exactly the storage
	// edge-cases (disruptive detach/remount, serial multi-node, slow large-volume,
	// feature-gated snapshot/expansion) we want proven. Capability-gated suites
	// (fsgroup, snapshot, topology, block) are still auto-excluded per testdriver
	// DriverInfo, so an inapplicable spec never runs regardless of tag. An empty
	// skip is rendered as NO --skip flag (an empty ginkgo --skip regex would match
	// and skip everything — see the ginkgoVars build).
	conformanceDefaultSkip = ``
	// conformanceSuiteTimeout is the ginkgo --timeout: a hard ceiling so a broken
	// provision (each framework poll can wait 5-15m) can't run the suite away
	// indefinitely. Four real-ZFS drivers, including both TLS transports, exceed
	// four hours on the static shared cluster even without a driver failure.
	conformanceSuiteTimeout = "360m"
	// conformanceNonBlockingTaints lists the taint keys the suite's node-readiness
	// preflight must ignore, so a tainted-but-healthy node doesn't block the run.
	// The storage node carries our custom NoSchedule storage taint; the CP carries
	// the standard control-plane taint. Both must be listed or the preflight
	// waits forever for those nodes to become "schedulable". Overridable via
	// E2E_NON_BLOCKING_TAINTS (conformanceInput.NonBlockingTaints) for shared
	// clusters that carry additional site-specific taints.
	conformanceNonBlockingTaints = "zfs.csi.randomvariable.co.uk/storage,node-role.kubernetes.io/control-plane"
	// conformanceStaticDefaultSkip is the default -skip for the static provider:
	// on a SHARED pre-existing cluster the [Disruptive] kubelet-restart specs and
	// [Serial] whole-cluster specs are unsafe by default. Opt back in with
	// E2E_CONFORMANCE_DISRUPTIVE=1 (requires the SSH key and a dedicated window).
	conformanceStaticDefaultSkip = `\[Disruptive\]|\[Serial\]`
)

// conformanceInput drives runStorageConformance. It mirrors the subset of
// kubetest.RunInput we need, plus the testdriver manifests.
type conformanceInput struct {
	// ClusterProxy is the workload-cluster framework proxy (provides the
	// kubeconfig path + REST config + node count).
	ClusterProxy framework.ClusterProxy
	// KubernetesVersion pins the conformance image tag (registry.k8s.io/
	// conformance:vX). Must be a clean upstream vX.Y.Z; any build-metadata
	// suffix (+vmware.1 etc.) is stripped before use.
	KubernetesVersion string
	// ConformanceImage optionally overrides the derived image (air-gapped /
	// mirror registries).
	ConformanceImage string
	// ArtifactsDirectory is the base for JUnit + e2e output (git-ignored
	// _artifacts/).
	ArtifactsDirectory string
	// ClusterName scopes the per-run report subdir.
	ClusterName string
	// TestDriverManifests are host-side paths to external-storage testdriver
	// YAMLs. Each is mounted into the container and passed as a repeated
	// -storage.testdriver flag.
	TestDriverManifests []string
	// Focus / Skip override the defaults (ginkgo regex). Empty => defaults.
	Focus string
	Skip  string
	// DryRun adds ginkgo --dry-run: compile regexes, load+register both
	// testdrivers (surfacing YAML/strict-field errors), walk the spec tree, and
	// print "Will run N of M" WITHOUT touching the cluster. The mandatory ~1-2m
	// pre-flight gate before a real (60-120m) run.
	DryRun bool
	// SSHPrivateKey is the workload-node private key. The skeleton provider
	// needs it for disruptive kubelet-down tests that SSH into nodes. Optional:
	// empty skips the key mount entirely (static provider, disruptive excluded).
	SSHPrivateKey []byte
	// SSHUser is the login shared by workload nodes and the optional bastion.
	SSHUser string
	// SSHBastion is a host:port jump endpoint used to route to private node IPs.
	SSHBastion string
	// NonBlockingTaints overrides --non-blocking-taints (comma-separated taint
	// keys). Empty keeps conformanceNonBlockingTaints.
	NonBlockingTaints string
	// AllowedNotReadyNodes is passed as --allowed-not-ready-nodes so a shared
	// cluster's unrelated NotReady node cannot hang the suite preflight.
	AllowedNotReadyNodes int
	// AfterRun is invoked after the conformance container exits but before JUnit
	// gathering. It captures live cluster state for failed runs.
	AfterRun func(context.Context) error
}

func conformanceSSHEnvironment(input conformanceInput) map[string]string {
	environment := map[string]string{}
	if len(input.SSHPrivateKey) > 0 {
		environment["KUBE_SSH_KEY_PATH"] = "/tmp/ssh-key"
	}
	if input.SSHUser != "" {
		environment["KUBE_SSH_USER"] = input.SSHUser
	}
	if input.SSHBastion != "" {
		environment["KUBE_SSH_BASTION"] = input.SSHBastion
	}
	return environment
}

// runStorageConformance runs the upstream external-storage conformance suite
// against the deployed zfs-csi driver by launching the version-matched
// conformance image as a host container, mounting the kubeconfig + testdriver
// manifests, and gathering JUnit reports.
func runStorageConformance(ctx context.Context, input conformanceInput) error {
	if input.ClusterProxy == nil {
		return errors.New("ClusterProxy must be provided")
	}
	if len(input.TestDriverManifests) == 0 {
		return errors.New("at least one TestDriverManifest must be provided")
	}
	if input.Focus == "" {
		input.Focus = conformanceDefaultFocus
	}
	if input.Skip == "" {
		input.Skip = conformanceDefaultSkip
	}

	if input.KubernetesVersion == "" && input.ConformanceImage == "" {
		return errors.New("either KubernetesVersion or ConformanceImage must be set")
	}

	input.ArtifactsDirectory = framework.ResolveArtifactsDirectory(input.ArtifactsDirectory)
	if input.ClusterName == "" {
		input.ClusterName = "conformance"
	}
	reportDir := path.Join(input.ArtifactsDirectory, "conformance", input.ClusterName)
	outputDir := path.Join(reportDir, "e2e-output")
	configDir := path.Join(reportDir, "config")
	// Pre-create the /output bind target (and children) BEFORE RunContainer.
	// If the host path doesn't exist, dockerd auto-creates it root:root, and the
	// container (which runs as the current uid/gid, not root) then gets EACCES
	// writing JUnit -> red at report time. Because WE create the dir here it is
	// owned by the current uid, and the conformance container runs as that same
	// uid (User/Group set from os/user below), so 0o750 is sufficient for the
	// container to write — no world-writable bit needed.
	for _, d := range []string{outputDir, configDir} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return err
		}
	}

	tmpKubeConfigPath, err := dockeriseConformanceKubeconfig(configDir, input.ClusterProxy.GetKubeconfigPath())
	if err != nil {
		return err
	}
	// The SSH key is optional (static provider, disruptive excluded): only
	// materialize + mount it when a key was supplied.
	sshKeyPath := ""
	if len(input.SSHPrivateKey) > 0 {
		sshKeyPath, err = writeConformanceSSHKey(configDir, input.SSHPrivateKey)
		if err != nil {
			return err
		}
	}

	// Ginkgo flags (single dash, before the "e2e.test --" separator).
	//
	// NOTE ginkgo v2 (shipped by k8s 1.34/1.36) renamed the v1 `-nodes` to
	// `--procs`; we omit it entirely and run SERIAL (procs=1 default) — the
	// external-storage suite has ordering-sensitive specs and a 2-worker cluster
	// won't reliably parallelise provisioning. `flake-attempts` is deliberately
	// NOT set (=1): this is a driver TRUTH run, retrying flakes would mask real
	// driver bugs and triple time on genuinely-failing specs. `--timeout` bounds
	// the whole suite so a broken provision (framework polls ~5-15m each) can't
	// run away for hours. slow-spec-threshold is only a warning, not a kill.
	ginkgoVars := map[string]string{
		"slow-spec-threshold": "120s",
		"focus":               input.Focus,
		"trace":               "true",
		"v":                   "true",
		"timeout":             conformanceSuiteTimeout,
	}
	// Only pass --skip when non-empty: ginkgo treats an empty --skip regex as a
	// match against every spec, which would skip the entire suite. The default
	// skip is intentionally empty (storage focus already scopes the run), so the
	// flag must be omitted rather than rendered as --skip=.
	if input.Skip != "" {
		ginkgoVars["skip"] = input.Skip
	}
	if input.DryRun {
		ginkgoVars["dry-run"] = "true"
	}

	// e2e.test flags (double dash, after the separator). Mirror kubetest's baked
	// values: skeleton provider, in-container kubeconfig path, report dir, node
	// count. --num-nodes is intentionally NOT set (autodetect via the suite);
	// a wrong explicit value fails node-count asserts.
	//
	// --non-blocking-taints: the suite's node-readiness preflight refuses to
	// start until all nodes are schedulable, treating tainted nodes as NotReady
	// unless their taint key is listed here. Our storage node carries a custom
	// NoSchedule taint (conformanceStorageTaintKey) AND the CP carries the
	// standard control-plane taint — without both, the preflight blocks forever
	// ("N out of N+1 nodes ready, need 1 more"). Discovered live in Phase 4.
	nonBlockingTaints := input.NonBlockingTaints
	if nonBlockingTaints == "" {
		nonBlockingTaints = conformanceNonBlockingTaints
	}
	e2eVars := map[string]string{
		"kubeconfig":           "/tmp/kubeconfig",
		"provider":             "skeleton",
		"report-dir":           "/output",
		"e2e-output-dir":       "/output/e2e-output",
		"dump-logs-on-failure": "false",
		"report-prefix":        fmt.Sprintf("conformance.%s.", input.ClusterName),
		"non-blocking-taints":  nonBlockingTaints,
		// --allowed-not-ready-nodes: a shared cluster may carry an unrelated
		// NotReady node; without this the suite preflight waits forever.
		"allowed-not-ready-nodes": strconv.Itoa(input.AllowedNotReadyNodes),
	}
	image := input.ConformanceImage
	if image == "" {
		image = conformanceImageForVersion(input.KubernetesVersion)
	}

	// Mount the kubeconfig + report dir, then each testdriver manifest, and build
	// the repeated -storage.testdriver args pointing at the in-container paths.
	volumeMounts := map[string]string{
		tmpKubeConfigPath: "/tmp/kubeconfig",
		reportDir:         "/output",
	}
	if sshKeyPath != "" {
		volumeMounts[sshKeyPath] = "/tmp/ssh-key"
	}
	var testDriverArgs []string
	for i, hostPath := range input.TestDriverManifests {
		dest := fmt.Sprintf("/tmp/testdriver-%d.yaml", i)
		volumeMounts[hostPath] = dest
		testDriverArgs = append(testDriverArgs, "-storage.testdriver="+dest)
	}

	usr, err := user.Current()
	if err != nil {
		return errors.Wrap(err, "unable to determine current user")
	}

	var args []string
	args = append(args, buildConformanceArgs(ginkgoVars, "-")...)
	args = append(args, "/usr/local/bin/e2e.test", "--")
	args = append(args, buildConformanceArgs(e2eVars, "--")...)
	args = append(args, testDriverArgs...)

	cwd, _ := os.Getwd()
	ginkgoextensions.Byf("Running external-storage conformance: dir=%s image=%q command=%q", cwd, image, args)

	containerRuntime, err := container.NewDockerClient()
	if err != nil {
		return errors.Wrap(err, "unable to create docker client for conformance")
	}
	ctx = container.RuntimeInto(ctx, containerRuntime)

	runErr := containerRuntime.RunContainer(ctx, &container.RunContainerInput{
		Image:           image,
		Network:         conformanceContainerNetwork,
		User:            usr.Uid,
		Group:           usr.Gid,
		Volumes:         volumeMounts,
		EnvironmentVars: conformanceSSHEnvironment(input),
		CommandArgs:     args,
		Entrypoint:      []string{"/usr/local/bin/ginkgo"},
		RestartPolicy:   dockercontainer.RestartPolicyDisabled,
	}, ginkgo.GinkgoWriter)

	// Capture live diagnostics only after the suite exits: pre-run snapshots miss
	// provision failures. Always gather JUnit too; every error is evidence.
	var diagnosticsErr error
	if input.AfterRun != nil {
		// A suite timeout cancels ctx; diagnostics still need a live API attempt.
		diagnosticsCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		diagnosticsErr = input.AfterRun(diagnosticsCtx)
		cancel()
	}
	gatherErr := framework.GatherJUnitReports(reportDir, input.ArtifactsDirectory)
	if runErr != nil {
		runErr = errors.Wrap(runErr, "external-storage conformance run failed")
	}
	return stderrors.Join(runErr, diagnosticsErr, gatherErr)
}

// writeConformanceSSHKey materializes the locally configured workload SSH key
// with a restrictive mode before passing it to the skeleton provider container.
func writeConformanceSSHKey(configDir string, privateKey []byte) (string, error) {
	if len(privateKey) == 0 {
		return "", errors.New("workload SSH private key is empty")
	}
	path := filepath.Join(configDir, "ssh-private-key")
	if err := os.WriteFile(path, privateKey, 0o600); err != nil {
		return "", errors.Wrap(err, "write conformance SSH private key")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return "", errors.Wrap(err, "chmod conformance SSH private key")
	}
	return path, nil
}

// conformanceImageForVersion maps a Kubernetes version to the upstream
// conformance image tag, stripping any build-metadata suffix (+vmware.1, CAPI
// suffixes) that has no matching registry.k8s.io/conformance tag.
func conformanceImageForVersion(kubernetesVersion string) string {
	tag := kubernetesVersion
	if i := strings.IndexByte(tag, '+'); i >= 0 {
		tag = tag[:i]
	}
	return conformanceStandardImage + ":" + tag
}

// dockeriseConformanceKubeconfig copies the workload kubeconfig, rewriting a
// loopback server to host.docker.internal only on non-linux/WSL (where the
// container can't reach the host's 127.0.0.1). On Linux with Network:"host" the
// server address is used verbatim — which is what we want (the ELB FQDN / VLAN
// IP the workstation already reaches).
func dockeriseConformanceKubeconfig(configDir, kubeConfigPath string) (string, error) {
	kubeConfig, err := clientcmd.LoadFromFile(kubeConfigPath)
	if err != nil {
		return "", err
	}
	newPath := path.Join(configDir, "kubeconfig")
	if runtime.GOOS != "linux" || os.Getenv("WSL_DISTRO_NAME") != "" {
		for i := range kubeConfig.Clusters {
			kubeConfig.Clusters[i].Server = strings.ReplaceAll(kubeConfig.Clusters[i].Server, "127.0.0.1", "host.docker.internal")
		}
	}
	if err := clientcmd.WriteToFile(*kubeConfig, newPath); err != nil {
		return "", err
	}
	return newPath, nil
}

// buildConformanceArgs converts a string map to --key=value (or -key=value)
// flags. Mirrors kubetest.buildArgs.
func buildConformanceArgs(kv map[string]string, flagMarker string) []string {
	args := make([]string, 0, len(kv))
	for k, v := range kv {
		args = append(args, flagMarker+k+"="+v)
	}
	return args
}
